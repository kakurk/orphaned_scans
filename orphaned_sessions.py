import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import configparser
import subprocess
import argparse
import pdb

# --- Configuration ---
ARCHIVE_ROOT = "/data/xnat/archive"
XNAT_INSTANCES = [
    "https://xnat2.bu.edu",
    "https://xnat.bu.edu"
]
CSV_FILENAME = "orphaned_sessions.csv"
AUTH_FILE = ".xnat_auth_alt"

def read_xnat_auth(auth_file=AUTH_FILE):
    """
    Reads authentication credentials from an INI file using configparser.
    
    Expected format:
    [auth]
    username = your_username
    password = your_password
    
    Returns:
        (username, password) tuple
    """
    config = configparser.ConfigParser()
    
    if not os.path.exists(auth_file):
        raise FileNotFoundError(f"Authentication file '{auth_file}' not found.")
    
    config.read(auth_file)
    
    if 'auth' not in config:
        raise ValueError("Section [auth] not found in auth file.")
    
    try:
        username = config.get('auth', 'username')
        password = config.get('auth', 'password')
    except (configparser.NoOptionError, configparser.NoSectionError) as e:
        raise ValueError(f"Missing username or password in auth file: {e}")
    
    return username, password

def get_last_modified(session_path):
    """Returns the last modified time of the directory as a ISO 8601 string."""
    try:
        timestamp = os.path.getmtime(session_path)
        return datetime.fromtimestamp(timestamp).isoformat()
    except Exception as e:
        print(f"Error getting last modified time for {session_path}: {e}")
        return None

def query_xnat_metadata(session, base_url, project_id, session_label):
    """
    Queries the XNAT REST API for session metadata using a requests.Session.
    Returns JSON data or None if not found or error.
    
    Parameters:
        session: requests.Session() object with auth configured.
    """
    url = f"{base_url}/data/projects/{project_id}/experiments/{session_label}?format=json"
    try:
        response = session.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as he:
        if response.status_code == 404:
            # Not found on this instance
            return None
        else:
            print(f"HTTP error on {url}: {he}")
            return None
    except Exception as e:
        print(f"Error querying {url}: {e}")
        return None

def query_sessions(start_date, end_date):
    cmd = f'find /data/xnat/archive/*/arc001 -mindepth 1 -maxdepth 1 -type d -newermt {start_date} ! -newermt {end_date}'
    result = subprocess.check_output(cmd, shell=True, universal_newlines=True)

    dirs = result.strip().split('\n')

    # if we do NOT find any directories, return an empty list
    if dirs == ['']:
        dirs = []

    return dirs

def find_sessions_and_metadata(auth, start_date, end_date):
    """
    Find sessions and collect last modified time and metadata.
    
    Uses a single requests.Session instance.
    """
    session_data = []
    total_projects = 0
    total_sessions = 0

    # Create one persistent session with authentication
    with requests.Session() as req_session:

        req_session.auth = auth

        dirs = query_sessions(start_date, end_date)

        for d in dirs:

            project = os.path.basename(os.path.dirname(os.path.dirname(d)))
            session = os.path.basename(d)
            last_modified = get_last_modified(d)

            metadata_from_instances = []
            for base_url in XNAT_INSTANCES:
                meta = query_xnat_metadata(req_session, base_url, project, session)
                metadata_from_instances.append(meta)

            session_data.append({
                "project": project,
                "session": session,
                "path": d,
                "last_modified": last_modified,
                "metadata_xnat2": bool(metadata_from_instances[0]),
                "metadata_xnat": bool(metadata_from_instances[1])
            })

    return session_data

def send_email(message, start_date, end_date, recipient="kkurkela@bu.edu", csvfile=None):
    """Send the report via system mail."""
    subject = f"Weekly Orphaned Scan Report ({start_date} to {end_date})"
    try:
        if csvfile:
            subprocess.run(
                f'echo "{message}" | mailx -s "{subject}" -a "{csvfile}" {recipient}',
                shell=True,
                check=True
            )
        else:
            subprocess.run(
                f'echo "{message}" | mailx -s "{subject}" {recipient}',
                shell=True,
                check=True
            )
        print(f"Report emailed to {recipient}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to send email: {e}")

def main():

    parser = argparse.ArgumentParser(description="Process XNAT session data.")
    parser.add_argument("--start-date", required=False, help='Start date (YYYY-MM-DD)')
    parser.add_argument("--end-date", required=False, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()

    # Default to last 7 days if not provided
    if not args.end_date:
        args.end_date = datetime.today().strftime("%Y-%m-%d")
    if not args.start_date:
        args.start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")

    try:
        username, password = read_xnat_auth()
        auth = (username, password)
    except Exception as e:
        print(f"Error reading authentication credentials: {e}")
        return

    data = find_sessions_and_metadata(auth, args.start_date, args.end_date)
    if not data:
        message = f"No scans found on disk between {args.start_date} and {args.end_date}."
        print(message)
        send_email(message, args.start_date, args.end_date)
        return

    df = pd.DataFrame(data)

    try:
        df.to_csv(CSV_FILENAME, index=False)
        print(f"Saved session metadata to {CSV_FILENAME}")  
    except Exception as e:
        print(f"Error saving CSV file: {e}")

    # how many orphans were found? Email an update
    num_orphans = (df['metadata_xnat2'] == False).sum()

    if num_orphans > 0:
        message = f"{num_orphans} orphans found out of {len(df)} scans found on disk."
        print(message)
        send_email(message, args.start_date, args.end_date, csvfile=CSV_FILENAME)
    else:
        message = f"No orphans found out of {len(df)} scans on disk."
        print(message)
        send_email(message, args.start_date, args.end_date, csvfile=CSV_FILENAME)

if __name__ == '__main__':
    main()