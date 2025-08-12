import os
import requests
import pandas as pd
from datetime import datetime
import configparser

# --- Configuration ---
ARCHIVE_ROOT = "/data/xnat/archive"
XNAT_INSTANCES = [
    "https://xnat2.bu.edu",
    "https://xnat.bu.edu"
]
CSV_FILENAME = "orphaned_sessions.csv"
AUTH_FILE = ".xnat_auth"

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

def find_sessions_and_metadata(auth):
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

        projects = sorted(os.listdir(ARCHIVE_ROOT))
        print(f"Found {len(projects)} projects in archive root.")

        for project in projects:
            arc_path = os.path.join(ARCHIVE_ROOT, project, 'arc001')

            if not os.path.isdir(arc_path):
                continue

            total_projects += 1
            print(f"Processing project: {project}")

            session_dirs = sorted(os.listdir(arc_path))
            print(f"  Found {len(session_dirs)} sessions in project {project}.")

            for session_dir in session_dirs:
                session_path = os.path.join(arc_path, session_dir)
                if not os.path.isdir(session_path):
                    continue

                total_sessions += 1
                print(f"    Processing session {session_dir} (#{total_sessions})")

                last_modified = get_last_modified(session_path)

                metadata_from_instances = []
                for base_url in XNAT_INSTANCES:
                    meta = query_xnat_metadata(req_session, base_url, project, session_dir)
                    metadata_from_instances.append(meta)

                session_data.append({
                    "project": project,
                    "session": session_dir,
                    "path": session_path,
                    "last_modified": last_modified,
                    "metadata_xnat2": metadata_from_instances[0],
                    "metadata_xnat": metadata_from_instances[1]
                })

        print(f"Finished processing. Total projects: {total_projects}, total sessions: {total_sessions}.")

    return session_data

def main():
    try:
        username, password = read_xnat_auth()
        auth = (username, password)
    except Exception as e:
        print(f"Error reading authentication credentials: {e}")
        return

    data = find_sessions_and_metadata(auth)
    if not data:
        print("No sessions found under archive root.")
        return

    df = pd.DataFrame(data)

    try:
        df.to_csv(CSV_FILENAME, index=False)
        print(f"Saved session metadata to {CSV_FILENAME}")
    except Exception as e:
        print(f"Error saving CSV file: {e}")

if __name__ == '__main__':
    main()