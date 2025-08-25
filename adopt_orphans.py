import os
import shutil
import argparse
import requests
import configparser

def get_credentials(config_path):
    config = configparser.ConfigParser()
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Credentials file not found: {config_path}")
    config.read(config_path)
    try:
        username = config['auth']['username']
        password = config['auth']['password']
    except KeyError as e:
        raise KeyError(f"Missing {e} in [auth] section of config file.")
    return username, password

def main():
    parser = argparse.ArgumentParser(
        description='Move a DICOM directory to XNAT inbox and trigger inbox import.'
    )
    parser.add_argument(
        'local_dicom_dir',
        help='Full path to your local DICOM directory.'
    )
    parser.add_argument(
        '--project', '-p', required=True,
        help='XNAT project ID.'
    )
    parser.add_argument(
        '--xnat-url', default='http://localhost:8080',
        help='XNAT server URL (default: http://localhost:8080).'
    )
    parser.add_argument(
        '--xnat-credentials', '-c', required=True,
        help='Path to config file with XNAT credentials ([auth] section with username and password).'
    )
    parser.add_argument(
        '--inbox-base', default='/data/xnat/inbox',
        help='XNAT inbox base directory (default: /data/xnat/inbox).'
    )

    args = parser.parse_args()
    local_dicom_dir = os.path.abspath(args.local_dicom_dir)
    inbox_base = args.inbox_base
    project_id = args.project
    xnat_url = args.xnat_url.rstrip('/')
    config_path = args.xnat_credentials

    try:
        xnat_user, xnat_pass = get_credentials(config_path)
    except Exception as e:
        print("Error loading XNAT credentials:", e)
        return

    # 1. Move local dicom directory into /data/xnat/inbox/PROJECT_ID/dirname
    dicom_dir_name = os.path.basename(local_dicom_dir.rstrip('/'))
    dest_inbox_dir = os.path.join(inbox_base, project_id, dicom_dir_name)
    os.makedirs(os.path.dirname(dest_inbox_dir), exist_ok=True)
    try:
        shutil.move(local_dicom_dir, dest_inbox_dir)
        print(f"Moved {local_dicom_dir} to {dest_inbox_dir}")
    except Exception as e:
        print(f"ERROR moving directory: {e}")
        return

    # 2. REST API call using context manager for session
    import_api_url = (
        f"{xnat_url}/data/services/import?"
        f"import-handler=inbox"
        f"&cleanupAfterImport=true"
        f"&PROJECT_ID={project_id}"
        f"&path={dest_inbox_dir}"
    )

    try:
        with requests.Session() as session:
            response = session.post(
                import_api_url,
                auth=(xnat_user, xnat_pass),
                verify=False  # for HTTPS, set to True and handle CA/SSL as needed
            )

            if response.status_code in [200, 202]:
                print("Import request successful.")
                print(response.text)
            else:
                print(f"Import request failed ({response.status_code}): {response.text}")
    except Exception as e:
        print(f"ERROR calling API: {e}")

if __name__ == '__main__':
    main()