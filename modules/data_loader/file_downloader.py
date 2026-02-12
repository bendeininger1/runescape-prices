import urllib.request
import os
import shutil

def download_file(url: str, dir_path: str, local_path: str):
    """
    Downloads a file from the given URL to the local_path.
    """
    os.makedirs(dir_path, exist_ok=True)

    with urllib.request.urlopen(url) as response, open(local_path, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)