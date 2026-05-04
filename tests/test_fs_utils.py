import os
import shutil
from time import time
from utils.fs_utils import archive_raw_files
from pathlib import Path

def test_archive_raw_files():
    """
    """

    # set paths
    source_path = "tests/data/fs_utils/archive_raw_files/source/"
    dest_path = "tests/data/fs_utils/archive_raw_files/destination/"
    time_filter = 5*60 #5 minutes

    # directory cleanup...
    if os.path.exists(source_path):
        shutil.rmtree(source_path)
    if os.path.exists(dest_path):
        shutil.rmtree(dest_path)

    # make directories for testing
    os.makedirs(source_path)
    os.makedirs(dest_path)

    # Get timestamp
    unix_timestamp = int(time())

    # Copy test files to source_path
    # This file should be moved based on the timestamp
    old_file_name = "1m_prices_1776697045.json"
    # This file should not be moved based on the timestamp
    recent_file_name = "1m_prices_"+str(unix_timestamp)+".json"

    # Copy both files to ../tests/data/fs_utils/archive/source for testing
    shutil.copyfile("tests/data/fs_utils/archive_raw_files/original_file/1m_prices_1776697045.json",
                    source_path+old_file_name)
    shutil.copyfile("tests/data/fs_utils/archive_raw_files/original_file/1m_prices_1776697045.json",
                    source_path+recent_file_name)

    archive_raw_files(source_path, dest_path, time_filter)

    # Original file should still be located here...
    assertion1 = os.path.exists("tests/data/fs_utils/archive_raw_files/original_file/1m_prices_1776697045.json") #True

    # Old file should be in dest_path
    assertion2 = os.path.exists("tests/data/fs_utils/archive_raw_files/destination/1m_prices_1776697045.json") #True
    # Old file should not be in source_path
    assertion3 = os.path.exists("tests/data/fs_utils/archive_raw_files/source/1m_prices_1776697045.json") #False

    # recent file should not be in dest_path
    assertion4 = os.path.exists("tests/data/fs_utils/archive_raw_files/destination/" + recent_file_name) #False
    # recent file should be in source_path
    assertion5 = os.path.exists("tests/data/fs_utils/archive_raw_files/source/" + recent_file_name) #True

    assert (assertion1, assertion2, assertion3, assertion4, assertion5 )==(True, True, False, False, True)

    ## DELETE temp files
    shutil.rmtree(source_path)
    shutil.rmtree(dest_path)

    