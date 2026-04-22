import os
import shutil
from time import time


def archive_raw_files(source_path, dest_path, time_filter):
    """
    Function to archive "old" raw ingested files based on a time filter
    This function handles source data files that hand a unix time code appended to the end of the file name
    
    Parameters
    source_path: str
        directory where source files are located
    dest_path: str
        directory where the files will be copied to
    time_filter: int (unix time in seconds)
        files with unix time before time filter will be moved.
        this delay is to ensure all files get processed before being moved.


    Returns
    
    """
    # Get timestamp
    unix_timestamp = int(time())
    # Files with unix timestamp before this will be moved.
    unix_time_filter = unix_timestamp - time_filter

    directory = os.fsencode(source_path)
        
    for file in os.listdir(directory):
        filename_with_ext = os.fsdecode(file)
        filename, file_ext = os.path.splitext(filename_with_ext)
        file_unixtime = filename[-10:]
        if int(file_unixtime)<unix_time_filter: 
            shutil.move(source_path+filename+file_ext, dest_path)
            print("moved file" + filename_with_ext, dest_path) 
            continue