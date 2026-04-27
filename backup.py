# backup.py
# Paste your backup.py code here.
import pandas as pd
import numpy as np
import requests, json, time, gzip, os, shutil
import subprocess
from google.cloud import pubsub_v1
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo



#---Data Structures---------------------------------------------------------
breadcrumb_count = 0
earliest_bc = None
backup_size = 0
unique_vehicles = set()
compression_time = None
wall_clock_time = None
throughput = 0.0
expected_bc = None


#--Files-------------------------------------------------------------------
backup_file_handle = None
current_backup_filename = None


#---Helper Functions-------------------------------------------------------
def format_time(raw_timestamp):
    '''
        Generic function to convert a raw time.time() to a better readable time format
        in Pacific Time Zone (America/LosAngeles).
        '''

    if raw_timestamp is None:
        return "Not time"
    formated_time = datetime.fromtimestamp(raw_timestamp, tz=ZoneInfo("America/Los_Angeles"))
    return formated_time.strftime('%Y-%m-%d %H:%M:%S')



#---Congiguration----------------------------------------------------------
PROJECT_ID       = 'de-project-bus-lightyear'
SUBSCRIPTION_ID  = 'backup_sub'
subscriber = pubsub_v1.SubscriberClient()
sub_path   = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
backup_log_directory = "/home/pawood/backup_logs/"


#---State Variables--------------------------------------------------------

sentinel_received = False

#---Callback Function------------------------------------------------------
def callback(message):
    global breadcrumb_count, earliest_bc, backup_size, unique_vehicles, compression_time, wall_clock_time, throughput, expected_bc, backup_file_handle, current_backup_filename, sentinel_received
    message.ack()
    breadcrumb = json.loads(message.data.decode('utf-8')) # one breadcrumb
    filename_date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y-%m-%d')
    desired_filename = f'breadcrumbs_{filename_date}.log'

    #---Process Sentinel---------------------------
    if breadcrumb['VEHICLE_ID'] == 0:
        print(f'Sentinel received!')
        expected_bc = breadcrumb["METERS"]
        sentinel_received = True
        #print(f'Sentinel count matches breadcrumbs received?: {breadcrumb["METERS"] == expected_bc}')
        #---Process Breadcrumb--------------------------------------------

    else:
        if earliest_bc is None: #start timer when first breadcrumb recieved
            earliest_bc = time.time()
            print(f"First breadcrumb received at {format_time(earliest_bc)}")

        unique_vehicles.add(breadcrumb["VEHICLE_ID"])

        #---write to log-----------------------------------------

        # Open a new file if it's a new day or no file is open
        if backup_file_handle is None or current_backup_filename != (backup_log_directory + desired_filename):
            if backup_file_handle is not None: # Close previous day's file if open
                backup_file_handle.close()
                print(f"Closed previous day's backup file: {current_backup_filename}")
            backup_file_handle = open(backup_log_directory + desired_filename, 'ab') # Open in append binary mode
            current_backup_filename = backup_log_directory + desired_filename
            print(f"Opened new backup file: {current_backup_filename}")

        breadcrumb_to_bytes = json.dumps(breadcrumb).encode('utf-8')
        backup_file_handle.write(breadcrumb_to_bytes + b'\n')


        #---update stats------------------------------------------
        breadcrumb_count = breadcrumb_count + 1
        unique_vehicles.add(breadcrumb['VEHICLE_ID'])
        if breadcrumb_count % 20000 == 0:
            print(f"Collected {breadcrumb_count} so far")

    if sentinel_received == True and expected_bc == breadcrumb_count:

        cfile = backup_log_directory + desired_filename + ".gz"
        if os.path.exists(cfile):
            subprocess.run(["rm", backup_log_directory + desired_filename + ".gz"])

        #----Log File Compression---------------------------------
        backup_size = os.path.getsize(current_backup_filename)
        compression_time = time.time()
        compress = subprocess.run(["gzip", backup_log_directory + desired_filename], capture_output=True, text=True)
        print(compress)
        wall_clock_time = compression_time - earliest_bc

        #---Summary Statistics------------------------------------
        print(f'\n\nFirst BreadCrumb recieved at: {format_time(earliest_bc)}')
        print(f'Elapsed Wall Time: {wall_clock_time:.3f}s')
        print(f'File size before compression: {backup_size} bytes')
        print(f'Total Breadcrumbs Received: {breadcrumb_count}')
        print(f'Unique Vehicle IDs: {len(unique_vehicles)}')
        print(f'Compression Time: {format_time(compression_time)}')
        print(f'Throughput: {(float(breadcrumb_count) / wall_clock_time):.3f} breadcrumbs/sec \n\n')

        #---Reset All Data Structures----------------------------
        breadcrumb_count = 0
        earliest_bc = None
        backup_size = 0
        unique_vehicles = set()
        compression_time = None
        wall_clock_time = None
        backup_file_handle = None
        throughput = 0.0
        sentinel_received = False
        #expected_bc = 1






#---Listening--------------------------------------------------------------
streaming_pull = subscriber.subscribe(sub_path, callback=callback)

print('Listening . . .')
with subscriber:
    try:
        streaming_pull.result()
    except Exception:
        streaming_pull.cancel()
        streaming_pull.result()
