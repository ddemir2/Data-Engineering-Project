# analysis.py
# Paste your analysis.py code here.
import requests, json, time
from google.cloud import pubsub_v1
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import logging
import pandas as pd

logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s %(levelname)s %(message)s"
)



#---Data Structures---------------------------------------------------------
breadcrumb_count = 0
earliest_bc = None
latest_bc = None
wall_clock_time = None
unique_vehicles = set()
unique_trips = set()
expected_count = None
sentinel_time = None
unvalidated_batch_df = pd.DataFrame(columns=['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP'])
validate_count = 0


#---Helper Functions-------------------------------------------------------

def validate_batch(unvalidated_batch_df):
	"""
	Run all implemented validation assertions against a batch of breadcrumbs in a df

	Parameters
	----------
	batch_df : df
		A df of breadcrumbs

	Returns
	-------
	validated_batch_df
		df containing only records which passed ALL assertions.
	"""
	violations_df = pd.DataFrame(columns=['ASSERTION_FAILURE', 'EVENT_NO_TRIP', 'EVENT_NO_STOP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP'])

	#---ASSERTION 1---[LIMIT]  GPS_LATITUDE must be non-null and in [-90, 90]-----
	lat_over_positive_90  = unvalidated_batch_df['GPS_LATITUDE'] > 90
	lat_under_negative_90 = unvalidated_batch_df['GPS_LATITUDE'] < -90
	null_lat = unvalidated_batch_df['GPS_LATITUDE'].isna()
	invalid_lat = unvalidated_batch_df[lat_over_positive_90 | lat_under_negative_90 | null_lat].copy()
	unvalidated_batch_df = unvalidated_batch_df.drop(invalid_lat.index) # remove offending rows from unvalidated_batch_df

	if not invalid_lat.empty:
		invalid_lat['ASSERTION_FAILURE'] = 'A1 [LIMIT]: GPS_LATITUDE is null or out of range [-90, 90]'
		if violations_df.empty:
			# If violations_df is initially empty, set it to the first set of violations
			violations_df = invalid_lat
		else:
			# Otherwise, concatenate with ignore_index=True to handle indices properly
			violations_df = pd.concat([violations_df, invalid_lat], ignore_index=True)


	#---At this point, invalid records have been fully removed from unvalidated_batch_df---
	if violations_df.empty:
		logging.info("No violations found")
	else:
		for row in violations_df.itertuples():
			logging.warning("VALIDATION VIOLATION - %s", row)


	violations_df.drop(violations_df.index, inplace=True) # empty violations df
	validated_batch_df = unvalidated_batch_df.copy()
	return validated_batch_df



def calc_breadcrumb_timestamp(opd_date, act_time):
        '''
        Each breadcrumb has it's datetime value split between two fields: OPD_DATE (string representing the correct day at midnight) >
        '''
        the_date = datetime.strptime(opd_date, '%d%b%Y:%H:%M:%S')
        the_date = the_date.date()
        time_elapsed = timedelta(seconds=act_time)
        proper_datetime=datetime.combine(the_date, datetime.min.time())+ time_elapsed
        return proper_datetime # return type is datetime object



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
SUBSCRIPTION_ID  = 'analysis_sub'
subscriber = pubsub_v1.SubscriberClient()
sub_path   = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

#---Callback Function------------------------------------------------------
def callback(message):
	global breadcrumb_count, unique_vehicles, unique_trips, earliest_bc, latest_bc, wall_clock_time
	global expected_count, sentinel_time, unvalidated_batch_df, validate_count
	message.ack()
	breadcrumb = json.loads(message.data.decode('utf-8')) # one breadcrumb
	breadcrumb_df = pd.DataFrame([breadcrumb])

	# analysis happens here
	if breadcrumb['VEHICLE_ID'] == 0:
		# When sentinel recieve, get expected count
		expected_count = breadcrumb['METERS']
		sentinel_time = time.time()
	else:
        	# Not sentinel so process data
		if wall_clock_time is None: #start timer when first breadcrumb recieved
			wall_clock_time = time.time()
			print(f"First breadcrumb received at {format_time(wall_clock_time)}")

		breadcrumb_count = breadcrumb_count + 1

		#-----DATA VALIDATION, TRANSOFRMATION, INSERT  GOES HERE-------------------------------------------
		if(len(unvalidated_batch_df) <= 20000):
			validate_count = validate_count + 1
			unvalidated_batch_df = pd.concat([unvalidated_batch_df, breadcrumb_df])
		else:
			validated_batch_df = validate_batch(unvalidated_batch_df)
			#pass validated df to transform function
			#pass transformed tables to database insert function
			validated_batch_df   = validated_batch_df.drop(validated_batch_df.index)
			unvalidated_batch_df = unvalidated_batch_df.drop(unvalidated_batch_df.index)
			print(f'{validate_count} breadcrumbs validated so far . . .')

		#-----------------------------------------------------------------------

		if breadcrumb_count % 100000 == 0:
			print(f"Collected {breadcrumb_count} so far")

		unique_vehicles.add(breadcrumb['VEHICLE_ID'])
		unique_trips.add(breadcrumb['EVENT_NO_TRIP'])

		raw_opd = breadcrumb['OPD_DATE']
		raw_act = breadcrumb['ACT_TIME']

		current_bc_time = calc_breadcrumb_timestamp(raw_opd, raw_act)

		if latest_bc is None or current_bc_time > latest_bc:
			latest_bc = current_bc_time
		if earliest_bc is None or current_bc_time < earliest_bc:
			earliest_bc = current_bc_time

	# After recieving Sentinel ensure that it hits expected count
	if expected_count is not None and breadcrumb_count == expected_count:
		elapsed_time = sentinel_time - wall_clock_time
		throughput = breadcrumb_count / elapsed_time

		#---Summary Statistics-----------------------------------------------------
		print("\nSentinel Recieved")
		print("Summary Statistics:")
		print(f"First message received: {format_time(wall_clock_time)}")
		print(f"Unique Vehicle IDs: {len(unique_vehicles)}")
		print(f"Earliest Breadcrumb from OPD and ACT: {earliest_bc}")
		print(f"Latest Breadcrumb from OPD and ACT: {latest_bc}")
		print(f"Unique Trip IDs: {len(unique_trips)}")
		print(f"Total Breadcrumbs Received: {breadcrumb_count}")
		print(f"Sentinel Received Time: {format_time(sentinel_time)}")
		print(f"Ellapsed Time: {elapsed_time:.3f}s")
		print(f"Throughput: {throughput:.3f} msg/s")

	#----Reset Data Structure(s)------------------------------------------------
		breadcrumb_count = 0
		expected_count = 0
		unique_vehicles.clear()
		unique_trips.clear()
		earliest_bc = None
		latest_bc = None
		wall_clock_time = None
		sentinel_time = None
		validate_count = 0


#---Listening--------------------------------------------------------------
streaming_pull = subscriber.subscribe(sub_path, callback=callback)

current_time = format_time(time.time())
print(f"{current_time} - Listening for messages on {SUBSCRIPTION_ID} . . . .")

with subscriber:
        try:
                streaming_pull.result()
        except Exception:
                streaming_pull.cancel()
                streaming_pull.result()
