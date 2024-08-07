# Observer and retrieval manager for Doppler Lidar data
import os
import sys
import time
import logging
import re
import datetime
from threading import Thread
from pathlib import Path
from queue import Queue
from multiprocessing import Pool

#from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

from dl_toolbox_runner.main import Runner
from dl_toolbox_runner.utils.file_utils import abs_file_path, round_datetime, find_file_time_windcube, get_instrument_id_and_scan_type
from dl_toolbox_runner.log import logger

X = 5 # Time window for the retrieval in minutes
BATCH_CREATION_THRESHOLD = 40 # Time in minutes after which a batch is considered too old

class RealTimeWatcher(FileSystemEventHandler):
    
    # Class to manage the file system events and start the wind retrieval
    def __init__(self, queue, file_prefix):
        logger.info('Initializing RealTimeWatcher')
        self.x = Runner(abs_file_path('dl_toolbox_runner/config/main_config.yaml'), single_process=False)
        self.file_prefix = file_prefix

        self.retrieval_batches = [] # list of dictionary to store the file batches
        #self.date_start = round_datetime(datetime.datetime.now() + datetime.timedelta(minutes=10), round_to_minutes=10)
        #self.date_end = self.date_start + datetime.timedelta(minutes=10)
        
        self.queue = queue
        
    def process(self, batch):  
        self.queue.put(batch)
        logging.info(f"Storing batch: {self.queue.qsize()}")
        #print("Producer queue: ", list(self.queue.queue))
    
    def create_batch(self, path, instrument_id, scan_type, file_start_time, file_end_time, file_length):
        # The first retrieval can only happen after 10+ minutes of data
        #self.date_start = round_datetime(file_datetime + datetime.timedelta(minutes=10), round_to_minutes=10)
        #self.date_end = self.date_start + datetime.timedelta(minutes=10)
            
        #print('First retrieval time window: ', self.date_start, self.date_end)
        retrieval_start_time = round_datetime(file_start_time, round_to_minutes=X)
        retrieval_stop_time = retrieval_start_time + datetime.timedelta(minutes=X)
        # otherwise, create a new batch
        self.retrieval_batches.append(
            {'files': [path], 
            'instrument_id': instrument_id,
            'scan_type': scan_type,
            'batch_start_time': file_start_time,
            'batch_end_time': file_end_time,
            'batch_length_sec': file_length,
            'retrieval_start_time': retrieval_start_time,
            'retrieval_end_time': retrieval_stop_time,
            'batch_creation_time': datetime.datetime.now(),
            }
        )
        print('New batch created for ID: ', instrument_id, 'and scan type: ', scan_type, 'from file, with retrieval border: ', retrieval_start_time, 'and', retrieval_stop_time)
        print('Number of batches: ', len(self.retrieval_batches))
        
    def on_created(self, event):
        # When a file is created, collect the path and store it
        # Start retrieval only when sufficient files are available for each measurement type
        # For each new files, store it in retrieval_batches dict with following format:
        # {'files': [file], 'instrument_id': file_dict[file]['instrument_id'], 'scan_type': file_dict[file]['scan_type']}
        
        path = event.src_path
        file = Path(path).name
        instrument_id, scan_type, file_datetime = get_instrument_id_and_scan_type(file, prefix=self.file_prefix)
        
        # TODO: remove this fix !
        if instrument_id == 'PAYWL' or instrument_id == 'GREWL':
            return
        
        file_start_time, file_end_time = find_file_time_windcube(path)
        print('####################')
        print('file: ', file, ' with start time: ', file_start_time, 'and end time: ', file_end_time)

        # Extract length of measurement in the file:
        file_length = (file_end_time - file_start_time).total_seconds()
               
        # As a first test, we can implement this based on the filename only
        # check if instrument_id and scan_type already exist in one of the batch
        # batch dictionary is formatted as: {'files': [file], 'instrument_id': instrument_id, 'scan_type': scan_type}
        
        if any((d['instrument_id'] == instrument_id) & (d['scan_type'] == scan_type) for d in self.retrieval_batches):
            new_batch = 0
            # if so, add the file to the list of files from the correct batch
            # WARNING: 2 (or more) batches for same types and instrument can co-exist if the files are not in the same time window
            # Need to check if the file fits in the time window of each batch
            for batch in self.retrieval_batches:
                if batch['batch_creation_time'] < datetime.datetime.now() - datetime.timedelta(minutes=BATCH_CREATION_THRESHOLD):
                    print('WARNING: TOO OLD BATCH FOUND, removing it but this should NEVER happened !')
                    self.retrieval_batches.remove(batch)
                if (batch['instrument_id'] == instrument_id) & (batch['scan_type'] == scan_type):
                    # if both instrument_id and scan_type match, 
                    # Check the time window of the batch
                    # add the file to the batch if it fits in the time window
                    if (file_end_time < batch['retrieval_start_time']):
                        print('File end time is before batch retrieval start time, ignoring file')
                        print('File end time: ', file_end_time, 'Batch retrieval start time: ', batch['retrieval_start_time'])
                        new_batch = 0
                        continue
                    elif (file_start_time > batch['retrieval_end_time']):
                        new_batch = 1
                        print('File start time is after batch end time:')
                        print('File start time: ', file_start_time, 'Batch end time: ', batch['retrieval_end_time'])
                        print('Checking other batches to see if the time fits...')
                        #self.create_batch(path, instrument_id, scan_type, file_start_time, file_end_time, file_length)
                        #print('Number of batches: ', len(self.retrieval_batches))
                        continue
                    else:
                        print('File added to existing batch for ID ', instrument_id, 'and scan type ', scan_type, ' with retrieval time border: ', batch['retrieval_start_time'], batch['retrieval_end_time'])
                        # Add file to batch and update the batch_end_time
                        batch['files'].append(path)
                        batch['batch_length_sec'] += file_length
                    
                        if file_start_time < batch['batch_start_time']:
                            batch['batch_start_time'] = file_start_time
                        if file_end_time > batch['batch_end_time']:
                            batch['batch_end_time'] = file_end_time
                                        
                        # When the batch_end_time becomes bigger thant date_end, do the retrieval for this batch and empty file list
                        #if (batch['batch_start_time'] < self.date_start) & (batch['batch_end_time'] > self.date_end):
                        if (batch['batch_length_sec'] > X*60) & (batch['batch_end_time'] > batch['retrieval_end_time']):
                            print('Added batch to queue')
                            self.process(batch)

                            #self.x.realtime_run(dry_run=False, retrieval_batches=[batch])
                            # remove the batch from the list
                            self.retrieval_batches.remove(batch)
                            print('Number of batches (after removal): ', len(self.retrieval_batches))
                        new_batch = 0
                        return
            if new_batch:
                # This should only create a new batch after having check all existing batches and make sure in all cases
                # that the start_time of the file was bigger than the retrieval_end_time
                self.create_batch(path, instrument_id, scan_type, file_start_time, file_end_time, file_length)
        else:
            self.create_batch(path, instrument_id, scan_type, file_start_time, file_end_time, file_length)
    
        #print('Retrieval batches: ', self.retrieval_batches)
        
    def on_modified(self, event):
        # Files should not get modified
        logger.critical(f'event type: {event.event_type}  path : {event.src_path}')
        pass
    
def process_load_queue(queue):
    while True:
        if not queue.empty():
            batch = queue.get()
            pool = Pool(processes=1)
            pool.apply_async(run_retrieval, (batch,))   
        else:
            time.sleep(1)
            
def run_retrieval(batch):
    time.sleep(2)
    print('####################################################################################')
    print('Retrieval triggered for ID: ', batch['instrument_id'], 'and scan type: ', batch['scan_type'])
    print('Retrieval batch time border: ', batch['batch_start_time'], batch['batch_end_time'])
    x = Runner(abs_file_path('dl_toolbox_runner/config/main_config.yaml'), single_process=False)
    x.realtime_run(dry_run=False, retrieval_batches=[batch])
    
def start_watchdog_queue(watch_path):
    logger.info(f"Starting Watchdog Observer, version with queuing\n")
    watchdog_queue = Queue()
    
    x = Runner(abs_file_path('dl_toolbox_runner/config/main_config.yaml'), single_process=False)
    file_prefix = x.conf['input_file_prefix'] 
    
    event_handler = RealTimeWatcher(watchdog_queue, file_prefix)
    observer = PollingObserver()
    observer.schedule(event_handler, watch_path, recursive=True)
    observer.start()

    worker = Thread(target=process_load_queue, args=(watchdog_queue,))
    worker.daemon = True
    worker.start()
    
    try:
        while True:
            time.sleep(2)
    except Exception as error:
        observer.stop()
        print(f"Error: {str(error)}")
    observer.join()
    
if __name__ == '__main__':
    watch_path = "/data/eprofile-dl-raw/"  # Directory to watch
    #watch_path = "s3://eprofile-dl-raw/"
    start_watchdog_queue(watch_path)