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
from dl_toolbox_runner.utils.file_utils import abs_file_path, round_datetime, find_file_time_windcube, get_instrument_id_and_scan_type, create_batch, get_insttype
from dl_toolbox_runner.log import logger

X = 10 # Time window for the retrieval in minutes
THESH_4_RETRIEVAL = 0.6 # % Threshold for the batch length to trigger the retrieval
BATCH_CREATION_THRESHOLD = 40 # Time in minutes after which a batch is considered too old

class RealTimeWatcher(FileSystemEventHandler):
    
    # Class to manage the file system events and start the wind retrieval
    def __init__(self, queue, file_prefix):
        logger.info('Initializing RealTimeWatcher')
        self.x = Runner(abs_file_path('dl_toolbox_runner/config/main_config.yaml'), single_process=False)
        self.file_prefix = file_prefix

        self.retrieval_batches = [] # list of dictionary to store the file batches
        self.retrieval_time = self.x.max_age # the retrieval time in minutes
        
        #self.date_start = round_datetime(datetime.datetime.now() + datetime.timedelta(minutes=10), round_to_minutes=10)
        #self.date_end = self.date_start + datetime.timedelta(minutes=10)
        
        self.max_age = 40 # Time in minutes after which a batch is considered too old and deleted
        
        self.queue = queue
        
    def process(self, batch):  
        self.queue.put(batch)
        logging.info(f"Storing batch: {self.queue.qsize()}")
        #print("Producer queue: ", list(self.queue.queue))
    
    def check_and_process_batch(self, batch, threshold=0.6, max_age=40):
        '''
        Dedicated function to check if the batch is ready for retrieval and trigger the retrieval
        
        '''
        if batch['batch_creation_time'] < datetime.datetime.now() - datetime.timedelta(minutes=max_age):
            logger.warning('Batch is too old, removing it from the batch list !')
            print(batch)
            self.retrieval_batches.remove(batch)
            return 1
        
        if (batch['batch_length_sec'] > threshold*self.retrieval_time*60):
            # Add batch to the the queue for retrieval
            self.queue.put(batch)
            # remove the batch from the list
            self.retrieval_batches.remove(batch)
            print('Added batch to queue and removing it, number of batches remaining: ', len(self.retrieval_batches))
            return 1
        
        return 0
    
    def on_created(self, event):
        try:
            # When a file is created, collect the path and store it
            # Start retrieval only when sufficient files are available for each measurement type
            # For each new files, store it in retrieval_batches dict with following format:
            # {'instrument_id':instrument_id, 'scan_type':scan_type, 'files':[file1, file2, ...], 'file_start_time':start_time, 'file_end_time':end_time, 'file_length':length}
            file_dict = {}
            # file here must be the full path !
            file = event.src_path
            filename = Path(file).name
            inst_type = get_insttype(file, base_filename='DWL_raw_XXXWL_', return_date=False)
            instrument_id, scan_type, scan_id, scan_resolution, file_datetime = get_instrument_id_and_scan_type(file, inst_type, prefix=self.file_prefix)
            
            if inst_type == 'windcube':
                file_start_time, file_end_time = find_file_time_windcube(file)
            elif inst_type == 'halo':
                return
            else:
                logger.error(f": {file}")
                return
            
            if file_start_time is None:
                logger.error(f"Problem while reading: {file}")
                return
            
            print('####################')
            print('file: ', filename, ' with start time: ', file_start_time, 'and end time: ', file_end_time)

            # Build file dictionary:
            file_dict['file'] = file
            file_dict['instrument_id'] = instrument_id
            file_dict['scan_type'] = scan_type
            file_dict['scan_id'] = scan_id
            file_dict['scan_resolution'] = scan_resolution
            file_dict['file_start_time'] = file_start_time
            file_dict['file_end_time'] = file_end_time
            file_dict['file_length'] = (file_end_time - file_start_time).total_seconds()
            
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
                        print(batch)
                        self.retrieval_batches.remove(batch)
                        continue
                    if (batch['instrument_id'] == instrument_id) & (batch['scan_type'] == scan_type):
                        # if both instrument_id and scan_type match, 
                        # Check the time window of the batch
                        # add the file to the batch if it fits in the time window
                        if (file_end_time < batch['retrieval_start_time']):
                            print('File end time is before batch retrieval start time, ignoring file')
                            #print('File end time: ', file_end_time, 'Batch retrieval start time: ', batch['retrieval_start_time'])
                            new_batch = 0
                            continue
                        elif (file_start_time > batch['retrieval_end_time']):
                            new_batch = 1
                            #print('File start time is after batch end time:')
                            #print('File start time: ', file_start_time, 'Batch end time: ', batch['retrieval_end_time'])
                            #print('Checking other batches to see if the time fits...')
                            #self.create_batch(path, instrument_id, scan_type, file_start_time, file_end_time, file_length)
                            #print('Number of batches: ', len(self.retrieval_batches))
                            continue
                        elif (file_start_time > batch['retrieval_start_time']) & (file_end_time < batch['retrieval_end_time']):
                            # We can only include files with time bounds within retrieval times !
                            # TODO: Can we do something with files overlapping retrieval bounds ?? 
                            print('File added to existing batch for ID ', instrument_id, 'and scan type ', scan_type, ' with retrieval time border: ', batch['retrieval_start_time'], batch['retrieval_end_time'])
                            # Add file to batch and update the batch_end_time
                            batch['files'].append(file)
                            batch['batch_length_sec'] += file_dict['file_length']
                        
                            if file_start_time < batch['batch_start_time']:
                                batch['batch_start_time'] = file_start_time
                            if file_end_time > batch['batch_end_time']:
                                batch['batch_end_time'] = file_end_time
                            
                            # >>> the following could be put in another for loop in case the same file should be used in multiple batches  
                            # When the batch_end_time becomes bigger thant date_end, do the retrieval for this batch and empty file list
                            #if (batch['batch_start_time'] < self.date_start) & (batch['batch_end_time'] > self.date_end):
                            if (batch['batch_length_sec'] > THESH_4_RETRIEVAL*X*60):
                                self.process(batch)
                                # remove the batch from the list
                                self.retrieval_batches.remove(batch)
                                print('Added batch to queue and removing it, number of batches remaining: ', len(self.retrieval_batches))
                            # >>>>
                            new_batch = 0
                            return
                        elif (file_start_time > batch['retrieval_start_time']) or (file_end_time < batch['retrieval_end_time']):
                            print('Part of the file is in the time window but not all --> ignoring it for now')
                            new_batch = 0
                            continue
                        else:
                            raise ValueError('Error in the code, check the logic')
                    else:
                        continue
                if new_batch:
                    # This should only create a new batch after having check all existing batches and make sure in all cases
                    # that the start_time of the file was bigger than the retrieval_end_time
                    retrieval_start_time = round_datetime(file_start_time, round_to_minutes=X)
                    retrieval_end_time = retrieval_start_time + datetime.timedelta(minutes=X)
                    newly_created = create_batch(file_dict, retrieval_start_time, retrieval_end_time)
                    if (newly_created['batch_length_sec'] > THESH_4_RETRIEVAL*X*60):
                        self.process(newly_created)
                        # remove the batch from the list
                        print('Added batch to queue and removing it, number of batches remaining: ', len(self.retrieval_batches))
                    else:
                        self.retrieval_batches.append(newly_created)

            else:
                retrieval_start_time = round_datetime(file_start_time, round_to_minutes=X)
                retrieval_end_time = retrieval_start_time + datetime.timedelta(minutes=X)
                newly_created = create_batch(file_dict, retrieval_start_time, retrieval_end_time)
                if (newly_created['batch_length_sec'] > THESH_4_RETRIEVAL*X*60):
                    self.process(newly_created)
                    # remove the batch from the list
                    print('Added batch to queue and removing it, number of batches remaining: ', len(self.retrieval_batches))
                else:
                    self.retrieval_batches.append(newly_created)
                    
            print('Number of batches: ', len(self.retrieval_batches))
        except Exception as error:
            logger.error(f"Error: {str(error)}, 'Ignoring this file...'")
            return
        
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
    
    # Make sure that the config path is the same as the one being watched !
    assert os.path.realpath(x.conf['input_dir']) == os.path.realpath(watch_path), f"Configured input directory {x.conf['input_dir']} does not match the one provided {watch_path}"
    
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
    watch_path = '/data/eprofile-dl-raw/'  # Directory to watch
    #watch_path = "s3://eprofile-dl-raw/"
    start_watchdog_queue(watch_path)