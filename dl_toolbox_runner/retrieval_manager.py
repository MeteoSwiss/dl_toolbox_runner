# Observer and retrieval manager for Doppler Lidar data
import os
import sys
import time
import datetime
import pandas as pd
from threading import Thread
from pathlib import Path
from queue import Queue
from multiprocessing import Pool

#from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

from dl_toolbox_runner.main import Runner
from dl_toolbox_runner.errors import LogicError, DLFileError
from dl_toolbox_runner.utils.file_utils import abs_file_path, round_datetime, find_file_time_windcube, get_instrument_id_and_scan_type, create_batch, get_insttype, read_halo
from dl_toolbox_runner.log import logger

class RealTimeWatcher(FileSystemEventHandler):
    
    # Class to manage the file system events and start the wind retrieval
    def __init__(self, queue, file_prefix):
        logger.info('Initializing RealTimeWatcher')
        self.x = Runner(abs_file_path('dl_toolbox_runner/config/main_config.yaml'), single_process=False)
        self.file_prefix = file_prefix

        self.retrieval_batches = [] # list of dictionary to store the file batches
        self.retrieval_time = 10 # Time window for the retrieval in minutes
        
        #self.date_start = round_datetime(datetime.datetime.now() + datetime.timedelta(minutes=10), round_to_minutes=10)
        #self.date_end = self.date_start + datetime.timedelta(minutes=10)
        
        self.threshold = 0.6 # % Threshold for the batch length to trigger the retrieval
        self.max_batch_age = 40 # Time in minutes after which a batch is considered too old and deleted
        
        self.queue = queue
            
    def check_and_process_batch(self, batch, threshold=0.6, max_batch_age=40, delay=10):
        '''
        Dedicated function to check if the batch is ready for retrieval and trigger the retrieval
        
        Arguments:
        batch: dictionary with the batch information
        threshold: threshold for the batch length to trigger the retrieval
        max_batch_age: maximum age of the batch in minutes
        delay: delay in minutes before starting the retrieval
        
        Returns (TODO: check if needed)
        0 if the batch has been processed (either deleted or retrieved)
        1 if the batch is not ready for retrieval
        
        '''
        if batch['batch_creation_time'] < datetime.datetime.now() - datetime.timedelta(minutes=max_batch_age):
            logger.warning('Batch is too old, removing it from the batch list !')
            print(batch)
            self.retrieval_batches.remove(batch)
            return 0
        
        # Check that there is enough measurement time AND leave a margin of 10 minutes in case new files would be added to the batch
        if (batch['batch_length_sec'] > threshold*self.retrieval_time*60) & (batch['batch_end_time'] < datetime.datetime.now() - datetime.timedelta(minutes=delay)):
            # Add batch to the the queue for retrieval
            self.queue.put(batch)
            logger.info(f"Adding batch to queue with size: {self.queue.qsize()}")
            # remove the batch from the list
            self.retrieval_batches.remove(batch)
            logger.info('Added batch to queue and removing it, number of batches remaining: ' + str(len(self.retrieval_batches)))
            return 0
        else:
            #self.retrieval_batches.append(batch)
            return 1
    
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
            
            # TODO: at the moment, scan_id is not defined for Halo and is set to default 0 (=instrument number)
            instrument_id, scan_type, scan_id, scan_resolution, file_datetime = get_instrument_id_and_scan_type(file, inst_type, prefix=self.file_prefix)            
            
            if inst_type == 'system_data':
                logger.info(f'File {file} is system data and will be skipped')
                return
            elif inst_type == 'windcube':
                file_start_time, file_end_time = find_file_time_windcube(file)
            elif inst_type == 'halo':
                logger.info(f'Reading: {file}')
                mheader, time_ds = read_halo(file)
                file_start_time, file_end_time = pd.to_datetime(time_ds.values[0]), pd.to_datetime(time_ds.values[-1])
            else:
                logger.error(f": {file} is not of a known instrument type")
                raise DLFileError
            
            if file_start_time is None:
                logger.error(f"Problem while reading: {file}")
                return
            
            print('####################')
            logger.info(f'file: {filename} from: {file_start_time} to: {file_end_time}')

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
            
            if any((d['instrument_id'] == instrument_id) & (d['scan_type'] == scan_type) & (d['scan_id'] == scan_id) for d in self.retrieval_batches):
                new_batch = 0
                # if so, add the file to the list of files from the correct batch
                # WARNING: 2 (or more) batches for same types and instrument can co-exist if the files are not in the same time window
                # Need to check if the file fits in the time window of each batch
                for batch in self.retrieval_batches:
                    if (batch['instrument_id'] == instrument_id) & (batch['scan_type'] == scan_type) & (batch['scan_id'] == scan_id):
                        # if both instrument_id and scan_type match, 
                        # Check the time window of the batch
                        # add the file to the batch if it fits in the time window
                        if (file_end_time < batch['retrieval_start_time']):
                            logger.info('File end time is before batch retrieval start time, ignoring file')
                            #print('File end time: ', file_end_time, 'Batch retrieval start time: ', batch['retrieval_start_time'])
                            new_batch = 0
                            continue
                        elif (file_start_time > batch['retrieval_end_time']):
                            new_batch = 1
                            continue
                        elif (file_start_time > batch['retrieval_start_time']) & (file_end_time < batch['retrieval_end_time']):
                            # Case when the files are completely withing the retrieval time window
                            logger.info('File added to existing batch for ' + instrument_id + ' and scan type: ' + scan_type + ' with retrieval time border: ' + str(batch['retrieval_start_time']) + '' + str(batch['retrieval_end_time']))
                            # Add file to batch and update the batch_end_time
                            batch['files'].append(file)
                            batch['batch_length_sec'] += file_dict['file_length']
                        
                            if file_start_time < batch['batch_start_time']:
                                batch['batch_start_time'] = file_start_time
                            if file_end_time > batch['batch_end_time']:
                                batch['batch_end_time'] = file_end_time
                            
                            # When the batch_end_time becomes bigger thant date_end, do the retrieval for this batch and empty file list
                            #if (batch['batch_start_time'] < self.date_start) & (batch['batch_end_time'] > self.date_end):
                            #check = self.check_and_process_batch(batch, threshold=self.threshold, max_batch_age=self.max_batch_age)
                            new_batch = 0
                            continue
                        elif (file_start_time > batch['retrieval_start_time']) or (file_end_time < batch['retrieval_end_time']):
                            #logger.info('Part of the file is in the time window but not all --> ignoring it for now')
                            logger.info('Part of the file is in the time window but not all --> we try adding it now')
                            batch['files'].append(file)
                            if file_start_time < batch['batch_start_time']:
                                batch['batch_start_time'] = file_start_time
                            if file_end_time > batch['batch_end_time']:
                                batch['batch_end_time'] = file_end_time
                            batch['batch_length_sec'] = (batch['batch_end_time'] - batch['batch_start_time']).total_seconds()
                            new_batch = 0
                            continue
                            #new_batch = 0
                            #continue
                        else:
                            logger.error('Error in the code, recheck the logic')
                            raise LogicError('Error in the code, recheck the logic')
                    else:
                        continue

                if new_batch:
                    # This should only create a new batch after having check all existing batches and make sure in all cases
                    # that the start_time of the file was bigger than the retrieval_end_time
                    retrieval_start_time = round_datetime(file_end_time, round_to_minutes=self.retrieval_time)
                    retrieval_end_time = retrieval_start_time + datetime.timedelta(minutes=self.retrieval_time)
                    batch = create_batch(file_dict, retrieval_start_time, retrieval_end_time)
                    self.retrieval_batches.append(batch)
                    logger.info('New batch created for ID '+file_dict['instrument_id']+' and scan type: '+file_dict['scan_type']+' from file, with retrieval border:'+ str(retrieval_start_time)+' and '+str(retrieval_end_time))
            else:
                retrieval_start_time = round_datetime(file_end_time, round_to_minutes=self.retrieval_time)
                retrieval_end_time = retrieval_start_time + datetime.timedelta(minutes=self.retrieval_time)
                batch = create_batch(file_dict, retrieval_start_time, retrieval_end_time)
                self.retrieval_batches.append(batch)
                logger.info('New batch created for ID '+file_dict['instrument_id']+' and scan type: '+file_dict['scan_type']+' from file, with retrieval border:'+ str(retrieval_start_time)+' and '+str(retrieval_end_time))
                #check = self.check_and_process_batch(batch, threshold=self.threshold, max_batch_age=self.max_batch_age)
            
            for batch in self.retrieval_batches:
                check = self.check_and_process_batch(batch, threshold=self.threshold, max_batch_age=self.max_batch_age)
            logger.info(f'Number of batches: {len(self.retrieval_batches)}')
        except Exception as error:
            logger.error(f"{str(error)}, Ignoring this file...")
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
    try:
        print('####################################################################################')
        logger.info('Retrieval triggered for ID: ' + batch['instrument_id'] + ' and scan type: ' + batch['scan_type'])
        logger.info('Retrieval batch time border: ' + str(batch['batch_start_time']) + ' ; ' + str(batch['batch_end_time']))
        runner = Runner(abs_file_path('dl_toolbox_runner/config/main_config.yaml'), single_process=False)
        runner.realtime_run(dry_run=False, retrieval_batches=[batch])
    except Exception as error:
        logger.error(f"{str(error)}, Ignoring this batch...")
        return
    
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