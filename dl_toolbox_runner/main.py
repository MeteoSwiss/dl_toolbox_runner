import glob
import os
import re
import time
import datetime

from hpl2netCDF_client.hpl2netCDF_client import hpl2netCDFClient

from dl_toolbox_runner.configure import Configurator
from dl_toolbox_runner.errors import DLConfigError
from dl_toolbox_runner.log import logger
from dl_toolbox_runner.utils.config_utils import get_main_config
from dl_toolbox_runner.utils.file_utils import abs_file_path, round_datetime, find_file_time_windcube, get_insttype, get_instrument_id_and_scan_type
    
class Runner(object):
    """Runner to execute (multiple) run(s) of DL-toolbox with config associated to data files

    Args:
        conf: configuration file or dictionary. Example in dl_toolbox_runner/config/main_config.yaml
    """

    def __init__(self, conf, single_process=False):
        if isinstance(conf, dict):
            self.conf = conf
        elif os.path.isfile(conf):
            self.conf = get_main_config(conf)
        else:
            logger.error("The argument 'conf' must be a conf dictionary or a path pointing to a config file")
            raise DLConfigError("The argument 'conf' must be a conf dictionary or a path pointing to a config file")

        self.retrieval_batches = []  # list of dicts with keys 'date', 'files' and 'conf' #EDIT: added 'instrument_id' and 'scan_type'
        self.single_process = single_process  # if True, create one batch per file, if False, group files with same instrument_id and scan_type
        # TODO harmonise file naming with mwr_l12l2 retrieval_batches is called retrieval_dict there
    
    def run(self, dry_run=False, instrument_id=None, date_end=None):
        start = time.time()
        logger.info('######################################################')
        logger.info('Starting retrieval process at '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))        
        logger.info('######################################################')
        logger.info('Searching for files to process')
        self.find_files(instrument_id=instrument_id)
        logger.info('Grouping files to batches')
        self.batch_files(single_process=self.single_process, date_end=date_end)
        logger.info('Assigning config files to batches')
        self.assign_conf()
        logger.info(f'Time taken to write the config files: {time.time()-start:.1f} seconds')

        if not dry_run:
            logger.info('Running DL-toolbox for the batches')
            self.run_toolbox()
        else:
            logger.info('Dry run, only creating the config files')

    def realtime_run(self, dry_run=False, retrieval_batches=[]):
        start = time.time()
        logger.info('######################################################')
        logger.info('Starting retrieval process at '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))        
        logger.info('######################################################')
        logger.info('From batch files created by watchdog:')
        print(retrieval_batches)
        self.retrieval_batches = retrieval_batches
        #self.batch_files(single_process=self.single_process, date_end=date_end)
        logger.info('Assigning config files to batches')
        self.assign_conf()
        logger.info(f'Time taken to write the config files: {time.time()-start:.1f} seconds')

        if not dry_run:
            logger.info('Running DL-toolbox for the batches')
            self.run_toolbox()
        else:
            logger.info('Dry run, only creating the config files')        

    def find_files(self, instrument_id=None):
        """find files and group them to batches for processing"""
        if instrument_id:
            logger.info(f'Searching files for instrument {instrument_id}')
            self.files = glob.glob(os.path.join(self.conf['input_dir'], self.conf['input_file_prefix'] + f'{instrument_id}*'))
        else:
            logger.info('Searching all files in input directory')
            self.files = glob.glob(os.path.join(self.conf['input_dir'], self.conf['input_file_prefix'] + '*'))

        if not self.files:
            logger.info(f'Found no files to process in {self.conf["input_dir"]}. Will exit now')
            exit()
        
    def batch_files(self, single_process=True, date_end=None):
        '''
        group files to batches for processing
        
        For now: files are batched by instrument_id and scan types. This is done using the filename only !
        
        single_process: bool: if True, create one batch per file, if False, group files with same instrument_id and scan_type
        '''
        if self.conf['max_age']: #TODO: here we should have a case for when max_age is None
            if date_end:
                date_start = date_end - datetime.timedelta(minutes=self.conf['max_age'])
                # check if file date is between date_end - max_age and date_end
                logger.info(f'Finding files between {date_start} and {date_end}')
            elif self.conf['max_age']:
                # check if file date is within now and max_age
                date_start = datetime.datetime.now() - datetime.timedelta(minutes=self.conf['max_age'])
                logger.info(f'Keeping files between {date_start} and {datetime.datetime.now}')
        else:
            date_start = None
            logger.error('No max_age defined in the config file, processing all')
            pass
            
        file_dict = {}
        for file in self.files:
            inst_type = get_insttype(file, base_filename='DWL_raw_XXXWL_', return_date=False)

            if inst_type != 'windcube':
                logger.error(f'Instrument type {inst_type} not supported')
                raise NotImplementedError(f'Instrument type {inst_type} not supported')
            
            file_dict[file] = {}
            # find instrument_id and scan_type for each file
            instrument_id, scan_type, file_datetime = get_instrument_id_and_scan_type(file, prefix=self.conf['input_file_prefix'])
            
            file_dict[file]['instrument_id'] = instrument_id
            
            if (date_start is not None) and (date_end is not None):
                if file_datetime < date_start or file_datetime > date_end:
                    continue
                else:
                    #We need to open the files and check the full time_bounds                   
                    file_start_time, file_end_time = find_file_time_windcube(file)
                    if file_start_time < date_start or file_end_time > date_end:
                        continue
                    else:
                        pass
            else:
                pass
               
            # scan type:
            if 'dbs' in file:
                scan_type = 'DBS'
            elif 'vad' in file:
                scan_type = 'VAD'                
            else:
                logger.error("No valid scan type identified for:"+self.datafile)
            
            if 'TP' in file:
                scan_type = scan_type+'_TP'
                
            file_dict[file]['scan_type'] = scan_type
            
            if single_process:
                self.retrieval_batches.append({'files': [file], 'instrument_id': file_dict[file]['instrument_id'], 'scan_type': file_dict[file]['scan_type']})
            else: 
                # As a first test, we can implement this based on the filename only
                # check if instrument_id and scan_type already exist in one of the batch
                if any((d['instrument_id'] == file_dict[file]['instrument_id']) & (d['scan_type'] == file_dict[file]['scan_type']) for d in self.retrieval_batches): 
                    # if so, add the file to the list of files from the correct batch
                    for batch in self.retrieval_batches:
                        if (batch['instrument_id'] == file_dict[file]['instrument_id']) & (batch['scan_type'] == file_dict[file]['scan_type']):
                            # if both instrument_id and scan_type match, add the file to the batch
                            batch['files'].append(file)
                else:
                    # otherwise, create a new batch
                    self.retrieval_batches.append({'files': [file], 'instrument_id': file_dict[file]['instrument_id'], 'scan_type': file_dict[file]['scan_type']})

        if self.retrieval_batches:
            logger.info(f'Found {len(self.retrieval_batches)} batches of files to process')
        else:
            logger.critical(f'Found no files to process in {self.conf["input_dir"]}. Will exit now')
            exit()

    def assign_conf(self):
        """assign a config file for the DL-toolbox run and a date to each bunch of files in self.retrieval_batches"""

        for ind, batch in enumerate(self.retrieval_batches):
            # create a config file for the DL-toolbox run of this batch
            logger.info(f'Creating config file for batch {ind+1} containing {len(batch["files"])} files')
            filename_conf = self.conf['toolbox_conf_prefix'] + f'{ind:03d}' + self.conf['toolbox_conf_ext']
            batch['conf'] = os.path.join(self.conf['toolbox_confdir'], filename_conf)
            tmp_conf = Configurator(batch['instrument_id'], batch['scan_type'], batch['files'][0], batch['conf'], self.conf)  # use first file in batch as reference
            tmp_conf.run()
            batch['date'] = tmp_conf.date.replace(hour=0, minute=0, second=0, microsecond=0)  # floor to the day

    def run_toolbox(self, parallel=False):
        """run the DL toolbox code on all entries of self.retrieval_batches"""
        if parallel:  # run multiple DL toolboxes in parallel
            raise NotImplementedError('parallel runs of DL_toolbox are not yet implemented')
        else:  # execute multiple runs of DL toolbox in sequence
            for batch in self.retrieval_batches:
                logger.info('######################################################')
                logger.info(f'Running DL-toolbox with {len(batch["files"])} files on {batch["date"]}')
                tl_time = time.time()
                self.run_toolbox_single(batch)
                logger.info(f'Time taken for this batch: {time.time()-tl_time :.1f} seconds')

    @staticmethod
    def run_toolbox_single(batch, cmd='lvl2_from_filelist', cmd_opt_args=('DWL_raw_XXXWL_', )):
        """do one run of DL toolbox on a single batch of files"""
        
        proc_dl = hpl2netCDFClient(batch['conf'], cmd, batch['date'])
        cmd_func = getattr(proc_dl, cmd)
        cmd_func(batch['files'], *cmd_opt_args)


if __name__ == '__main__':
    x = Runner(abs_file_path('dl_toolbox_runner/config/main_config.yaml'), single_process=False)
    # Find the latest "round" time (e.g. 13:00, 13:10, 13:20, 13:30) and use this as date_end
    date_end = round_datetime(datetime.datetime.now(), round_to_minutes=10)#.strftime('%Y-%m-%d_%H-%M-%S')
    #date_end = datetime.datetime(2024,7,11,5,30)
    x.run(dry_run=True, date_end=date_end, instrument_id=None)
    pass
