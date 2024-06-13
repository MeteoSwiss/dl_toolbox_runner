import glob
import os

from hpl2netCDF_client.hpl2netCDF_client import hpl2netCDFClient

from dl_toolbox_runner.configure import Configurator
from dl_toolbox_runner.errors import DLConfigError
from dl_toolbox_runner.log import logger
from dl_toolbox_runner.utils.config_utils import get_main_config
from dl_toolbox_runner.utils.file_utils import abs_file_path


class Runner(object):
    """Runner to execute (multiple) run(s) of DL-toolbox with config associated to data files

    Args:
        conf: configuration file or dictionary. Example in dl_toolbox_runner/config/main_config.yaml
    """

    def __init__(self, conf):
        if isinstance(conf, dict):
            self.conf = conf
        elif os.path.isfile(conf):
            self.conf = get_main_config(conf)
        else:
            logger.error("The argument 'conf' must be a conf dictionary or a path pointing to a config file")
            raise DLConfigError("The argument 'conf' must be a conf dictionary or a path pointing to a config file")

        self.retrieval_batches = []  # list of dicts with keys 'date', 'files' and 'conf' #EDIT: added 'instrument_id' and 'scan_type'
        # TODO harmonise file naming with mwr_l12l2 retrieval_batches is called retrieval_dict there

    def run(self):
        self.find_files()
        self.batch_files(single_process=False)
        self.assign_conf()
        self.run_toolbox()

    def find_files(self):
        """find files and group them to batches for processing"""

        self.files = glob.glob(os.path.join(self.conf['input_dir'], self.conf['input_file_prefix'] + '*'))
        # TODO: check for file age with conf['max_age']
        if not self.files:
            logger.info(f'Found no files to process in {self.conf["input_dir"]}. Will exit now')
            exit()
        
    def batch_files(self, single_process=True):
        '''
        group files to batches for processing
        
        For now: files are batched by instrument_id and scan types. This is done using the filename only !
        
        single_process: bool: if True, create one batch per file, if False, group files with same instrument_id and scan_type
        '''        
        file_dict = {}
        for file in self.files:
            # find instrument_id and scan_type for each file
            file_dict[file] = {}
            idx_id = file.find(self.conf['input_file_prefix'])+len(self.conf['input_file_prefix'])
            file_dict[file]['instrument_id'] = file[idx_id:idx_id+5]
                
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

    def assign_conf(self):
        """assign a config file for the DL-toolbox run and a date to each bunch of files in self.retrieval_batches"""

        for ind, batch in enumerate(self.retrieval_batches):
            filename_conf = self.conf['toolbox_conf_prefix'] + f'{ind:03d}' + self.conf['toolbox_conf_ext']
            batch['conf'] = os.path.join(self.conf['toolbox_confdir'], filename_conf)
            tmp_conf = Configurator(batch['instrument_id'], batch['scan_type'], batch['files'][0], batch['conf'], self.conf, 'dl_toolbox_runner/config/default_config_windcube.yaml')  # use first file in batch as reference
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
                self.run_toolbox_single(batch)

    @staticmethod
    def run_toolbox_single(batch, cmd='lvl2_from_filelist', cmd_opt_args=('DWL_raw_XXXWL_', )):
        """do one run of DL toolbox on a single batch of files"""
        
        proc_dl = hpl2netCDFClient(batch['conf'], cmd, batch['date'])
        cmd_func = getattr(proc_dl, cmd)
        cmd_func(batch['files'], *cmd_opt_args)


if __name__ == '__main__':
    x = Runner(abs_file_path('dl_toolbox_runner/config/main_config.yaml'))
    x.run()
    pass
