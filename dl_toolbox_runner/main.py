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

        self.retrieval_batches = []  # list of dicts with keys 'date', 'files' and 'conf'
        # TODO harmonise file naming with mwr_l12l2 retrieval_batches is called retrieval_dict there

    def run(self):
        self.find_files()
        self.assign_conf()
        self.run_toolbox()

    def find_files(self, single_process=True):
        """find files and group them to batches for processing"""

        files = glob.glob(os.path.join(self.conf['input_dir'], self.conf['input_file_prefix'] + '*'))
        # TODO: check for file age with conf['max_age']
        if not files:
            logger.info(f'Found no files to process in {self.conf["input_dir"]}. Will exit now')
            exit()

        if single_process:
            for file in files:
                self.retrieval_batches.append({'files': [file]})
        else:
            raise NotImplementedError('processing multiple files in same batch (single_process=False) '
                                      'is not yet implemented')
            # TODO: implement grouping of files with same config. Possibly easier to be done after assign_conf using
            #  date and conf

    def assign_conf(self):
        """assign a config file for the DL-toolbox run and a date to each bunch of files in self.retrieval_batches"""

        for ind, batch in enumerate(self.retrieval_batches):
            filename_conf = self.conf['toolbox_conf_prefix'] + f'{ind:03d}' + self.conf['toolbox_conf_ext']
            batch['conf'] = os.path.join(self.conf['toolbox_confdir'], filename_conf)
            tmp_conf = Configurator(batch['files'][0], batch['conf'])  # use first file in batch as reference
            tmp_conf.run()
            batch['date'] = tmp_conf.date.replace(hour=0, minute=0, second=0, microsecond=0)  # floor to the day

    def run_toolbox(self, parallel=False):
        """run the DL toolbox code on all entries of self.retrieval_batches"""
        if parallel:  # run multiple DL toolboxes in parallel
            raise NotImplementedError('parallel runs of DL_toolbox are not yet implemented')
        else:  # execute multiple runs of DL toolbox in sequence
            for batch in self.retrieval_batches:

                # BEGIN OF HACK for using finished config file
                import datetime as dt
                if batch['date'] == dt.datetime.strptime('2023-01-01', '%Y-%m-%d'):
                    batch['conf'] = abs_file_path('dl_toolbox_runner/data/toolbox/sample_config/PAYWL_DBS_TP.conf')
                elif batch['date'] == dt.datetime.strptime('2023-02-09', '%Y-%m-%d'):
                    batch['conf'] = abs_file_path('dl_toolbox_runner/data/toolbox/sample_config/PAYWL_VAD_TP.conf')
                logger.critical('using hack to attribute sample config file instead of real one in main.run_toolbox')
                # END OF HACK
                # TODO: remove hack above when configurator is ready

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
