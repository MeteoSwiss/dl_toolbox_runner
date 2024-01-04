import glob
import os

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

        self.retrieval_batches = []  # list of dicts with keys 'files' and 'conf'
        # TODO harmonise file naming with mwr_l12l2 retrieval_batches is called retrieval_dict there

    def run(self):
        self.find_files()
        pass

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
                                      'is not yet implemented')  # TODO: implement grouping of files with same config

    def assign_conf(self):
        pass


if __name__ == '__main__':
    x = Runner(abs_file_path('dl_toolbox_runner/config/main_config.yaml'))
    x.run()
    pass
