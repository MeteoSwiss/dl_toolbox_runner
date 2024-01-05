from dl_toolbox_runner.errors import MissingConfig
from dl_toolbox_runner.utils.config_utils import get_conf
from dl_toolbox_runner.utils.file_utils import abs_file_path, get_insttype, dict_to_file


class Configurator(object):
    """Class for setting up config file for usage in DL toolbox run

    The required information is taken from the filename and contents of the data file, from a main and an inst config

    Args:
        datafile: filename of the data file to be processed
        configfile: filename of the configuration file generated by running this class
    """

    def __init__(self, datafile, configfile, file_default_config='dl_toolbox_runner/config/default_config.yaml',
                 file_conf_param_match='dl_toolbox_runner/config/conf_match.yaml'):

        self.conf = get_conf(abs_file_path(file_default_config))  # init with defaults from file, update later in code
        self.datafile = datafile
        self.configfile = configfile
        self.conf_param_match = get_conf(abs_file_path(file_conf_param_match))
        self.date = None  # datetime.datetime object for timesteamp of datafile

    def run(self):
        self.conf['inst_type'], self.date = get_insttype(self.datafile, base_filename='DWL_raw_XXXWL_',
                                                         return_date=True)
        self.from_datafile()
        self.to_file()

    def from_datafile(self):
        pass

    def to_file(self):
        try:
            conf_out = {key_out: self.conf[key_in] for key_in, key_out in self.conf_param_match.items()}
        except KeyError as err:
            raise MissingConfig(f'missing entry for {err} in config dict')
        dict_to_file(conf_out, self.configfile, '=')


if __name__ == '__main__':
    datafile = abs_file_path('dl_toolbox_runner/data/input/DWL_raw_PAYWL_2023-01-01_00-06-12_dbs_303_50mTP.nc')
    configfile = abs_file_path('dl_toolbox_runner/data/output/PAYWL_1.conf')
    x = Configurator(datafile, configfile)
    x.run()
    pass
