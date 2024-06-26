import xarray as xr

from dl_toolbox_runner.errors import MissingConfig
from dl_toolbox_runner.utils.config_utils import get_conf
from dl_toolbox_runner.utils.file_utils import abs_file_path, get_insttype, dict_to_file, rewrite_time_reference_units
from dl_toolbox_runner.log import logger

class Configurator(object):
    """Class for setting up config file for usage in DL toolbox run

    The required information is taken from the filename and contents of the data file, from a main and an inst config

    Args:
        datafile: filename of the data file to be processed
        configfile: filename of the configuration file generated by running this class
    """

    def __init__(self, instrument_id, scan_type, datafile, configfile, main_config, file_default_config='dl_toolbox_runner/config/default_config.yaml',
                 file_conf_param_match='dl_toolbox_runner/config/conf_match.yaml'):

        self.conf = get_conf(abs_file_path(file_default_config))  # init with defaults from file, update later in code
        self.conf['NC_instrument_id'] = instrument_id
        self.conf['scan_type'] = scan_type
        self.datafile = datafile
        self.configfile = configfile
        self.main_config = main_config
        self.conf_param_match = get_conf(abs_file_path(file_conf_param_match))
        self.date = None  # datetime.datetime object for timesteamp of datafile

    def run(self):
        basename = self.main_config['input_file_prefix']+'XXXWL_'
        self.conf['inst_type'], self.date = get_insttype(self.datafile, base_filename=basename,
                                                         return_date=True)
        
        # Test if the instrument type corresponds to the one in the main config
        if self.conf['system'] != self.conf['inst_type']:
            logger.error(f"Configured instrument type ({self.conf['inst_type']}) does not match the one in the main config ({self.conf['system']})")
            raise MissingConfig("Configured instrument type does not match the one in the main config")
        
        self.from_datafile()
        self.to_file()
        logger.info('Config file for '+self.conf['NC_instrument_id']+f' written to {self.configfile}')

    def from_datafile(self):
        # Some parameters needs to be read in the filename / file
        ds = xr.open_dataset(self.datafile)
                
        if self.conf['inst_type'] == 'windcube':
            # From filename:
            self.conf['NC_L2_path'] = self.main_config['output_dir']
            self.conf['NC_L2_basename'] = self.main_config['output_file_prefix'] + self.conf['NC_instrument_id'] + '_'

            # General variables
            self.conf['system_longitude'] = float(ds.longitude.data)
            self.conf['system_latitude'] = float(ds.latitude.data)
            self.conf['system_altitude'] = float(ds.altitude.data)
            
            # From the Sweep group:
            try: 
                ds_sweep = xr.open_dataset(self.datafile, group=ds.sweep_group_name.data[0])
            except ValueError:
                ds_sweep = rewrite_time_reference_units(self.datafile, group_name=ds.sweep_group_name.data[0])
                #ds = xr.decode_cf(ds_sweep)
            except:
                logger.error("No valid time reference found in the file")
                
            self.conf['range_gate_lenth'] = float(ds_sweep.range_gate_length.data)
            self.conf['number_of_gates'] = len(ds_sweep.gate_index.data)
        
        elif self.conf['inst_type'] == 'halo':
            logger.error("Halo configuration not implemented yet")
            raise NotImplementedError("Halo configuration not implemented yet")
        pass
        

    def to_file(self):
        try:
            conf_out = {key_out: self.conf[key_in] for key_in, key_out in self.conf_param_match.items()}
        except KeyError as err:
            raise MissingConfig(f'missing entry for {err} in config dict')
        dict_to_file(conf_out, self.configfile, '=')


if __name__ == '__main__':
    datafile = abs_file_path('dl_toolbox_runner/data/input/DWL_raw_PAYWL_2023-01-01_00-06-12_dbs_303_50mTP.nc')
    configfile = abs_file_path('dl_toolbox_runner/data/toolbox/sample_config/PAYWL_DBS_TP.conf')
    defaultconfigfile = abs_file_path('dl_toolbox_runner/config/default_config_windcube.yaml')
    mainconfigfile = abs_file_path('dl_toolbox_runner/config/main_config.yaml')
    x = Configurator('PAYWL', 'DBS_TP',  datafile, configfile, main_config=get_conf(mainconfigfile), file_default_config=defaultconfigfile)
    x.run()
    pass
