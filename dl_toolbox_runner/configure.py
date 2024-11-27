import xarray as xr
import numpy as np 
import pandas as pd

from dl_toolbox_runner.errors import MissingConfig
from dl_toolbox_runner.utils.config_utils import get_conf
from dl_toolbox_runner.utils.file_utils import abs_file_path, get_config_path, get_insttype, dict_to_file, open_sweep_group, read_halo
from dl_toolbox_runner.log import logger

from hpl2netCDF_client.hpl_files.hpl_files import hpl_files

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
        self.instrument_id = instrument_id
        self.scan_type = scan_type
        self.datafile = datafile
        self.configfile = configfile
        self.main_config = main_config
        self.conf_param_match = get_conf(abs_file_path(file_conf_param_match))
        self.date = None  # datetime.datetime object for timesteamp of datafile

    def run(self):
        logger.info('Treating: '+self.instrument_id)
        basename = self.main_config['input_file_prefix']+'XXXWL_'
        instrument_type, self.date = get_insttype(self.datafile, base_filename=basename,
                                                         return_date=True)
        
        # get config file corresponding to instrument type
        config_filepath = get_config_path(self.main_config['inst_config_dir'] + self.main_config['inst_config_file_prefix'] + instrument_type + '.yaml')
        self.conf = get_conf(config_filepath)
        
        # Test if the instrument type corresponds to the one in the main config
        if self.conf['system'] != instrument_type:
            logger.error(f"Configured instrument type ({instrument_type}) does not match the one in the main config ({self.conf['system']})")
            raise MissingConfig("Configured instrument type does not match the one in the main config")

        logger.info(f'Configuring {instrument_type} with scan type {self.scan_type}')
        logger.info(f'Read config file: {config_filepath}')
        self.conf['inst_type'] = instrument_type
        self.conf['scan_type'] = self.scan_type
        self.conf['NC_instrument_id'] = self.instrument_id

        try:
            self.from_datafile()
        except Exception as e:
            logger.error(f"Error during configuration reading: {e}")
            raise MissingConfig(f"Error during configuration reading: {e}")
        try:
            self.to_file()
        except Exception as e:
            logger.error(f"Error during configuration writing: {e}")
            raise MissingConfig(f"Error during configuration writing: {e}")
        logger.info('Config file for '+self.conf['NC_instrument_id']+f' written to {self.configfile}')

    def from_datafile(self):
        '''
        Extracting configurations from the raw file
        '''              
        if self.conf['inst_type'] == 'windcube':      
            # Some parameters needs to be read in the filename / file
            ds = xr.open_dataset(self.datafile)      
            
            # From filename:
            self.conf['NC_L2_path'] = self.main_config['output_dir']
            self.conf['NC_L2_basename'] = self.main_config['output_file_prefix'] + self.conf['NC_instrument_id'] + '_'

            # General variables
            self.conf['system_longitude'] = float(ds.longitude.data)
            self.conf['system_latitude'] = float(ds.latitude.data)
            #self.conf['system_altitude'] =  float(ds.altitude.data)
            
            # Reading the altitude of the instrument from config file as this is not (always) present in the raw file
            logger.warning("Altitude not read from file but from csv file")
            # if the altitude is not given in the file, we read the altitude from the site in the csv config file
            dl_list_filename = get_config_path(self.main_config['inst_config_dir'] + self.main_config['dl_list_filename'])
            # read altitude from csv file
            dl_list = pd.read_csv(dl_list_filename)
            dl = dl_list[dl_list['identifier'] == self.instrument_id]
            self.conf['system_altitude'] = dl.altitude.values[0]
            
            year = dl.year.values[0]
            
            # Name of the sweep group (always different in Windcube files)
            group_name = ds.sweep_group_name.data[0]
            
            ds_sweep = open_sweep_group(self.datafile, group_name)
            
            self.conf['range_gate_length'] = float(ds_sweep.range_gate_length.data)
            self.conf['number_of_gates'] = len(ds_sweep.gate_index.data)
            self.conf['accumulation_time'] = 1e-3*float(ds_sweep.ray_accumulation_time.data)
            
            # Try to extract number of direction from raw file directly:
            try:
                # Sould be the number of unique azimuth values in the sweep group NOT considering vertical beam
                # We need to round these value a little to avoid float comparison issues
                rounded_directions = np.round(ds_sweep.azimuth.data, 1)
                self.conf['number_of_direction'] = len(np.unique(rounded_directions))
                # Check whether 0 and 360 are both present -> remove 1
                # The toolbox work with bins so it will consider both 0 and 360 as same angle
                if 0 in rounded_directions and 360 in rounded_directions:
                    self.conf['number_of_direction'] = len(np.unique(rounded_directions)) - 1
                                    
                logger.info(f"Number of directions found in raw file: {self.conf['number_of_direction']}")
            except Exception as e:
                logger.error(f"Error during number of directions extraction: {e}")
                raise MissingConfig(f"Error during number of directions extraction: {e}")
            
            # Some parameters are determined by the scan type, by the range_gate_length or the year of installation !
            # Values are taken from VM_DL_toolbox report
            if self.conf['range_gate_length'] == 25:
                if 'TP' in self.scan_type:
                    self.conf['puls_repetition_freq'] = 15e3
                    self.conf['puls_duration'] = 120e-9
                else:
                    if year < 2022:
                        self.conf['puls_repetition_freq'] = 40e3
                        self.conf['puls_duration'] = 120e-9
                    else:
                        self.conf['puls_repetition_freq'] = 23e3
                        self.conf['puls_duration'] = 120e-9            
            elif self.conf['range_gate_length'] == 50:
                if 'TP' in self.scan_type:
                    self.conf['puls_repetition_freq'] = 10e3
                    self.conf['puls_duration'] = 200e-9
                else:
                    if year < 2022:
                        self.conf['puls_repetition_freq'] = 20e3
                        self.conf['puls_duration'] = 230e-9
                    else:
                        self.conf['puls_repetition_freq'] = 18.5e3
                        self.conf['puls_duration'] = 200e-9       
            elif self.conf['range_gate_length'] == 75:
                if 'TP' in self.scan_type:
                    self.conf['puls_repetition_freq'] = 10e3
                    self.conf['puls_duration'] = 300e-9
                else:
                    if year < 2022:
                        self.conf['puls_repetition_freq'] = 10e3
                        self.conf['puls_duration'] = 440e-9
                    else:
                        self.conf['puls_repetition_freq'] = 15e3
                        self.conf['puls_duration'] = 300e-9   
            elif self.conf['range_gate_length'] == 100:
                if 'TP' in self.scan_type:
                    logger.error("No TP mode known for 100m range gate")
                    raise NotImplementedError("No TP mode known for 100m range gate")
                else:
                    if year < 2022:
                        self.conf['puls_repetition_freq'] = 10e3
                        self.conf['puls_duration'] = 440e-9
                    else:
                        self.conf['puls_repetition_freq'] = 12.5e3
                        self.conf['puls_duration'] = 410e-9   
            else:
                logger.error("Range gate length not implemented")
                raise NotImplementedError("Range gate length not implemented")
            self.conf['pulses_per_direction'] = int(self.conf['accumulation_time'] * self.conf['puls_repetition_freq'])
            self.conf['number_of_gate_points'] = 16 #2*self.conf['range_gate_length'] / self.conf['system_wavelength']
        elif self.conf['inst_type'] == 'halo':
            # For HALO instruments
            # From filename:
            self.conf['NC_L2_path'] = self.main_config['output_dir']
            self.conf['NC_L2_basename'] = self.main_config['output_file_prefix'] + self.conf['NC_instrument_id'] + '_'
           
            # For halo instrument, lon-lat-alt are not in the file
            logger.warning("Lon-Lat-Alt not read from file but from csv file")
            # if the altitude is not given in the file, we read the altitude from the site in the csv config file
            dl_list_filename = get_config_path(self.main_config['inst_config_dir'] + self.main_config['dl_list_filename'])
            # read altitude from csv file
            dl_list = pd.read_csv(dl_list_filename)
            dl = dl_list[dl_list['identifier'] == self.instrument_id]
            self.conf['system_longitude'] = dl.longitude.values[0]
            self.conf['system_latitude'] = dl.latitude.values[0]
            self.conf['system_altitude'] = dl.altitude.values[0]

            # Some parameters needs to be read in the filename / file
            mheader, time_ds = read_halo(abs_file_path(self.datafile))
                            
            self.conf['range_gate_length'] = float(mheader['Range gate length (m)'])
            self.conf['number_of_gates'] = int(mheader['Number of gates'])
            #self.conf['accumulation_time'] = 1e-3*float(ds_sweep.ray_accumulation_time.data)

            # Some parameters are determined by the scan type or by the range_gate_length
            self.conf['pulses_per_direction'] = int(mheader['Pulses/ray'])
            
            self.conf['number_of_gate_points'] = int(mheader['Gate length (pts)'])
            #self.conf['focus'] = mheader['Focus range']
            self.conf['velocity_resolution'] = float(mheader['Resolution (m/s)'])
            
            try:
                self.conf['number_of_direction'] = int(mheader['No. of rays in file'])
                logger.info(f"Number of directions found in raw file: {self.conf['number_of_direction']}")
            except Exception as e:
                logger.warning(f"Error during number of directions extraction: {e}")
                logger.info("Keeping defaults for number of directions")
                
            # Not sure where to find the accumulation time for Halo instrument ?
            self.conf['puls_repetition_freq'] = 10e3 #self.conf['pulses_per_direction'] / int(self.conf['accumulation_time'])
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
