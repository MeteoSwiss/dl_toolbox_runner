import unittest

import xarray as xr

from dl_toolbox_runner.errors import FilenameError
from dl_toolbox_runner.configure import Configurator, get_conf
from dl_toolbox_runner.utils.file_utils import abs_file_path


class TestConfigure(unittest.TestCase):
    
    def test_GetConf(self):
        """Test for the get_conf function"""
        conf = get_conf(abs_file_path('dl_toolbox_runner/config/default_config_windcube.yaml'))
        self.assertIsInstance(conf, dict)
        self.assertEqual(conf['system'], 'windcube')

    def test_Configurator(self):
        """Test for the Configurator class"""
        datafile = abs_file_path('dl_toolbox_runner/data/input/DWL_raw_PAYWL_2023-01-01_00-06-12_dbs_303_50mTP.nc')
        configfile = abs_file_path('dl_toolbox_runner/data/toolbox/sample_config/PAYWL_DBS_TP.conf')
        defaultconfigfile = abs_file_path('dl_toolbox_runner/config/default_config_windcube.yaml')
        mainconfigfile = abs_file_path('dl_toolbox_runner/config/main_config.yaml')
        x = Configurator('PAYWL', 'DBS_TP',  datafile, configfile, main_config=get_conf(mainconfigfile), file_default_config=defaultconfigfile)
        x.run()
        pass