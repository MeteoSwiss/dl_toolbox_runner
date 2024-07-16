import os
import shutil
import unittest

import xarray as xr

from dl_toolbox_runner.errors import FilenameError
from dl_toolbox_runner.configure import Configurator, get_conf
from dl_toolbox_runner.utils.file_utils import abs_file_path

datafile = abs_file_path('dl_toolbox_runner/data/input/DWL_raw_PAYWL_2023-01-01_00-06-12_dbs_303_50mTP.nc')
# TODO: potentially use data file present under test folder (once test_retrieval.py has been improved)
outdir = abs_file_path('tests/tmp_test_configurator')
#configfile = os.path.join(outdir, 'PAYWL_conftest.conf')

configfile = abs_file_path('dl_toolbox_runner/data/toolbox/sample_config/PAYWL_DBS_TP.conf')
mainconfigfile = abs_file_path('tests/config/config_test.yaml')
default_configfilename = 'default_config_windcube.yaml'

class TestConf(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.mkdir(outdir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(outdir)

    def test_full(self):
        """Test for the Configurator class"""
        main_config=get_conf(mainconfigfile)
        x = Configurator('PAYWL', 'DBS_TP',  datafile, configfile, main_config, file_default_config=main_config['inst_config_dir']+default_configfilename)
        x.run()

    # TODO: test outputfile against a reference file
    # TODO: test different situations
    
    def test_GetConf(self):
        """Test for the get_conf function"""
        main_config=get_conf(mainconfigfile)
        conf = get_conf(main_config['inst_config_dir']+default_configfilename)
        self.assertIsInstance(conf, dict)
        self.assertEqual(conf['system'], 'windcube')