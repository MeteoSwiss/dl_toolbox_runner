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
defaultconfigfile = abs_file_path('dl_toolbox_runner/config/default_config_windcube.yaml')
mainconfigfile = abs_file_path('dl_toolbox_runner/config/main_config.yaml')


class TestConf(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.mkdir(outdir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(outdir)

    def test_full(self):
        """Test for the Configurator class"""

        x = Configurator('PAYWL', 'DBS_TP',  datafile, configfile, main_config=get_conf(mainconfigfile), file_default_config=defaultconfigfile)
        x.run()

    # TODO: test outputfile against a reference file
    # TODO: test different situations
    
    def test_GetConf(self):
        """Test for the get_conf function"""
        conf = get_conf(abs_file_path('dl_toolbox_runner/config/default_config_windcube.yaml'))
        self.assertIsInstance(conf, dict)
        self.assertEqual(conf['system'], 'windcube')