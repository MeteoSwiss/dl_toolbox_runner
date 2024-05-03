import os
import shutil
import unittest

from dl_toolbox_runner.configure import Configurator
from dl_toolbox_runner.utils.file_utils import abs_file_path

datafile = abs_file_path('dl_toolbox_runner/data/input/DWL_raw_PAYWL_2023-01-01_00-06-12_dbs_303_50mTP.nc')
# TODO: potentially use data file present under test folder (once test_retrieval.py has been improved)
outdir = abs_file_path('tests/tmp_test_configurator')
configfile = os.path.join(outdir, 'PAYWL_conftest.conf')


class TestConf(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.mkdir(outdir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(outdir)

    def test_full(self):
        """test for setting up the configuraiton file for the DL-toolbox run"""
        x = Configurator(datafile, configfile)
        x.run()

    # TODO: test outputfile against a reference file
    # TODO: test different situations
