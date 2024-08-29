import unittest

import xarray as xr

from dl_toolbox_runner.errors import FilenameError
from dl_toolbox_runner.utils.file_utils import abs_file_path, get_insttype, rewrite_time_reference_units


class TestFileUtils(unittest.TestCase):

    def test_get_insttype(self):
        """Test for get_insttype function"""
        testfile_wc = 'dl_toolbox_runner/data/input/DWL_raw_PAYWL_2023-01-01_00-06-12_dbs_303_50mTP.nc'
        self.assertEqual(get_insttype(testfile_wc), 'windcube')
        self.assertEqual(get_insttype(abs_file_path(testfile_wc)), 'windcube')

        testfile_halo = 'DWL_raw_LINWL_User1_142_20110108_160345.hpl'
        self.assertEqual(get_insttype(testfile_halo), 'halo')

        self.assertRaises(FilenameError, get_insttype, 'DWL_raw_LINWL_User1_142_20110108_160345.abc') 
        
    def test_rewrite_time_reference_units(self):
        """Test for rewrite_time_reference_units function"""
        testfile = abs_file_path('dl_toolbox_runner/data/input/DWL_raw_SHAWL_2024-07-10_12-11-42_dbs_34_50m.nc')
        ds = rewrite_time_reference_units(testfile, group='Sweep_125742')
        self.assertIsInstance(ds, xr.Dataset)