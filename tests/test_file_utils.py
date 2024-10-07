import unittest

import pandas as pd
import xarray as xr

from dl_toolbox_runner.errors import FilenameError
from dl_toolbox_runner.utils.file_utils import abs_file_path, get_insttype, rewrite_time_reference_units, read_system_data


class TestFileUtils(unittest.TestCase):

    def test_get_insttype(self):
        """Test for get_insttype function"""
        testfile_wc = 'dl_toolbox_runner/data/input/DWL_raw_PAYWL_2023-01-01_00-06-12_dbs_303_50mTP.nc'
        self.assertEqual(get_insttype(testfile_wc), 'windcube')
        self.assertEqual(get_insttype(abs_file_path(testfile_wc)), 'windcube')
        
        testfile_halo = 'DWL_raw_LINWL_User1_142_20110108_160345.hpl'
        self.assertEqual(get_insttype(testfile_halo), 'halo')

        # System and environmental data:
        testfile_system_wc = 'dl_toolbox_runner/data/input/DWL_raw_CABWL_environmental_data_2024-10-07_10-00-00.csv'
        self.assertEqual(get_insttype(testfile_system_wc), 'system_data')
        
        
        testfile_system_halo = 'dl_toolbox_runner/data/input/DWL_raw_IAOWL_system_parameters_142_202410.txt'
        self.assertEqual(get_insttype(testfile_system_halo), 'system_data')
        
        self.assertRaises(FilenameError, get_insttype, 'DWL_raw_LINWL_User1_142_20110108_160345.abc')
        
    def test_rewrite_time_reference_units(self):
        """Test for rewrite_time_reference_units function"""
        testfile = abs_file_path('dl_toolbox_runner/data/input/DWL_raw_SHAWL_2024-07-10_12-11-42_dbs_34_50m.nc')
        ds = rewrite_time_reference_units(testfile, group='Sweep_125742')
        self.assertIsInstance(ds, xr.Dataset)
        
    def test_read_system_data(self):
        testfile_system_wc = 'dl_toolbox_runner/data/input/DWL_raw_CABWL_environmental_data_2024-10-07_10-00-00.csv'
        df_wc = read_system_data(testfile_system_wc)
        self.assertIsInstance(df_wc, pd.DataFrame)
        
        testfile_system_halo = 'dl_toolbox_runner/data/input/DWL_raw_IAOWL_system_parameters_142_202410.txt'
        df_halo = read_system_data(testfile_system_halo)
        self.assertIsInstance(df_halo, pd.DataFrame)
        