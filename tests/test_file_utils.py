import unittest

from dl_toolbox_runner.errors import FilenameError
from dl_toolbox_runner.utils.file_utils import abs_file_path, get_insttype


class TestFileUtils(unittest.TestCase):

    def test_get_insttype(self):
        """Test for get_insttype function"""
        testfile_wc = 'dl_toolbox_runner/data/input/DWL_raw_PAYWL_2023-01-01_00-06-12_dbs_303_50mTP.nc'
        self.assertEqual(get_insttype(testfile_wc), 'windcube')
        self.assertEqual(get_insttype(abs_file_path(testfile_wc)), 'windcube')

        testfile_halo = 'DWL_raw_LINWL_20110108_160345.hpl'
        self.assertEqual(get_insttype(testfile_halo), 'halo')

        self.assertRaises(FilenameError, get_insttype, 'DWL_raw_LINWL_20110108_160345.abc')
