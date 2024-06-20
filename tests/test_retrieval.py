import unittest
import os

from dl_toolbox_runner.main import Runner
from dl_toolbox_runner.utils.file_utils import abs_file_path


class TestRetrieval(unittest.TestCase):
    # TODO: set up SetUp(Class) and TearDown(Class) methods similarly to mwr_raw2l1
    # TODO: isolate test config and data from examples at root of repo where necessary

    def test_full(self):
        """integration test for sample retrieval with DL toolbox"""
        x = Runner(abs_file_path('dl_toolbox_runner/config/config_test.yaml'),  single_process=True)
        x.run(instrument_id='PAYWL')

        # Check if output file has been written:
        assert os.path.isfile(os.path.join(x.conf['output_dir'], (x.conf['output_file_prefix'] + 'PAYWL_202301010006.nc')))

    # TODO: test outputfile against a reference file
    # TODO: test different situations
