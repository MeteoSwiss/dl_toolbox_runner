import os
from pathlib import Path
import warnings  # cannot import logger as it would create circular import with abs_file_path, hence use warnings here

from hpl2netCDF_client.hpl_files.hpl_files import hpl_files

import dl_toolbox_runner
from dl_toolbox_runner.errors import FilenameError


def abs_file_path(*file_path):
    """
    Make a relative file_path absolute in respect to the mwr_l12l2 project directory.
    Absolute paths wil not be changed
    """
    path = Path(*file_path)
    if path.is_absolute():
        return path
    return Path(dl_toolbox_runner.__file__).parent.parent / path


def get_insttype(filename, base_filename='DWL_raw_XXXWL_'):
    inst_types_exts = {'windcube': ['.nc'], 'halo': ['.hpl']}  # rely on preserved order of dict (>= python 3.6)

    files = [os.path.basename(filename)]
    for tried_type, exts in inst_types_exts.items():
        try:
            hpl_files.filelist_to_hpl_files(files, tried_type, base_filename)
        except ValueError:
            pass
        else:
            if os.path.splitext(files[0])[-1] in exts:
                return tried_type
            else:
                warnings.warn(f"filename would match pattern for '{tried_type}', but extension is not one of the "
                              f"expected ({exts}). Will continue testing other instrument types.")

    # if above return statement was never reached it means that filename pattern does not correspond to known inst type
    msg = f'filename pattern does not correspond to any of the known instrument types ({list(inst_types_exts.keys())})'
    raise FilenameError(msg)


if __name__ == '__main__':
    inst_type = get_insttype(abs_file_path('dl_toolbox_runner/data/input/DWL_raw_PAYWL_2023-01-01_00-00-59_dbs_303_50mTP.hpl'))
    print(inst_type)
