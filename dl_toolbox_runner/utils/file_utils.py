import os
from pathlib import Path
import warnings  # cannot import logger as it would create circular import with abs_file_path, hence use warnings here

import numpy as np
import xarray as xr
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


def dict_to_file(data, file, sep, header=None, remove_brackets=False, remove_parentheses=False, remove_braces=False):
    """write dictionary contents to a file. One item per line matching keys and values using 'sep'.

    Args:
        data: dictionary to write to file in question. Numpy 1d-arrays as values are ok, matrices not
        file: output file incl. path and extension
        sep: separator sign between key and value as string. Can include whitespaces around separator.
        header: header string to write to the head of the file before the first dictionary item. Defaults to None
        remove_brackets (optional): Remove square brackets [ and ], e.g. from lists, while printing to file.
            Defaults to False
        remove_parentheses (optional): Remove parentheses ( and ), e.g. from tuples, while printing to file.
            Defaults to False
        remove_braces (optional): Remove curly braces { and } while printing to file. Defaults to False
    """

    with open(file, 'w') as f:
        if header is not None:
            f.write(header + '\n')
        for key, val in data.items():
            if isinstance(val, np.ndarray):  # ensure numpy arrays are printed with elements separated by commas
                val = list(val)
            val = str(val)
            if remove_brackets:
                val = val.replace('[', '').replace(']', '')
            if remove_parentheses:
                val = val.replace('(', '').replace(')', '')
            if remove_braces:
                val = val.replace('{', '').replace('}', '')
            f.write(sep.join([key, val]) + '\n')


def get_insttype(filename, base_filename='DWL_raw_XXXWL_', return_date=False):
    inst_types_exts = {'windcube': ['.nc'], 'halo': ['.hpl']}  # rely on preserved order of dict (>= python 3.6)

    files = [os.path.basename(filename)]
    for tried_type, exts in inst_types_exts.items():
        try:
            x = hpl_files.filelist_to_hpl_files(files, tried_type, base_filename)
        except ValueError:
            pass
        else:
            if os.path.splitext(files[0])[-1] in exts:
                if return_date:
                    return tried_type, x.time[0]  # filelist 'files' always has one single element in this function
                else:
                    return tried_type
            else:
                warnings.warn(f"filename would match pattern for '{tried_type}', but extension is not one of the "
                              f"expected ({exts}). Will continue testing other instrument types.")

    # if above return statement was never reached it means that filename pattern does not correspond to known inst type
    msg = f'filename pattern does not correspond to any of the known instrument types ({list(inst_types_exts.keys())})'
    raise FilenameError(msg)

def rewrite_time_reference_units(filename, group_name='Sweep'):
    '''
    Rewrite the time reference of the dataset to the standard reference
    This is sometimes necessary as the time reference is not always correctly set, especially it seems that some files 
    have the time_reference variable in the group Sweep whereas some have it in the main group.
    '''
    # Open the ds without time decoding
    ds = xr.open_dataset(filename, group=group_name, decode_times=False)
    
    encoding = str(ds.time_reference.data)
    new_encoding = ds.time.units.replace('time_reference', encoding)
    ds.time.encoding['units'] = new_encoding

    #ds_decoded = xr.decode_cf(ds)
    return ds
    

if __name__ == '__main__':
    inst_type = get_insttype(abs_file_path('dl_toolbox_runner/data/input/DWL_raw_PAYWL_2023-01-01_00-00-59_dbs_303_50mTP.nc'))
    print(inst_type)