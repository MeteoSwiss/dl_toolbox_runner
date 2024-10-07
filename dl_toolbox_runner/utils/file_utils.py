import os
from pathlib import Path
import warnings  # cannot import logger as it would create circular import with abs_file_path, hence use warnings here
import datetime
import re
import numpy as np
import pandas as pd
import xarray as xr
from hpl2netCDF_client.hpl_files.hpl_files import hpl_files

import dl_toolbox_runner
from dl_toolbox_runner.errors import FilenameError

def abs_file_path(*file_path):
    """
    Make a relative file_path absolute in respect to the dl_toolbox_runner project directory.
    Absolute paths wil not be changed
    """
    path = Path(*file_path)
    if path.is_absolute():
        return path
    return Path(dl_toolbox_runner.__file__).parent.parent / path

def get_home_dir():
    return Path.home()

def get_config_path(*dir_path):
    path = Path(*dir_path)
    if path.is_absolute():
        return path
    else:
        home_dir = get_home_dir()
        return home_dir / path

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
    # First check to avoid passing system data further:
    if os.path.splitext(files[0])[-1] in ['.csv', '.txt']:
        return 'system_data'
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

def open_sweep_group(filename, group_name):
    # From the Sweep group:
    try: 
        ds_sweep = xr.open_dataset(filename, group=group_name)
    except ValueError:
        ds_sweep = rewrite_time_reference_units(filename, group=group_name)
    except Exception as e:
        warnings.warn("No valid time reference found in the file")
    
    return ds_sweep
                
def rewrite_time_reference_units(filename, group='Sweep'):
    '''
    Rewrite the time reference of the dataset to the standard reference
    This is sometimes necessary as the time reference is not always correctly set, especially it seems that some files 
    have the time_reference variable in the group Sweep whereas some have it in the main group.
    '''
    # Open the ds without time decoding
    ds_recoded = xr.open_dataset(filename, group=group, decode_times=False)
    
    encoding = str(ds_recoded.time_reference.data)
    new_encoding = ds_recoded.time.units.replace('time_reference', encoding)
    
    new_time = [datetime.datetime.fromtimestamp(t) for t in ds_recoded.time.data]
    
    ds_recoded['time'] = new_time
    ds_recoded.time.encoding['units'] = new_encoding
    ds_recoded.time.encoding['calendar'] = 'standard'
    
    return ds_recoded

def round_datetime(dt, round_to_minutes=10):
    """Round a datetime object to the nearest minute"""
    return dt - datetime.timedelta(minutes=dt.minute % round_to_minutes, seconds=dt.second, microseconds=dt.microsecond)

def get_instrument_id_and_scan_type(filepath, inst_type, prefix):
    if inst_type == 'windcube':
        # find instrument_id, scan_type file_datetime and scan ID and resolution for a windcube file
        
        # Extract the filename from file (filepath) to avoid bug with the path (e.g. a DBS file in a VAD folder...)
        file = os.path.basename(filepath)
        
        file_components = file.split('_')
        resolution_part = file_components[-1].rsplit('.',1)[0]
        
        idx_id = file.find(prefix)+len(prefix)
        instrument_id = file[idx_id:idx_id+5]
                
        # scan type:
        if 'dbs' in file:
            scan_type = 'DBS'
        elif 'vad' in file:
            scan_type = 'VAD'
        elif 'fixed' in file:
            scan_type = 'FIXED_VAD'
        else:
            warnings("No valid scan type identified for:"+file)

        if 'TP' in resolution_part:
            scan_type = scan_type+'_TP'
                
        # Extract scan ID (3 digits part after the scan type):
        scan_id = int(file_components[-2])
        
        # scan resolution (the number before "m" in resolution_part) as an integer:
        scan_resolution = int(re.search(r'\d+', resolution_part).group())
        
        file_datestring = re.search(r'\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}', file)
        file_datetime =  datetime.datetime.strptime(file_datestring.group(), '%Y-%m-%d_%H-%M-%S')
          
        return instrument_id, scan_type, scan_id, scan_resolution, file_datetime
    elif inst_type == 'halo':
        # find instrument_id, scan_type file_datetime and scan ID and resolution for a halo file
        # Extract the filename from file (filepath) to avoid bug with the path (e.g. a DBS file in a VAD folder...)
        file = os.path.basename(filepath)
        
        file_components = file.split('_')
        
        idx_id = file.find(prefix)+len(prefix)
        instrument_id = file[idx_id:idx_id+5]
        
        scan_type = file_components[3]
        
        # Extract scan ID (Should be 3rd component of the filename):
        scan_id = file_components[4]
        
        # scan resolution (the number before "m" in resolution_part) as an integer:
        scan_resolution = None
        
        file_datestring = re.search(r'\d{4}\d{2}\d{2}_\d{2}\d{2}\d{2}', file)
        file_datetime =  datetime.datetime.strptime(file_datestring.group(), '%Y%m%d_%H%M%S')
          
        return instrument_id, scan_type, scan_id, scan_resolution, file_datetime
    else:
        raise ValueError("Instrument type: "+ inst_type +" not yet supported !")

def read_halo(filename):
    # This function is copy pasted from the DL_toolbox from M. Kayser
    # In principle we only need to read the header to get the information about the file and fill the config file
    # Check if filename is a string:
    if isinstance(filename, str):
        filename = Path(filename)
    if not filename.exists():
        print("Oops, file doesn't exist!")
    else:
        print('reading file: ' + filename.name)
        
    with filename.open() as infile:
        header_info = True
        mheader = {}
        for line in infile:
            if line.startswith("****"):
                header_info = False
                ## Adjust header in order to extract data formats more easily
                ## 1st for 'Data line 1' , i.e. time of beam etc.
                tmp = [x.split() for x in mheader['Data line 1'].split('  ')]
                if len(tmp) > 3:
                    tmp.append(" ".join([tmp[2][2],tmp[2][3]]))
                    tmp.append(" ".join([tmp[2][4],tmp[2][5]]))
                tmp[0] = " ".join(tmp[0])
                tmp[1] = " ".join(tmp[1])
                tmp[2] = " ".join([tmp[2][0],tmp[2][1]])
                mheader['Data line 1'] = tmp
                tmp = mheader['Data line 1 (format)'].split(',1x,')
                tmp.append(tmp[-1])
                tmp.append(tmp[-1])
                mheader['Data line 1 (format)'] = tmp
                ## Adjust header in order to extract data formats more easily
                ## 2st for 'Data line 2' , i.e. actual data
                tmp = [x.split() for x in mheader['Data line 2'].split('  ')]
                tmp[0] = " ".join(tmp[0])
                tmp[1] = " ".join(tmp[1])
                tmp[2] = " ".join(tmp[2])
                tmp[3] = " ".join(tmp[3])
                mheader['Data line 2'] = tmp
                tmp = mheader['Data line 2 (format)'].split(',1x,')
                mheader['Data line 2 (format)'] = tmp
                ## start counter for time and range gates
                counter_jj = 0
                continue # stop the loop and continue with the next line

            tmp = hpl_files.switch(header_info,line)
            ## this temporary variable indicates whether the a given data line includes
            # the spectral width or not, so 2d information can be distinguished from
            # 1d information.
            indicator = len(line[:10].split())

            if header_info == True:
                try:
                    if tmp[0][0:1] == 'i':
                        tmp_tmp = {'Data line 2 (format)': tmp[0]}
                    else:
                        tmp_tmp = {tmp[0]: tmp[1]}
                except:
                    if tmp[0][0] == 'f':
                        tmp_tmp = {'Data line 1 (format)': tmp[0]}
                    else:
                        tmp_tmp = {'blank': 'nothing'}
                mheader.update(tmp_tmp)
            elif (header_info == False):
                if (counter_jj == 0):
                    n_o_rays = (len(filename.open().read().splitlines())-17)//(int(mheader['Number of gates'])+1)
                    mbeam = np.recarray((n_o_rays,),
                                        dtype=np.dtype([('time', 'f8')
                                            , ('azimuth', 'f4')
                                            ,('elevation','f4')
                                            ,('pitch','f4')
                                            ,('roll','f4')]))
                    mdata = np.recarray((n_o_rays,int(mheader['Number of gates'])),
                                        dtype=np.dtype([('range gate', 'i2')
                                                ,('velocity', 'f4')
                                                ,('snrp1','f4')
                                                ,('beta','f4')
                                                ,('dels', 'f4')]))
                    mdata[:, :] = np.full(mdata.shape, -999.)

                # store tmp in time array
                if  (indicator==1):
                    dt=np.dtype([('time', 'f8'), ('azimuth', 'f4'),('elevation','f4'),('pitch','f4'),('roll','f4')])
                    if len(tmp) < 4:
                        tmp.extend(['-999']*2)
                    if counter_jj < n_o_rays:
                        mbeam[counter_jj] = np.array(tuple(tmp), dtype=dt)
                        counter_jj = counter_jj+1
                # store tmp in range gate array        
                elif (indicator==2):
                    dt=np.dtype([('range gate', 'i2')
                                , ('velocity', 'f4')
                                ,('snrp1','f4')
                                ,('beta','f4')
                                ,('dels', 'f4')])
                    ii_index = np.array(tmp[0], dtype=dt[0])

                    if (len(tmp) == 4):
                        tmp.append('-999')
                        mdata[counter_jj-1, ii_index] = np.array(tuple(tmp), dtype=dt)
                    elif (len(tmp) == 5):
                        mdata[counter_jj-1, ii_index] = np.array(tuple(tmp), dtype=dt)
    
    #set time information
    time_tmp= pd.to_numeric(pd.to_timedelta(pd.DataFrame(mbeam)['time'], unit = 'h')
                        +pd.to_datetime(datetime.datetime.strptime(mheader['Start time'], '%Y%m%d %H:%M:%S.%f').date())
                    ).values / 10**9
    time_ds= [ x+(datetime.timedelta(days=1)).total_seconds()
            if time_tmp[0]-x>0 else x
            for x in time_tmp
            ]
    return mheader, pd.to_timedelta(pd.DataFrame(mbeam)['time'], unit = 'h') +pd.to_datetime(datetime.datetime.strptime(mheader['Start time'], '%Y%m%d %H:%M:%S.%f').date())#, mbeam #, mdata, time_ds

def create_batch(file_dict, retrieval_start_time, retrieval_end_time):
    ''''
    Function to create a batch from a file dictionary
    '''
    batch = {
        'files': [file_dict['file']],
        'instrument_id': file_dict['instrument_id'],
        'scan_type': file_dict['scan_type'],
        'scan_id':  file_dict['scan_id'],
        'scan_resolution': file_dict['scan_resolution'],
        'batch_start_time': file_dict['file_start_time'],
        'batch_end_time':  file_dict['file_end_time'],
        'batch_length_sec':  file_dict['file_length'],
        'retrieval_start_time': retrieval_start_time,
        'retrieval_end_time': retrieval_end_time,
        'batch_creation_time': datetime.datetime.now()
    }
    return batch

def find_file_time_windcube(filename):
    '''
    Function to extract the start and end from the file content
    '''
    try:
        ds = xr.open_dataset(filename)
    except:
        print("Could not open file: "+filename)
        return None, None
        
    group_name = ds.sweep_group_name.data[0]
                    
    ds_sweep = open_sweep_group(filename, group_name)
    start_time = pd.to_datetime(ds_sweep.time.data[0])
    end_time = pd.to_datetime(ds_sweep.time.data[-1])
    return start_time, end_time

def read_system_data(filename):
    '''
    Function to read system or environmental data from E-Profile DWLs
    '''
    if not os.path.exists(filename):
        print("File does not exist: "+filename)
        return None
    else:
        print("Reading file: "+filename)
        # Check extension to define the reading method
        if os.path.splitext(filename)[-1] == '.csv':
            try:
                df = pd.read_csv(filename, delimiter=';', header=None)
            except:
                print("Could not read file: "+filename)
                return None
        elif os.path.splitext(filename)[-1] == '.txt':
            try:
                df = pd.read_table(filename, header=None)
            except:
                print("Could not read file: "+filename)
                return None
        else:
            print("File extension not supported: "+filename)
            return None
    return df

if __name__ == '__main__':
    inst_type = get_insttype(abs_file_path('dl_toolbox_runner/data/input/DWL_raw_PAYWL_2023-01-01_00-00-59_dbs_303_50mTP.nc'))
    print(inst_type)
    