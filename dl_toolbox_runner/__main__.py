import argparse
import datetime

import dl_toolbox_runner
from dl_toolbox_runner.main import Runner
from dl_toolbox_runner.utils.file_utils import abs_file_path, round_datetime

def main():
    """command line entry point for the dl_toolbox_runner package. Type 'python3 -m dl_toolbox_runner -h' for more info"""
    # instantiate parser
    parser = argparse.ArgumentParser(prog='python3 -m {}'.format(dl_toolbox_runner.__name__),
                                     description='Transform MWR native files to (E-PROFILE) NetCDF format.')

    # add arguments
    parser.add_argument('main_conf',
                        help='path to the main configuration file'
                             ' (absolute or relative to dl_toolbox_runner project dir)'
                             ' (e.g. dl_toolbox_runner/config/main_config.yaml))')
    parser.add_argument('--conf_match',
                        help='path to the configuration file specifying the matching of parameters between input and output'
                             ' (absolute or relative to dl_toolbox_runner project dir)')
    parser.add_argument('--conf_log',
                        help='path to the configuration file specifying the logging'
                             ' (absolute or relative to dl_toolbox_runner project dir)')
    parser.add_argument('--dry_run', action='store_true',
                        help='run the script without actually processing any files. Only write config files')
    parser.add_argument('--single_process', action='store_true',
                        help='to process each file separately. Default is to group files with same instrument_id and scan_type')
    parser.add_argument('--round_to_minutes', nargs='?', type=int, default=10,
                        help='round the end_time to the nearest x minute. Note that this is NOT the averaging time which is defined in the main config file.')
    parser.add_argument('--instrument_id', type=str, default=None,
                        help='instrument_id to process (e.g. "PAYWL")')
    
    args = parser.parse_args()

    # interpret arguments and run dl_toolbox_runner
    # Not needed as such, but could be useful to have a dict of arguments to pass to the Runner
    kwargs = {'main_conf': abs_file_path(args.main_conf)}  # dict matching keyword of main.run with its value
    if args.conf_match:
        kwargs['conf_match'] = args.conf_match
    if args.conf_log:
        kwargs['conf_log'] = args.conf_log
    
    # Some argument are set by default
    kwargs['single_process'] = args.single_process
    kwargs['dry_run'] = args.dry_run
    kwargs['round_to_minutes'] = args.round_to_minutes
    kwargs['instrument_id'] = args.instrument_id

    # Initialize the Runner
    x = Runner(kwargs['main_conf'], single_process=kwargs['single_process'])
    
    # Find the latest "round" time (e.g. 13:00, 13:10, 13:20, 13:30) and use this as date_end
    date_end = round_datetime(datetime.datetime.now() - datetime.timedelta(minutes=10), round_to_minutes=kwargs['round_to_minutes'])
    
    # Run the wind retrievals
    x.run(dry_run=kwargs['dry_run'], date_end=date_end, instrument_id= kwargs['instrument_id'])

if __name__ == '__main__':
    main()
    