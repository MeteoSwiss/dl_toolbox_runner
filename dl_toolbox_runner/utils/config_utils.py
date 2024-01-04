import logging
import yaml

from dl_toolbox_runner.errors import MissingConfig, DLConfigError
from dl_toolbox_runner.utils.file_utils import abs_file_path


def get_conf(file):
    """get conf dictionary from yaml files. Don't do any checks on contents"""
    with open(file) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    return conf


def check_conf(conf, mandatory_keys, miss_description):
    """check for mandatory keys of conf dictionary

    if key is missing raises MissingConfig('xxx is a mandatory key ' + miss_description)
    """
    for key in mandatory_keys:
        if key not in conf:
            err_msg = ("'{}' is a mandatory key {}".format(key, miss_description))
            raise MissingConfig(err_msg)


def to_abspath(conf, keys):
    """transform paths corresponding to keys in conf dictionary to absolute paths and return conf dict"""
    for key in keys:
        conf[key] = abs_file_path(conf[key])
    return conf


def get_main_config(file):
    """get main configuration and check for completeness of config file"""

    mandatory_keys = ['max_age', 'output_dir', 'output_file_prefix', 'input_dir', 'input_file_prefix',
                      'toolbox_confdir']
    paths = ['output_dir', 'input_dir', 'toolbox_confdir']

    conf = get_conf(file)
    check_conf(conf, mandatory_keys,
               'of main config files but is missing in {}'.format(file))
    conf = to_abspath(conf, paths)

    return conf


def get_log_config(file):
    """get configuration for logger and check for completeness of config file"""

    mandatory_keys = ['logger_name', 'loglevel_stdout', 'write_logfile']
    mandatory_keys_file = ['logfile_path', 'logfile_basename', 'logfile_ext', 'logfile_timestamp_format',
                           'loglevel_file']

    conf = get_conf(file)
    check_conf(conf, mandatory_keys,
               'of logs config files but is missing in {}'.format(file))
    if conf['write_logfile']:
        check_conf(conf, mandatory_keys_file,
                   "of logs config files if 'write_logfile' is True, but is missing in {}".format(file))

    conf = interpret_loglevel(conf)

    return conf


def interpret_loglevel(conf):
    """helper function to replace logs level strings in logs level of logging library"""

    pattern = 'loglevel'

    level_keys = [s for s in conf.keys() if pattern in s]
    for level_key in level_keys:
        conf[level_key] = getattr(logging, conf[level_key].upper(), None)
        if not isinstance(conf[level_key], int):
            raise DLConfigError("value of '{}' in logs config does not correspond to any known logs level of logging"
                                 .format(level_key))

    return conf
