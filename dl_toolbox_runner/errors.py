class DLError(Exception):
    """Base exception for DL"""


###############################
class DLFileError(DLError):
    """Base exception for problems with files used by dl_toolbox or dl_toolbox_runner"""


class DLInputError(DLError):
    """Base exception for calling of DL functions"""


class DLDataError(DLError):
    """Raised if something with the input data goes wrong"""


class DLConfigError(DLError):
    """Raised if something with the configuration file is wrong"""


class DLTestError(DLError):
    """Raised if something goes wrong during set up or clean up of testing"""


###############################
class MissingConfig(DLConfigError):
    """Raised if a mandatory entry of the config file is missing"""


###############################
class MissingDataError(DLDataError):
    """Raised if some expected data is not present"""


###############################
class FilenameError(DLFileError):
    """Raised if the filename does not correspond to the expected pattern"""

