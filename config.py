import os, sys
from configparser import ConfigParser

dirsep = os.path.sep
folder = sys.path[0] + dirsep
configfile = folder + '..' + dirsep + 'freqanal.config'

parser = ConfigParser()

def read_config(section):
    parser.read(configfile)
    configPart = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            configPart[item[0]] = item[1]
        return configPart
    else:
        raise Exception('CONFIG_PARSER: Section {0} not found in the config file: {1}'.format(section, configfile))
