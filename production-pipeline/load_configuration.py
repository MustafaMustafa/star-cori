import os
import logging
import yaml

def load_configuration(configuration_file):
    """Load parameters from configuration file and return dictionary"""

    logging.info("-------------------------------------------------------------------------")
    # open configuration file
    if os.path.exists(configuration_file):
        logging.info("Loading configuration file %s", configuration_file)
    else:
        logging.error("Configuration file %s doesn't exist!", configuration_file)
        exit(1)

    conf_file = file(configuration_file, 'r')
    parameters = yaml.load(conf_file)
    conf_file.close()

    for key in parameters:
        logging.info('%s: %s', key, parameters[key])
        if key.find('path') > 0:
            if not os.path.isdir(parameters[key]):
                logging.error("Path %s does not exist or is not a directory!", parameters[key])
                exit(1)

    # set heartbeat parameters
    # set_config_parameter(parameters, 'heartbeat')
    # if __global_parameters['heartbeat']:
    #     set_config_parameter(parameters, 'heartbeat_interval', 'seconds')
    # else:
    #     __logger.info("Heart beat disabled in configuration file")
    #
    logging.info("Done loading configuration")
    logging.info("-------------------------------------------------------------------------")

    return parameters
