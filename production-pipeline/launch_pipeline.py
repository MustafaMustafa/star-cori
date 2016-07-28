"""A script to launch all pipeline daemons"""

import os
import sys
import threading
import time
import logging
import yaml
import load_configuration
from daq_files_watcher import daq_files_watcher
from submitter import submitter
from jobs_validator import jobs_validator
from merger import merger
from buffers_cleaner import buffers_cleaner

# pylint: disable=C0103
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(module)s %(thread)d: %(message)s')
__logger = logging.getLogger(__name__)
# pylint: enable=C0103

def main():
    """A script to launch all pipeline daemons"""

    # load configuration and update production tag
    args = load_configuration.get_args(__doc__)
    config = load_configuration.load_configuration(args.configuration)
    config['config_files_dir'] = os.path.abspath(config['config_files_dir'])

    for daemon in config['daemons_configs']:
        config['daemons_configs'][daemon] = os.path.join(config['config_files_dir'], config['daemons_configs'][daemon])

    override_daemons_configs(config)

    # spwan workers
    workers = {'daq_files_watcher': daq_files_watcher,
               'submitter': submitter,
               'jobs_validator': jobs_validator,
               'merger': merger,
               'buffers_cleaner': buffers_cleaner}

    for daemon, daemon_config in config['daemons_configs'].iteritems():
        if daemon not in workers:
            logging.error("%s no such daemon exists", daemon)
            continue

        logging.info("Spawning %s daemon...", daemon)
        thread = threading.Thread(target=workers[daemon], args=(daemon_config,))
        thread.setDaemon(True)
        thread.start()

        logging.info("Waiting for %i seconds before spwaning the next daemon...", config['interval_between_workers'])
        time.sleep(config['interval_between_workers'])

    logging.info("All daemons are working...")

    while True:
        time.sleep(1e4)

def override_daemons_configs(config):
    """Update production tag in daemons yaml files"""

    logging.info("Updating production tag in all daemons configuration files...")

    for daemon_yml in config['daemons_configs'].values():

        if os.path.exists(daemon_yml):

            with open(daemon_yml, 'r') as tmpf:
                yml = yaml.load(tmpf)

            yml['heartbeat'] = config['heartbeat']
            yml['heartbeat_interval'] = config['heartbeat_interval']
            yml['production_tag'] = config['production_tag']

            with open(daemon_yml, 'w') as tmpf:
                yaml.dump(yml, tmpf)
        else:
            logging.error("Configuration file %s doesn't exist!", daemon_yml)
            exit(1)

if __name__ == '__main__':
    main()
