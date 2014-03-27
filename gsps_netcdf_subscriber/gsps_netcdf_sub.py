#!/usr/bin/python

# Subscribes to the Glider Singleton Publishing Service Socket.
# When a new set is published, it outputs a new NetCDF to a given
# output directory.
#
# By: Michael Lindemuth
# University of South Florida
# College of Marine Science
# Ocean Technology Group

import daemon
import zmq

import argparse
import sys
import shutil

import os
import json

import logging
logger = logging.getLogger('gsps_netcdf_sub')

from glider_netcdf_writer import (
    open_glider_netcdf
)

from netCDF4 import default_fillvals as NC_FILL_VALUES

from threading import Thread

from gsps_netcdf_subscriber.generators import (
    generate_global_attributes,
    generate_filename,
    generate_set_key
)

import lockfile

import numpy as np

from glider_utils.yo import find_yo_extrema

from glider_utils.yo.filters import (
    filter_profile_depth,
    filter_profile_time,
    filter_profile_distance,
    filter_profile_number_of_points
)

from glider_utils.gps import interpolate_gps

from glider_utils.ctd.salinity import calculate_practical_salinity


class GliderDataset(object):
    """Represents a complete glider dataset
    """

    def __init__(self, handler_dataset):
        self.glider = handler_dataset['glider']
        self.segment = handler_dataset['segment']
        self.headers = handler_dataset['headers']
        self.__parse_lines(handler_dataset['lines'])
        self.__interpolate_glider_gps()
        self.__calculate_salinity()

    def __interpolate_glider_gps(self):
        if 'm_gps_lat-lat' in self.data_by_type:
            dataset = np.column_stack((
                self.times,
                self.data_by_type['m_gps_lat-lat'],
                self.data_by_type['m_gps_lon-lon']
            ))
            gps = interpolate_gps(dataset)
            self.data_by_type['lat-lat'] = gps[:, 1]
            self.data_by_type['lon-lon'] = gps[:, 2]

    def __calculate_salinity(self):
        if 'sci_water_cond-s/m' in self.data_by_type:
            dataset = np.column_stack((
                self.times,
                self.data_by_type['sci_water_cond-s/m'],
                self.data_by_type['sci_water_temp-degc'],
                self.data_by_type['sci_water_pressure-bar']
            ))
            salinity_dataset = calculate_practical_salinity(dataset)[:, 4]
            salinity_dataset[np.isnan(salinity_dataset)] = (
                NC_FILL_VALUES['f8']
            )
            self.data_by_type['salinity-psu'] = salinity_dataset

    def __parse_lines(self, lines):
        self.time_uv = NC_FILL_VALUES['f8']
        self.times = []
        self.data_by_type = {}

        for header in self.headers:
            self.data_by_type[header] = []

        for line in lines:
            self.times.append(line['timestamp'])
            for key in self.data_by_type.keys():
                if key in line:
                    datum = line[key]
                    if key == 'm_water_vx-m/s':
                        self.time_uv = line['timestamp']
                else:
                    datum = NC_FILL_VALUES['f8']
                self.data_by_type[key].append(datum)

    def calculate_profiles(self):
        profiles = []
        if 'm_depth-m' in self.data_by_type:
            dataset = np.column_stack((
                self.times,
                self.data_by_type['m_depth-m']
            ))
            profiles = find_yo_extrema(dataset)
            profiles = filter_profile_depth(profiles)
            profiles = filter_profile_time(profiles)
            profiles = filter_profile_distance(profiles)
            profiles = filter_profile_number_of_points(profiles)

        return profiles[:, 2]


def write_netcdf(configs, sets, set_key):
    dataset = GliderDataset(sets[set_key])

    # No longer need the dataset stored by handlers
    del sets[set_key]

    global_attributes = (
        generate_global_attributes(configs, dataset)
    )

    filename = generate_filename(configs, dataset)
    tmp_path = '/tmp/' + filename
    with open_glider_netcdf(tmp_path, 'w') as glider_nc:
        glider_nc.set_global_attributes(global_attributes)
        glider_nc.set_platform(
            configs[dataset.glider]['deployment']['platform']
        )
        glider_nc.set_trajectory_id(
            configs[dataset.glider]['deployment']['trajectory_id']
        )
        glider_nc.set_segment_id(dataset.segment)
        glider_nc.set_datatypes(configs['datatypes'])
        glider_nc.set_instruments(configs[dataset.glider]['instruments'])
        glider_nc.set_times(dataset.times)
        glider_nc.set_time_uv(dataset.time_uv)
        glider_nc.set_profile_ids(dataset.calculate_profiles())
        for datatype, data in dataset.data_by_type.items():
            glider_nc.insert_data(datatype, data)

    deployment_path = (
        configs['output_directory'] + '/'
        + configs[dataset.glider]['deployment']['directory']
    )
    if not os.path.exists(deployment_path):
        os.mkdir(deployment_path)
    file_path = deployment_path + '/' + filename
    shutil.move(tmp_path, file_path)

    logger.info("Datafile written to %s" % file_path)


def handle_set_start(configs, sets, message):
    """Handles the set start message from the GSPS publisher

    Initializes the new dataset store in memory
    """
    set_key = generate_set_key(message)

    sets[set_key] = {
        'glider': message['glider'],
        'segment': message['segment'],
        'headers': [],
        'lines': []
    }

    for header in message['headers']:
        key = header['name'] + '-' + header['units']
        sets[set_key]['headers'].append(key)

    logger.info(
        "Dataset start for %s @ %s"
        % (message['glider'], message['start'])
    )


def handle_set_data(configs, sets, message):
    """Handles all new data coming in for a GSPS dataset

    All datasets must already have been initialized by a set_start message.
    Appends new data lines to the set lines variable.
    """
    set_key = generate_set_key(message)

    if set_key in sets:
        sets[set_key]['lines'].append(message['data'])
    else:
        logger.error(
            "Unknown dataset passed for key glider %s dataset @ %s"
            % (message['glider'], message['start'])
        )


def handle_set_end(configs, sets, message):
    """Handles the set_end message coming from GSPS

    Checks for empty dataset.  If not empty, it hands
    off dataset to thread.  Thread writes NetCDF data to
    new file in output directory.
    """

    set_key = generate_set_key(message)

    if set_key in sets:
        if len(sets[set_key]['lines']) == 0:
            logger.info(
                "Empty set: for glider %s dataset @ %s"
                % (message['glider'], message['start'])
            )
            return  # No data in set, do nothing

        thread = Thread(
            target=write_netcdf,
            args=(configs, sets, set_key)
        )
        thread.start()

    logger.info(
        "Dataset end for %s @ %s.  Processing..."
        % (message['glider'], message['start'])
    )


message_handlers = {
    'set_start': handle_set_start,
    'set_data': handle_set_data,
    'set_end': handle_set_end
}


def load_configs(configs_directory):
    configs = {}

    for filename in os.listdir(configs_directory):
        # Skip hidden directories
        if filename[0] == '.':
            continue

        ext_sep = filename.find('.')
        if ext_sep != -1:
            key = filename[:ext_sep]
        else:
            key = filename
        full_path = configs_directory + '/' + filename
        # Glider configurations are in directories.
        # Load configs recursively
        if os.path.isdir(full_path):
            configs[key] = load_configs(full_path)
        # Load configuration from file
        else:
            with open(full_path, 'r') as f:
                contents = f.read()
                conf = {}
                try:
                    conf = json.loads(contents)
                except Exception, e:
                    logger.error('Error processing %s: %s' % (filename, e))
            configs[key] = conf

    return configs


def run_subscriber(configs):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(configs['zmq_url'])
    socket.setsockopt(zmq.SUBSCRIBE, '')

    sets = {}

    while True:
        try:
            message = socket.recv_json()
            if message['message_type'] in message_handlers:
                message_type = message['message_type']
                message_handlers[message_type](configs, sets, message)
        except Exception, e:
            logger.error("Subscriber exited: %s" % (e))
            break


def main():
    parser = argparse.ArgumentParser(
        description="Subscribes to the Glider Singleton Publishing Service "
                    "Socket.  When a new set is published, it outputs a new "
                    "NetCDF to a given output directory."
    )

    parser.add_argument(
        "--zmq_url",
        default="tcp://localhost:8008",
        help="ZMQ url for the GSPS publisher. Default: tcp://localhost:8008"
    )
    parser.add_argument(
        "--configs",
        default="/etc/gsps_netcdf_sub",
        help="Folder to look for NetCDF global and glider "
             "JSON configuration files.  Default: /etc/gsps_netcdf_sub"
    )
    parser.add_argument(
        "--daemonize",
        type=bool,
        help="To daemonize or not to daemonize.  Default: false",
        default=False
    )
    parser.add_argument(
        "--log_file",
        help="Path of log file.  Default: ./gsps_netcdf_sub.log",
        default="./gsps_netcdf_sub.log"
    )
    parser.add_argument(
        "--pid_file",
        help="Path of PID file for daemon.  Default: ./gsps_netcdf_sub.pid",
        default="./gsps_netcdf_sub.pid"
    )
    parser.add_argument(
        "output_directory",
        help="Where to place the newly generated netCDF file.",
        default=False
    )

    args = parser.parse_args()

    # Setup logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s "
                                  "- %(levelname)s - %(message)s")
    if args.daemonize:
        log_handler = logging.FileHandler(args.log_file)
    else:
        log_handler = logging.StreamHandler(sys.stdout)
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)

    configs_directory = args.configs
    if configs_directory[-1] == '/':
        configs_directory = configs_directory[:-1]
    configs = load_configs(configs_directory)

    output_directory = args.output_directory
    if args.output_directory[-1] == '/':
        output_directory = args.output_directory[:-1]
    configs['output_directory'] = output_directory

    configs['zmq_url'] = args.zmq_url

    if args.daemonize:
        logger.info('Starting')
        daemon_context = daemon.DaemonContext(
            pidfile=lockfile.FileLock(args.pid_file),
            files_preserve=[log_handler.stream.fileno()],
        )
        with daemon_context:
            run_subscriber(configs)
    else:
        run_subscriber(configs)

    logger.info('Stopped')

if __name__ == '__main__':
    sys.exit(main())
