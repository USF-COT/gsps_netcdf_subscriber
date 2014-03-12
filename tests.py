import unittest

from gsps_netcdf_subscriber.gsps_netcdf_sub import (
    load_configs
)

from datetime import datetime


class TestLoadConfigs(unittest.TestCase):
    def test_load_config(self):
        test_path = '/home/localuser/gsps_netcdf_subscriber/example_config'
        configs = load_configs(test_path)
        self.assertIn('usf-bass', configs)


class TestGenerators(unittest.TestCase):
    def setUp(self):
        pass

    def test_set_key(self):
        message = {
            'glider': 'bass',
            'start': datetime.utcnow().isoformat()
        }
        self.assetEqual(
            message['gilder'] + '-' + message['start']
        )


if __name__ == '__main__':
    unittest.main()
