"""Tests for Config Reader Util."""
from test.utils.config_reader import ConfigReader
import mock

class TestConfigReader:
    """Testing method _get_config from Config Reader File."""

    def test_get_config(self):
        _config_path = "test/config.yaml"
        _config_path_test = "test/test/assets/test_config.yaml"
        conf_reader = ConfigReader()
        set_config = conf_reader.set_config_path(_config_path_test)
        expected = conf_reader._get_config()
        actual = "{'spark_etl_processing': {'TASK1': 'true', 'TASK2': 'true', 'TASK3': 'true'}, 'spark_etl': " \
                 "{'DATA_FORMAT': 'com.databricks.spark.csv'}, 'dev_results_data_hdfs_path': {'RESULT_TWO': " \
                 "'/data/gowalla/results/two/', 'RESULT_ONE': '/data/gowalla/results/one/', 'RESULT_THREE': " \
                 "'/data/gowalla/results/three/'}, 'dev_raw_data_hdfs_path': {'RELA': '/data/gowalla/test/users.txt', " \
                 "'CHECK': '/data/gowalla/test/users_checkins.txt'}}"
        #print expected
        assert str(expected) == actual

    def test_get_config_from_file(self):
        """Testing method Get Config From File in Config Reader File."""
        _config_path = "test/config.yaml"
        conf_reader = ConfigReader()
        set_config = conf_reader.set_config_path(_config_path)
        date_range = conf_reader.get_config_from_file("dev_results_data_hdfs_path")
        expected = date_range.get("RESULT_ONE")
        actual = "/data/gowalla/results/one/"
        assert expected == actual
