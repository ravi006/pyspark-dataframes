"""Create Project Constants."""

from test.utils.config_reader import ConfigReader
from test.utils.exceptions import TestFileError


class Constants:
    """Define Constants based on Config File."""

    config_reader = ConfigReader()

    try:
        config_file = "test/config.yaml"
    except:
        raise TestFileError

    config_reader.set_config_path(config_file)

    # SPARK ETL PROCESSING
    status = config_reader.get_config_from_file("spark_etl_processing")
    _TASK1_FLAG = status.get("TASK1")
    _TASK2_FLAG = status.get("TASK2")
    _TASK3_FLAG = status.get("TASK3")

    spark_etl = config_reader.get_config_from_file("spark_etl")
    _DATA_FORMAT = spark_etl.get("DATA_FORMAT")

    # PROCESSING RAW DATA
    raw_data_hdfs_path = config_reader.get_config_from_file("dev_raw_data_hdfs_path")
    _RELATION_RAW = raw_data_hdfs_path.get("RELA")
    _CHECKINS_RAW = raw_data_hdfs_path.get("CHECK")

    # SAVE RESULTS DATA
    preprocessed_data_hdfs_path = config_reader.get_config_from_file("dev_results_data_hdfs_path")
    _RESULT_ONE = preprocessed_data_hdfs_path.get("RESULT_ONE")
    _RESULT_TWO = preprocessed_data_hdfs_path.get("RESULT_TWO")
    _RESULT_THREE = preprocessed_data_hdfs_path.get("RESULT_THREE")
