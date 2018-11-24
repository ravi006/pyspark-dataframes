"""HDFS Read Write."""

from test.utils.exceptions import SonobiFileError
from test.utils.parser.constants import Constants


class HdfsReadWrite(Constants):
    """Read Write Operation."""

    _DELIMITER = "\\t"
    _DATABRICKS_FORMAT = "com.databricks.spark.csv"
    _HEADER_STATUS = "true"
    _EMPTY_VALUE_STATUS = "true"
    _NULL_VALUE_DEFULT = "null"

    @classmethod
    def read_csv_from_hdfs(cls, hivecontext, path, df_schema):
        """
        Read CSV file from HDFS path.

        :param hivecontext:sparksession object, Spark session with Hive Integration
        :param path:string, HDFS Path
        :param df_schema:Array, StructField Array of the Schema of the file
        :return: datfarame, Dataframe of the file read
        """
        try:
            dataframe = hivecontext.read.format(cls._DATABRICKS_FORMAT) \
                .schema(df_schema)\
                .option("header", cls._HEADER_STATUS)\
                .option("delimiter", cls._DELIMITER) \
                .option("inferSchema", "false") \
                .option("treatEmptyValuesAsNulls", cls._EMPTY_VALUE_STATUS)\
                .option("nullValue", cls._NULL_VALUE_DEFULT)\
                .load(path)

            return dataframe

        except Exception as e:
            print str(e)
            raise SonobiFileError

    @classmethod
    def write_to_hdfs(cls, df, path, write_format):
        """
        Write Dataframe to HDFS in format mentioned.

        :param df:dataframe, Dataframe to write to HDFS
        :param path:string, HDFS Path
        :param write_format:string, format of file to write
        """
        try:
            df\
                .write\
                .format(write_format)\
                .mode("overwrite")\
                .save(path)

        except Exception as e:
            print str(e)
            raise SonobiFileError
