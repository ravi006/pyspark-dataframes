
from datetime import datetime
from pyspark import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

from test.problem.input_data import ProcessInputData
from test.problem.get_all_gowalla_users import AllGowallaUsers
from test.utils.parser.constants import Constants
from test.utils.connectors.data_read_write import HdfsReadWrite
from test.utils.logger import TestLogger

logger = TestLogger(__name__)


class TestRunner(Constants):

    def run_test(self):
        """ Run Test tasks"""


        conf = SparkConf().setAppName('Test')
        spark_context = SparkContext(conf=conf)
        hivecontext = HiveContext(spark_context)

        user_relations = ProcessInputData.get_relationships_df(hivecontext)
        user_checkins = ProcessInputData.get_user_checkins_df(hivecontext)

        logger.info("Reading files from HDFS is Done ...............................................................")

        if Constants._TASK1_FLAG:
            cross_join = AllGowallaUsers.get_all_gowalla_users(user_relations, user_checkins)
            cross_join.printSchema()
            cross_join.write.format(Constants._DATA_FORMAT).save(Constants._RESULT_ONE)
            logger.info("Task One is completed .......................................................................")

        if Constants._TASK2_FLAG:
            # User having highest friends
            users_total_friends = AllGowallaUsers.get_users_by_highest_friends(user_relations)
            users_total_friends.printSchema()
            users_total_friends.show()
            users_total_friends.write.format(Constants._DATA_FORMAT).save(Constants._RESULT_TWO)
            logger.info("Task Two is completed .......................................................................")

        if Constants._TASK3_FLAG:
            users_checkins_results = AllGowallaUsers.get_user_by_checkins_with_threshold(user_relations, user_checkins)
            users_checkins_results.printSchema()
            users_checkins_results.show()
            users_checkins_results.write.format(Constants._DATA_FORMAT).save(Constants._RESULT_THREE)
            logger.info("Task Three is completed .....................................................................")

if __name__ == "__main__":
    test = TestRunner()
    test.run_test()
