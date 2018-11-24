
from test.utils.connectors.data_read_write import HdfsReadWrite
from test.utils.parser.constants import Constants
from test.src.schema.users_locations import UsersLocationSchema

class ProcessInputData(Constants):

    @staticmethod
    def get_relationships_df(hive_context):
        """
        Users Relations Dataframe.

        :param hive_context: HiveContext
        :return: It returns Users Relationship DF
        """
        user_relations_df = HdfsReadWrite\
            .read_csv_from_hdfs(hive_context, Constants._RELATION_RAW, UsersLocationSchema.get_users_relation_schema())
        return user_relations_df

    @staticmethod
    def get_user_checkins_df(hive_context):
        """
        Users Checkins Dataframe.

        :param hive_context: HiveContext
        :return: It returns Users checkins DF.
        """
        user_checkins_df = HdfsReadWrite\
            .read_csv_from_hdfs(hive_context, Constants._CHECKINS_RAW, UsersLocationSchema.get_users_checkin_info_schema())
        return user_checkins_df
