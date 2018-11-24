

from pyspark.sql.types import LongType, IntegerType, DoubleType, StructField, TimestampType, StructType

class UsersLocationSchema:

    @staticmethod
    def get_users_relation_schema():
        return StructType([
            StructField("user_id", IntegerType(), False),
            StructField("friend_id", IntegerType(), False)
        ])

    @staticmethod
    def get_users_checkin_info_schema():
        return StructType([
            StructField("user_checkin_id", IntegerType(), False),
            StructField("checkin_time", TimestampType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("londitude", DoubleType(), False),
            StructField("location_id", LongType(), False)
        ])
