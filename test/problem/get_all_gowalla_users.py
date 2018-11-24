

from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

class AllGowallaUsers:

    @staticmethod
    def get_all_gowalla_users(users_df, users_checkins_df):
        """
        To get All listed users in gowalla
        :param users_df: Dataframe of User Relations.
        :return: All users
        """
        two_tables_cross_join = users_df.join(users_checkins_df)
        listed_gowalla_users = two_tables_cross_join.select(two_tables_cross_join.user_id).distinct()
        return listed_gowalla_users

    @staticmethod
    def get_users_by_highest_friends(users_df):
        """
        To Get users, who having friends more than average friends of friends.
        :param users_df:
        :return: Users List
        """

        # Get Sum of friends by each user from 1st  Table(User Relations)
        # Results from this phase:
        # user_id  sum_of_user_friends
        #   A           4
        #   B           5
        #   C           1
        users_all_friends = users_df\
            .map(lambda users: (users.user_id, 1))\
            .reduceByKey(lambda a, b: a + b).toDF()
        users_all_friends_1 = users_all_friends\
            .withColumnRenamed('_1', 'user_sum_id')\
            .withColumnRenamed('_2', 'total_friends')

        # Join above results with User Relations Table to get user friends sum.
        # Results in this phase:
        # user_id   friend_id   total_friends
        #   A           B           5
        #   A           C           1
        users_friend_sum = users_df\
            .join(users_all_friends_1, users_all_friends_1.user_sum_id == users_df.friend_id, "inner")\
            .drop(users_all_friends_1.user_sum_id)

        # To calculate friends of friends average.
        users_groupby = users_friend_sum.groupBy('user_id').mean('total_friends')
        users_groupby_schema = users_groupby\
            .withColumnRenamed('avg(total_friends)', 'friends_avg')

        # Join the above results with sum of friends of friends Dataframe (users_all_friends_1) to get relations of user
        #  total friends and friends of friends average.
        users_comb_avg = users_all_friends_1\
            .join(users_groupby_schema, users_groupby_schema.user_id == users_all_friends_1.user_sum_id, "inner")\
            .drop(users_groupby_schema.user_id)

        # Result.
        users_result = users_comb_avg.filter(users_comb_avg.total_friends >= users_comb_avg.friends_avg)\
            .select(users_comb_avg.user_sum_id, users_comb_avg.total_friends, users_comb_avg.friends_avg)
        return users_result

    @staticmethod
    def get_user_by_checkins_with_threshold(users_df, users_checkins_df):
        """
        To Get Users list that their average friends checkin locations is more than the Threshold value.
        :param users_df: Dataframe of User Relations.
        :param users_checkins_df: Dataframe of User checkin Locations.
        :return: Users List
        """

        _THRESHOLD_VALUE = 1
        _FREQUENCY_CAP = 2

        # Get Total Locations by each user from 2nd  Table(Users Location Checkins)
        # Results from this phase:
        # user_id  total_location_checkins
        #   A           4
        #   B           5
        #   C           1
        users_total_locations = users_checkins_df.select(users_checkins_df.user_checkin_id)\
            .map(lambda users: (users.user_checkin_id, 1))\
            .reduceByKey(lambda a, b: a + b).toDF()

        users_total_locations_1 = users_total_locations \
            .withColumnRenamed('_1', 'user_loc_id') \
            .withColumnRenamed('_2', 'total_locations')

        # From 1st Table(Users Relations) Join the above Results by friend_id of User Relation Table and by user Id
        # of 2nd Table
        # Results from this phase
        # user_id   friend_id   total_locations
        #   A           B           5
        #   A           C           1
        user_friend_join_with_locations = users_df\
            .join(users_total_locations_1, users_total_locations_1.user_loc_id == users_df.friend_id, "inner")\
            .drop(users_total_locations_1.user_loc_id)

        # UDF to get minmum of (_FREQUENCY_CAP, TOTAL_LOCATIONS)
        function = udf(lambda item: item if _FREQUENCY_CAP >= item else _FREQUENCY_CAP, IntegerType())

        # To get Minimum of (_FREQUENCY_CAP, TOTAL_LOCATIONS
        # Results in this phase
        # user_id   friend_id   min_of_t
        #   A           B           2
        #   A           C           1
        min_of_t_friend = user_friend_join_with_locations\
            .select("user_id", "friend_id", function(col("total_locations")).alias('min_of_t'))
        # Find Average of User Friends Location Checkins
        avg_user_friend_checkins = min_of_t_friend.groupBy('user_id').mean('min_of_t')

        users_groupby_schema = avg_user_friend_checkins.withColumnRenamed('avg(min_of_t)', 'compare_with_t')

        # Filter Data by THRESHOLD VALUE
        results = users_groupby_schema\
            .filter(users_groupby_schema.compare_with_t >= _THRESHOLD_VALUE)

        return results

