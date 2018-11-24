from pyspark.sql.types import StructType
from sonobi.src.schema.users_locations import UsersLocationSchema


class TestUsersLocationSchema:
    """Test Schema for User Locations Checkins."""

    def test_get_user_location_schema(self):
        """Test Schema for User Relations."""

        schema = UsersLocationSchema.get_users_relation_schema()
        assert isinstance(schema, StructType)
        fields = [f for f in schema.fields]
        assert len(fields) == 2

    def test_get_user_checkin_schema(self):
        """Test Schema for User Checkins."""
        schema = UsersLocationSchema.get_users_checkin_info_schema()
        assert isinstance(schema, StructType)
        fields = [f for f in schema.fields]
        assert len(fields) == 5
