from test.utils.parser.constants import Constants
from mock import MagicMock as Mock

class TestConstants:
    """Test Define Project Wise Constants based on Config File."""

    def test_constants(self):
        """Test Constants"""
        # my_mock = Mock()
        assert Constants._TASK3_FLAG == "true"
        assert Constants._RELATION_RAW == "/data/gowalla/test/users.txt"
