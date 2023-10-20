import numpy as np
import pandas as pd
import pytest

from src.udp_data_processor import UDPDataProcessor, get_total_nanoseconds

# Sample data for testing
sample_data = {
    'channel': ['A', 'B', 'A', 'B', 'A'],
    'message_number': [1, 1, 2, 2, 3],
    'meta_ts': [pd.Timestamp('2023-01-01 12:00:00'), pd.Timestamp('2023-01-01 12:01:00'),
                pd.Timestamp('2023-01-01 12:02:00'), pd.Timestamp('2023-01-01 12:03:00'),
                pd.Timestamp('2023-01-01 12:04:00')]
}


@pytest.mark.parametrize("test_input,expected", [((50, 'ns'), 50),
                                                 ((1e6+50, 'ns'), 1e6+50),
                                                 ((50, 's'), 50*1e9)])
def test_total_nanoseconds(test_input, expected):
    assert get_total_nanoseconds(pd.Timedelta(*test_input)) == expected


def test_total_nanoseconds_nat():
    assert np.isnan(get_total_nanoseconds(pd.NaT))


class TestUDPDataProcessor:
    def setup_class(self):
        # Set up a temporary DataFrame with sample data
        self.df = pd.DataFrame(sample_data)

    def test_calculate_metrics(self):
        # Test the calculate_metrics method with sample data
        processor = UDPDataProcessor(data_source='')
        processor.df = self.df
        processor.calculate_metrics()

        # Check the metrics
        metrics = processor.metrics
        assert metrics['Total_number_of_packets_by_side'] == {'A': 3, 'B': 2}
        assert metrics['Packets_without_counterpart'] == {'A': 1, 'B': 0}
        assert metrics['Number_of_packets_faster'] == {'A': 2, 'B': 0}
        assert metrics['Average_speed_advantage']['A'] == 60000000000
        assert np.isnan(metrics['Average_speed_advantage']['B'])