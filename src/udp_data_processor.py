import pandas as pd

import logzero
logzero.logfile("/tmp/udp_data_processor.log", maxBytes=1e6, backupCount=3)
from logzero import logger

def get_total_nanoseconds(timedelta):
    """
    Pandas timedelta is actually broken, so have to do this
    """
    return timedelta.total_seconds() *1e9 + timedelta.nanoseconds

class UDPDataProcessor:
    def __init__(self, data_source='parced_packets'):
        self.data_source = data_source
        self.metrics = {}

    def load_data(self):
        self.df = pd.read_parquet(self.data_source)

    def calculate_metrics(self):
        """
        Keeping them together for now, can be moved to a separate class for easier
        """
        self.metrics['Total_number_of_packets_by_side'] = self.df.groupby('channel').size().to_dict()
        pivot = self.df.pivot(columns='channel', index='message_number', values='meta_ts')
        self.metrics['Packets_without_counterpart'] = {
            'A': pivot.loc[pivot['B'].isna() & ~pivot['A'].isna()].shape[0],
            'B': pivot.loc[pivot['A'].isna() & ~pivot['B'].isna()].shape[0]
        }
        self.metrics['Number_of_packets_faster'] = {
            'A': pivot.loc[pivot['A'] < pivot['B']].shape[0],
            'B': pivot.loc[pivot['B'] < pivot['A']].shape[0],
        }
        pivot['diff'] = pivot['A'] - pivot['B']
        self.metrics['Average_speed_advantage'] = {
            'A': get_total_nanoseconds(-pivot.loc[pivot['A'] < pivot['B'], 'diff'].mean()),
            'B': get_total_nanoseconds(pivot.loc[pivot['B'] < pivot['A'], 'diff'].mean()),
        }

    def process_data(self):
        self.load_data()
        self.calculate_metrics()
        self.display_metrics()

    def display_metrics(self):
        # Display the calculated metrics
        print("Metrics:")
        for key, value in self.metrics.items():
            print(f"{key}: {value}")
