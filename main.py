import argparse
import os

from src.udp_data_parser import UDPDataParser
from src.udp_data_processor import UDPDataProcessor

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", default='raw_data', type=str)
    args = parser.parse_args()
    # First pipeline: Parse the file and store raw_data
    for input_file in os.listdir(args.folder):
        parser = UDPDataParser(f'{args.folder}/{input_file}')
        parser.process_data()

    # Second pipeline: Calculate metrics from the stored raw_data
    processor = UDPDataProcessor('parced_packets')
    processor.process_data()