import lzma
from datetime import datetime

import pandas as pd
from scapy.all import rdpcap

import logzero
logzero.logfile("/tmp/udp_data_parcer.log", maxBytes=1e6, backupCount=3)
from logzero import logger


class UDPDataParser:
    def __init__(self, input_file: str):
        self.input_file = input_file
        self.packets = []

    def extract_data(self):
        """
        Extract UDP packet raw data from the input file
        """
        with lzma.open(self.input_file, 'rb') as xz_file:
            # Use rdpcap to load the PCAP raw data
            self.packets = rdpcap(xz_file)  # Some further check that we have only UDP packets could be well

    def parse_udp_packets(self):
        self.parced_packets = []
        for packet in self.packets:
            self.parced_packets.append(self.parse_udp_packet(packet))

    def parse_udp_packet(self, packet):
        """
        Extract relevant information (e.g., message number, timestamp, A/B side)

        :param packet: scapy packet object
        :return: dict with packet information
        """
        raw_data = packet.getlayer("Raw")
        trailer_data = raw_data.payload.load
        seconds = int.from_bytes(trailer_data[8:12], byteorder='big', signed=False)
        nanoseconds = int.from_bytes(trailer_data[12:16], byteorder='big', signed=False)

        meta_timestamp = datetime.utcfromtimestamp(seconds + nanoseconds / 1e9)

        packet_data = raw_data.load
        message_number = int.from_bytes(packet_data[:4][::-1], byteorder='big', signed=False)
        cme_timestamp = datetime.utcfromtimestamp(
            int.from_bytes(packet_data[11:3:-1], byteorder='big', signed=False) / 1e9)

        channel = self.get_channel(packet.getlayer('UDP').dport)
        # Return the parsed packet as a dictionary or object
        return {'message_number': message_number, 'cme_ts': cme_timestamp, 'meta_ts': meta_timestamp, 'channel': channel}

    def get_channel(self, dport):
        return {14310: 'A', 15310: 'B'}[dport]  # Can be put into separate config file

    def store_data(self, output_folder: str = 'parced_packets'):
        df = pd.DataFrame(self.parced_packets)
        df['date'] = df['cme_ts'].dt.date
        df.to_parquet(output_folder, partition_cols=['channel', 'date'],
                      existing_data_behavior='delete_matching')

    def process_data(self):
        self.extract_data()
        self.parse_udp_packets()
        self.store_data()


