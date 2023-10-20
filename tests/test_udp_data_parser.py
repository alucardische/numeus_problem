import pytest
from datetime import datetime
from scapy.layers.l2 import Ether
from scapy.layers.inet import IP, UDP

from src.udp_data_parser import UDPDataParser

class TestUDPDataParser:
    def test_parse_udp_packet(self):
        # Test the parse_udp_packet method with a sample UDP packet
        trailer_data = (b'\xc4\x8b\xae\xf4\x00\xdf\x1e ]h\x06\xdc\x13@\x18\x88\x03\x00(\x11')
        packet_data = (b'\xbbI\xb6\x01\x98k\x01\xfd\xdcs\xbf\x15X\x00\x0b\x00.\x00\x01\x00')

        # Create a custom payload by concatenating the packet_data and trailer_data
        udp_packet = (
                Ether()
                / IP(src="192.168.1.1", dst="192.168.1.2")
                / UDP(sport=12345, dport=14310)
                / bytes(packet_data)
                / bytes(trailer_data)# Use bytes() to convert the payload to bytes
        )

        parser = UDPDataParser("dummy_file.xz")
        parsed_packet = parser.parse_udp_packet(udp_packet)

        assert parsed_packet['message_number'] == 28723643
        assert parsed_packet['cme_ts'] == datetime(2019, 8, 29, 17, 9, 48, 322950)
        assert parsed_packet['meta_ts'] == datetime(2019, 8, 29, 17, 9, 48, 322968)
        assert parsed_packet['channel'] == 'A'

    def test_get_channel(self):
        # Test the get_channel method to ensure it returns the correct channel
        parser = UDPDataParser("")
        assert parser.get_channel(14310) == 'A'
        assert parser.get_channel(15310) == 'B'
        with pytest.raises(KeyError):
            parser.get_channel(0)

