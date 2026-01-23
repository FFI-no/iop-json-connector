import struct
import pytest
from types import SimpleNamespace
from fkie_iop_json_connector.message_serializer import MessageSerializer
from fkie_iop_json_connector.message import Message


valid_data = SimpleNamespace(
    messageId="4b00",
    messageName="ReportIdentification",
    jausIdSrc="127.100.1",
    jausIdDst="127.255.255",
    data=SimpleNamespace(
        HeaderRec=SimpleNamespace(MessageID="4b00"),
        ReportIdentificationRec=SimpleNamespace(
            QueryType="System Identification",
            Type="VEHICLE",
            Identification="TestVehicle"
        )
    )
)

invalid_data_missing_field = SimpleNamespace(
    messageId="4b00",
    messageName="ReportIdentification",
    jausIdSrc="127.100.1",
    jausIdDst="127.255.255",
    data=SimpleNamespace(
        HeaderRec=SimpleNamespace(MessageID="4b00"),
        ReportIdentificationRec=SimpleNamespace(
            QueryType="System Identification",
            Type="VEHICLE"
            # Identification fehlt absichtlich
        )
    )
)

@pytest.fixture
def serializer(tmp_path=""):
    return MessageSerializer(tmp_path, loglevel='debug')


def test_unsigned_byte_overflow(serializer):
    data = serializer._safe_pack('unsigned byte', 300)
    assert data == b'\xff'


def test_unsigned_short_ok(serializer):
    data = serializer._safe_pack('unsigned short integer', 65535)
    assert data == b'\xff\xff'


def test_signed_byte_underflow(serializer):
    data = serializer._safe_pack('byte', -200)
    assert data == b'\x80'

def test_string_encoding(serializer):
    s = "äöü"
    encoded = s.encode('utf-8')
    packed = struct.pack(f'{len(encoded)}s', encoded)
    assert packed.endswith(encoded)

def test_array_length_clamping(serializer):
    data = serializer._safe_pack('unsigned byte', 1000)
    assert data == b'\xff'

def test_bitfield_or():
    bitfield = 0
    value = 3       # 0b11
    shift = 4
    bitfield |= (value << shift)
    assert bitfield == 0b00110000


# -----------------------------
# Tests
# -----------------------------
def test_pack_valid(serializer):
    msg = Message(int(valid_data.messageId, 16))
    result = serializer.pack(valid_data, msg)
    assert result is True
    assert len(msg.payload) > 0
    assert isinstance(msg.payload, bytes)

def test_unpack_valid(serializer):
    msg = Message(int(valid_data.messageId, 16))
    serializer.pack(valid_data, msg)
    unpacked = serializer.unpack(msg)
    assert unpacked["data"]["HeaderRec"]["MessageID"] == "4b00"
    assert unpacked["data"]["ReportIdentificationRec"]["Identification"] == "TestVehicle"

def test_pack_missing_required(serializer):
    msg = Message(int(valid_data.messageId, 16))
    result = serializer.pack(invalid_data_missing_field, msg)
    # Der Serializer sollte False zurückgeben oder Fehler loggen
    assert result is False