# ****************************************************************************
#
# Copyright (c) 2014-2024 Fraunhofer FKIE
# Author: Alexander Tiderko
# License: MIT
#
# ****************************************************************************

import struct
import sys
from fkie_iop_json_connector.jaus_address import JausAddress
from fkie_iop_json_connector.logger import MyLogger
from fkie_iop_json_connector.message import Message

IS_PYTHON_2 = sys.version_info[0] < 3


def print_data(msg, data):
    print(msg, [ord(char) for char in data])


class MessageParser:

    MIN_PACKET_SIZE_V1 = 16
    MIN_PACKET_SIZE_V2 = 14

    def __init__(self, sender, stream=False, loglevel='info'):
        '''
        :param AddressBook.Endpoint sender: the socket where the packed was received or the sender.
        :param bool stream: if stream is `True` received data will be concatenated. In this case the message version byte will be checked only in first message.
        '''
        self._sender = sender
        self._version_only_first = stream
        self._stream = stream
        name = sender
        if name is not None:
            name = sender.address.replace('.', '_')
        self.logger = MyLogger('msg[%s]' % name, loglevel=loglevel)
        self._data = b''
        self._version = None

    def unpack(self, data, version=None):
        if self._stream:
            self._data += data
        else:
            self._data = data
        msg_list = []
        # self.logger.debug("decode received %dBytes" % len(data))
        with_version_byte = False
        # read version
        while self._data:
            # TODO: check for version for each packet in UDP and UDS, for TCP only on connect.
            msg = Message()
            msg.tinfo_src = self._sender
            offset = 0
            msg_endidx = 0
            data_len = len(self._data)
            pkg_vers = version
            if self._version_only_first:
                # in TCP connection only first message contains version byte. Use known version if it is already set in this parser.
                pkg_vers = self._version
            if pkg_vers is None:
                # determine the message version
                if IS_PYTHON_2:
                    pkg_vers = ord(self._data[0])
                else:
                    pkg_vers = self._data[0]
                offset = 1
                with_version_byte = True
                if self._version_only_first:
                    self._version = pkg_vers
            # parse message depending on version
            if pkg_vers == 1:
                # check for valid message length
                if data_len < self.MIN_PACKET_SIZE_V1 + offset:
                    self.logger.error(
                        "return, received data length is to small for message version 1")
                    return msg_list
                msg.version = Message.AS5669
                offset += 4
                (flags, _msg_vers, msg.cmd_code, dst_id, src_id, data_flags,
                 msg.seqnr) = struct.unpack('<BBHIIHH', self._data[offset:offset + 16])
                msg.dst_id = JausAddress(dst_id)
                msg.src_id = JausAddress(src_id)
                prio = flags & 0b00001111
                msg._priority = 3 if prio == 15 else int((prio - 3) / 3)
                msg._acknak = (flags & 0b00110000) >> 4
                msg._data_size = data_flags & 0b00001111111111111111
                offset += 16
                msg_endidx = offset + msg._data_size
                msg.set_raw(self._data[:msg_endidx], offset, with_version_byte)
                # read message id
                if msg._data_size >= 2:  # check message size before extract message id
                    (msg_id, ) = struct.unpack(
                        '<H', self._data[offset:offset + 2])
                    msg._msg_id = int(msg_id)
                msg_list.append(msg)
            elif pkg_vers == 2:
                # check for valid message length
                if data_len < self.MIN_PACKET_SIZE_V2 + offset:
                    self.logger.error("return, received data length %d is to small for message version 2 (%d)" % (
                        data_len, self.MIN_PACKET_SIZE_V2 + offset))
                    return msg_list
                msg.version = Message.AS5669A
                (flags, msg._data_size) = struct.unpack(
                    '<BH', self._data[offset:offset + 3])
                msg.message_type = flags & 0b00111111
                hc_flags = flags & 0b11000000 >> 6
                offset += 3
                if hc_flags:
                    msg.hc_flags = hc_flags
                    offset += 2
                    # TODO: add support for header compression
                (data_flags, dst_id, src_id) = struct.unpack(
                    '<BII', self._data[offset:offset + 9])
                msg.dst_id = JausAddress(dst_id)
                msg.src_id = JausAddress(src_id)
                # decode data flags
                msg.priority = data_flags & 0b00000011
                msg.bcast = (data_flags & 0b00001100) >> 2
                msg.acknak = (data_flags & 0b00110000) >> 4
                msg.data_flags = (data_flags & 0b11000000) >> 6
                # 1 is a byte for message version (no version byte in TCP connections)
                msg_endidx = msg._data_size + (1 if with_version_byte else 0)
                if data_len < msg_endidx:
                    # handling of short message
                    self.logger.error("return, received data %d is smaller than data length in header %d" % (
                        data_len, msg_endidx))
                    return msg_list
                (msg.seqnr, ) = struct.unpack(
                    '<H', self._data[msg_endidx - 2:msg_endidx])
                offset += 9
                msg.set_raw(self._data[:msg_endidx], offset, with_version_byte)
                # read message id
                if offset + 2 <= msg_endidx:  # check message size before extract message id
                    if msg.data_flags in [0, 1]:  # only on single or first packets
                        (msg_id, ) = struct.unpack(
                            '<H', self._data[offset:offset + 2])
                        msg._msg_id = int(msg_id)
                msg_list.append(msg)
            else:
                return msg_list
            # self.logger.debug("decoded %s" % msg)
            # remove message bytes from data
            if self._stream:
                self._data = self._data[msg_endidx:]
            else:
                self._data = b''
        return msg_list
