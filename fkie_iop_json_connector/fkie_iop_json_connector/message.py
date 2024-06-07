# ****************************************************************************
#
# Copyright (c) 2014-2024 Fraunhofer FKIE
# Author: Alexander Tiderko
# License: MIT
#
# ****************************************************************************

import struct
import sys
import time
from fkie_iop_json_connector.jaus_address import JausAddress

IS_PYTHON_2 = sys.version_info[0] < 3


class Message(object):

    AS5669 = 1
    AS5669A = 2

    CODE_NONE = 0
    CODE_CONNECT = 1
    CODE_ACCEPT = 2
    CODE_CANCEL = 3

    DF_SINGLE = 0
    DF_FIRST = 1
    DF_MIDDLE = 2
    DF_LAST = 3

    def __init__(self, msg_id=0, version=AS5669A):
        self.version = version
        self.cmd_code = Message.CODE_NONE
        self.message_type = 0
        ''':ivar message_type: Values 1 through 32 are reserved for future use.
        '''
        self._msg_id = msg_id
        self.hc_flags = 0  # currently ignored while serialization
        self._data_size = 0
        ''':ivar _data_size:
        This is the size of the entire message including the General Transport Header. It has a minimum value of 14 decimal and
        a maximum value of 65535. Note, however, that each supported medium may further restrict this maximum value as
        described in the Maximum Packet Size sections. When Header Compression is used, the Data Size is the size of the
        compressed message. The packet length shall be presented formatted in Little-Endian format.
        '''
        self.src_id = JausAddress(0)
        self.dst_id = JausAddress(0)
        self._priority = 1
        ''':ivar priority: Low=0, Standard=1, High=2, Safety Critical=3'''
        self._bcast = 0
        ''':ivar bcast:
            - No Broadcast (Decimal value 0): The message is intended for a single destination.
            - Local Broadcast (Decimal value 1): The message is intended for the local destinations only.
            - Global Broadcast (Decimal value 2): The message is intended for all available destinations.
        '''
        self._bcast_recved_size = 0  # for statistical purposes, since raw size will be reseted for broadcast messages
        self._acknak = 0
        ''':ivar acknak:
            - Message Originator (0 or 1)
                ACK/NAK = 0 No response required
                ACK/NAK = 1 Response required
            - Message Responder (2 or 3)
                ACK/NAK = 2 Message negative acknowledge
                ACK/NAK = 3 Message acknowledged OK
        '''
        self.data_flags = 0
        ''':ivar data_flags:
            00: Only data packet in single-packet stream
            01: First data packet in multi-packet stream
            10: Normal (middle) data packet
            11: Last data packet in stream
        '''
        self._seqnr = 0
        ''':ivar seqnr:
         It starts at 0 with the first message and increments to 65535 and then wraps back to zero.
         Retransmitted packets because of a perceived Nak are transmitted with the same sequence number
         as the original transmission. The Sequence Number shall be presented in Little-Endian format.
        '''
        self._payload = b''

        # ---------------------------------------------------------------------
        # avoid serialization of the messages which are forward without modifications
        # ---------------------------------------------------------------------
        self._raw = b''
        # used to extract payload on changes after raw bytes are set
        self._raw_prefix_length = 0
        self._raw_size = 0

        # ---------------------------------------------------------------------
        # for sending
        # ---------------------------------------------------------------------
        self.tinfo_src = None
        ''':ivar tinfo_src:
            Receiver info is set by socket which receives this message.
        '''
        self.tinfo_dst = None
        ''':ivar tinfo_dst:
            The destination will be set by address book. If it is set the sockets should use this one to forward this message.
            If value is None the destination was not found and it should be send as broadcast message.
        '''
        # ---------------------------------------------------------------------
        # for statistics
        # ---------------------------------------------------------------------
        self.ts_receive = time.time()
        self.forward = False

    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, value):
        if self._priority == value:
            # skip to avoid re-serialization of the message
            return
        # extract payload for further packaging and drop raw data to force re-serialization of the message
        self._extract_payload(clear_raw=True)
        self._priority = value

    @property
    def bcast(self):
        return self._bcast

    @bcast.setter
    def bcast(self, value):
        if self._bcast == value:
            # skip to avoid re-serialization of the message
            return
        # extract payload for further packaging and drop raw data to force re-serialization of the message
        self._extract_payload(clear_raw=True)
        self._bcast = value

    @property
    def acknak(self):
        return self._acknak

    @acknak.setter
    def acknak(self, value):
        if self._acknak == value:
            # skip to avoid re-serialization of the message
            return
        # extract payload for further packaging and drop raw data to force re-serialization of the message
        self._extract_payload(clear_raw=True)
        self._acknak = value

    @property
    def seqnr(self):
        return self._seqnr

    @seqnr.setter
    def seqnr(self, value):
        if self._seqnr == value:
            # skip to avoid re-serialization of the message
            return
        # extract payload for further packaging and drop raw data to force re-serialization of the message
        self._extract_payload(clear_raw=True)
        self._seqnr = value

    def _extract_payload(self, clear_raw=True):
        if self._raw:
            if not self._payload:
                if self.version == Message.AS5669:
                    self.payload = self._raw[self._raw_prefix_length:]
                elif self.version == Message.AS5669A:
                    # skip last to bytes; it is sequence number
                    self.payload = self._raw[self._raw_prefix_length:len(
                        self._raw) - 2]
                else:
                    raise Exception(
                        "Message version not supported: %d" % self.version)
            if clear_raw:
                self._raw = b''
                self._raw_prefix_length = 0
                self._raw_size = 0

    @property
    def payload(self):
        self._extract_payload(clear_raw=False)
        return self._payload

    @payload.setter
    def payload(self, data):
        '''
        Sets the payload and updates the data size of the message.

        :param bytes|str data: bytes (str with python 2)
        '''
        self._data_size = len(data)  # + Message.header_size(self.version)
        self._payload = data

    def appendPayload(self, data):
        self._data_size += len(data)
        self._payload += data

    @staticmethod
    def header_size(version):
        if version == Message.AS5669:
            return 16
        elif version == Message.AS5669A:
            # header length without compression
            return 14
        else:
            raise ValueError("Unknow message version: %d" % version)

    @property
    def raw_size(self):
        return self._raw_size

    @property
    def msg_id(self):
        return self._msg_id

    def __repr__(self):
        result = "Message<"
        result += " version=%d" % self.version
        result += " cmd_code=%d" % self.cmd_code
        result += " message_type=%d" % self.message_type
        result += " src=%s" % self.src_id
        result += " dst=%s" % self.dst_id
        result += " prio=%s" % self.priority
        result += " bcast=%d" % self.bcast
        result += " acknak=%d" % self.acknak
        result += " data_flags=%d" % self.data_flags
        result += " seqnr=%d" % self.seqnr
        result += " data_size=%d" % self._data_size
        result += " raw_prefix_length=%d" % self._raw_prefix_length
        result += " len_msg=%d" % len(self._raw)
        result += ">"
        return result

    def set_raw(self, raw, header_length, with_version_byte):
        if with_version_byte:
            self._raw = raw
        else:
            if IS_PYTHON_2:
                self._raw = chr(self.version)
            else:
                self._raw = bytes(chr(self.version), 'ascii')
            self._raw += raw
        self._raw_prefix_length = header_length
        self._raw_size = len(self._raw)
        self._bcast_recved_size = self._raw_size

    def bytes(self, version=None, prepend_version=True):
        result = b''
        vers = version
        if vers is None:
            vers = self.version
        if self._raw and vers == self.version:
            if prepend_version:
                return self._raw
            else:
                return self._raw[1:]
        if prepend_version:
            if IS_PYTHON_2:
                result += chr(self.version)
            else:
                result += bytes(chr(self.version), 'ascii')
        if vers == Message.AS5669:
            # add header
            result += struct.pack('<H', 0)  # header compression
            result += struct.pack('>H', self._data_size +
                                  Message.header_size(vers))
            prio = 3 * self.priority + 3
            result += struct.pack('<BBHIIHH',
                                  (prio | (self.acknak) << 4),
                                  2,  # version
                                  self.cmd_code,  # command code
                                  self.dst_id.value,
                                  self.src_id.value,
                                  self._data_size | (self.data_flags_as_AS5669() << 12), self.seqnr)
            # add payload
            result += self.payload
            # no footer in v1
        else:
            # add header
            result += struct.pack('<BH', self.message_type,
                                  self._data_size + Message.header_size(vers))
            # TODO: support header compression, currently not
            # if IS_PYTHON_2:
            #     print("PACK", self.priority, self.bcast, self.acknak, self.data_flags, hex(ord(pp[0])))
            # else:
            #     print("PACK", self.priority, self.bcast, self.acknak, self.data_flags, hex(int(pp[0])))
            result += struct.pack('<B', (self.priority | (self.bcast << 2)
                                  | (self.acknak << 4) | (self.data_flags << 6)))
            result += struct.pack('<II', self.dst_id.value, self.src_id.value)
            # add payload
            result += self.payload
            # add footer
            result += struct.pack('<H', self.seqnr)
        self._raw = result
        self._raw_size = len(self._raw)
        return self._raw

    def data_flags_as_AS5669(self):
        # :note: ignore MiddleResentMsg and convert last to 8
        if self.data_flags == self.DF_LAST:
            return 8
        return self.data_flags

    def __lt__(self, other):
        return self.priority < other.priority
