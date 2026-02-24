# ****************************************************************************
#
# Copyright (c) 2014-2024 Fraunhofer FKIE
# Author: Alexander Tiderko
# License: MIT
#
# ****************************************************************************

from __future__ import division, absolute_import, print_function, unicode_literals

import errno
import socket
import threading
import time
import traceback

import fkie_iop_json_connector.queue as queue
from fkie_iop_json_connector.address_book import AddressBook
from fkie_iop_json_connector.jaus_address import JausAddress
from fkie_iop_json_connector.message_parser import MessageParser
from fkie_iop_json_connector.message import Message
from fkie_iop_json_connector.transport.net import getaddrinfo, localifs
from fkie_iop_json_connector.logger import MyLogger


class UDPucSocket(socket.socket):

    def __init__(self, port=0, router=None, address_book=None, interface='', logger_name='udp', default_dst=None, send_buffer=0, recv_buffer=65535, queue_length=0, loglevel='info'):
        '''
        Creates a socket, bind it to a given interface+port for unicast send/receive.
        IPv4 and IPv6 are supported.

        :param int port: the port to bind the socket. If zero an empty one will be used.
        :param router: class which provides `route_udp_msg(fkie_iop_node_manager.message.Message)` method. If `None` receive will be disabled.
        :param str interface: The interface to bind to. If empty, it binds to all interfaces
        :param tuple(str,int) default_dst: used for loopback to send messages to predefined destination.
        '''
        self._closed = False
        self.interface = interface
        self.port = port
        self._router = router
        self._address_book = address_book
        self._default_dst = default_dst
        self._locals = [ip for _ifname, ip in localifs()]
        self._locals.append('localhost')
        self._hostname = socket.gethostname()
        if not self._hostname:
            self._hostname = "localhost"
        else:
            self._locals.append(self._hostname)
        self.logger = MyLogger(f"{logger_name}[{interface}:{self.port}]", loglevel=loglevel)
        self._recv_buffer = 65535
        self._seqNr = 0
        if recv_buffer > 0 and recv_buffer <= 65535:
            self._recv_buffer = recv_buffer
        self._sender_endpoints = {}
        self.sock_5_error_printed = []
        # If interface isn't specified, try to find an non localhost interface to
        # get some info for binding. Otherwise use localhost
        # if not self.interface:
        #     ifaces = localifs()
        #     for iface in ifaces:
        #         if not (iface[1].startswith('127') or iface[1].startswith('::1')):
        #             self.interface = iface[1]
        #             break
        self.logger.info(
            f"+ Bind to unicast socket @({self.interface}:{self.port})")
        socket_type = socket.AF_INET
        bind_ip = self.interface
        if self.interface:
            addrInfo = getaddrinfo(self.interface)
            socket_type = addrInfo[0]
            bind_ip = addrInfo[4][0]
            # Configure socket type
        socket.socket.__init__(
            self, socket_type, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        # Bind to the port
        try:
            self.logger.debug(f"Ucast bind to: ({bind_ip}:{self.port})")
            self.bind((bind_ip, self.port))
        except socket.error as errObj:
            msg = str(errObj)
            self.logger.critical(
                f"Unable to bind unicast to interface: {bind_ip}, check that it exists: {msg}")
            raise
        if self.port == 0:
            self.port = self.getsockname()[1]
#         if send_buffer:
#             # update buffer size
#             old_bufsize = self.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
#             if old_bufsize != send_buffer:
#                 self.setsockopt(socket.SOL_SOCKET,
#                                 socket.SO_SNDBUF, send_buffer)
# #                self.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, send_buffer)
#                 bufsize = self.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
#                 self.logger.debug(
#                     f"Changed buffer size from {old_bufsize} to {bufsize}")
        self._parser_ucast = MessageParser(None, loglevel=loglevel)
        self._queue_send = queue.PQueue(
            queue_length, f"queue_{logger_name}_send", loglevel=loglevel)
        # create a thread to handle the received unicast messages
        if self._router is not None:
            self._thread_recv = threading.Thread(target=self._loop_recv)
            self._thread_recv.start()
        self._thread_send = threading.Thread(target=self._loop_send)
        self._thread_send.start()

    def close(self):
        """ Cleanup and close the socket"""
        self._closed = True
        self.logger.info("Close unicast socket")
        try:
            # shutdown to cancel recvfrom()
            socket.socket.shutdown(self, socket.SHUT_RD)
        except socket.error:
            pass
        socket.socket.close(self)
        self._queue_send.clear()

    def connectJausAddress(self, jausAddress: JausAddress):
        # send connect message
        connMsg = Message()
        connMsg.version = Message.AS5669
        connMsg.cmd_code = Message.CODE_CONNECT
        connMsg.src_id = jausAddress
        connMsg.tinfo_src = AddressBook.Endpoint(
            AddressBook.Endpoint.UDP, self._hostname, self.port)
        self.logger.info(f"send IOP connect message")
        self._queue_send.put(connMsg)

    def disconnectJausAddress(self, jausAddress: JausAddress):
        # send disconnect message
        connMsg = Message()
        connMsg.version = Message.AS5669
        connMsg.cmd_code = Message.CODE_CANCEL
        connMsg.src_id = jausAddress
        connMsg.tinfo_src = AddressBook.Endpoint(
            AddressBook.Endpoint.UDP, self._hostname, self.port)
        self._queue_send.put(connMsg)

    def send_queued(self, msg: Message):
        try:
            # test msg: "{"messageId": "46967", "jausIdDst": "127.255.255", "jausIdSrc": "127.100.1"}"
            # {"messageId": "2b00", "data": {"HeaderRec": { "MessageID": 11008}, "QueryIdentificationRec": {"QueryType": 1} }, "jausIdDst": "127.255.255", "jausIdSrc": "127.100.1"}
            # print(msg.messageId)
            # print(f"jausIdSrc: {msg.jausIdSrc}")
            # print(f"jausIdDst: {msg.jausIdDst}")
            # iopMsg = Message(msg.messageId)
            # iopMsg.src_id = JausAddress.from_string(msg.jausIdSrc)
            # iopMsg.dst_id = JausAddress.from_string(msg.jausIdDst)
            msg.tinfo_src = AddressBook.Endpoint(
                AddressBook.Endpoint.UDP, self._hostname, self.port)
            self._queue_send.put(msg)
        except queue.Full as full:
            print(traceback.format_exc())
            self.logger.warning(f"Can't send message: {full}")
        except Exception as e:
            self.logger.warning(f"Error while put message into queue: {e}")

    def _loop_send(self):
        while not self._closed:
            # Waits for next available Message. This method cancel waiting on clear() of PQueue and return None.
            msg = self._queue_send.get()
            if not self._closed and msg is not None:
                dst = msg.tinfo_dst
                if self._default_dst is not None:
                    if dst is None:
                        # it is a loopback socket, send to fictive debug destination
                        dst = AddressBook.Endpoint(
                            AddressBook.Endpoint.UDP, self._default_dst[0], self._default_dst[1])
                if dst is not None:
                    # send to given addresses
                    msg.seqnr = self._seqNr
                    self._seqNr += 1
                    self._sendto(msg.bytes(), dst.address, dst.port)
                # else:
                #     # send to local clients through UDP connections
                #     for local_dst in self._address_book.get_local_udp_destinations():
                #         self._sendto(msg, local_dst.address, local_dst.port)
                #     # send to all addresses defined in the configuration of the address book
                #     for entry in self._address_book.get_static_udp_entries(msg):
                #         self._sendto(msg, entry)
            # TODO: add retry mechanism?

    def _sendto(self, msg, addr, port):
        '''
        Sends the given message to the joined multicast group. Some errors on send
        will be ignored (``ENETRESET``, ``ENETDOWN``, ``ENETUNREACH``)

        :param str msg: message to send
        :param str addr: IPv4 or IPv6 address
        :param int port: destination port
        '''
        try:
            self.logger.debug("Send to %s:%d" % (addr, port))
            self.sendto(msg, (addr, port))
        except socket.error as errObj:
            msg = str(errObj)
            if errObj.errno in [-5]:
                if addr not in self.sock_5_error_printed:
                    self.logger.warning(
                        f"socket.error[{errObj.errno}]: {msg}, addr: {addr}")
                    self.sock_5_error_printed.append(addr)
            elif errObj.errno in [errno.EINVAL, -2]:
                raise
            elif errObj.errno not in [errno.ENETDOWN, errno.ENETUNREACH, errno.ENETRESET]:
                raise

    def _loop_recv(self):
        '''
        This method handles the received unicast messages.
        '''
        while not self._closed:
            try:
                data, address = self.recvfrom(65535) #self._recv_buffer)
                # data = self.recv(2048)
                # print(f"received: {address}, ({data})")
                if data and not self._closed:
                    msgs = self._parser_ucast.unpack(data)
                    # print(f"  count parsed {len(msgs)}")
                    for msg in msgs:
                        if msg.dst_id.zero or msg.cmd_code > 0:
                            # handle connection requests/closing
                            try:
                                if msg.cmd_code == Message.CODE_ACCEPT:
                                    self.nmConnected = True
                                # if msg.cmd_code == Message.CODE_CONNECT:
                                #     # Connection request from client.
                                #     self.logger.debug(
                                #         "Connection request from %s" % msg.src_id)
                                #     resp = Message()
                                #     resp.version = Message.AS5669
                                #     resp.dst_id = msg.src_id
                                #     resp.cmd_code = Message.CODE_ACCEPT
                                #     resp.ts_receive = time.time()
                                #     resp.tinfo_src = AddressBook.Endpoint(
                                #         AddressBook.Endpoint.UDP_LOCAL, self.mgroup, self.getsockname()[1])
                                #     resp.tinfo_dst = AddressBook.Endpoint(
                                #         AddressBook.Endpoint.UDP_LOCAL, address[0], address[1])
                                #     self.send_queued(resp)
                                elif msg.cmd_code == Message.CODE_CANCEL:
                                    self.nmConnected = False
                                    # Disconnect client.
                                    self.logger.debug(
                                        f"Disconnect request from {msg.src_id}")
                                    self._address_book.remove(msg.src_id)
                            except Exception as e:
                                import traceback
                                print(traceback.format_exc())
                                self.logger.warning(
                                    f"Error while handle connection management message: {e}")
                        else:
                            try:
                                msg.tinfo_src = self._sender_endpoints[address]
                                # print(f"  from: {msg.tinfo_src}")
                            except KeyError:
                                eType = AddressBook.Endpoint.UDP
                                if address[0] in self._locals:
                                    eType = AddressBook.Endpoint.UDP_LOCAL
                                endpoint = AddressBook.Endpoint(
                                    eType, address[0], address[1])
                                msg.tinfo_src = endpoint
                                self._sender_endpoints[address] = endpoint

                        self.logger.debug(f"Received {msg}")
                        self._router.route_udp_msg(msg)
            except queue.Full as full_error:
                self.logger.warning(
                    f"Error while process received unicast message: {full_error}")
            except socket.error:
                import traceback
                if not self._closed:
                    self.logger.warning(
                        f"unicast socket error: {traceback.format_exc()}")
