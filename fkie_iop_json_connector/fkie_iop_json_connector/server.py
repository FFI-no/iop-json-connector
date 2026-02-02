# ****************************************************************************
#
# Copyright (c) 2014-2024 Fraunhofer FKIE
# Author: Alexander Tiderko
# License: MIT
#
# ****************************************************************************

import fnmatch
import json
import os
import threading
import time
from types import SimpleNamespace
from typing import Tuple
from typing import Union
from urllib.parse import urlparse

from simple_websocket_server import WebSocketServer, WebSocket

from fkie_iop_json_connector.address_book import AddressBook
from fkie_iop_json_connector.jaus_address import JausAddress
from fkie_iop_json_connector.logger import MyLogger
from fkie_iop_json_connector.message import Message
from fkie_iop_json_connector.message_serializer import MessageSerializer
from fkie_iop_json_connector.transport.udp_uc import UDPucSocket


class SelfEncoder(json.JSONEncoder):
    def default(self, obj):
        result = {}
        for key, value in vars(obj).items():
            if key[0] != '_':
                result[key] = value
        return result


loggerWS = None


class WsClientHandler(WebSocket):
    msgSerializer = None
    udpSocket = None
    clients = []

    # store all jaus ids received via this handler and connect/disconnect to/from iop node manager
    jausAddresses = []

    def handle(self):
        # for client in WsClientHandler.clients:
        #     if client != self:
        #         client.send_message(self.address[0] + u' - ' + self.data)
        if (self.udpSocket is not None):
            try:
                msg = json.loads(
                    self.data, object_hook=lambda d: SimpleNamespace(**d))
                printLog = self.logger.message(msg, "recv WS")
                jAddr = JausAddress.from_string(msg.jausIdSrc)
                if jAddr not in self.jausAddresses:
                    self.jausAddresses.append(jAddr)
                    # TODO wait for accept from node manager before put it into jausAddresses
                    self.udpSocket.connectJausAddress(jAddr)
                iopMsg = Message(msg.messageId)
                if self.msgSerializer.pack(msg, iopMsg):
                    self.udpSocket.send_queued(iopMsg)
            except:
                import traceback
                print(traceback.format_exc())

    def connected(self):
        try:
            global loggerWS
            self.logger = loggerWS
            self.logger.info(f"{self.address} connected")
        except:
            import traceback
            print(traceback.format_exc())
        # for client in WsClientHandler.clients:
        #     client.send_message(self.address[0] + u' - connected')
        self.clients.append(self)

    def handle_close(self):
        self.clients.remove(self)
        # disconnect from iop node manager
        for jAddr in self.jausAddresses:
            self.udpSocket.disconnectJausAddress(jAddr)
        self.jausAddresses.clear()
        self.logger.info(f'{self.address} closed')
        for client in self.clients:
            client.send_message(self.address[0] + u' - disconnected')


class Server():

    def __init__(self, *, port: int, iopUri: str, logLevel: str = 'info', schemesPath='', logMessages=[], version: str = ''):
        self.logLevel = logLevel
        self.logMessages = logMessages
        self.logger = MyLogger('server', loglevel=self.logLevel, logMessages=logMessages)
        global loggerWS
        loggerWS = MyLogger('ws', loglevel=self.logLevel, logMessages=logMessages)
        self.address_book = AddressBook()
        self.wsPort = port
        self.iopScheme, self.iopHost, self.iopPort = self.splitUri(iopUri)
        self.schemesPath = schemesPath
        self._stop = False
        self._server = None
        self._udp = None
        self._lock = threading.RLock()
        self._threadServeForever = None

    def start(self, block=True):
        self._udp = UDPucSocket(self.wsPort+1, router=self, address_book=self.address_book, default_dst=(self.iopHost, self.iopPort), interface=self.iopHost,
                                send_buffer=0, recv_buffer=0, queue_length=0, loglevel=self.logger.level())
        WsClientHandler.msgSerializer = MessageSerializer(
            self.schemesPath, self.logLevel)
        WsClientHandler.udpSocket = self._udp
        self.logger.info("+ Bind to websocket @(%s:%s)" % ('0.0.0.0', self.wsPort))
        self._server = WebSocketServer('0.0.0.0', self.wsPort, WsClientHandler)
        self._threadServeForever = threading.Thread(
            target=self._server.serve_forever, daemon=True)
        self._threadServeForever.start()
        # TODO use TCP
        try:
            while not self._stop and block:
                time.sleep(1)
        except KeyboardInterrupt:
            print("caught keyboard interrupt, exiting")

    def shutdown(self):
        print('shutdown server')
        self._stop = True
        if self._udp is not None:
            try:
                WsClientHandler.udpSocket = None
                self._udp.close()
            except Exception as err:
                print("Exception while close udp interfaces: ", err)
        print('  ... server stopped')

    def splitUri(self, uri: str) -> Union[Tuple[str, str, int], None]:
        '''
        Splits URI or address into scheme, address and port and returns them as tuple.
        Scheme or tuple are empty if no provided.
        :param str uri: some URI or address
        :rtype: (str, str, int)
        '''
        (scheme, hostname, port) = ('', '', -1)
        if uri is None:
            return None
        if not uri:
            return uri
        try:
            o = urlparse(uri)
            scheme = o.scheme
            hostname = o.hostname
            port = o.port
        except AttributeError:
            pass
        if hostname is None:
            res = uri.split(':')
            if len(res) == 2:
                hostname = res[0]
                port = res[1]
            elif len(res) == 3:
                if res[0] == 'SHM':
                    hostname = 'localhost'
                    port = res[2]
                else:
                    # split if more than one address
                    hostname = res[1].strip('[]')
                    port = res[2]
            elif len(res) == 4 and res[1] == 'SHM':
                hostname = 'localhost'
                port = res[3]
            else:
                hostname = uri
                port = -1
        try:
            port = int(port)
        except TypeError:
            port = -1
        return (scheme, hostname, port)

    def route_udp_msg(self, msg):
        jsonObj = WsClientHandler.msgSerializer.unpack(msg)
        printLog = self.logger.message(jsonObj, "recv UDP")
        for client in WsClientHandler.clients:
            if printLog:
                self.logger.info(f"  -> forward to: {client.jausAddresses}")
            client.send_message(json.dumps(jsonObj, cls=SelfEncoder))
