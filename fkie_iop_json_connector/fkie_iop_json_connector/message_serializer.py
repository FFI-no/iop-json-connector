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
import struct
import sys
from types import SimpleNamespace
from fkie_iop_json_connector.jaus_address import JausAddress
from fkie_iop_json_connector.logger import MyLogger
from fkie_iop_json_connector.message import Message
from fkie_iop_json_connector.schemes import init_schemes, JSON_SCHEMES


def print_data(msg, data):
    print(msg, [ord(char) for char in data])


class MessageSerializer:

    MIN_PACKET_SIZE_V1 = 16
    MIN_PACKET_SIZE_V2 = 14
    JAUS_TYPES = {'byte': 1, 'short integer': 2, 'integer': 4, 'long integer': 8, 'unsigned byte': 1,
                  'unsigned short integer': 2, 'unsigned integer': 4, 'unsigned long integer': 8,
                  'float': 4, 'long float': 8, 'string': 1}
    PACK_FORMAT_FROM_JAUS = {'byte': 'b',
                             'short integer': '<h',
                             'integer': '<i',
                             'long integer': '<l',
                             'unsigned byte': 'B',
                             'unsigned short integer': '<H',
                             'unsigned integer': '<I',
                             'unsigned long integer': '<L',
                             'float': '<f',
                             'long float': '<d'}
    UNPACK_FORMAT_FROM_JAUS = {'byte': 'b',
                               'short integer': '<h',
                               'integer': '<i',
                               'long integer': '<l',
                               'unsigned byte': 'B',
                               'unsigned short integer': '<H',
                               'unsigned integer': '<I',
                               'unsigned long integer': '<L',
                               'float': '<f',
                               'long float': '<d'}

    def __init__(self, schemesPath, loglevel='info'):
        '''
        :param str schemesPath: path to the directory with message schemes.
        '''
        self.logger = MyLogger('serializer', loglevel)
        # load json message schemes
        init_schemes(schemesPath, loglevel)

    def pack(self, jsonObj, message: Message) -> bool:
        # Creates iop binary message from JSON object with this struct
        # {
        #   "messageId": HEX-Value
        #   "messageName": Name of the message
        #   "data": JSON-message following schema
        #   "jausIdDst": "127.255.255"
        #   "jausIdSrc": "127.100.1"
        # }
        try:
            schemas = JSON_SCHEMES[jsonObj.messageId]
            if len(schemas) == 1:
                message.src_id = JausAddress.from_string(jsonObj.jausIdSrc)
                message.dst_id = JausAddress.from_string(jsonObj.jausIdDst)
                self._addProperties(jsonObj.data, message, schemas[0])
                return True
            else:
                for schema in schemas:
                    if schema.title == jsonObj.messageName:
                        message.src_id = JausAddress.from_string(jsonObj.jausIdSrc)
                        message.dst_id = JausAddress.from_string(jsonObj.jausIdDst)
                        self._addProperties(jsonObj.data, message, schema)
                        return True
        except:
            import traceback
            print(traceback.format_exc())
        return False

    def unpack(self, message: Message):
        # Unpacks the IOP binary message and returns JSON object with this struct
        # {
        #   "messageId": HEX-Value
        #   "messageName": Name of the message
        #   "data": JSON-message following schema
        #   "jausIdDst": "127.255.255"
        #   "jausIdSrc": "127.100.1"
        # }
        msgId = f"{message.msg_id:x}".zfill(4)
        result = {"messageId": msgId,
                  "jausIdDst": message.dst_id.jaus_id,
                  "jausIdSrc": message.src_id.jaus_id}
        if message.msg_id != 0:
            try:
                schemas = JSON_SCHEMES[msgId]
            except KeyError:
                self.logger.warning(
                    f"No JSON schema for message {msgId} found!")
                import traceback
                print(traceback.format_exc())
            else:
                for schema in schemas:
                    try:
                        self.logger.debug(f"parse message {schema.title}({msgId})")
                        data = {}
                        self._getProperties(data, message.payload, 0, schema)
                        result["data"] = data
                    except:
                        import traceback
                        print(traceback.format_exc())

            # parse json schema
        return result

    def _generatePresenceVector(self, jsonObj, schema):
        presenceVector = 0
        presenceIndex = 0
        for propertyName in schema.properties.__dict__:
            if propertyName == 'presenceVector':
                # handle presenceVector
                presenceIndex = 1
            elif propertyName not in schema.required:
                if hasattr(jsonObj, propertyName):
                    presenceVector |= presenceIndex
                presenceIndex = presenceIndex << 1
        return presenceVector

    def _addProperties(self, jsonObj, message, schema):
        # print(schema.properties.__dict__)
        bitFieldValue = 0
        for propertyName in schema.properties.__dict__:
            property = getattr(schema.properties, propertyName)
            if property.type == 'object':
                # print(f"property {propertyName}: {property.type}")
                # check if it is a payload object
                if hasattr(property, 'fieldFormat'):
                    if property.fieldFormat == 'JAUS MESSAGE':
                        try:
                            jsonAttr = None
                            if hasattr(jsonObj, propertyName):
                                jsonAttr = getattr(jsonObj, propertyName)
                            if jsonAttr is None:
                                raise Exception('no payload message specified')
                            schemas = JSON_SCHEMES[jsonAttr.payloadMessageId]
                            for schema in schemas:
                                try:
                                    # on error we try to use a different schema
                                    payloadMessage = Message()
                                    self._addProperties(jsonAttr.payload, payloadMessage, schema)
                                    # set size of the payload message in the current message
                                    payloadData = payloadMessage.payload
                                    sizeData = struct.pack(self._getPackFormat(property.jausType), len(payloadData))
                                    # print(f"PAYLOAD_size: {len(payloadData)} -> {sizeData}")
                                    message.appendPayload(sizeData)
                                    message.appendPayload(payloadData)
                                except Exception as err:
                                    import traceback
                                    print(traceback.format_exc())
                        except Exception as err:
                            import traceback
                            print(traceback.format_exc())
                            raise err
                    else:
                        raise Exception(
                            f"payload format {property.fieldFormat} not implemented!")
                else:
                    if hasattr(property, 'bitField'):
                        # handle bit field, see AS5684
                        bitFieldResult = self._addProperties(
                            getattr(jsonObj, propertyName), message, property)
                        bitFieldData = struct.pack(self._getPackFormat(
                            property.bitField), bitFieldResult)
                        message.appendPayload(bitFieldData)
                    else:
                        self._addProperties(
                            getattr(jsonObj, propertyName), message, property)

            elif property.type == 'number':
                if propertyName == 'presenceVector':
                    # handle presenceVector
                    presenceVector = self._generatePresenceVector(
                        jsonObj, schema)
                    sizeData = struct.pack(self._getPackFormat(
                        property.jausType), presenceVector)
                    message.appendPayload(sizeData)
                else:
                    typeSize = self._getTypeSize(property.jausType)
                    value = None
                    if hasattr(jsonObj, propertyName):
                        value = getattr(jsonObj, propertyName)
                    elif propertyName in schema.required:
                        # it is not optional parameter, pack to the message
                        value = 0
                    # print(f"property {propertyName}: {property.type}, value: {value}")
                    if value is not None:
                        if hasattr(property, 'scaleRange'):
                            # determine scaled value, see AS5684
                            value = int(round((value - property.scaleRange.bias) /
                                              property.scaleRange.scaleFactor, 0))
                            # print(f"  -> scaled value: {value}")
                        data = struct.pack(self._getPackFormat(
                            property.jausType), value)
                        message.appendPayload(data)
                    if hasattr(property, 'bitRange'):
                        # handle bit field, see AS5684
                        if hasattr(jsonObj, propertyName):
                            value = getattr(jsonObj, propertyName)
                            bitFieldValue += value >> getattr(property.bitRange, 'from')
                        continue  # TODO
            elif property.type == 'string':
                # JSON properties of string type could be: HEX (message id), value set, variable or const string.
                if propertyName == 'MessageID' and hasattr(property, 'const'):
                    # handle hex value of the message id
                    value = int(property.const, 16)
                    # print("pack message id", property.const, " ,,,,", value, "---", self._getPackFormat(property.jausType))
                    data = struct.pack(self._getPackFormat(
                        property.jausType), value)
                    message.appendPayload(data)
                elif hasattr(property, 'enum') and hasattr(property, 'valueSet'):
                    # handle value set
                    value = 0
                    valueStr = ''
                    if hasattr(jsonObj, propertyName):
                        valueStr = getattr(jsonObj, propertyName)
                        if isinstance(valueStr, int):
                            value = valueStr
                        else:
                            for enum in property.valueSet:
                                if hasattr(enum, "valueEnum") and enum.valueEnum.enumConst == valueStr:
                                    value = enum.valueEnum.enumIndex
                    if hasattr(property, 'bitRange'):
                        # handle bit field, see AS5684
                        bitFieldValue += value >> getattr(property.bitRange, 'from')
                        continue
                    # print("pack value set", propertyName, " ,,,,", value, "---", self._getPackFormat(property.jausType))
                    data = struct.pack(self._getPackFormat(
                        property.jausType), value)
                    message.appendPayload(data)
                elif hasattr(property, 'minLength') and hasattr(property, 'maxLength'):
                    # print("pack str", propertyName)
                    strLength = 0
                    valueStr = ''
                    if hasattr(jsonObj, propertyName):
                        valueStr = getattr(jsonObj, propertyName)
                    if property.minLength == property.maxLength:
                        # handle constant string
                        strLength = property.maxLength
                        valueStr = valueStr.ljust(strLength, '\x00')
                    else:
                        # handle variable string length
                        strLength = len(valueStr)
                        if strLength > property.maxLength:
                            strLength = property.maxLength
                        # add string length to message payload
                        data = struct.pack(self._getPackFormat(
                            property.jausType), property.maxLength)
                        message.appendPayload(data)
                    # add string itself to message payload
                    if strLength > 0:
                        data = struct.pack(f'{strLength}s', valueStr)
                        message.appendPayload(data)
            elif property.type == 'array':
                if hasattr(jsonObj, propertyName):
                    arrayObj = getattr(jsonObj, propertyName)
                # print("pack array, len: ", len(arrayObj))
                if not property.isVariant:
                    # add size of the array
                    data = struct.pack(self._getPackFormat(
                        property.jausType), len(arrayObj))
                    message.appendPayload(data)
                    for item in arrayObj:
                        # property.items.anyOf[0]
                        self._addProperties(
                            item, message, property.items.anyOf[0])
            else:
                self.logger.error(
                    f"ERROR: property {propertyName}: {property.type} not implemented")

            # TODO
        return bitFieldValue

    def _getTypeSize(self, jausType):
        return self.JAUS_TYPES[jausType]

    def _getPackFormat(self, jausType):
        return self.PACK_FORMAT_FROM_JAUS[jausType]

    def _getUnPackFormat(self, jausType):
        return self.UNPACK_FORMAT_FROM_JAUS[jausType]

    def _getProperties(self, jsonObj, payload, payloadIndex, schema):
        # print(schema.properties.__dict__)
        presenceVector = None
        presenceIndex = 0
        index = payloadIndex
        for propertyName in schema.properties.__dict__:
            property = getattr(schema.properties, propertyName)
            if presenceVector is not None:
                if propertyName not in schema.required:
                    if not (presenceVector & presenceIndex):
                        # not in presence vector, skip unpack this value
                        # increase the presence index for next value
                        presenceIndex = presenceIndex << 1
                        continue
                    else:
                        # increase the presence index for next value
                        presenceIndex = presenceIndex << 1
            if property.type == 'object':
                # check if it is a payload object
                if hasattr(property, 'fieldFormat'):
                    if property.fieldFormat == 'JAUS MESSAGE':
                        try:
                            typeSize = self._getTypeSize(property.jausType)
                            (payloadSize, ) = struct.unpack(self._getUnPackFormat(
                                property.jausType), payload[index:index+typeSize])
                            index += typeSize
                            if payloadSize >= 2:
                                # get message id of the payload
                                (msgId, ) = struct.unpack(self._getUnPackFormat(
                                    'unsigned short integer'), payload[index:index+2])
                                jsonPayloadObj = jsonObj[propertyName] = {}
                                jsonPayloadObj['payloadMessageId'] = f'{msgId:x}'
                                # unpack payload message
                                schemas = JSON_SCHEMES[jsonPayloadObj['payloadMessageId']]
                                for schema in schemas:
                                    try:
                                        self.logger.debug(
                                            f"parse payload message {schema.title}({jsonPayloadObj['payloadMessageId']})")
                                        jsonPayloadObj['payload'] = {}
                                        self._getProperties(
                                            jsonPayloadObj['payload'], payload, index, schema)
                                        return
                                    except:
                                        pass
                        except:
                            import traceback
                            print(traceback.format_exc())
                    else:
                        raise Exception(
                            f"payload format {property.fieldFormat} not implemented!")
                if not hasattr(jsonObj, propertyName):
                    jsonObj[propertyName] = {}
                index = self._getProperties(jsonObj[propertyName],
                                            payload, index, property)
                if hasattr(property, 'bitField'):
                    # handle bit field, see AS5684
                    index += self._getTypeSize(property.bitField)
            elif property.type == 'number':
                typeSize = self._getTypeSize(property.jausType)
                (value, ) = struct.unpack(self._getUnPackFormat(
                    property.jausType), payload[index:index+typeSize])
                # print(f"property {propertyName}: {property.type}, value: {value}")
                if hasattr(property, 'scaleRange'):
                    # determine real value, see AS5684
                    value = value * property.scaleRange.scaleFactor + property.scaleRange.bias
                    # print(f"  -> real value: {value}")
                if hasattr(property, 'bitRange'):
                    # handle bit field, see AS5684
                    bitRangeValue = 0
                    for bit in range(getattr(property.bitRange, 'from'), getattr(property.bitRange, 'to')+1, 1):
                        bitRangeValue += int(value & 1 << bit)
                    bitRangeValue = bitRangeValue >> getattr(
                        property.bitRange, 'from')
                    jsonObj[propertyName] = bitRangeValue
                    continue  # TODO
                jsonObj[propertyName] = value
                index += typeSize
                if propertyName == 'presenceVector':
                    # this is a presence_vector field, get value and prepare to read next values depending on this bit field
                    presenceVector = value
                    # print(f"  found presence vector: {presenceVector:016b}")
                    presenceIndex = 1
            elif property.type == 'string':
                # handle fixed length string first
                if hasattr(property, "minLength") and hasattr(property, "maxLength"):
                    if property.minLength == property.maxLength:
                        value = payload[index:index+property.maxLength]
                        index += property.maxLength
                        jsonObj[propertyName] = value.decode().rstrip('\x00')
                        continue
                typeSize = self._getTypeSize(property.jausType)
                (strLength, ) = struct.unpack(self._getUnPackFormat(
                    property.jausType), payload[index:index+typeSize])
                index += typeSize
                if propertyName == 'MessageID' and hasattr(property, 'const'):
                    # handle hex value of the message id
                    jsonObj[propertyName] = f'{strLength:x}'
                elif hasattr(property, 'enum') and hasattr(property, 'valueSet'):
                    # handle bit range
                    if hasattr(property, 'bitRange'):
                        index -= typeSize
                        bitRangeValue = 0
                        for bit in range(getattr(property.bitRange, 'from'), getattr(property.bitRange, 'to')+1, 1):
                            bitRangeValue += int(strLength & 1 << bit)
                        bitRangeValue = bitRangeValue >> getattr(
                            property.bitRange, 'from')
                        strLength = bitRangeValue
                    # handle value set
                    for enum in property.valueSet:
                        if hasattr(enum, "valueEnum") and enum.valueEnum.enumIndex == int(strLength):
                            jsonObj[propertyName] = enum.valueEnum.enumConst
                else:
                    # handle string value
                    value = payload[index:index+strLength]
                    index += strLength
                    jsonObj[propertyName] = value.decode()
            elif property.type == 'array':
                if hasattr(property, "jausType"):
                    # list or variant
                    typeSize = self._getTypeSize(property.jausType)
                    (arrLength, ) = struct.unpack(self._getUnPackFormat(
                        property.jausType), payload[index:index+typeSize])
                    index += typeSize
                    if hasattr(property, 'isVariant') and property.isVariant:
                        # handle variant
                        index = self._getProperties(
                            jsonObj, payload, index, property.items.anyOf[arrLength])
                    else:
                        # handle list
                        jsonObj[propertyName] = []
                        for i in range(0, arrLength):
                            listItem = {}
                            index = self._getProperties(
                                listItem, payload, index, property.items.anyOf[0])
                            jsonObj[propertyName].append(listItem)
                elif property.minItems == property.maxItems:
                    # it is an array
                    arrLength = property.maxItems
                    jsonObj[propertyName] = []
                    for i in range(0, arrLength):
                        listItem = {}
                        index = self._getProperties(
                            listItem, payload, index, property.items.anyOf[0])
                        jsonObj[propertyName].append(listItem)
            else:
                self.logger.error(
                    f"property {propertyName}: {property.type} not implemented")
        return index

        # TODO
