# ****************************************************************************
#
# Copyright (c) 2014-2024 Fraunhofer FKIE
# Author: Alexander Tiderko
# License: MIT
#
# ****************************************************************************


import struct
from fkie_iop_json_connector.jaus_address import JausAddress
from fkie_iop_json_connector.logger import MyLogger
from fkie_iop_json_connector.message import Message
from fkie_iop_json_connector.schemes import init_schemes, JSON_SCHEMES


def print_data(msg, data):
    print(msg, [ord(char) for char in data])


class MessageSerializer:

    MIN_PACKET_SIZE_V1 = 16
    MIN_PACKET_SIZE_V2 = 14
    JAUS_TYPES = {
        'byte': 1, 'short integer': 2, 'integer': 4, 'long integer': 8, 'unsigned byte': 1,
        'unsigned short integer': 2, 'unsigned integer': 4, 'unsigned long integer': 8,
        'float': 4, 'long float': 8, 'string': 1
    }
    PACK_FORMAT = {
        'byte': 'b',
        'short integer': '<h',
        'integer': '<i',
        'long integer': '<l',
        'unsigned byte': 'B',
        'unsigned short integer': '<H',
        'unsigned integer': '<I',
        'unsigned long integer': '<L',
        'float': '<f',
        'long float': '<d'
    }

    def __init__(self, schemesPath, loglevel='info'):
        '''
        :param str schemesPath: path to the directory with message schemes.
        '''
        self.logger = MyLogger('serializer', loglevel)
        # load json message schemes
        init_schemes(schemesPath, loglevel)

    def pack(self, jsonObj, message: Message) -> bool:
        """
        Pack JSON object into JAUS message binary payload.
        jsonObj: {
          "messageId": HEX-Value
          "messageName": Name of the message
          "data": JSON-message following schema
          "jausIdDst": "127.255.255"
          "jausIdSrc": "127.100.1"
        }
        """
        try:
            schemas = JSON_SCHEMES[jsonObj.messageId]
            message.src_id = JausAddress.from_string(jsonObj.jausIdSrc)
            message.dst_id = JausAddress.from_string(jsonObj.jausIdDst)
            if len(schemas) == 1:
                self._addProperties(jsonObj.data, message, schemas[0])
                return True
            else:
                for schema in schemas:
                    if schema.title == jsonObj.messageName:
                        self._addProperties(jsonObj.data, message, schema)
                        return True
        except:
            import traceback
            self.logger.error(traceback.format_exc())
            return False
        self.logger.warning(f"No schema found for message {jsonObj.messageId}")
        return False

    def unpack(self, message: Message):
        """
        Unpack JAUS binary payload into JSON object.
        result: {
          "messageId": HEX-Value
          "messageName": Name of the message
          "data": JSON-message following schema
          "jausIdDst": "127.255.255"
          "jausIdSrc": "127.100.1"
        }
        """
        msgId = f"{message.msg_id:x}".zfill(4)
        result = {"messageId": msgId,
                  "jausIdDst": message.dst_id.jaus_id,
                  "jausIdSrc": message.src_id.jaus_id}
        try:
            schemas = JSON_SCHEMES[msgId]
        except KeyError:
            self.logger.warning(f"No JSON schema for message {msgId}")
            return result

        for schema in schemas:
            try:
                self.logger.debug(f"parse message {schema.title}({msgId})")
                data = {}
                self._getProperties(data, message.payload, 0, schema)
                result["data"] = data
            except:
                import traceback
                self.logger.error(traceback.format_exc())
        return result

    # -----------------------------
    # Internal helpers
    # -----------------------------
    def _safe_pack(self, jausType, value):
        """Clip numeric values to prevent struct.pack overflow."""
        
        fmt = self._packFmt(jausType)
        limits = {
            'b': (-128, 127), 'B': (0, 255),
            '<h': (-32768, 32767), '<H': (0, 65535),
            '<i': (-2147483648, 2147483647), '<I': (0, 4294967295),
            '<l': (-9223372036854775808, 9223372036854775807),
            '<L': (0, 18446744073709551615)
        }
        if fmt in limits:
            minv, maxv = limits[fmt]
            if value < minv: value = minv
            if value > maxv: value = maxv
        return struct.pack(fmt, value)

    def _typeSize(self, jausType):
        return self.JAUS_TYPES[jausType]
    
    def _packFmt(self, jausType):
        return self.PACK_FORMAT[jausType]

    def _generatePresenceVector(self, jsonObj, schema):
        presenceVector = 0
        bit = 0
        for name in schema.properties.__dict__:
            if name == 'presenceVector':
                # handle presenceVector
                bit = 1
            elif name not in schema.required:
                if hasattr(jsonObj, name):
                    presenceVector |= bit
                bit <<= 1
        return presenceVector

    def _addProperties(self, jsonObj, message, schema):
        bitFieldValue = 0
        requiredProps = set(schema.required)
        for name, prop in schema.properties.__dict__.items():
            # print(f"property {name}: {prop.type}")
            if hasattr(jsonObj, name) and name in requiredProps:
                requiredProps.remove(name)
            if prop.type == 'object':
                # check if it is a payload object
                if hasattr(prop, 'fieldFormat'):
                    if prop.fieldFormat == 'JAUS MESSAGE':
                        try:
                            jsonAttr = None
                            if hasattr(jsonObj, name):
                                jsonAttr = getattr(jsonObj, name)
                            if jsonAttr is None:
                                raise Exception('no payload message specified')
                            schemas = JSON_SCHEMES[jsonAttr.payloadMessageId]
                            failed = []
                            for schema in schemas:
                                try:
                                    # on error we try to use a different schema
                                    payloadMessage = Message(int(schema.messageId, 16))
                                    self._addProperties(jsonAttr.payload, payloadMessage, schema)
                                    # set size of the payload message in the current message
                                    payloadData = payloadMessage.payload
                                    sizeData = self._safe_pack(prop.jausType, len(payloadData))
                                    # print(f"PAYLOAD_size: {len(payloadData)} -> {sizeData}")
                                    message.appendPayload(sizeData)
                                    message.appendPayload(payloadData)
                                except Exception as err:
                                    import traceback
                                    failed.append((schema.title, schema.messageId, traceback.format_exc()))
                            if len(schemas) == len(failed):
                                for (msgName, msgId, message) in failed:
                                    self.logger.warning(f"failed create IOP message {msgName} ({msgId}): {message}")
                        except Exception as err:
                            import traceback
                            print(traceback.format_exc())
                            raise err
                    else:
                        raise Exception(
                            f"payload format {prop.fieldFormat} not implemented!")
                else:
                    if hasattr(prop, 'bitField'):
                        # handle bit field, see AS5684
                        bitFieldResult = self._addProperties(
                            getattr(jsonObj, name), message, prop)
                        bitFieldData = self._safe_pack(prop.bitField, bitFieldResult)
                        message.appendPayload(bitFieldData)
                    else:
                        if hasattr(jsonObj, name) or name in requiredProps:
                            self._addProperties(
                                getattr(jsonObj, name), message, prop)

            elif prop.type == 'number':
                if name == 'presenceVector':
                    # handle presenceVector
                    presenceVector = self._generatePresenceVector(
                        jsonObj, schema)
                    sizeData = self._safe_pack(prop.jausType, presenceVector)
                    message.appendPayload(sizeData)
                else:
                    typeSize = self._typeSize(prop.jausType)
                    value = None
                    if hasattr(jsonObj, name):
                        value = getattr(jsonObj, name)
                    elif name in schema.required:
                        # it is not optional parameter, pack to the message
                        value = 0
                    # print(f"property {name}: {prop.type}, value: {value}")
                    if value is not None:
                        if hasattr(prop, 'scaleRange'):
                            # determine scaled value, see AS5684
                            value = int(round((value - prop.scaleRange.bias) /
                                              prop.scaleRange.scaleFactor, 0))
                            # print(f"  -> scaled value: {value}")
                        data = self._safe_pack(prop.jausType, value)
                        message.appendPayload(data)
                    if hasattr(prop, 'bitRange'):
                        # handle bit field, see AS5684
                        if hasattr(jsonObj, name):
                            value = getattr(jsonObj, name)
                            bitFieldValue += value >> getattr(prop.bitRange, 'from')
                        continue  # TODO
            elif prop.type == 'string':
                # JSON properties of string type could be: HEX (message id), value set, variable or const string.
                if name == 'MessageID' and hasattr(prop, 'const'):
                    # handle hex value of the message id
                    value = int(prop.const, 16)
                    # print("pack message id", prop.const, " ,,,,", value, "---", self._getPackFormat(prop.jausType))
                    data = self._safe_pack(prop.jausType, value)
                    message.appendPayload(data)
                elif hasattr(prop, 'enum') and hasattr(prop, 'valueSet'):
                    # handle value set
                    value = 0
                    valueStr = ''
                    if hasattr(jsonObj, name):
                        valueStr = getattr(jsonObj, name)
                        if isinstance(valueStr, int):
                            value = valueStr
                        else:
                            for enum in prop.valueSet:
                                if hasattr(enum, "valueEnum") and enum.valueEnum.enumConst == valueStr:
                                    value = enum.valueEnum.enumIndex
                    if hasattr(prop, 'bitRange'):
                        # handle bit field, see AS5684
                        bitFieldValue += value >> getattr(prop.bitRange, 'from')
                        continue
                    # print("pack value set", name, " ,,,,", value, "---", self._getPackFormat(prop.jausType))
                    data = self._safe_pack(prop.jausType, value)
                    message.appendPayload(data)
                elif hasattr(prop, 'minLength') and hasattr(prop, 'maxLength'):
                    # print("pack str", name)
                    strLength = 0
                    valueStr = ''
                    if hasattr(jsonObj, name):
                        valueStr = getattr(jsonObj, name)
                    if prop.minLength == prop.maxLength:
                        # handle constant string
                        strLength = prop.maxLength
                        valueStr = valueStr.ljust(strLength, '\x00')
                    else:
                        # handle variable string length
                        strLength = len(valueStr)
                        if strLength > prop.maxLength:
                            strLength = prop.maxLength
                        # add string length to message payload
                        data = self._safe_pack(prop.jausType, prop.maxLength)
                        message.appendPayload(data)
                    # add string itself to message payload
                    if strLength > 0:
                        data = struct.pack(f'{strLength}s', valueStr.encode('utf-8'))
                        message.appendPayload(data)
            elif prop.type == 'array':
                if hasattr(jsonObj, name):
                    arrayObj = getattr(jsonObj, name)
                # print("pack array, len: ", len(arrayObj))
                if not prop.isVariant:
                    # add size of the array
                    data = self._safe_pack(prop.jausType, len(arrayObj))
                    message.appendPayload(data)
                    for item in arrayObj:
                        # prop.items.anyOf[0]
                        self._addProperties(
                            item, message, prop.items.anyOf[0])
            else:
                self.logger.error(
                    f"ERROR: property {name}: {prop.type} not implemented")

            # TODO
        if len(requiredProps) > 0:
            raise AttributeError(f"missed fields {requiredProps}")
        return bitFieldValue

    def _getProperties(self, jsonObj, payload, payloadIndex, schema):
        # print(schema.properties.__dict__)
        presenceVector = None
        presenceIndex = 0
        index = payloadIndex
        for name, prop in schema.properties.__dict__.items():
            if presenceVector is not None:
                if name not in schema.required:
                    if not (presenceVector & presenceIndex):
                        # not in presence vector, skip unpack this value
                        # increase the presence index for next value
                        presenceIndex = presenceIndex << 1
                        continue
                    else:
                        # increase the presence index for next value
                        presenceIndex = presenceIndex << 1
            if prop.type == 'object':
                # check if it is a payload object
                if hasattr(prop, 'fieldFormat'):
                    if prop.fieldFormat == 'JAUS MESSAGE':
                        try:
                            typeSize = self._typeSize(prop.jausType)
                            (payloadSize, ) = struct.unpack(self._packFmt(
                                prop.jausType), payload[index:index+typeSize])
                            index += typeSize
                            if payloadSize >= 2:
                                # get message id of the payload
                                (msgId, ) = struct.unpack(self._packFmt(
                                    'unsigned short integer'), payload[index:index+2])
                                jsonPayloadObj = jsonObj[name] = {}
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
                            f"payload format {prop.fieldFormat} not implemented!")
                if not hasattr(jsonObj, name):
                    jsonObj[name] = {}
                index = self._getProperties(jsonObj[name], payload, index, prop)
                if hasattr(prop, 'bitField'):
                    # handle bit field, see AS5684
                    index += self._typeSize(prop.bitField)
            elif prop.type == 'number':
                typeSize = self._typeSize(prop.jausType)
                (value, ) = struct.unpack(self._packFmt(
                    prop.jausType), payload[index:index+typeSize])
                # print(f"property {name}: {prop.type}, value: {value}")
                if hasattr(prop, 'scaleRange'):
                    # determine real value, see AS5684
                    value = value * prop.scaleRange.scaleFactor + prop.scaleRange.bias
                    # print(f"  -> real value: {value}")
                if hasattr(prop, 'bitRange'):
                    # handle bit field, see AS5684
                    bitRangeValue = 0
                    for bit in range(getattr(prop.bitRange, 'from'), getattr(prop.bitRange, 'to')+1, 1):
                        bitRangeValue += int(value & 1 << bit)
                    bitRangeValue = bitRangeValue >> getattr(
                        prop.bitRange, 'from')
                    jsonObj[name] = bitRangeValue
                    continue  # TODO
                jsonObj[name] = value
                index += typeSize
                if name == 'presenceVector':
                    # this is a presence_vector field, get value and prepare to read next values depending on this bit field
                    presenceVector = value
                    # print(f"  found presence vector: {presenceVector:016b}")
                    presenceIndex = 1
            elif prop.type == 'string':
                # handle fixed length string first
                if hasattr(prop, "minLength") and hasattr(prop, "maxLength"):
                    if prop.minLength == prop.maxLength:
                        value = payload[index:index+prop.maxLength]
                        index += prop.maxLength
                        jsonObj[name] = value.decode().rstrip('\x00')
                        continue
                typeSize = self._typeSize(prop.jausType)
                (strLength, ) = struct.unpack(self._packFmt(
                    prop.jausType), payload[index:index+typeSize])
                index += typeSize
                if name == 'MessageID' and hasattr(prop, 'const'):
                    # handle hex value of the message id
                    jsonObj[name] = f'{strLength:x}'
                elif hasattr(prop, 'enum') and hasattr(prop, 'valueSet'):
                    # handle bit range
                    if hasattr(prop, 'bitRange'):
                        index -= typeSize
                        bitRangeValue = 0
                        for bit in range(getattr(prop.bitRange, 'from'), getattr(prop.bitRange, 'to')+1, 1):
                            bitRangeValue += int(strLength & 1 << bit)
                        bitRangeValue = bitRangeValue >> getattr(
                            prop.bitRange, 'from')
                        strLength = bitRangeValue
                    # handle value set
                    for enum in prop.valueSet:
                        if hasattr(enum, "valueEnum") and enum.valueEnum.enumIndex == int(strLength):
                            jsonObj[name] = enum.valueEnum.enumConst
                else:
                    # handle string value
                    value = payload[index:index+strLength]
                    index += strLength
                    jsonObj[name] = value.decode()
            elif prop.type == 'array':
                if hasattr(prop, "jausType"):
                    # list or variant
                    typeSize = self._typeSize(prop.jausType)
                    (arrLength, ) = struct.unpack(self._packFmt(
                        prop.jausType), payload[index:index+typeSize])
                    index += typeSize
                    if hasattr(prop, 'isVariant') and prop.isVariant:
                        # handle variant
                        index = self._getProperties(
                            jsonObj, payload, index, prop.items.anyOf[arrLength])
                    else:
                        # handle list
                        jsonObj[name] = []
                        for i in range(0, arrLength):
                            listItem = {}
                            index = self._getProperties(
                                listItem, payload, index, prop.items.anyOf[0])
                            jsonObj[name].append(listItem)
                elif prop.minItems == prop.maxItems:
                    # it is an array
                    arrLength = prop.maxItems
                    jsonObj[name] = []
                    for i in range(0, arrLength):
                        listItem = {}
                        index = self._getProperties(
                            listItem, payload, index, prop.items.anyOf[0])
                        jsonObj[name].append(listItem)
            else:
                self.logger.error(
                    f"property {name}: {prop.type} not implemented")
        return index

        # TODO
