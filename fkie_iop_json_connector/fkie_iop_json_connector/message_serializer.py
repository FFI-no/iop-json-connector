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
        self.logger = MyLogger('serializer', loglevel=loglevel)
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
        schemas = JSON_SCHEMES[jsonObj.messageId]
        message.src_id = JausAddress.from_string(jsonObj.jausIdSrc)
        message.dst_id = JausAddress.from_string(jsonObj.jausIdDst)
        if len(schemas) == 1:
            try:
                self._addProperties(jsonObj.data, message, schemas[0])
                return True
            except:
                import traceback
                self.logger.error(f"{schemas[0].title}.{schemas[0].messageId}: {traceback.format_exc()}")
        else:
            for schema in schemas:
                try:
                    if schema.title == jsonObj.messageName:
                        self._addProperties(jsonObj.data, message, schema)
                        return True
                except:
                    import traceback
                    self.logger.error(f"{schema.title}.{schema.messageId}: {traceback.format_exc()}")
        if len(schemas) == 0:
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
            if len(message.payload) == 0:
                self.logger.warning(f"Empty payload for message {msgId}")
            if msgId == 0000:
                schemas = []
            else:
                schemas = JSON_SCHEMES[msgId]
        except KeyError:
            self.logger.warning(f"No JSON schema for message {msgId}; payload length: {len(message.payload)}")
            return result

        for schema in schemas:
            try:
                self.logger.debug(f"parse message {schema.title}({msgId})")
                data = {}
                self._getProperties(data, message.payload, 0, schema)
                result["data"] = data
            except:
                import traceback
                self.logger.error(f"{schema.title}.{schema.messageId}: {traceback.format_exc()}")
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
            if value < minv:
                value = minv
            if value > maxv:
                value = maxv
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

    def _addProperties(self, jsonObj, message, schema, filter=[]):
        bitFieldValue = 0
        requiredProps = set(schema.required) if len(filter) == 0 else set(filter)
        for name, prop in schema.properties.__dict__.items():
            if len(filter) > 0 and name not in filter:
                continue
            # print(f"property {name}: {prop.type}")
            if hasattr(jsonObj, name) and name in requiredProps:
                requiredProps.remove(name)
            if prop.type == 'object':
                # check if it is a payload object
                if hasattr(prop, 'encapsulatedMessage'):
                    jsonAttr = None
                    if hasattr(jsonObj, name):
                        jsonAttr = getattr(jsonObj, name)
                    if jsonAttr is None:
                        raise Exception('no payload message specified')

                    # There are two different ways to specify the encapsulated message
                    fieldFormat = "unknown"
                    if prop.encapsulatedMessage == 'simple':
                        # e.g. variable_length_field
                        fieldFormat = prop.fieldFormat
                    else:
                        # e.g. variable_format_field with formatField
                        fieldFormat = jsonAttr.formatField
                        self._addProperties(getattr(jsonObj, name), message, prop, filter=["formatField"])

                    if fieldFormat in ['JAUS MESSAGE', 'JAUS_MESSAGE']:
                        try:
                            schemas = JSON_SCHEMES[jsonAttr.payloadMessageId]
                            failed = []
                            for schema in schemas:
                                try:
                                    # On error we try to use a different schema
                                    payloadMessage = Message(int(schema.messageId, 16))
                                    self._addProperties(jsonAttr.payload, payloadMessage, schema)
                                    payloadData = payloadMessage.payload

                                    # Enforce length constraints from schema (variable_length_field: minCount/maxCount)
                                    payload_len = len(payloadData)
                                    min_count = getattr(prop, 'minCount', None)
                                    max_count = getattr(prop, 'maxCount', None)
                                    if min_count is not None and payload_len < min_count:
                                        self.logger.warning(
                                            f"payload length {payload_len} < minCount {min_count} for '{name}'"
                                        )
                                    if max_count is not None and payload_len > max_count:
                                        self.logger.warning(
                                            f"payload length {payload_len} > maxCount {max_count} for '{name}', truncating"
                                        )
                                        payload_len = max_count
                                        payloadData = payloadData[:max_count]

                                    # Write payload length to parent message
                                    sizeData = self._safe_pack(prop.jausType, payload_len)
                                    message.appendPayload(sizeData)
                                    # Write payload data
                                    message.appendPayload(payloadData)
                                except Exception:
                                    import traceback
                                    failed.append((schema.title, schema.messageId, traceback.format_exc()))
                            if len(schemas) == len(failed):
                                for (msgName, msgId, msg) in failed:
                                    self.logger.warning(f"failed create IOP message {msgName} ({msgId}): {msg}")
                        except Exception:
                            import traceback
                            print(traceback.format_exc())
                            raise
                    else:
                        # TODO: pack payload data to user-defined format
                        raise Exception(
                            f"Error in attribute '{name}': payload format {fieldFormat} not implemented! prop: {jsonObj}"
                        )
                    continue

                # -------- isVariant handling (packing) --------
                if hasattr(prop, "isVariant") and prop.isVariant:
                    # jsonObjVar is the value of the variant field (e.g., CostMap2DPoseVar)
                    if not hasattr(jsonObj, name):
                        continue
                        # raise AttributeError(f"no variant value set for '{name}'")
                    jsonObjVar = getattr(jsonObj, name)

                    # Determine which alternative is selected:
                    # either dict with exactly one key or object with one non‑None attribute
                    if isinstance(jsonObjVar, dict):
                        if len(jsonObjVar) != 1:
                            raise AttributeError(f"variant '{name}' must have exactly one selected alternative")
                        variant_key, variant_value = next(iter(jsonObjVar.items()))
                    else:
                        variant_key = None
                        variant_value = None
                        for k in vars(prop.properties).keys():
                            if hasattr(jsonObjVar, k):
                                v = getattr(jsonObjVar, k)
                                if v is not None:
                                    variant_key = k
                                    variant_value = v
                                    break
                        if variant_key is None:
                            continue
                            # raise AttributeError(f"no alternative selected for variant '{name}'")

                    # Determine variant index (ordering of properties defines index)
                    prop_keys = list(vars(prop.properties).keys())
                    try:
                        variant_index = prop_keys.index(variant_key)
                    except ValueError:
                        raise AttributeError(f"unknown variant key '{variant_key}' for '{name}'")

                    # Write variant index to payload
                    if not hasattr(prop, "jausType"):
                        raise AttributeError(f"variant '{name}' has no jausType for index field")
                    idx_data = self._safe_pack(prop.jausType, variant_index)
                    message.appendPayload(idx_data)

                    # Get schema of selected alternative
                    variantSchema = getattr(prop.properties, variant_key)

                    # If the selected alternative is an array (e.g., CostMap2DDataVar.*List)
                    if hasattr(variantSchema, "items") and getattr(variantSchema, "type", None) == 'array':
                        array_value = variant_value
                        if not isinstance(array_value, (list, tuple)):
                            raise AttributeError(f"variant '{name}.{variant_key}' must be a list/array")

                        # Write array length
                        if not hasattr(variantSchema, "jausType"):
                            raise AttributeError(f"array variant '{name}.{variant_key}' has no jausType for length field")
                        len_data = self._safe_pack(variantSchema.jausType, len(array_value))
                        message.appendPayload(len_data)

                        # Pack each array element using the element schema
                        for item in array_value:
                            self._addProperties(item, message, variantSchema.items.anyOf[0])
                    else:
                        # Normal object as variant alternative
                        self._addProperties(variant_value, message, variantSchema)

                    # Skip normal object handling for this property
                    continue
                # -------- end isVariant handling --------

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

    def _getProperties(self, jsonObj, payload, payloadIndex, schema, filter=[]):
        # print(schema.properties.__dict__)
        presenceVector = None
        presenceIndex = 0
        index = payloadIndex

        for name, prop in schema.properties.__dict__.items():
            if len(filter) > 0 and name not in filter:
                continue
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
                if hasattr(prop, 'encapsulatedMessage'):
                    jsonPayloadObj = jsonObj.setdefault(name, {})
                    fieldFormat = "unknown"

                    if prop.encapsulatedMessage == 'simple':
                        # e.g. variable_length_field
                        fieldFormat = prop.fieldFormat
                    else:
                        # e.g. variable_format_field: read formatField first
                        formatFieldProp = getattr(prop.properties, "formatField")
                        typeSize = self._typeSize(formatFieldProp.jausType)
                        (rawFormat, ) = struct.unpack(
                            self._packFmt(formatFieldProp.jausType),
                            payload[index:index+typeSize]
                        )
                        index += typeSize

                        if hasattr(formatFieldProp, 'enum') and hasattr(formatFieldProp, 'valueSet'):
                            # Optional: handle bitRange on formatField
                            if hasattr(formatFieldProp, 'bitRange'):
                                index -= typeSize
                                bitRangeValue = 0
                                for bit in range(getattr(formatFieldProp.bitRange, 'from'),
                                                getattr(formatFieldProp.bitRange, 'to') + 1):
                                    bitRangeValue += int(rawFormat & (1 << bit))
                                rawFormat = bitRangeValue >> getattr(formatFieldProp.bitRange, 'from')

                            # Map numeric value to enum constant
                            for enum in formatFieldProp.valueSet:
                                if hasattr(enum, "valueEnum") and enum.valueEnum.enumIndex == int(rawFormat):
                                    jsonPayloadObj["formatField"] = enum.valueEnum.enumConst
                                    fieldFormat = enum.valueEnum.enumConst

                    # Read size of encapsulated payload
                    typeSize = self._typeSize(prop.jausType)
                    (payloadSize, ) = struct.unpack(
                        self._packFmt(prop.jausType),
                        payload[index:index+typeSize]
                    )
                    index += typeSize

                    # Enforce length constraints (variable_length_field: minCount/maxCount)
                    min_count = getattr(prop, 'minCount', None)
                    max_count = getattr(prop, 'maxCount', None)
                    if min_count is not None and payloadSize < min_count:
                        self.logger.warning(
                            f"payload size {payloadSize} < minCount {min_count} for '{name}'"
                        )
                    if max_count is not None and payloadSize > max_count:
                        self.logger.warning(
                            f"payload size {payloadSize} > maxCount {max_count} for '{name}', truncating"
                        )
                        payloadSize = max_count

                    if fieldFormat in ['JAUS MESSAGE', 'JAUS_MESSAGE']:
                        if payloadSize >= 2:
                            # Read message id of payload
                            (msgId, ) = struct.unpack(
                                self._packFmt('unsigned short integer'),
                                payload[index:index+2]
                            )
                            jsonPayloadObj['payloadMessageId'] = f'{msgId:x}'.zfill(4)

                            # Unpack payload message
                            schemas = JSON_SCHEMES[jsonPayloadObj['payloadMessageId']]
                            for schema in schemas:
                                try:
                                    self.logger.debug(
                                        f"parse payload message {schema.title}({jsonPayloadObj['payloadMessageId']})"
                                    )
                                    jsonPayloadObj['payload'] = {}
                                    index = self._getProperties(
                                        jsonPayloadObj['payload'], payload, index, schema
                                    )
                                    return index
                                except Exception:
                                    pass
                    else:
                        # Generic user-defined format: store raw payload bytes
                        jsonPayloadObj['payload'] = payload[index:index+payloadSize]
                        index += payloadSize
                        return index

                if hasattr(prop, "isVariant") and prop.isVariant:
                    # Get array length (variant or normal)
                    if not hasattr(prop, "jausType"):
                        raise AttributeError(f"variant '{name}' has no jausType for index field")
                    typeSize = self._typeSize(prop.jausType)
                    (variantIndex, ) = struct.unpack(
                        self._packFmt(prop.jausType),
                        payload[index:index+typeSize]
                    )
                    index += typeSize
                    key_index = list(vars(prop.properties).keys())[variantIndex]
                    variantSchema = getattr(prop.properties, key_index)

                    if hasattr(variantSchema, "items") and variantSchema.type == 'array':
                        if hasattr(variantSchema, "jausType"):
                            typeSize = self._typeSize(variantSchema.jausType)
                            (arrLength, ) = struct.unpack(self._packFmt(
                                variantSchema.jausType), payload[index:index+typeSize])
                            index += typeSize
                            # handle list
                            jsonObj[name] = {key_index: []}
                            for i in range(0, arrLength):
                                listItem = {}
                                index = self._getProperties(
                                    listItem, payload, index, variantSchema.items.anyOf[0])
                                jsonObj[name][key_index].append(listItem)
                            continue
                    # Store the variant under the first required field name
                    # For variant, usually only one element (according to schema), adapt if more
                    element = {}
                    index = self._getProperties(element, payload, index, variantSchema)
                    jsonObj[name] = {key_index: element}
                    continue
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
                    jsonObj[name] = f'{strLength:x}'.zfill(4)
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
                    found_index = False
                    for enum in prop.valueSet:
                        if hasattr(enum, "valueEnum") and enum.valueEnum.enumIndex == int(strLength):
                            jsonObj[name] = enum.valueEnum.enumConst
                            found_index = True
                    if not found_index:
                        jsonObj[name] = strLength
                else:
                    # handle string value
                    value = payload[index:index+strLength]
                    index += strLength
                    jsonObj[name] = value.decode()
            elif prop.type == 'array':
                # print(f"add array: {name}")
                if hasattr(prop, "jausType"):
                    typeSize = self._typeSize(prop.jausType)
                    (arrLength, ) = struct.unpack(self._packFmt(
                        prop.jausType), payload[index:index+typeSize])
                    index += typeSize
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
