var Int64 = require("int64-native");
require("bufferext");

function parse(schema, data, offset) {
    offset = offset || 0;
    var startOffset = offset,
        result = {},
        fields = schema.fields || [],
        field, i, j, numElements, elements, element, elementSchema,
        bytes, string, length, value, flags, flag;

    for (i=0;i<fields.length;i++) {
        field = fields[i];
        switch (field.type) {
            case "array":
                elements = [];
                if ("length" in field) {
                    numElements = field.length;
                } else {
                    numElements = data.readUInt32LE(offset);
                    offset += 4;
                }
                if ("debuglength" in field) {
                    numElements = field.debuglength;
                }
                if (field.elementSchema) {
                    for (j=0;j<numElements;j++) {
                        element = parse(field.elementSchema, data, offset);
                        offset += element.length;
                        elements.push(element.result);
                    }
                } else if (field.elementType) {
                    elementSchema = {
                        fields: [{name: "element", type: field.elementType}]
                    };
                    for (j=0;j<numElements;j++) {
                        element = parse(elementSchema, data, offset);
                        offset += element.length;
                        elements.push(element.result.element);
                    }
                }
                result[field.name] = elements;
                break;
            case "debugoffset":
                result[field.name] = offset;
                break;
            case "debugbytes":
                result[field.name] = data.readBytes(offset, field.length);
                break;
            case "bytes":
                bytes = data.readBytes(offset, field.length);
                if (bytes.length > 20) {
                    bytes.toJSON = function() {
                        return "[" + this.length + " " + "bytes]";
                    };
                }
                result[field.name] = bytes;
                offset += field.length;
                break;
            case "byteswithlength":
                length = data.readUInt32LE(offset);
                offset += 4;
                if (field.schema) {
                    element = parse(field.schema, data, offset);
                    if (element) {
                        result[field.name] = element.result;
                    }
                } else {
                    bytes = data.readBytes(offset, length);
                    if (bytes.length > 20) {
                        bytes.toJSON = function() {
                            return "[" + this.length + " " + "bytes]";
                        };
                    }
                    result[field.name] = bytes;
                }
                offset += length;
                break;
            case "uint32":
                result[field.name] = data.readUInt32LE(offset);
                offset += 4;
                break;
            case "int32":
                result[field.name] = data.readInt32LE(offset);
                offset += 4;
                break;
            case "uint16":
                result[field.name] = data.readUInt16LE(offset);
                offset += 2;
                break;
            case "int16":
                result[field.name] = data.readInt16LE(offset);
                offset += 2;
                break;
            case "uint8":
                result[field.name] = data.readUInt8(offset);
                offset += 1;
                break;
            case "int8":
                result[field.name] = data.readInt8(offset);
                offset += 1;
                break;
            case "uint64":
                var int64 = new Int64(data.readUInt32LE(offset), data.readUInt32LE(offset+4));
                result[field.name] = int64.toString();
                offset += 8;
                break;
            case "variabletype8":
                var vtypeidx = data.readUInt8(offset);
                offset += 1;
                var variableSchema = {
                        fields: [{name: "element", type: field.types[vtypeidx]}]
                };
                var variable = parse(variableSchema, data, offset);
                offset += variable.length;
                result[field.name] = {
                    type: vtypeidx,
                    value: variable.result.element
                };
                break;
            case "bitflags":
                value = data.readUInt8(offset);
                flags = {};
                for (j=0;j<field.flags.length;j++) {
                    flag = field.flags[j];
                    flags[flag.name] = !!(value & (1 << flag.bit));
                }
                result[field.name] = flags;
                offset += 1;
                break;
            case "float":
                result[field.name] = data.readFloatLE(offset);
                offset += 4;
                break;
            case "floatvector2":
                result[field.name] = [
                    data.readFloatLE(offset),
                    data.readFloatLE(offset+4)
                ];
                offset += 8;
                break;
            case "floatvector3":
                result[field.name] = [
                    data.readFloatLE(offset),
                    data.readFloatLE(offset+4),
                    data.readFloatLE(offset+8)
                ];
                offset += 12;
                break;
            case "floatvector4":
                result[field.name] = [
                    data.readFloatLE(offset),
                    data.readFloatLE(offset+4),
                    data.readFloatLE(offset+8),
                    data.readFloatLE(offset+12)
                ];
                offset += 16;
                break;
            case "boolean":
                result[field.name] = !!data.readUInt8(offset);
                offset += 1;
                break;
            case "string":
                string = data.readPrefixedStringLE(offset);
                result[field.name] = string;
                offset += 4 + string.length;
                break;
        }
        //console.log(field.name, String(result[field.name]).substring(0,50));
    }
    return {
        result: result,
        length: offset - startOffset
    };
}

function calculateDataLength(schema, object) {
    var length = 0,
        fields = schema.fields || [],
        field, i, j, elements;
    for (i=0;i<fields.length;i++) {
        field = fields[i];
        if (!(field.name in object)) {
            if ("defaultValue" in field) {
                value = field.defaultValue;
            } else {
                throw "Field " + field.name + " not found in data object: " + JSON.stringify(object, null, 4);
            }
        } else {
            value = object[field.name];
        }
        switch (field.type) {
            case "array":
                length += 4;
                elements = object[field.name];
                if (field.elementSchema) {
                    for (j=0;j<elements.length;j++) {
                        length += calculateDataLength(field.elementSchema, elements[j]);
                    }
                } else if (field.elementType) {
                    elementSchema = {
                        fields: [{name: "element", type: field.elementType}]
                    };
                    for (j=0;j<elements.length;j++) {
                        length += calculateDataLength(elementSchema, {element: elements[j]});
                    }
                }
                break;
            case "bytes":
                length += field.length;
                break;
            case "byteswithlength":
                length += 4;
                if (field.schema) {
                    length += calculateDataLength(field.schema, value);
                } else {
                    length += value.length;
                }
                break;
            case "uint64":
                length += 8;
                break;
            case "uint32":
            case "int32":
            case "float":
                length += 4;
                break;
            case "floatvector2":
                length += 8;
                break;
            case "floatvector3":
                length += 12;
                break;
            case "floatvector4":
                length += 16;
                break;
            case "uint16":
            case "int16":
                length += 2;
                break;
            case "uint8":
            case "int8":
            case "boolean":
            case "bitflags":
                length += 1;
                break;
            case "string":
                length += 4 + value.length;
                break;
            case "variabletype8":
                length += 1;
                var variableSchema = {
                    fields: [{name: "element", type: field.types[value.type]}]
                };
                length += calculateDataLength(variableSchema, {element: value.value});
                break;
        }
    }
    return length;
}

function pack(schema, object, data, offset) {
    var fields, dataLength, field, value,
        i, j, result, startOffset, elementSchema, flag, flagValue;

    if (!schema.fields) {
        return {
            data: new Buffer(0),
            length: 0
        };
    }

    if (!data) {
        dataLength = calculateDataLength(schema, object);
        data = new Buffer(dataLength);
    }
    offset = offset || 0;
    startOffset = offset;

    fields = schema.fields;

    for (i=0;i<fields.length;i++) {
        field = fields[i];
        if (!(field.name in object)) {
            if ("defaultValue" in field) {
                value = field.defaultValue;
            } else {
                throw "Field " + field.name + " not found in data object and no default value";
            }
        } else {
            value = object[field.name];
        }
        switch (field.type) {
            case "array":
                data.writeUInt32LE(value.length, offset);
                offset += 4;
                if (field.elementSchema) {
                    for (j=0;j<value.length;j++) {
                        result = pack(field.elementSchema, value[j], data, offset);
                        offset += result.length;
                    }
                } else if (field.elementType) {
                    elementSchema = {
                        fields: [{name: "element", type: field.elementType}]
                    };
                    for (j=0;j<value.length;j++) {
                        result = pack(elementSchema, {element: value[j]}, data, offset);
                        offset += result.length;
                    }
                } else {
                    throw "Invalid array schema";
                }
                break;
            case "bytes":
                if (!Buffer.isBuffer(value)) {
                    value = new Buffer(value);
                }
                data.writeBytes(value, offset, field.length);
                offset += field.length;
                break;
            case "byteswithlength":
                if (field.schema) {
                    value = pack(field.schema, value).data;
                }
                if (!Buffer.isBuffer(value)) {
                    value = new Buffer(value);
                }
                data.writeUInt32LE(value.length, offset);
                offset += 4;
                data.writeBytes(value, offset);
                offset += value.length;
                break;
            case "uint64":
                var int64 = new Int64(value);
                data.writeUInt32LE(int64.high32(), offset);
                data.writeUInt32LE(int64.low32(), offset + 4);
                offset += 8;
                break;
            case "uint32":
                data.writeUInt32LE(value, offset);
                offset += 4;
                break;
            case "int32":
                data.writeInt32LE(value, offset);
                offset += 4;
                break;
            case "uint16":
                data.writeUInt16LE(value, offset);
                offset += 2;
                break;
            case "int16":
                data.writeInt16LE(value, offset);
                offset += 2;
                break;
            case "uint8":
                data.writeUInt8(value, offset);
                offset += 1;
                break;
            case "int8":
                data.writeInt8(value, offset);
                offset += 1;
                break;
            case "bitflags":
                flagValue = 0;
                for (j=0;j<field.flags.length;j++) {
                    flag = field.flags[j];
                    if (value[flag.name]) {
                        flagValue = flagValue | (1 << flag.bit);
                    }
                }
                data.writeUInt8(flagValue, offset);
                offset += 1;
                break;
            case "float":
                data.writeFloatLE(value, offset);
                offset += 4;
                break;
            case "floatvector2":
                data.writeFloatLE(value[0], offset);
                data.writeFloatLE(value[1], offset + 4);
                offset += 8;
                break;
            case "floatvector3":
                data.writeFloatLE(value[0], offset);
                data.writeFloatLE(value[1], offset + 4);
                data.writeFloatLE(value[2], offset + 8);
                offset += 12;
                break;
            case "floatvector4":
                data.writeFloatLE(value[0], offset);
                data.writeFloatLE(value[1], offset + 4);
                data.writeFloatLE(value[2], offset + 8);
                data.writeFloatLE(value[3], offset + 12);
                offset += 16;
                break;
            case "boolean":
                data.writeUInt8(value ? 1 : 0, offset);
                offset += 1;
                break;
            case "string":
                data.writePrefixedStringLE(value, offset);
                offset += 4 + value.length;
                break;
            case "variabletype8":
                data.writeUInt8(value.type, offset);
                offset++;
                var variableSchema = {
                    fields: [{name: "element", type: field.types[value.type]}]
                };
                result = pack(variableSchema, {element: value.value}, data, offset);
                offset += result.length;
                break;
        }
    }
    return {
        data: data,
        length: offset - startOffset
    };
}

exports.parse = parse;
exports.pack = pack;