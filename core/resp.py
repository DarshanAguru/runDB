from typing import Any


# RESPProcessor implements the parser for the REdis Serialization Protocol (RESP):
# - Dispatch Decoding: Branches parsing based on the initial prefix byte (+ for simple strings, 
#   - for errors, : for integers, $ for bulk strings, * for arrays).
# - Partial Stream Support: Catches IndexErrors during parsing of incomplete/fragmented payloads,
#   returning successfully parsed command values and offset for next read iteration.
class RESPProcessor:
    # Parses the length of a RESP bulk string or array
    @staticmethod
    def __readLength(data: bytes) -> tuple[int, int]:
        pos, length = 0, 0
        while pos < len(data):
            val = data[pos]
            if not (ord('0') <= val <= ord('9')):
                return length, pos+2
            length = length * 10 + (val - ord('0'))
            pos +=1
        return 0, 0

    # Reads a simple string starting with '+'
    @staticmethod
    def __readSimpleString(data: bytes) -> tuple[str, int, Exception | None]:
        pos = 1
        while pos < len(data) and data[pos] != ord('\r'):
            pos += 1
        if pos >= len(data) or pos + 1 >= len(data) or data[pos+1] != ord('\n'):
            raise IndexError("Incomplete simple string")
        return data[1:pos].decode("utf-8"), pos+2, None
    
    # Reads an error message starting with '-'
    @staticmethod
    def __readError(data: bytes) -> tuple[Exception, int, Exception | None]:
        val, pos, err = RESPProcessor.__readSimpleString(data)
        if err is not None:
            return Exception("Invalid Error"), 0, err
        return Exception(val), pos, None
    
    # Reads an integer starting with ':'
    @staticmethod
    def __readInteger(data: bytes) -> tuple[int, int, Exception | None]:
        pos = 1
        val = 0
        while pos < len(data) and data[pos] != ord('\r'):
            val = val * 10 + data[pos] - ord('0')
            pos+=1
        if pos >= len(data) or pos + 1 >= len(data) or data[pos+1] != ord('\n'):
            raise IndexError("Incomplete integer")
        return val, pos+2, None
    
    # Reads a bulk string starting with '$'
    @staticmethod
    def __readBulkString(data: bytes) -> tuple[str, int, Exception | None]:
        pos = 1
        length, delta = RESPProcessor.__readLength(data[pos:])
        if delta == 0:
            raise IndexError("Incomplete bulk string length")
        pos += delta
        if len(data) < pos + length + 2:
            raise IndexError("Incomplete bulk string payload")
        if data[pos+length] != ord('\r') or data[pos+length+1] != ord('\n'):
            raise IndexError("Incomplete bulk string termination")
        return data[pos: (pos + length)].decode("utf-8"), pos + length + 2, None
    
    # Reads a RESP array starting with '*'
    @staticmethod
    def __readArray(data: bytes) -> tuple[list[Any] | None, int, Exception | None]:
        pos = 1
        length, delta = RESPProcessor.__readLength(data[pos:])
        if delta == 0:
            raise IndexError("Incomplete array length")
        pos += delta

        arr: list[Any] = [None] * length

        for i in range(length):
            if pos >= len(data):
                raise IndexError("Incomplete array elements")
            ele, delta, err = RESPProcessor.__decodeOne(data[pos:])
            if err is not None:
                return None, 0, err
            arr[i] = ele
            pos += delta
        return arr, pos, None

    # Dispatcher to read the next RESP element based on the first byte
    @staticmethod
    def __decodeOne(data: bytes) -> tuple[Any, int, Exception | None]:
        if len(data) == 0:
            return None, 0, Exception("No Data")
        
        if data[0] == ord("+"):
            return RESPProcessor.__readSimpleString(data)
        elif data[0] == ord("-"):
            return RESPProcessor.__readError(data)
        elif data[0] == ord(":"):
            return RESPProcessor.__readInteger(data)
        elif data[0] == ord("$"):
            return RESPProcessor.__readBulkString(data)
        elif data[0] == ord("*"):
            return RESPProcessor.__readArray(data)
        else:
            return None, 0, Exception("Invalid Data")

    # Public method to decode raw RESP data, supporting partial parsing and pipelining
    @staticmethod
    def decode(data: bytes) -> tuple[list[Any] | None, int, Exception | None]:
        if len(data) == 0:
            return [], 0, None
        idx = 0
        vals = []
        try:
            while idx < len(data):
                val, delta, err = RESPProcessor.__decodeOne(data[idx:])
                if err is not None:
                    return None, idx, err
                idx += delta
                vals.append(val)
            return vals, idx, None
        except IndexError:
            # We hit incomplete data; return what was parsed so far and the consumed bytes
            return vals, idx, None
    
    # Helper to decode a single RESP array specifically as a list of strings (for commands)
    @staticmethod
    def decodeArrayString(data: bytes) -> tuple[list[str] | None, Exception | None]:
        try:
            # Parse only the first RESP object from the bytes
            val, _, err = RESPProcessor.__decodeOne(data)
            if err is not None:
                return None, err
            if not isinstance(val, list):
                return None, Exception("Expected Array Input")
            
            # Convert all elements to strings (standard for Redis commands)
            tokens: list[str] = [str(ele) for ele in val]
            return tokens, None
        except Exception as err:
            return None, err
