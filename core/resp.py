from typing import Any


class Core:

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

    @staticmethod
    def __readSimpleString(data: bytes) -> tuple[str, int, Exception | None]:
        pos = 1
        while data[pos] != ord('\r'):
            pos += 1
        return data[1:pos].decode("utf-8"), pos+2, None
    
    @staticmethod
    def __readError(data: bytes) -> tuple[Exception, int, Exception | None]:
        val, pos, err = Core.__readSimpleString(data)
        if err is not None:
            return Exception("Invalid Error"), 0, err
        return Exception(val), pos, None
    
    @staticmethod
    def __readInteger(data: bytes) -> tuple[int, int, Exception | None]:
        pos = 1
        val = 0
        while data[pos] != ord('\r'):
            val = val * 10 + data[pos] - ord('0')
            pos+=1
        return val, pos+2, None
    
    @staticmethod
    def __readBulkString(data: bytes) -> tuple[str, int, Exception | None]:
        pos = 1
        length, delta = Core.__readLength(data[pos:])
        pos += delta
        return data[pos: (pos + length)].decode("utf-8"), pos + length + 2, None
    
    @staticmethod
    def __readArray(data: bytes) -> tuple[list[Any] | None, int, Exception | None]:
        pos = 1
        length, delta = Core.__readLength(data[pos:])
        pos += delta

        arr: list[Any] = [None] * length

        for i in range(length):
            ele, delta, err = Core.__decodeOne(data[pos:])
            if err is not None:
                return None, 0, err
            arr[i] = ele
            pos += delta
        return arr, pos, None

    @staticmethod
    def __decodeOne(data: bytes) -> tuple[Any, int, Exception | None]:
        if len(data) == 0:
            return None, 0, Exception("No Data")
        
        if data[0] == ord("+"):
            return Core.__readSimpleString(data)
        elif data[0] == ord("-"):
            return Core.__readError(data)
        elif data[0] == ord(":"):
            return Core.__readInteger(data)
        elif data[0] == ord("$"):
            return Core.__readBulkString(data)
        elif data[0] == ord("*"):
            return Core.__readArray(data)
        else:
            return None, 0, Exception("Invalid Data")

    @staticmethod
    def decode(data: bytes) -> tuple[Any, Exception | None]:
        if len(data) == 0:
            return None, Exception("No Data")
        
        val, _, err = Core.__decodeOne(data)
        return val, err
    
    @staticmethod
    def decodeArrayString(data: bytes) -> tuple[list[str] | None, Exception | None]:
        try:
            val, err = Core.decode(data)
            if err is not None:
                return None, err
            if not isinstance(val, list):
                return None, Exception("Expected Array Input")
            tokens: list[str] = []
            for ele in val:
                tokens.append(str(ele))
            return tokens, None
        except Exception as err:
            return None, err
