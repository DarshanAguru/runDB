import os
from typing import Union


class FDComm:
    def __init__(self, fd: int):
        self.FD = fd
    
    # We need to be careful while writing to a non-blocking file descriptor.
    # If the write operation is blocked, we should try again.
    def send(self, data: Union[bytes, bytearray]) -> int:
        total_sent = 0

        while total_sent < len(data):
            sent = os.write(self.FD, data[total_sent:])

            if sent == 0:
                raise ConnectionError(
                    "Connection closed"
                )

            total_sent += sent

        return total_sent

    def recv(self, size: int) -> bytes:
        return os.read(self.FD, size)
