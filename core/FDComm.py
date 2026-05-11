import os
from typing import Union


class FDComm:
    def __init__(self, fd: int):
        self.FD = fd

    def send(self, b: Union[bytes, bytearray]) -> int:
        return os.write(self.FD, b)

    def recv(self, b: int) -> bytes:
        return os.read(self.FD, b)
