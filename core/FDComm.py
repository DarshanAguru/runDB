import os
from typing import Union


# FDComm manages non-blocking descriptor I/O buffers:
# - Buffer Queueing: Holds read_buffer and write_buffer to aggregate incoming/outgoing command fragments.
# - Non-blocking Socket Handling: Catches BlockingIOError (EAGAIN/EWOULDBLOCK) to suspend read/write loops
#   until EPOLLIN/EPOLLOUT signals trigger resumptions.
# - State Tracking: Checks connection status by detecting empty bytes (EOF) or OS socket errors.
class FDComm:
    def __init__(self, fd: int):
        self.FD = fd
        self.read_buffer = bytearray()
        self.write_buffer = bytearray()
    
    # Reads all currently available data from the non-blocking socket into read_buffer.
    # Returns False if the client closed the connection (EOF), True otherwise. 
    def readFromFd(self) -> bool:
        while True:
            try:
                # Read chunks of 4096 bytes
                chunk = os.read(self.FD, 4096)
                if len(chunk) == 0:
                    # EOF (client closed connection)
                    return False
                self.read_buffer.extend(chunk)
            except BlockingIOError:
                # EAGAIN/EWOULDBLOCK: No more data left in socket buffer for now
                break
            except OSError as e:
                # Any socket-level error (e.g. ConnectionResetError)
                raise e
        return True

    # Attempts to write as much buffered data in write_buffer as possible to the socket.
    # Returns the number of bytes written in this call.
    def writeToFd(self) -> int:
        total_sent = 0
        while len(self.write_buffer) > 0:
            try:
                sent = os.write(self.FD, self.write_buffer)
                if sent == 0:
                    raise ConnectionError("Connection closed during write")
                self.write_buffer = self.write_buffer[sent:]
                total_sent += sent
            except BlockingIOError:
                # EAGAIN/EWOULDBLOCK: TCP window/socket buffer is full, wait for next EPOLLOUT
                break
            except OSError as e:
                raise e
        return total_sent

    # Appends data to the write buffer and flushes as much as possible.
    def send(self, data: Union[bytes, bytearray]) -> int:
        self.write_buffer.extend(data)
        return self.writeToFd()

    # Returns True if there are any bytes in the write buffer.
    def hasPendingWrites(self) -> bool:
        return len(self.write_buffer) > 0

    # Kept for backward compatibility interface definitions.
    def recv(self, size: int) -> bytes:
        return os.read(self.FD, size)
