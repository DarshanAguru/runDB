from core.RedisCmd import RedisCmd
from core.FDComm import FDComm
from typing import List, Union

class ResponseAccumulator:
    def __init__(self):
        self.buffer = bytearray()

    def send(self, data: Union[bytes, bytearray]) -> int:
        self.buffer.extend(data)
        return len(data)

class Client:
    def __init__(self, comm: FDComm):
        self.cqueue = []
        self.isTrans = False
        self.comm = comm
        self.sock = None  # Set by Server when connection is accepted

    def write(self, b: List[bytes] = None) -> int:
        return self.comm.writeToFd()

    def read(self) -> bool:
        return self.comm.readFromFd()

    def send(self, data: Union[bytes, bytearray]) -> int:
        return self.comm.send(data)

    def TransBegin(self):
        self.isTrans = True

    def TransExec(self) -> bytes:
        from core.evaluator import Evaluator
        accumulator = ResponseAccumulator()
        for cmd in self.cqueue:
            Evaluator.evalAndRespond([cmd], accumulator)

        # Return a Redis array response of the executed transaction results
        response = f"*{len(self.cqueue)}\r\n".encode("utf-8") + accumulator.buffer
        self.isTrans = False
        self.cqueue = []
        return response

    def TransDiscard(self):
        self.isTrans = False
        self.cqueue = []
