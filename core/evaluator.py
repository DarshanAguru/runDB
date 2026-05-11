from typing import Protocol, List, Any

from .RedisCmd import RedisCmd

import time

from .store import Value, Store

class SupportsSend(Protocol):
    def send(self, data: bytes) -> int:
        ...



NIL = b"$-1\r\n"
TTL_NIL = b":-1\r\n"
TTL_EXPIRED = b":-2\r\n"


class Evaluator:
    # Main entry point to evaluate a command and send the response back
    @staticmethod
    def evalAndRespond(cmd: RedisCmd, conn: SupportsSend) -> Exception | None:
        if cmd.cmd == "PING":
            return Evaluator.__evalPING(cmd.args, conn)
        elif cmd.cmd == "SET":
            return Evaluator.__evalSET(cmd.args, conn)
        elif cmd.cmd == "GET":
            return Evaluator.__evalGET(cmd.args, conn)
        elif cmd.cmd == "TTL":
            return Evaluator.__evalTTL(cmd.args, conn)
        elif cmd.cmd == "DEL":
            return Evaluator.__evalDEL(cmd.args, conn)
        elif cmd.cmd == "EXPIRE":
            return Evaluator.__evalEXPIRE(cmd.args, conn)
        else:
            return Evaluator.__sendError(conn)
    
    @staticmethod
    def __sendError(conn: SupportsSend) -> Exception | None:
        try:
            val = Evaluator.__encode("Command not found, please check the command.", err=True)
            conn.send(val)
            return None
        except Exception as err:
            return err

    # Handles the SET command with optional EX (expiration in seconds)
    @staticmethod
    def __evalSET(args: List[str], conn: SupportsSend) -> Exception | None:
        if len(args) <= 1:
            return Exception("ERR Wrong number of arguments for 'set' command")

        ex_duration_ms = -1
        key, value = args[0], args[1]

        i = 2
        while i < len(args):
            arg = args[i].lower()
            if arg == "ex":
                i += 1
                if i == len(args):
                    return Exception("ERR syntax error")
                try:
                    ex_duration_sec = int(args[i])
                except Exception as err:
                    return Exception("ERR value is not an integer or is out of range")
                ex_duration_ms = ex_duration_sec * 1000
            else:
                return Exception("ERR syntax error")
            i += 1

        Store.put(key, Value(value, ex_duration_ms))
        try:
            resp = Evaluator.__encode("OK", bulk=False)
            conn.send(resp)
            return None
        except Exception as err:
            return err


    # Handles the GET command, checking for existence and expiration
    @staticmethod
    def __evalGET(args: List[str], conn: SupportsSend) -> Exception | None:
        if len(args) != 1:
            return Exception("ERR Wrong number of arguments for 'get' command")
        
        key = args[0]

        val = Store.get(key)

        if val is None:
            resp = Evaluator.__encode(NIL)
            conn.send(resp)
            return None
        
        if val.isExpired():
            resp = Evaluator.__encode(NIL)
            conn.send(resp)
            return None
        
        try:
            resp = Evaluator.__encode(val.getValue(), bulk=True)
            conn.send(resp)
            return None
        except Exception as err:
            return err
        
    
    # Returns the remaining Time To Live in seconds for a key
    @staticmethod
    def __evalTTL(args: List[str], conn: SupportsSend) -> Exception | None:
        if len(args) != 1:
            return Exception("ERR Wrong number of arguments for 'ttl' command")
        
        key = args[0]
        val = Store.get(key)
        try:
            if val is None:
                resp =  Evaluator.__encode(TTL_EXPIRED)   
                conn.send(resp)
                return None
        
            if val.getExpiresAt() == -1:
                resp =  Evaluator.__encode(TTL_NIL)   
                conn.send(resp)
                return None
        
            if val.isExpired():
                resp =  Evaluator.__encode(TTL_EXPIRED)   
                conn.send(resp)
                return None

            duration_ms = val.getExpiresAt() - time.time() * 1000
        
            resp = Evaluator.__encode(int(duration_ms // 1000))
            conn.send(resp)
            return None
        except Exception as err:
            return err
    
    # Deletes one or more keys and returns the count of deleted keys
    @staticmethod
    def __evalDEL(args: List[str], conn: SupportsSend) -> Exception | None:
        count_deleted = 0

        for key in args:
            if Store.delete(key):
                count_deleted += 1
        
        try:
            resp = Evaluator.__encode(count_deleted)
            conn.send(resp)
            return None
        except Exception as err:
            return err
    
    # Sets or updates the expiration of an existing key
    @staticmethod
    def __evalEXPIRE(args: List[str], conn: SupportsSend) -> Exception | None:
        if len(args) <= 1:
            return Exception("ERR Wrong number of arguments for 'expire' command")
        
        key = args[0]
        try:
            ex_duration_sec = int(args[1])
        except Exception as err:
            return Exception("ERR value is not an integer or is out of range")
        
        val = Store.get(key)

        if val is None:
            try:
                resp = Evaluator.__encode(0)
                conn.send(resp)
                return None
            except Exception as err:
                return err
        
        val.setExpiresAt(ex_duration_sec * 1000)
        try:
            resp = Evaluator.__encode(1)
            conn.send(resp)
            return None
        except Exception as err:
            return err


    # Simple PING/PONG for connectivity checks
    @staticmethod
    def __evalPING(args: List[str], conn: SupportsSend) -> Exception | None:

        if len(args) >= 2:
            return Exception("ERR Wrong number of arguments for 'ping' command")
        
        if len(args) == 0:
            val = Evaluator.__encode("PONG", bulk=False)
        else:
            val = Evaluator.__encode(args[0], bulk=True)
        
        try:
            conn.send(val)
            return None
        except Exception as err:
            return err

    
    # Encodes data into RESP format based on type and flags
    @staticmethod
    def __encode(val: Any, bulk: bool = False, err: bool = False) -> bytes:
        if val is NIL:
            return NIL
    
        if val is TTL_NIL:
            return TTL_NIL
        
        if val is TTL_EXPIRED:
            return TTL_EXPIRED

        if isinstance(val, int):
            return f":{val}\r\n".encode("utf-8")
        
        if bulk:
            return f"${len(val)}\r\n{val}\r\n".encode("utf-8")
        elif err:
            return f"-{val}\r\n".encode("utf-8")
        else:
            return f"+{val}\r\n".encode("utf-8")

         
