import select
import socket
import logging
from typing import Protocol
import time
from core import Core, RedisCmd, Evaluator, FDComm, Store, Expiration
from config import Config


class ConnectionLike(Protocol):
    def recv(self, size: int) -> bytes:
        ...

    def send(self, data: bytes) -> int:
        ...


class Server:

    @staticmethod
    def __readCommand(con: ConnectionLike) -> tuple[RedisCmd | None, Exception | None]:
        try:
            msg = con.recv(1024)
            if len(msg) == 0:
                return None, None
            tokens, err = Core.decodeArrayString(msg)
            if err is not None:
                return None, err
            if tokens is None or len(tokens) == 0:
                return None, Exception("Invalid Command")
            return RedisCmd(
                tokens[0].upper(),
                tokens[1:]
            ), None
        except Exception as err:
            return None, err

    @staticmethod
    def __respondError(err: Exception, con: ConnectionLike) -> None:
        con.send(b"-"+f"{err}".encode("utf-8")+b"\r\n")

    @staticmethod
    def __respond(cmd: RedisCmd, con: ConnectionLike) -> None:
        err = Evaluator.evalAndRespond(cmd, con)
        if err is not None:
            Server.__respondError(err, con)

    @staticmethod
    def __deleteExpiredKeys(logger: logging.Logger) -> None:
        frac = Expiration.expireSamples()
        while frac > 0.25:
            frac = Expiration.expireSamples()
        logger.debug(f"Frac: {frac}")
        logger.debug(f"Deleted the expired but undeleted keys. total keys: {len(Store.store)}")

    @staticmethod
    def runAsyncTcpServer(host: str, port: int, logger: logging.Logger) -> None:
        logger.info("Starting the Asynchronous TCP Server")
        con_clients = 0
        cron_freq_sec = 1
        last_cron_exec_time_sec = time.time()
        clients: dict[int, socket.socket] = {}

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serverSock:
                serverSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                serverSock.bind((host, port))
                serverSock.listen()
                serverSock.setblocking(False)
                logger.info(f"Server Listening on {host}:{port}")
                epoll = select.epoll()
                try:
                    epoll.register(serverSock.fileno(), select.EPOLLIN)
                    while True:
                        
                        if time.time() - last_cron_exec_time_sec >= cron_freq_sec:
                            Server.__deleteExpiredKeys(logger)
                            last_cron_exec_time_sec = time.time()

                        events = epoll.poll()
                        for fd, event in events:
                            if fd == serverSock.fileno():
                                sock, addr = serverSock.accept()
                                sock.setblocking(False)
                                con_clients += 1
                                if con_clients > Config.MAX_CLIENTS:
                                    logger.warning(f"Maximum number of clients reached: {con_clients}")
                                    sock.close()
                                    con_clients -= 1
                                    continue
                                clients[sock.fileno()] = sock
                                epoll.register(sock.fileno(), select.EPOLLIN)
                                logger.info(f"Connected to remote address: {addr} and fd: {sock.fileno()}, no. of concurrent clients: {con_clients}")
                            else:
                                if event & (select.EPOLLHUP | select.EPOLLERR):
                                    epoll.unregister(fd)
                                    sock = clients.pop(fd, None)
                                    if sock is not None:
                                        sock.close()
                                    con_clients -= 1
                                    logger.info(f"Connection closed for fd: {fd}, no. of concurrent clients: {con_clients}")
                                    continue

                                comm = FDComm(fd)
                                cmd, err = Server.__readCommand(comm)
                                if err is not None:
                                    logger.error(f"Error reading from fd: {fd}: {err}")
                                    epoll.unregister(fd)
                                    sock = clients.pop(fd, None)
                                    if sock is not None:
                                        sock.close()
                                    con_clients -= 1
                                    continue
                                if not cmd:
                                    epoll.unregister(fd)
                                    sock = clients.pop(fd, None)
                                    if sock is not None:
                                        sock.close()
                                    con_clients -= 1
                                    logger.info(f"Connection closed for fd: {fd}, no. of concurrent clients: {con_clients}")
                                    continue
                                logger.debug(f"Received command from fd: {fd}: {cmd.cmd} {cmd.args}")
                                Server.__respond(cmd, comm)
                except OSError as err:
                    logger.error(f"Epoll error: {err}")
                except KeyboardInterrupt:
                    logger.info("Server shutting down due to keyboard interrupt")
                finally:
                    for sock in clients.values():
                        sock.close()
                    epoll.close()
        except socket.error as err:
            logger.error(f"Server error: {err}")

        

    @staticmethod
    def runSyncTcpServer(host: str, port: int, logger: logging.Logger) -> None:
        logger.info("Starting the Synchronous TCP Server")
        con_clients = 0

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
            try:
                soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                soc.bind((host, port))
                soc.listen()
                logger.info(f"Server Listening on {host}:{port}")

                while True:
                    conn, addr = soc.accept()
                    con_clients +=1
                    logger.info(f"Connected to remote address: {addr}, no. of concurrent clients: {con_clients}")
                    try:
                        while True:
                            cmd, err = Server.__readCommand(conn)
                            if err is not None:
                                logger.error(f"Error reading from {addr}: {err}")
                                break
                            if not cmd:
                                logger.info(f"Client {addr} closed connection")
                                break
                            logger.debug(f"Received command from {addr}: {cmd.cmd} {cmd.args}")
                            Server.__respond(cmd, conn)
                            
                    finally:
                        conn.close()
                        con_clients -= 1
                        logger.info(f"Connection closed for {addr}, no. of concurrent clients: {con_clients}")
                    
            except KeyboardInterrupt:
                logger.info("Server shutting down due to keyboard interrupt")
            except Exception as e:
                logger.error(f"Server error: {e}")
            finally:
                logger.info("Exiting server")
        
        
