import select
import socket
import logging
from typing import Protocol
import time
from core import RESPProcessor, RedisCmd, Evaluator, FDComm, Store, Expiration
from config import Config


# Empty class for type hinting
class ConnectionLike(Protocol):
    def recv(self, size: int) -> bytes:
        ...

    def send(self, data: bytes) -> int:
        ...


class Server:

    # Reads raw messages from the connection and decodes them into a list of RedisCmds (pipelining)
    @staticmethod
    def __readCommands(con: ConnectionLike) -> tuple[list[RedisCmd] | None, Exception | None]:
        try:
            msg = con.recv(1024)
            if len(msg) == 0:
                return None, None
            
            # decode returns a list of all RESP objects found in the message
            vals, err = RESPProcessor.decode(msg)
            if err is not None:
                return None, err
            
            cmds = []
            for v in vals:
                # Each command must be a RESP array (represented as a list in Python)
                if not isinstance(v, list) or len(v) == 0:
                    continue # Skip invalid commands or empty arrays
                
                cmds.append(RedisCmd(
                    str(v[0]).upper(),
                    [str(arg) for arg in v[1:]]
                ))
            
            if not cmds:
                return None, Exception("No valid commands found")
                
            return cmds, None
        except Exception as err:
            return None, err

    @staticmethod
    def __respondError(err: Exception, con: ConnectionLike) -> None:
        con.send(b"-"+f"{err}".encode("utf-8")+b"\r\n")

    @staticmethod
    def __respond(cmds: list[RedisCmd], con: ConnectionLike) -> None:
        err = Evaluator.evalAndRespond(cmds, con)
        if err is not None:
            Server.__respondError(err, con)

    # Periodically samples and deletes expired keys from the store
    @staticmethod
    def __deleteExpiredKeys(logger: logging.Logger) -> None:
        frac = Expiration.expireSamples()
        while frac > 0.25:
            frac = Expiration.expireSamples()
        logger.debug(f"Frac: {frac}")
        logger.debug(f"Deleted the expired but undeleted keys. total keys: {len(Store.store)}")

    # Main loop for the high-concurrency asynchronous server using epoll
    @staticmethod
    def runAsyncTcpServer(host: str, port: int, logger: logging.Logger) -> None:
        logger.info("Starting the Asynchronous TCP Server")
        con_clients = 0
        cron_freq_sec = 1
        last_cron_exec_time_sec = time.time()
        clients: dict[int, socket.socket] = {}

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serverSock:
                # Initialize TCP socket (IPv4, Streaming)
                serverSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                serverSock.bind((host, port))
                serverSock.listen()
                serverSock.setblocking(False) # Non-blocking for async handling
                logger.info(f"Server Listening on {host}:{port}")
                
                # Initialize epoll for event-driven I/O
                epoll = select.epoll()
                try:
                    # Register server socket to listen for incoming connections
                    epoll.register(serverSock.fileno(), select.EPOLLIN)
                    while True:
                        
                        # Background task to clean up expired keys
                        if time.time() - last_cron_exec_time_sec >= cron_freq_sec:
                            Server.__deleteExpiredKeys(logger)
                            last_cron_exec_time_sec = time.time()

                        # Wait for I/O events (blocks until an event occurs)
                        events = epoll.poll()
                        for fd, event in events:
                            # Accept new connections
                            if fd == serverSock.fileno():
                                sock, addr = serverSock.accept()
                                sock.setblocking(False) # Ensure client socket is also non-blocking
                                con_clients += 1
                                if con_clients > Config.MAX_CLIENTS:
                                    logger.warning(f"Maximum number of clients reached: {con_clients}")
                                    sock.close()
                                    con_clients -= 1
                                    continue
                                clients[sock.fileno()] = sock
                                # Register client socket to watch for incoming data
                                epoll.register(sock.fileno(), select.EPOLLIN)
                                logger.info(f"Connected to remote address: {addr} and fd: {sock.fileno()}, no. of concurrent clients: {con_clients}")
                            else:
                                # Handle client disconnection or errors
                                if event & (select.EPOLLHUP | select.EPOLLERR):
                                    epoll.unregister(fd)
                                    sock = clients.pop(fd, None)
                                    if sock is not None:
                                        sock.close()
                                    con_clients -= 1
                                    logger.info(f"Connection closed for fd: {fd}, no. of concurrent clients: {con_clients}")
                                    continue
                                
                                # Handle incoming data from an existing client
                                comm = FDComm(fd)
                                cmds, err = Server.__readCommands(comm)
                                if err is not None:
                                    logger.error(f"Error reading from fd: {fd}: {err}")
                                    epoll.unregister(fd)
                                    sock = clients.pop(fd, None)
                                    if sock is not None:
                                        sock.close()
                                    con_clients -= 1
                                    continue
                                if not cmds:
                                    epoll.unregister(fd)
                                    sock = clients.pop(fd, None)
                                    if sock is not None:
                                        sock.close()
                                    con_clients -= 1
                                    logger.info(f"Connection closed for fd: {fd}, no. of concurrent clients: {con_clients}")
                                    continue
                                
                                logger.debug(f"Received {len(cmds)} commands from fd {fd}")
                                Server.__respond(cmds, comm)
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
                            cmds, err = Server.__readCommands(conn)
                            if err is not None:
                                logger.error(f"Error reading from {addr}: {err}")
                                break
                            if not cmds:
                                logger.info(f"Client {addr} closed connection")
                                break
                            logger.debug(f"Received {len(cmds)} commands from {addr}")
                            Server.__respond(cmds, conn)
                            
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
        
        
