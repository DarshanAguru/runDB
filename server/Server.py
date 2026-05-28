import select
import socket
import logging
from typing import Protocol
import time
from core import RESPProcessor, RedisCmd, Evaluator, FDComm, Store, Expiration
from config import Config

logger = logging.getLogger(__name__)

# Empty class for type hinting
class ConnectionLike(Protocol):
    def recv(self, size: int) -> bytes:
        ...

    def send(self, data: bytes) -> int:
        ...


class Server:

    # Reads and decodes commands from the FDComm's read_buffer, consuming parsed bytes.
    # Returns (cmds, client_closed, error)
    @staticmethod
    def __readCommands(comm: FDComm) -> tuple[list[RedisCmd] | None, bool, Exception | None]:
        try:
            active = comm.readFromFd()
            
            # If client closed and no unparsed bytes in buffer, initiate a close.
            if not active and len(comm.read_buffer) == 0:
                return None, True, None
            
            # Decode as many complete RESP objects as possible from the read_buffer
            vals, consumed, err = RESPProcessor.decode(comm.read_buffer)
            if err is not None:
                return None, not active, err
            
            if consumed > 0:
                # Remove successfully parsed bytes from the read buffer
                comm.read_buffer = comm.read_buffer[consumed:]
            
            cmds = []
            if vals:
                for v in vals:
                    # Each command must be a RESP array
                    if not isinstance(v, list) or len(v) == 0:
                        continue
                    
                    cmds.append(RedisCmd(
                        str(v[0]).upper(),
                        [str(arg) for arg in v[1:]]
                    ))
            
            # If the client closed the connection and we didn't receive any complete new commands, close it
            if not active and not cmds:
                return None, True, None
                
            return cmds, False, None
        except Exception as err:
            return None, True, err

    @staticmethod
    def __respondError(err: Exception, con: ConnectionLike) -> None:
        con.send(b"-"+f"{err}".encode("utf-8")+b"\r\n")

    @staticmethod
    def __respond(cmds: list[RedisCmd], con: ConnectionLike) -> None:
        err = Evaluator.evalAndRespond(cmds, con)
        if err is not None:
            Server.__respondError(err, con)
    
    @staticmethod
    def __closeConnection(fd: int, clients: dict[int, socket.socket], comms: dict[int, FDComm], epoll: select.epoll) -> None:
        try:
            epoll.unregister(fd)
        except OSError:
            pass
        sock = clients.pop(fd, None)
        comms.pop(fd, None)
        if sock is not None:
            sock.close()


    # Main loop for the high-concurrency asynchronous server using epoll
    @staticmethod
    def runAsyncTcpServer(host: str, port: int) -> None:
        logger.info("Starting the Asynchronous TCP Server")
        con_clients = 0
        cron_freq_sec = Config.CRON_FREQ_INTERVAL
        last_cron_exec_time_sec = time.time()
        clients: dict[int, socket.socket] = {}
        comms: dict[int, FDComm] = {}

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serverSock:
                # Initialize TCP socket (IPv4, Streaming)
                serverSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                serverSock.bind((host, port))
                serverSock.listen()
                serverSock.setblocking(False) # Non-blocking for async handling
                logger.info(f"Server Listening on {host}:{port}")
                
                # NOTE: Due to usage of 'epoll' system call which is linux specific,
                # this server works in Linux environment only
                if not hasattr(select, "epoll"):
                    raise RuntimeError("epoll not supported; Linux required.")
                
                # Initialize epoll for event-driven I/O
                epoll = select.epoll()
                try:
                    # Register server socket to listen for incoming connections
                    epoll.register(serverSock.fileno(), select.EPOLLIN)
                    while True:
                        
                        # Background task to clean up expired keys
                        if time.time() - last_cron_exec_time_sec >= cron_freq_sec:
                            # Delete Expired Keys periodically
                            Expiration.deleteExpiredKeys()
                            
                            # Update the last checked cycle
                            last_cron_exec_time_sec = time.time()

                        # Wait for I/O events
                        # We poll for 1 second so as to not block the server for too long
                        # and can check on expired keys time to time
                        events = epoll.poll(1)
                        for fd, event in events:
                            # Accept new connections
                            if fd == serverSock.fileno():
                                try:
                                    sock, addr = serverSock.accept()
                                except BlockingIOError:
                                    continue
                                sock.setblocking(False) # Ensure client socket is also non-blocking
                                con_clients += 1
                                if con_clients > Config.MAX_CLIENTS:
                                    logger.warning(f"Maximum number of clients reached: {con_clients}")
                                    sock.close()
                                    con_clients -= 1
                                    continue
                                clients[sock.fileno()] = sock
                                # Store the FDComms object to manage its read/write buffers
                                comms[sock.fileno()] = FDComm(sock.fileno())
                                # Register client socket to watch for incoming data
                                epoll.register(sock.fileno(), select.EPOLLIN)
                                logger.info(f"Connected to remote address: {addr} and fd: {sock.fileno()}, no. of concurrent clients: {con_clients}")
                            else:
                                # Handle client disconnection or errors
                                if event & (select.EPOLLHUP | select.EPOLLERR):
                                    Server.__closeConnection(fd, clients, comms, epoll)
                                    con_clients -= 1
                                    logger.info(f"Connection closed for fd: {fd}, no. of concurrent clients: {con_clients}")
                                    continue
                                
                                # Handle outgoing data (write)
                                if event & select.EPOLLOUT:
                                    comm = comms.get(fd)
                                    if comm is not None:
                                        try:
                                            comm.writeToFd()
                                        except Exception as write_err:
                                            logger.error(f"Error writing to fd: {fd}: {write_err}")
                                            Server.__closeConnection(fd, clients, comms, epoll)
                                            con_clients -= 1
                                            logger.info(f"Connection closed for fd: {fd}, no. of concurrent clients: {con_clients}")
                                            continue
                                            
                                        # If all pending writes are flushed, stop watching EPOLLOUT
                                        if not comm.hasPendingWrites():
                                            epoll.modify(fd, select.EPOLLIN)

                                # Handle incoming data from an existing client (read)
                                if event & select.EPOLLIN:
                                    comm = comms.get(fd)
                                    if comm is not None:
                                        cmds, client_closed, err = Server.__readCommands(comm)
                                        if err is not None:
                                            logger.error(f"Error reading from fd: {fd}: {err}") 
                                            Server.__closeConnection(fd, clients, comms, epoll)
                                            con_clients -= 1
                                            logger.info(f"Connection closed for fd: {fd}, no. of concurrent clients: {con_clients}")
                                            continue
                                        if client_closed:
                                            Server.__closeConnection(fd, clients, comms, epoll)
                                            con_clients -= 1
                                            logger.info(f"Connection closed for fd: {fd}, no. of concurrent clients: {con_clients}")
                                            continue
                                        
                                        if cmds:
                                            logger.debug(f"Received {len(cmds)} commands from fd {fd}")
                                            Server.__respond(cmds, comm)
                                        
                                        # Modify epoll registration based on whether we have pending writes
                                        if comm.hasPendingWrites():
                                            epoll.modify(fd, select.EPOLLIN | select.EPOLLOUT)
                                        else:
                                            epoll.modify(fd, select.EPOLLIN)
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