import select
import asyncio
import socket
import logging
from typing import Protocol
import time
import errno
from core import RESPProcessor, RedisCmd, Evaluator, FDComm, Store, Expiration
from core.Client import Client
from config import Config
from .Shutdown import Shutdown, ENGINE_IDLE, ENGINE_BUSY, ENGINE_SHUTDOWN
from .Printer import Printer

logger = logging.getLogger(__name__)

class Server:
    con_clients = dict()

    # Reads and decodes commands from the Client's FDComm read_buffer, consuming parsed bytes.
    # Returns (cmds, client_closed, error)
    @staticmethod
    def __readCommands(client: Client) -> tuple[list[RedisCmd] | None, bool, Exception | None]:
        try:
            active = client.read()
            
            # If client closed and no unparsed bytes in buffer, initiate a close.
            if not active and len(client.comm.read_buffer) == 0:
                return None, True, None
            
            # Decode as many complete RESP objects as possible from the read_buffer
            vals, consumed, err = RESPProcessor.decode(client.comm.read_buffer)
            if err is not None:
                return None, not active, err
            
            if consumed > 0:
                # Remove successfully parsed bytes from the read buffer
                client.comm.read_buffer = client.comm.read_buffer[consumed:]
            
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
    def __respondError(err: Exception, con: Client) -> None:
        con.send(b"-"+f"{err}".encode("utf-8")+b"\r\n")

    @staticmethod
    def __respond(cmds: list[RedisCmd], con: Client) -> None:
        err = Evaluator.evalAndRespond(cmds, con)
        if err is not None:
            Server.__respondError(err, con)
    
    @staticmethod
    def __closeConnection(fd: int, epoll: select.epoll) -> None:
        try:
            epoll.unregister(fd)
        except OSError:
            pass
        client = Server.con_clients.pop(fd, None)
        if client is not None:
            if hasattr(client, 'sock') and client.sock is not None:
                client.sock.close()


    # Main loop for the high-concurrency asynchronous server using epoll
    @staticmethod
    async def runAsyncTcpServer(host: str, port: int) -> None:
        logger.debug("Starting the Asynchronous TCP Server")
        cron_freq_sec = Config.CRON_FREQ_INTERVAL
        last_cron_exec_time_sec = time.time()

        # Restore database state from AOF log file if present
        from core.aof import AOF
        AOF.loadAllAOF()

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serverSock:
                # Initialize TCP socket (IPv4, Streaming)
                serverSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                serverSock.bind((host, port))
                serverSock.listen()
                serverSock.setblocking(False) # Non-blocking for async handling
                
                # NOTE: Due to usage of 'epoll' system call which is linux specific,
                # this server works in Linux environment only
                if not hasattr(select, "epoll"):
                    raise RuntimeError("epoll not supported; Linux required.")
                
                # Initialize epoll for event-driven I/O
                epoll = select.epoll()
                server_registered = True
                try:
                    # Register server socket to listen for incoming connections
                    epoll.register(serverSock.fileno(), select.EPOLLIN)
                    while Shutdown.estatus.load() != ENGINE_SHUTDOWN:
                        # Background task to clean up expired keys
                        if time.time() - last_cron_exec_time_sec >= cron_freq_sec:
                            # Safely mark engine as busy before running key deletion to avoid concurrent shutdown races
                            if Shutdown.estatus.compare_and_swap(ENGINE_IDLE, ENGINE_BUSY):
                                try:
                                    Expiration.deleteExpiredKeys()
                                finally:
                                    # Reset engine status back to idle after completion
                                    Shutdown.estatus.compare_and_swap(ENGINE_BUSY, ENGINE_IDLE)
                            
                            # Update the last checked cycle
                            last_cron_exec_time_sec = time.time()

                        # Wait for I/O events
                        # We poll for 1 second so as to not block the server for too long
                        # and can check on expired keys time to time
                        try:
                            events = epoll.poll(1)
                        except (InterruptedError, OSError) as poll_err:
                            if isinstance(poll_err, OSError) and poll_err.errno != errno.EINTR:
                                raise poll_err
                            await asyncio.sleep(0.01)
                            continue

                        if events:
                            status = Shutdown.estatus.load()
                            # Process events if idle (transition to busy) or if in shutdown/draining phase
                            if status == ENGINE_IDLE:
                                # Mark engine as busy atomically so that shutdown is held off until execution finishes
                                if Shutdown.estatus.compare_and_swap(ENGINE_IDLE, ENGINE_BUSY):
                                    try:
                                        for fd, event in events:
                                            # Accept new connections
                                            if fd == serverSock.fileno():
                                                if Shutdown.is_shutdown_requested:
                                                    continue
                                                try:
                                                    sock, addr = serverSock.accept()
                                                except BlockingIOError:
                                                    continue
                                                sock.setblocking(False) # Ensure client socket is also non-blocking
                                                client = Client(FDComm(sock.fileno()))
                                                client.sock = sock
                                                Server.con_clients[sock.fileno()] = client
                                                if len(Server.con_clients) > Config.MAX_CLIENTS:
                                                    logger.warning(f"Maximum number of clients reached: {len(Server.con_clients)}")
                                                    del Server.con_clients[sock.fileno()]
                                                    sock.close()
                                                    continue
                                                # Register client socket to watch for incoming data
                                                epoll.register(sock.fileno(), select.EPOLLIN)
                                                logger.debug(f"Connected to remote address: {addr} and fd: {sock.fileno()}, no. of concurrent clients: {len(Server.con_clients)}")
                                            else:
                                                # Handle client disconnection or errors
                                                if event & (select.EPOLLHUP | select.EPOLLERR):
                                                    Server.__closeConnection(fd, epoll)
                                                    logger.debug(f"Connection closed for fd: {fd}, no. of concurrent clients: {len(Server.con_clients)}")
                                                    continue
                                                
                                                # Handle outgoing data (write)
                                                if event & select.EPOLLOUT:
                                                    client = Server.con_clients.get(fd)
                                                    if client is not None:
                                                        try:
                                                            client.write()
                                                        except Exception as write_err:
                                                            logger.error(f"Error writing to fd: {fd}: {write_err}")
                                                            Server.__closeConnection(fd, epoll)
                                                            logger.debug(f"Connection closed for fd: {fd}, no. of concurrent clients: {len(Server.con_clients)}")
                                                            continue
                                                            
                                                        # If all pending writes are flushed, stop watching EPOLLOUT
                                                        if not client.comm.hasPendingWrites():
                                                            epoll.modify(fd, select.EPOLLIN)

                                                # Handle incoming data from an existing client (read)
                                                if event & select.EPOLLIN:
                                                    client = Server.con_clients.get(fd)
                                                    if client is not None:
                                                        cmds, client_closed, err = Server.__readCommands(client)
                                                        if err is not None:
                                                            logger.error(f"Error reading from fd: {fd}: {err}") 
                                                            Server.__closeConnection(fd, epoll)
                                                            logger.debug(f"Connection closed for fd: {fd}, no. of concurrent clients: {len(Server.con_clients)}")
                                                            continue
                                                        if client_closed:
                                                            Server.__closeConnection(fd, epoll)
                                                            logger.debug(f"Connection closed for fd: {fd}, no. of concurrent clients: {len(Server.con_clients)}")
                                                            continue
                                                        
                                                        if cmds:
                                                            logger.debug(f"Received {len(cmds)} commands from fd {fd}")
                                                            Server.__respond(cmds, client)
                                                        
                                                        # Modify epoll registration based on whether we have pending writes
                                                        if client.comm.hasPendingWrites():
                                                            epoll.modify(fd, select.EPOLLIN | select.EPOLLOUT)
                                                        else:
                                                            epoll.modify(fd, select.EPOLLIN)
                                    finally:
                                        # Done processing current events; mark engine as idle again
                                        Shutdown.estatus.compare_and_swap(ENGINE_BUSY, ENGINE_IDLE)
                            elif status == ENGINE_SHUTDOWN:
                                # Draining phase: process events for remaining active clients, but accept no new connections
                                for fd, event in events:
                                    if fd != serverSock.fileno():
                                        # Handle client disconnection or errors
                                        if event & (select.EPOLLHUP | select.EPOLLERR):
                                            Server.__closeConnection(fd, epoll)
                                            logger.debug(f"Connection closed for fd: {fd}, no. of concurrent clients: {len(Server.con_clients)}")
                                            continue
                                        
                                        # Handle outgoing data (write)
                                        if event & select.EPOLLOUT:
                                            client = Server.con_clients.get(fd)
                                            if client is not None:
                                                try:
                                                    client.write()
                                                except Exception as write_err:
                                                    logger.error(f"Error writing to fd: {fd}: {write_err}")
                                                    Server.__closeConnection(fd, epoll)
                                                    logger.debug(f"Connection closed for fd: {fd}, no. of concurrent clients: {len(Server.con_clients)}")
                                                    continue
                                                    
                                                # If all pending writes are flushed, stop watching EPOLLOUT
                                                if not client.comm.hasPendingWrites():
                                                    epoll.modify(fd, select.EPOLLIN)

                                        # Handle incoming data from an existing client (read)
                                        if event & select.EPOLLIN:
                                            client = Server.con_clients.get(fd)
                                            if client is not None:
                                                cmds, client_closed, err = Server.__readCommands(client)
                                                if err is not None:
                                                    logger.error(f"Error reading from fd: {fd}: {err}") 
                                                    Server.__closeConnection(fd, epoll)
                                                    logger.debug(f"Connection closed for fd: {fd}, no. of concurrent clients: {len(Server.con_clients)}")
                                                    continue
                                                if client_closed:
                                                    Server.__closeConnection(fd, epoll)
                                                    logger.debug(f"Connection closed for fd: {fd}, no. of concurrent clients: {len(Server.con_clients)}")
                                                    continue
                                                
                                                if cmds:
                                                    logger.debug(f"Received {len(cmds)} commands from fd {fd}")
                                                    Server.__respond(cmds, client)
                                                
                                                # Modify epoll registration based on whether we have pending writes
                                                if client.comm.hasPendingWrites():
                                                    epoll.modify(fd, select.EPOLLIN | select.EPOLLOUT)
                                                else:
                                                    epoll.modify(fd, select.EPOLLIN)

                        if Shutdown.is_shutdown_requested:
                            if server_registered:
                                Printer.printShutdownStopping()
                                try:
                                    epoll.unregister(serverSock.fileno())
                                except OSError:
                                    pass
                                server_registered = False

                            # Close any active client connections that have no pending reads/writes
                            for fd in list(Server.con_clients.keys()):
                                client = Server.con_clients.get(fd)
                                if client is not None and not client.comm.hasPendingWrites() and len(client.comm.read_buffer) == 0:
                                    logger.debug(f"Closing idle client connection for fd: {fd}")
                                    Server.__closeConnection(fd, epoll)

                            # If no clients are left, break the loop and finish shutdown
                            if len(Server.con_clients) == 0:
                                break
                        
                        await asyncio.sleep(0.01)
                except OSError as err:
                    logger.error(f"Epoll error: {err}")
                except KeyboardInterrupt:
                    logger.info("Server shutting down due to keyboard interrupt")
                finally:
                    for client in list(Server.con_clients.values()):
                        if hasattr(client, 'sock') and client.sock is not None:
                            client.sock.close()
                    Server.con_clients.clear()
                    epoll.close()
        except socket.error as err:
            logger.error(f"Server error: {err}")