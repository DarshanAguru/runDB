import argparse
from server import Server, Shutdown, Printer
import logging
from config import Config
import asyncio


async def main_async(args):
    # Setup signal handlers
    await Shutdown.handleGracefully()

    # Start the server and signal monitor concurrently
    server_task = asyncio.create_task(Server.runAsyncTcpServer(args.host, args.port))
    monitor_task = asyncio.create_task(Shutdown.waitForSignal())

    # Wait for signal monitor to finish (SIGTERM/SIGINT triggered, AOF written)
    await monitor_task

    # Cancel the running server task
    server_task.cancel()

    # Wait for the server task to cleanly terminate
    await asyncio.gather(server_task, return_exceptions=True)

# Entry point to parse arguments and start the RunDB server
def pre_run_check(logger):
    import sys
    import select
    
    # 1. Check if OS is Linux
    if not sys.platform.startswith("linux"):
        logger.error("Pre-run check failed: runDB is compatible with Linux environments only.")
        print("Error: runDB is compatible with Linux environments only (requires select.epoll).", file=sys.stderr)
        sys.exit(1)
        
    # 2. Check if select.epoll is supported
    if not hasattr(select, "epoll"):
        logger.error("Pre-run check failed: select.epoll is not available on this platform.")
        print("Error: select.epoll is not available on this platform.", file=sys.stderr)
        sys.exit(1)
        
    # 3. Check if ctypes standard C library binding is working
    try:
        from core.internals.Malloc_internal import libc
        if libc is None:
            raise OSError("libc library could not be bound via ctypes.")
    except Exception as e:
        logger.error(f"Pre-run check failed: Native C Allocator binding check failed. {e}")
        print(f"Error: Native C Allocator check failed. Ensure ctypes is supported and standard C library is available. Details: {e}", file=sys.stderr)
        sys.exit(1)
        
    logger.info("Pre-run checks passed successfully. System matches all prerequisites (Linux, select.epoll, C-allocator).")

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Run pre-run check
    pre_run_check(logger)
    
    parser = argparse.ArgumentParser(description="RUNDB: A simple  Key-Value store")
    parser.add_argument("--host", type=str, default=Config.HOST , help="Host address for run DB │ address (default: %(default)s)")
    parser.add_argument("--port", type=int, default=Config.PORT, help="Port for RunDB  │ number (default: %(default)s)")                                   
    args = parser.parse_args()
   
    Printer.printRunDBHeader(args.host, args.port)
    
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        logger.info("Main program interrupted and exited.")

if __name__ == "__main__":
    main()

