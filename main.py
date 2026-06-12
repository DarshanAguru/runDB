import argparse
from server import Server
from server.util import Shutdown, Printer
import logging
from config import Config
import asyncio
import os

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
    
    # Check if OS is Linux
    if not sys.platform.startswith("linux"):
        logger.error("Pre-run check failed: RunDB is compatible with Linux environments only.")
        print("Error: RunDB is compatible with Linux environments only (requires select.epoll).", file=sys.stderr)
        sys.exit(1)
        
    # Check if select.epoll is supported
    if not hasattr(select, "epoll"):
        logger.error("Pre-run check failed: select.epoll is not available on this platform.")
        print("Error: select.epoll is not available on this platform.", file=sys.stderr)
        sys.exit(1)
        
    # Check if ctypes standard C library binding is working
    try:
        from core.internals.Malloc_internal import libc
        if libc is None:
            raise OSError("libc library could not be bound via ctypes.")
    except Exception as e:
        logger.error(f"Pre-run check failed: Native C Allocator binding check failed. {e}")
        print(f"Error: Native C Allocator check failed. Ensure ctypes is supported and standard C library is available. Details: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Check for Memory allocator and set the ENV variable if 'libjemalloc.so' allocator is present
    # else use default native C allocator
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        jemalloc_path = os.path.join(base_dir, 'dll', 'libjemalloc.so')
        
        if os.path.isfile(jemalloc_path):
            current_preload = os.environ.get('LD_PRELOAD', '')
            if 'libjemalloc.so' not in current_preload:
                os.environ['LD_PRELOAD'] = jemalloc_path
                logger.info(f"jemalloc detected. Setting LD_PRELOAD to jemalloc.so. Using jemalloc for memory management.")
            else:
                logger.info("jemalloc is active and preloaded. Using jemalloc for memory management.")
        else:
            logger.info("Using native C library (libc) for memory management.")
    except Exception as e:
        logger.info(f"Could not check/set jemalloc.so. {e}. Using native C allocator.")

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
   
    Printer.printRunDBBanner(args.host, args.port)
    
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        logger.info("Main program interrupted and exited.")

if __name__ == "__main__":
    main()

