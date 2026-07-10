import argparse
from server import Server
from server.util import Shutdown, Printer
import logging
from config import Config
import asyncio
import os

async def main_async():
    # Setup signal handlers
    await Shutdown.handleGracefully()

    # Start the server and signal monitor concurrently
    server_task = asyncio.create_task(Server.runAsyncTcpServer(Config.HOST, Config.PORT))
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
    
    parser = argparse.ArgumentParser(description="RUNDB: A simple Key-Value store")
    parser.add_argument("--config", type=str, default=None, help="Path to config file (e.g. rundb.conf)")
    parser.add_argument("--host", type=str, default=None, help="Host address for RunDB")
    parser.add_argument("--port", type=int, default=None, help="Port for RunDB")
    parser.add_argument("--memory-limit", type=int, default=None, help="Max memory limit in bytes")
    parser.add_argument("--max-clients", type=int, default=None, help="Max number of concurrent clients")
    parser.add_argument("--cron-freq-interval", type=float, default=None, help="Periodic interval (in secs) for expiring keys")
    parser.add_argument("--aof-file", type=str, default=None, help="File name/path for AOF Logging")
    parser.add_argument("--eviction-strategy", type=str, default=None, help="Eviction strategy name")
    parser.add_argument("--eviction-ratio", type=float, default=None, help="Eviction ratio (fraction of keys to evict)")
    parser.add_argument("--db-count", type=int, default=None, help="Number of databases")
    parser.add_argument("--eviction-pool-size", type=int, default=None, help="Eviction pool size for allkeys-lru")
    parser.add_argument("--eviction-sample-size", type=int, default=None, help="Sample size for allkeys-lru")
    
    args = parser.parse_args()
   
    # Resolution Precedence:
    # 1. Config file (if specified) overrides defaults / environment variables
    if args.config:
        Config.load_from_file(args.config)
        
    # 2. Command-line flags override configuration files, env vars, and defaults
    if args.host is not None:
        Config.HOST = args.host
    if args.port is not None:
        Config.PORT = args.port
    if args.memory_limit is not None:
        Config.MEMORY_LIMIT = args.memory_limit
    if args.max_clients is not None:
        Config.MAX_CLIENTS = args.max_clients
    if args.cron_freq_interval is not None:
        Config.CRON_FREQ_INTERVAL = args.cron_freq_interval
    if args.aof_file is not None:
        Config.AOF_FILE = args.aof_file
    if args.eviction_strategy is not None:
        Config.EVICTION_STRATEGY = args.eviction_strategy
    if args.eviction_ratio is not None:
        Config.EVICTION_RATIO = args.eviction_ratio
    if args.db_count is not None:
        Config.DB_COUNT = args.db_count
    if args.eviction_pool_size is not None:
        Config.EVICTION_POOL_SIZE = args.eviction_pool_size
    if args.eviction_sample_size is not None:
        Config.EVICTION_SAMPLE_SIZE = args.eviction_sample_size
        
    Printer.printRunDBBanner(Config.HOST, Config.PORT)
    
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Main program interrupted and exited.")

if __name__ == "__main__":
    main()

