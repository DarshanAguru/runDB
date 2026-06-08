import argparse
from server import Server, Shutdown
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
def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description="RUNDB: A simple  Key-Value store")
    parser.add_argument("--host", type=str, default=Config.HOST , help="Host address for run DB │ address (default: %(default)s)")
    parser.add_argument("--port", type=int, default=Config.PORT, help="Port for RunDB  │ number (default: %(default)s)")                                   
    args = parser.parse_args()
   
    logger.info("🏃‍♂️ RunDB is starting to sprint! Let's burn some memory! 🔥")
    
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        logger.info("Main program interrupted and exited.")

if __name__ == "__main__":
    main()
