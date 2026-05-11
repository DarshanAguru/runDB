import argparse
from server import Server
import logging
from config import Config

# Entry point to parse arguments and start the RunDB server
def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description="RUNDB: A simple  Key-Value store")
    parser.add_argument("--host", type=str, default=Config.HOST , help="Host address for run DB │ address (default: %(default)s)")
    parser.add_argument("--port", type=int, default=Config.PORT, help="Port for RunDB  │ number (default: %(default)s)")                                   
    args = parser.parse_args()
   
    logger.info("Running the RunDB")
    
    # Server.runSyncTcpServer(args.host, args.port, logger) #Sync Server, only one client at a time
    Server.runAsyncTcpServer(args.host, args.port, logger) #Async Server, can handle multiple clients.

if __name__ == "__main__":
    main()
