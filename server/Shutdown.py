import os
import signal
import logging

logger = logging.getLogger(__name__)

class Shutdown:
    is_shutdown_requested = False
    signal_received = None

    @staticmethod
    def handleGracefully():
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)

        signal.signal(signal.SIGTERM, Shutdown.shutdown_handler)
        signal.signal(signal.SIGINT, Shutdown.shutdown_handler)
        signal.signal(signal.SIGBUS, Shutdown.shutdown_handler)
        signal.signal(signal.SIGILL, Shutdown.shutdown_handler)
        signal.signal(signal.SIGSEGV, Shutdown.shutdown_handler)

    @staticmethod
    def shutdown_handler(signum, frame):
        logger.warning(f"Received signal {signum}. Initiating graceful shutdown...")
        Shutdown.is_shutdown_requested = True
        Shutdown.signal_received = signum
        