import os
import signal
import logging
import asyncio
import threading
from config import Config
from .Printer import Printer

logger = logging.getLogger(__name__)

# Engine statuses
ENGINE_IDLE = 0
ENGINE_BUSY = 1
ENGINE_SHUTDOWN = 2

class AtomicInt:
    def __init__(self, value: int = 0):
        self._value = value
        self._lock = threading.Lock()

    def load(self) -> int:
        with self._lock:
            return self._value

    def store(self, value: int) -> None:
        with self._lock:
            self._value = value

    def compare_and_swap(self, old: int, new: int) -> bool:
        with self._lock:
            if self._value == old:
                self._value = new
                return True
            return False

class Shutdown:
    is_shutdown_requested = False
    signal_received = None
    
    # Shared atomic status variable
    estatus = AtomicInt(ENGINE_IDLE)

    # Asyncio Event to signal shutdown request
    shutdown_event = asyncio.Event()

    @staticmethod
    async def handleGracefully():
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)

        signal.signal(signal.SIGTERM, Shutdown.shutdown_handler)
        signal.signal(signal.SIGINT, Shutdown.shutdown_handler)
        signal.signal(signal.SIGBUS, Shutdown.shutdown_handler)
        signal.signal(signal.SIGILL, Shutdown.shutdown_handler)
        signal.signal(signal.SIGSEGV, Shutdown.shutdown_handler)

    @staticmethod
    def shutdown_handler(signum, frame):
        Printer.printShutdownInitiated(signum)
        Shutdown.is_shutdown_requested = True
        Shutdown.signal_received = signum
        
        # Set the event in the running loop thread-safely
        try:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(Shutdown.shutdown_event.set)
        except RuntimeError:
            pass
    
    @staticmethod
    async def waitForSignal():
        # Wait for the signal event to be set
        await Shutdown.shutdown_event.wait()

        # Wait while the engine is busy processing commands or cron tasks (draining active in-flight executions)
        while Shutdown.estatus.load() == ENGINE_BUSY:
            await asyncio.sleep(0.01)

        # Transition status to SHUTDOWN atomically using compare_and_swap to prevent new commands from starting
        if Shutdown.estatus.compare_and_swap(ENGINE_IDLE, ENGINE_SHUTDOWN):
            logger.debug("Engine status transitioned to SHUTDOWN atomically.")
        else:
            # If it failed to swap from IDLE, force set it to SHUTDOWN to ensure we enter shutdown state
            Shutdown.estatus.store(ENGINE_SHUTDOWN)
            logger.debug("Engine status set to SHUTDOWN.")

        # Perform core shutdown Operations (Dump all data to AOF file)
        Shutdown.saveOperation()
    
    @staticmethod
    def saveOperation():
        aof_path = os.path.abspath(Config.AOF_FILE)
        Printer.printShutdownSaving(aof_path)
        from core.aof import AOF
        from core.internals.Malloc_internal import MemTracker
        try:
            pct_used = (MemTracker.allocated / Config.MEMORY_LIMIT) * 100
            # 1% of memory capacity utilized = 100 memory calories burnt
            calories = pct_used * 100
            
            AOF.dumpAllAOF()
            Printer.printShutdownSaved(aof_path)
            Printer.printShutdownComplete(calories, pct_used, MemTracker.allocated)
        except Exception as e:
            logger.error(f"Error during final AOF dump: {e}")
