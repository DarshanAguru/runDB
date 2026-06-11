import sys
import os
import time
import random
import subprocess

#STORM THE DB WITH SET COMMANDS WITH EXPIRATION (expiration) IN SECONDS (!0 default)
#Change the expiration time in main block.

# Dynamic path resolution to safely import Config when run directly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import Config

def setStormWithExpiration(expiration: int):
    port = str(Config.PORT)
    print(f"Starting SET storm on port {port} using redis-cli... Press Ctrl+C to stop.")
    process = None
    try:
        # Spawning a main process
        process = subprocess.Popen(
            ["redis-cli", "-p", port],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True
        )
        
        while True:
            key = "storm_key:" + str(random.randint(1, 100000))
            value = "storm_value:" + str(random.randint(1, 100000))
            
            # Write into the main process
            process.stdin.write(f"SET {key} {value} ex {expiration}\n")
            process.stdin.flush()
            print(f"Set {key} -> {value} with expiration {expiration}")
            
            time.sleep(0.2)
            
    except KeyboardInterrupt:
        print("\nStorm terminated by user (Ctrl+C).")
    except Exception as e:
        print(f"\nError : {e}")
    finally:
        # Terminating the main process
        if process:
            try:
                process.stdin.close()
                process.terminate()
                process.wait(timeout=1)
            except Exception:
                pass

if __name__ == "__main__":
    expiration = 10
    setStormWithExpiration(expiration)