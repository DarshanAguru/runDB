import sys
import os
import time
import random
import subprocess

# Dynamic path resolution to safely import Config when run directly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import Config

def evictionStorm():
    port = str(Config.PORT)
    print(f"Starting Eviction Storm on port {port}...")
    print(f"This will flood the server with large keys to trigger configured eviction strategy ({Config.EVICTION_STRATEGY}).")
    print("Press Ctrl+C to stop.")

    process = None
    try:
        # Spawn redis-cli as a subprocess
        process = subprocess.Popen(
            ["redis-cli", "-p", port],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        key_count = 0
        # Use large values to quickly exceed the 1MB default memory limit
        large_payload = "A" * 50000  # ~50 KB per payload

        while True:
            key = f"evict_key:{key_count}"
            # Send SET command
            process.stdin.write(f"SET {key} {large_payload}\n")
            process.stdin.flush()
            
            # Read response from SET command
            response = process.stdout.readline().strip()
            print(f"Inserted {key} (~50KB) -> Response: {response}")
            
            # Every 10 iterations, query INFO to see keyspace and memory stats
            if key_count % 10 == 0:
                process.stdin.write("INFO\n")
                process.stdin.flush()
                print("\n--- Server Info & Stats ---")
                # INFO returns a bulk string: first line is length $num, then lines of info, then empty line
                # Read lines until we finish reading the info payload
                line = process.stdout.readline().strip()
                if line.startswith("$"):
                    bytes_to_read = int(line[1:])
                    # Read the rest of the lines
                    bytes_read = 0
                    while bytes_read < bytes_to_read:
                        info_line = process.stdout.readline()
                        bytes_read += len(info_line)
                        print(info_line.strip())
                print("---------------------------\n")

            key_count += 1
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\nEviction Storm terminated by user.")
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        if process:
            try:
                process.stdin.close()
                process.stdout.close()
                process.stderr.close()
                process.terminate()
                process.wait(timeout=1)
            except Exception:
                pass

if __name__ == "__main__":
    evictionStorm()
