import sys
import os
import time
import random
import subprocess

# Dynamic path resolution to safely import Config when run directly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import Config

def transactionStorm():
    port = str(Config.PORT)
    print(f"Starting Transaction Storm on port {port}...")
    print("This will send rapid MUTLI/EXEC transaction blocks to test queueing and transaction execution.")
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

        tx_count = 0
        while True:
            print(f"\n--- Starting Transaction #{tx_count} ---")
            
            # Start MULTI block
            process.stdin.write("MULTI\n")
            process.stdin.flush()
            print(f"MULTI -> {process.stdout.readline().strip()}")
            
            # Queue two SET commands and one GET command
            k1, v1 = f"tx_key_A:{tx_count}", f"val_A:{random.randint(1,100)}"
            k2, v2 = f"tx_key_B:{tx_count}", f"val_B:{random.randint(1,100)}"
            
            process.stdin.write(f"SET {k1} {v1}\n")
            process.stdin.flush()
            print(f"SET {k1} {v1} -> {process.stdout.readline().strip()}")

            process.stdin.write(f"SET {k2} {v2}\n")
            process.stdin.flush()
            print(f"SET {k2} {v2} -> {process.stdout.readline().strip()}")

            process.stdin.write(f"GET {k1}\n")
            process.stdin.flush()
            print(f"GET {k1} -> {process.stdout.readline().strip()}")

            # Execute the transaction
            process.stdin.write("EXEC\n")
            process.stdin.flush()
            
            # Exec response is a RESP array. In redis-cli, it outputs the results line-by-line
            # For 3 commands inside transaction:
            # 1) OK
            # 2) OK
            # 3) val_A
            # We read those 3 responses
            print("EXEC execution results:")
            for _ in range(3):
                res_line = process.stdout.readline().strip()
                print(f"  - {res_line}")
                
            tx_count += 1
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nTransaction Storm terminated by user.")
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
    transactionStorm()
