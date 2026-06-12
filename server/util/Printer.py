class Printer:
    class Colors:
        HEADER = '\033[95m'
        BLUE = '\033[94m'
        CYAN = '\033[96m'
        GREEN = '\033[92m'
        YELLOW = '\033[93m'
        RED = '\033[91m'
        END = '\033[0m'
        BOLD = '\033[1m'
        GRAY = '\033[90m'

    @staticmethod
    def printRunDBBanner(ip, port):
        res = r"""
    ███████████                         ██████████   ███████████    
    ░░███░░░░░███                       ░░███░░░░███ ░░███░░░░░███   
     ░███    ░███  █████ ████ ████████   ░███   ░░███ ░███    ░███   
     ░██████████  ░░███ ░███ ░░███░░███  ░███    ░███ ░██████████    
     ░███░░░░░███  ░███ ░███  ░███ ░███  ░███    ░███ ░███░░░░░███   
     ░███    ░███  ░███ ░███  ░███ ░███  ░███    ███  ░███    ░███   
     █████   █████ ░░████████ ████ █████ ██████████   ███████████    
    ░░░░░   ░░░░░   ░░░░░░░░ ░░░░ ░░░░░ ░░░░░░░░░░   ░░░░░░░░░░░                                                                                 
        """
        
        # Print the header
        print(res)
        
        # Add a friendly message
        print("""
        RUNDB: The Redis-inspired NoSQL Key-Value Store
        Version 1.0.0 - Built with ❤️  by Darshan
        """)
        
        line1_text = f"Server is running at {ip}:{port}"
        line2_text = f"Try running: redis-cli -p {port}"
        
        line1 = f"  {line1_text}".ljust(45)
        line2 = f"  {line2_text}".ljust(45)
        
        print(f"        ┌─────────────────────────────────────────────┐")
        print(f"        │{line1}│")
        print(f"        │{line2}│")
        print(f"        └─────────────────────────────────────────────┘\n")

    @staticmethod
    def printAOFLoading(aof_file):
        print(f"        {Printer.Colors.BLUE}📂 AOF{Printer.Colors.END}  Loading database from AOF file: {Printer.Colors.BOLD}{aof_file}{Printer.Colors.END}")

    @staticmethod
    def printAOFEmpty():
        print(f"        {Printer.Colors.YELLOW}📂 AOF{Printer.Colors.END}  AOF file is empty. Starting with empty database.")

    @staticmethod
    def printAOFRestored(aof_file, saved_time, cmd_count, mem_bytes):
        print(f"        {Printer.Colors.GREEN}🔄 AOF{Printer.Colors.END}  Restored checkpoint from {Printer.Colors.BOLD}{saved_time}{Printer.Colors.END}")
        print(f"        {Printer.Colors.GREEN}✅ AOF{Printer.Colors.END}  Successfully restored {Printer.Colors.BOLD}{cmd_count}{Printer.Colors.END} commands.")
        print(f"        {Printer.Colors.GREEN}💾 AOF{Printer.Colors.END}  Memory restored: {Printer.Colors.BOLD}{mem_bytes} Bytes{Printer.Colors.END}\n")

    @staticmethod
    def printShutdownInitiated(signum):
        print(f"\n        {Printer.Colors.YELLOW}⚠️ SYS{Printer.Colors.END}  Received signal {signum}. Initiating graceful shutdown...")

    @staticmethod
    def printShutdownStopping():
        print(f"        {Printer.Colors.YELLOW}🛑 SYS{Printer.Colors.END}  Shutdown requested. Stop accepting new client connections.")
        print(f"        {Printer.Colors.YELLOW}🚪 SYS{Printer.Colors.END}  All client requests processed. Exiting server loop.")

    @staticmethod
    def printShutdownSaving(aof_path):
        print(f"        {Printer.Colors.BLUE}💾 AOF{Printer.Colors.END}  Saving database checkpoint: dumping memory into file {aof_path}...")

    @staticmethod
    def printShutdownSaved(aof_path):
        print(f"        {Printer.Colors.GREEN}✅ AOF{Printer.Colors.END}  Successfully dumped data into file: {aof_path}")

    @staticmethod
    def printShutdownComplete(calories, pct_used, mem_bytes):
        print(f"        {Printer.Colors.GREEN}🏁 SYS{Printer.Colors.END}  RunDB server shutdown complete.")
        print(f"        {Printer.Colors.GREEN}👋 SYS{Printer.Colors.END}  Bye bye! You burnt {Printer.Colors.BOLD}{calories:.2f}{Printer.Colors.END} memory calories ({pct_used:.2f}% of memory capacity used, {mem_bytes} Bytes) while running!🏃")
        print(f"        {Printer.Colors.BLUE}🚀 SYS{Printer.Colors.END}  See You Soon!!!\n")