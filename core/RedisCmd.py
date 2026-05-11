class RedisCmd:
    def __init__(self, cmd: str = "", args: list[str] | None = None):
        self.cmd = cmd
        self.args = [] if args is None else args
