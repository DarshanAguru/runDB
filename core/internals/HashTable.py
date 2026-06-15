from .HashMap import HashMap
from typing import Any

# HashTable wraps a native open-addressing C-heap HashMap to act as a Set.
# Key type is dynamic, mapped to a dummy int32 value.
class HashTable:
    def __init__(self, keyType: str = "string", ptr=None):
        # HashTable representing a Set maps keys to a dummy int32 value
        self.map = HashMap(keyType, "int32", ptr=ptr)

    def add(self, key: Any) -> bool:
        if key in self.map:
            return False
        self.map.set(key, 1)
        return True

    def remove(self, key: Any) -> bool:
        return self.map.delete(key)

    def __contains__(self, key: Any) -> bool:
        return key in self.map

    def __len__(self) -> int:
        return len(self.map)

    def __iter__(self):
        return iter(self.map)

    def release(self) -> int:
        return self.map.release()

    def get_random_member(self) -> Any:
        return self.map.get_random_key()
