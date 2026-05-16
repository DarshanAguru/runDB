class RedisAssertions:
    # Validates if the object type matches the expected Redis type
    @staticmethod
    def assertObjectType(obj_type: int, redis_type: int) -> bool:
        return obj_type == redis_type
    
    # Validates if the object encoding matches the expected Redis encoding
    @staticmethod
    def assertObjectEncoding(obj_encoding: int, redis_encoding: int) -> bool:
        return obj_encoding == redis_encoding