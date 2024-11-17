
import pickle
import time
from typing import Any, List

from redis import Redis


class MyRedisCache():

    def __init__(self) -> None:
        # 连接redis, 返回redis客户端
        self._redis_client = self._connet_redis()

    @staticmethod
    def _connet_redis() -> Redis:
        """
        连接redis, 返回redis客户端, 这里按需配置redis连接信息
        :return:
        """
        return Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            password="12345",
        )

    def get(self, key: str) -> Any:
        """
        从缓存中获取键的值, 并且反序列化
        :param key:
        :return:
        """
        value = self._redis_client.get(key)
        if value is None:
            return None
        return pickle.loads(value)

    def set_expire(self, key: str, value: Any, expire_time: int) -> None:
        """
        将键的值设置到缓存中, 并且序列化
        :param key:
        :param value:
        :param expire_time:
        :return:
        """
        self._redis_client.set(key, pickle.dumps(value), ex=expire_time)

    def set(self, key: str, value: Any) -> None:
        """
        将键的值设置到缓存中, 不设置过期时间
        :param key:
        :param value:
        :return:
        """
        self._redis_client.set(key, pickle.dumps(value))

    def is_exists(self, key: str) -> bool:
        """
        判断键是否存在
        :param key:
        :return:
        """
        return self._redis_client.exists(key)

    def keys(self, pattern: str) -> List[str]:
        """
        获取所有符合pattern的key
        """
        return [key.decode() for key in self._redis_client.keys(pattern)]


if __name__ == '__main__':
    redis_cache = MyRedisCache()
    # basic usage
    redis_cache.set_expire("name", "程序员阿江-Relakkes", 1)
    print(redis_cache.get("name"))  # Relakkes
    print(redis_cache.keys("*"))  # ['name']
    print(redis_cache.is_exists("name"))  # True
    time.sleep(2)
    print(redis_cache.get("name"))  # None
    print(redis_cache.is_exists("name"))  # False
    # special python type usage
    # list
    redis_cache.set_expire("list", [1, 2, 3], 10)
    _value = redis_cache.get("list")
    print(_value, f"value type:{type(_value)}")  # [1, 2, 3]
