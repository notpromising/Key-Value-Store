# We need a way to synchronize writes to each node's local value-store from
# multiple processes spawned by Flask. Abstracting this to another process
# allows us to effectively use our KVS as one would any normal database
# within a Flask application.
# https://stackoverflow.com/questions/28423069/store-large-data-or-a-service-connection-per-flask-session/28426819#28426819

from multiprocessing import Lock, Process
from multiprocessing.managers import BaseManager
from time import sleep


class KVSManager(BaseManager):
    pass


class LocalKVS:
    def __init__(self):
        # Our dict matches keys to a three-tuple containing the key's value,
        # last updated timestamp, and the key's dependency list
        self.kvs_dict: dict[str, tuple[str, int, dict[str, int]]] = {}
        self.lock = Lock()

    def setDictValue(self, key: str, value: str | None, timestamp: int, dependencies: dict[str, int] | None) -> bool:
        """
        Adds a new value to set in our process list. It will be committed
        whenever our server has all the dependency requirements.
        :param key: key to update
        :param timestamp:
        :param value: value to update with
        :param dependencies: list of dependencies we must meet to commit this update
        :return: bool of whether the key was replaced or not
        """
        with self.lock:
            # give dependencies a default value on None
            old_val = self.kvs_dict.get(key)
            if dependencies is None:
                dependencies = {}

            # ignore dependencies on self
            dependencies.pop(key, None)
            # always write a new value
            if old_val is None:
                self.kvs_dict[key] = (value, timestamp, dependencies)
                return False

            # don't overwrite if we have a newer val
            if old_val[1] > timestamp:
                return True

            self.kvs_dict[key] = (value, timestamp, dependencies)
            return True

    def getDictValue(self, key: str) -> tuple[str, int, dict[str, int]] | None:
        with self.lock:
            return self.kvs_dict.get(key)

    def removeDictValue(self, key: str, timestamp: int, dependencies: dict[str, int]) -> bool:
        """
        Same as setDictValue, but always sets value to None
        :param timestamp:
        :param key: key to update
        :param dependencies: list of dependencies we must meet to commit this update
        :return: new version number of removed key
        """
        return self.setDictValue(key, None, timestamp, dependencies)

    def setDict(self, val: dict[str, str]) -> None:
        with self.lock:
            self.kvs_dict = val

    def getDict(self) -> dict[str, tuple[str, int, dict[str, int]]]:
        with self.lock:
            return self.kvs_dict.copy()


kvs = LocalKVS()


def get() -> LocalKVS:
    return kvs


def getKVSManager() -> KVSManager:
    # Our manager should only bind to localhost
    # Pick high af port number to avoid clashes
    manager = KVSManager(address=('127.0.0.1', 51234), authkey=b'')
    manager.register("get", get)
    return manager


def putProcessingDaemon() -> None:
    while True:
        kvs.processPuts()
        sleep(.1)


def main():
    manager = getKVSManager()
    server = manager.get_server()
    server.serve_forever()


if __name__ == "__main__":
    main()
