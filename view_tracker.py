# We need a way to synchronize writes to each node's local value-store from
# multiple processes spawned by Flask. Abstracting this to another process
# allows us to effectively use our KVS as one would any normal database
# within a Flask application.
# https://stackoverflow.com/questions/28423069/store-large-data-or-a-service-connection-per-flask-session/28426819#28426819

from multiprocessing import Lock
from multiprocessing.managers import BaseManager


class ViewManager(BaseManager):
    pass


class View:
    def __init__(self):
        self.view_list = []
        self.lock = Lock()
        self.initialized = False
        self.init_lock = Lock()

    def setView(self, new_view: list[str]) -> None:
        with self.lock:
            self.view_list = new_view

    def getView(self) -> list[str]:
        with self.lock:
            return self.view_list.copy()

    def isInitialized(self) -> bool:
        with self.init_lock:
            return self.initialized

    def setInitialized(self, val: bool) -> None:
        with self.init_lock:
            self.initialized = val


view = View()


def get():
    return view


def getViewManager() -> ViewManager:
    # Our manager should only bind to localhost
    # Pick high af port number to avoid clashes
    manager = ViewManager(address=('127.0.0.1', 51235), authkey=b'')
    manager.register("get", get)
    return manager


def main():
    manager = getViewManager()
    server = manager.get_server()
    server.serve_forever()


if __name__ == "__main__":
    main()
