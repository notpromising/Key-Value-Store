#  conftest.py will be automatically imported by pytest
# https://docs.pytest.org/en/latest/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files
# These are pytest fixtures https://docs.pytest.org/en/6.2.x/fixture.html to be
# that should be used tests to signify what initialization they require
import pytest
import subprocess
from flask_app import app
from modules.view_tracker import getViewManager
from modules.local_database import getKVSManager


# based on https://flask.palletsprojects.com/en/2.2.x/testing/
@pytest.fixture
def flaskApp():
    app.config.update({
        "TESTING": True
    })

    yield app


@pytest.fixture
def flaskClient(flaskApp):
    return flaskApp.test_client()


@pytest.fixture
def resetViewTracker():
    """
    Connect to view_tracker and reset it to default values
    """
    view_manager = getViewManager()
    view_manager.connect()
    view_manager.get().setView({})
    view_manager.get().setInitialized(False)


@pytest.fixture
def resetLocalDatabase():
    """
    Connect to local_database and reset it to default values
    """
    kvs_manager = getKVSManager()
    kvs_manager.connect()
    kvs_manager.get().setDict({})


@pytest.fixture
def initNode(flaskClient):
    # We can get away with not giving a proper view as the spec states:
    #   "the node that receives the request is either starting a new
    #    cluster with no prior data, or if it already was part of a view,
    #    it is also in the new view."
    # So any node that gets a request on this endpoint knows to initialize itself
    view_manager = getViewManager()
    view_manager.connect()
    view_manager.get().setInitialized(True)


# The fixtures below for spawning processes are troublesome, instead use test.sh
@pytest.fixture
def localDatabase():
    p = subprocess.Popen(["python3", "../local_database.py"], shell=False)
    yield p
    p.kill()


@pytest.fixture
def viewTracker():
    p = subprocess.Popen(["python3", "../view_tracker.py"], shell=False)
    yield p
    p.kill()
