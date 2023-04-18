from flask import Flask
import os

app = Flask(__name__)
our_address = os.environ.get("ADDRESS")

import flask_app.check_initialization
import flask_app.data_endpoints
import flask_app.view_endpoints
import flask_app.gossip_endpoints
