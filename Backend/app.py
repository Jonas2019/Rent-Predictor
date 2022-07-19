from multiprocessing.reduction import AbstractReducer
from flask import Flask
from flask_pymongo import PyMongo
from flask_cors import CORS
import os
from dotenv import load_dotenv
import utils

app = Flask(__name__)
CORS(app)
load_dotenv('.env')
app.config['CORS_HEADERS'] = 'Content-Type'
app.config["MONGO_URI"] = os.getenv('MONGODB_URI')
mongo = PyMongo(app)

# API Section


@app.route("/")
def index():
    res = utils.get_all_documents(mongo)
    res = res.drop(['_id'], axis=1)
    return res.to_json(orient="records")
