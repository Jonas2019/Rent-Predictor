from flask import Flask
from flask_pymongo import PyMongo
from flask_cors import CORS
import os
from dotenv import load_dotenv
# Set up
# config = configparser.ConfigParser()
# config.read(os.path.abspath(os.path.join(".ini")))


# client = MongoClient('mongodb+srv://JW:Stemcell2018@cluster0.q4g0hww.mongodb.net/test')
# filter={
#     'validation_location': True
# }
# sort=list({
#     'time': -1
# }.items())

# result = client['RentPredictorDatabase']['RentPredictorCollection'].find(
#   filter=filter,
#   sort=sort
# )


app = Flask(__name__)
CORS(app)
load_dotenv('.env')
app.config['CORS_HEADERS'] = 'Content-Type'
app.config["MONGO_URI"] = os.getenv('MONGODB_URI')


# 'mongodb+srv://{}:{}@cluster0.q4g0hww.mongodb.net/?retryWrites=true&w=majority'.format(
#     os.getenv('MONGODB_USR'),
#     os.getenv('MONGODB_USR_PASSWORD'))

# API Section


@app.route("/")
def index():
    print(app.config['MONGO_URI'])
    return "Hello World"
