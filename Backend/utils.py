import pandas as pd
from pandas import DataFrame


def find_all_documents(mongo) -> list:
    return list(mongo.db.RentPredictorCollection.find())


def parse_documents_to_df(documents: list) -> DataFrame:
    return pd.DataFrame(documents)


def get_all_documents(mongo):
    documents = find_all_documents(mongo)
    return parse_documents_to_df(documents)
