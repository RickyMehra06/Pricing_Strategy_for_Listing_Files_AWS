import pymongo
import pandas as pd
import numpy as np
import json
import os,sys
from os import listdir
import warnings
warnings.filterwarnings("ignore")

from src.config import mongo_client

DATA_FILES_PATH              = "/config/workspace/Input_FIles"
MASTER_DATA_FILE_PATH        = "/config/workspace/Input_Master_Files/Master_File.xlsx"
SHIPPING_CHARGE_FILE_PATH    = "/config/workspace/Input_Master_Files/Shipping_Charge.xlsx"

DATABASE_NAME = "amazon_listing_db"
COLLECTION_NAME ="inventory_collection"
MASTER_DATA_COLLECTION_NAME = "master_data_collection"
SHIPPING_CHARGE_COLLECTION_NAME = "shipping_charge_collection"

if __name__=="__main__":
    
    df = pd.DataFrame()
    for file in listdir(DATA_FILES_PATH):
        temp_file = pd.read_excel(DATA_FILES_PATH+"/"+file)
        df = df.append(temp_file, ignore_index=True)

    df['ISBN'] = [str(int(x)) if isinstance(x, int) else str(x) for x in df['ISBN']]
    df['ISBN'] = df['ISBN'].str.split(".").str[0]    

    df['Date'] = df['Date'].apply(str).str.split('T').str[0].str.split(" ").str[0]
    df.reset_index(drop=True,inplace=True)
    print(f"The size of input file dataset is: {df.shape}")

    #Convert dataframe to json to dump these record in mongodb
    json_record = list(json.loads(df.T.to_json()).values())
    #json_record = df.to_dict(orient="records")

    mongo_client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)
    print(f"{df.shape[0]} records have been uploaded into Mongodb collection:-> {COLLECTION_NAME}")



    #Loading master data file into Mongodb
    master_df = pd.read_excel(MASTER_DATA_FILE_PATH)
    master_df['ISBN'] = [str(int(x)) if isinstance(x, int) else str(x) for x in master_df['ISBN']]
    master_df['ISBN'] = master_df['ISBN'].str.split(".").str[0]
    master_df.reset_index(drop=True,inplace=True)
    print(f"The size of Master dataset is: {master_df.shape}")

    #Convert dataframe to json to dump these record in mongodb
    json_record = list(json.loads(master_df.T.to_json()).values())

    mongo_client[DATABASE_NAME][MASTER_DATA_COLLECTION_NAME].insert_many(json_record)
    print(f"{master_df.shape[0]} records have been uploaded into Mongodb collection:-> {MASTER_DATA_COLLECTION_NAME}")



    #Loading shipping charge data file into Mongodb
    shipping_df = pd.read_excel(SHIPPING_CHARGE_FILE_PATH)
    shipping_df.reset_index(drop=True,inplace=True)
    print(f"The size of shipping charge dataset is: {shipping_df.shape}")

    #Convert dataframe to json to dump these record in mongodb
    #json_record = list(json.loads(shipping_df.T.to_json()).values())
    json_record = shipping_df.to_dict(orient="records")

    mongo_client[DATABASE_NAME][SHIPPING_CHARGE_COLLECTION_NAME].insert_many(json_record)
    print(f"{shipping_df.shape[0]} records have been uploaded into Mongodb collection:-> {SHIPPING_CHARGE_COLLECTION_NAME}")
