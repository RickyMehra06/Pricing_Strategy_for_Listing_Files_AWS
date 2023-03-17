from src.exception import CustomException
from src.logger import logging
from src import utils

from src.entity import config_entity
from src.entity import artifact_entity
from src.config import mongo_client

from datetime import datetime
import os, sys
import pandas as pd 
import numpy as np 


class DataIngestion:
    def __init__(self, data_ingestion_config: config_entity.DataIngestionConfig):
        try:
            logging.info(f"{'>>'*10} Stage 01- Data Ingestion initiated {'<<'*10}")
            self.data_ingestion_config = data_ingestion_config

        except Exception as e:
            raise CustomException(e, sys)

    def get_collection_as_dataframe(self, database_name:str, collection_name:str)-> pd.DataFrame:
        try:
            logging.info(f"Reading data from Mongodb from database-{database_name} and collection-{collection_name}")
            df = pd.DataFrame(list(mongo_client[database_name][collection_name].find()))
            
            if "_id" in df.columns:
                logging.info("Removing _id column from the dataframe")
                df.drop("_id", axis=1, inplace =True)
            logging.info(f"Rows and colums: {df.shape}")
            return df

        except Exception as e:
            raise CustomException(e, sys)
        
    def initiate_data_ingestion(self)-> artifact_entity.DataIngestionArtifact:
        try:
            logging.info(f"Loading data from Mongodb")

            # Loading Invetory data from Mongodb
            data = self.get_collection_as_dataframe(
                database_name= config_entity.DATABASE_NAME, 
                collection_name= config_entity.COLLECTION_NAME)            
            data_ingestion_dir = os.path.join(self.data_ingestion_config.data_ingestion_dir)
            os.makedirs(data_ingestion_dir, exist_ok=True)

            data.to_excel(self.data_ingestion_config.raw_data_file_path, index=False, header=True)
            logging.info(f"Loaded data saved into raw_data_file_path")


            # Loading master data from Mongodb
            master_df = self.get_collection_as_dataframe(
                database_name= config_entity.DATABASE_NAME, 
                collection_name= config_entity.MASTER_DATA_COLLECTION_NAME)          
            master_df.to_excel(self.data_ingestion_config.master_data_file_path, index=False, header=True)
            logging.info(f"Master dataset has been saved into master_data_file_path")


            # Loading master data from Mongodb
            shipping_charge_df = self.get_collection_as_dataframe(
                database_name= config_entity.DATABASE_NAME, 
                collection_name= config_entity.SHIPPING_CHARGE_COLLECTION_NAME)          
            shipping_charge_df.to_excel(self.data_ingestion_config.shipping_charge_file_path, index=False, header=True)
            logging.info(f"Shipping charge dataset has been saved into shipping_charge_file_path")

            # prepare artifact
            data_ingestion_artifact = artifact_entity.DataIngestionArtifact(
                raw_data_file_path = self.data_ingestion_config.raw_data_file_path,
                master_data_file_path = self.data_ingestion_config.master_data_file_path,
                shipping_charge_file_path = self.data_ingestion_config.shipping_charge_file_path)
            
            logging.info(f"Stage 01- Data ingestion artifact: {data_ingestion_artifact}\n")
            return data_ingestion_artifact

        except Exception as e:
            raise CustomException(e, sys)
