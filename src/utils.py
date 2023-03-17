from src.exception import CustomException
from src.logger import logging

import pandas as pd
import numpy as np
import os, sys
import yaml
import pickle
from datetime import datetime



IND_holiday_list = ['2022-10-02', '2022-10-05','2022-10-24','2022-11-08','2022-12-25']
US_holiday_list = ['2022-10-10', '2022-11-24', '2022-12-25', '2023-01-01']

 
def write_yaml_file(file_path, data:dict):
    try:
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir, exist_ok=True)
        with open(file_path,"w") as file_obj:
            yaml.dump(data, file_obj)

    except Exception as e:
        raise CustomException(e, sys)

def save_data(file_path:str, df)-> None:
    try:
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir, exist_ok=True)

        df.to_excel(file_path, index=False, header=True)

    except Exception as e:
        raise CustomException(e, sys)

def load_data(file_path:str) -> object:
    try:
        if not os.path.exists(file_path):
            raise Exception(f"The dataframe {file_path} does not exist.")

        return pd.read_excel(file_path)

    except Exception as e:
        raise CustomException(e, sys)

def save_object(file_path:str, obj:object)->None:
    try:
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir, exist_ok=True)
        
        with open(file_path, "wb") as file_obj:
            pickle.dump(obj, file_obj)
        logging.info(f"{file_path.split('/')[-1]} object has been saved thorough utils")
    except Exception as e:
        raise CustomException(e, sys)

def load_object(file_path:str) -> object:
    try:
        if not os.path.exists(file_path):
            raise Exception(f"The file {file_path} does not exist.")
    
        with open(file_path, "rb") as file_obj:
            return pickle.load(file_obj)

    except Exception as e:
        raise CustomException(e, sys)