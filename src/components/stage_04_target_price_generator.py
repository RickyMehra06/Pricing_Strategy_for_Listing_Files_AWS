from src.exception import CustomException
from src.logger import logging
from src.entity import config_entity, artifact_entity
from src import utils
import os, sys
import pandas as pd
import numpy as np
from typing import Optional
from typing import Dict


import warnings
warnings.filterwarnings('ignore')

user_inputs_path = "/config/workspace/User_Inputs_Api/Inputs/User_Inputs.pkl"

class TargetPriceGenerator:
    def __init__(self, 
                    target_price_generator_config: config_entity.TargetPriceGeneratorConfig,
                    data_transformation_artifact: artifact_entity.DataTransformationArtifact):
        try:
            logging.info(f"{'>>'*10} Stage-04 Target Price Generator initiated {'<<'*10}")
            self.target_price_generator_config=target_price_generator_config
            self.data_transformation_artifact=data_transformation_artifact
            
        except Exception as e:
            raise CustomException(e, sys)
   
    def get_selling_price(self, df, cp_col, sc, fixed_fee, req_profit):
        # ncp: Net_Cost_Price in respective curreny
        # sc: Shipping charge taken from the buyer
        # fixed_fee: fixed fee is charged per item sold
        # req_profit: required amount of profit the Seller wants in respective dollar 
        try:
            sp = []
            profit_column = []
            gift_wrap = 0

            for x in df[cp_col]:                
                learning_rate = 1.5
                profit = req_profit   

                while (profit >= req_profit):
                    temp_sp = x*learning_rate        
                    total = temp_sp + sc + gift_wrap                
                    fee = total*.15 + fixed_fee
                    earning = total - fee
                    profit = round((earning-x),2)                

                    if profit >= req_profit:
                        learning_rate = learning_rate - 0.0001
                    else:
                        sp.append(round(temp_sp,2))
                        profit_column.append(profit)

            logging.info("Target selling price has been achieved.")
            return sp, profit_column

        except Exception as e:
            raise CustomException(e, sys)        
    
    def get_min_selling_price(self, df, cp_col, sc, fixed_fee, min_profit):
        # ncp: Net_Cost_Price in respective curreny
        # sc: Shipping charge taken from the buyer
        # fixed_fee: fixed fee is charged per item sold
        # min_profit: minimum amount of profit the Seller wants in respective dollar 
        try:
            min_sp = []
            min_profit_column = []
            gift_wrap = 0

            for x in df[cp_col]:                
                learning_rate = 1.5
                minimum_profit = min_profit

                while (minimum_profit >= min_profit):
                    temp_min_sp = x*learning_rate             
                    total = temp_min_sp + sc + gift_wrap                
                    fee = total*.15 + fixed_fee
                    earning = total - fee
                    minimum_profit = round((earning-x),2)                

                    if minimum_profit >= min_profit:
                        learning_rate = learning_rate - 0.0001
                    else:
                        min_sp.append(round(temp_min_sp,2))
                        min_profit_column.append(minimum_profit) 
                        
            logging.info("Target minimum selling price has been achieved.")
            return min_sp, min_profit_column

        except Exception as e:
            raise CustomException(e, sys)

    def initiate_target_price_generator(self)->artifact_entity.TargetPriceGeneratorArtifact:

        try:
            user_inputs = utils.load_object(file_path=user_inputs_path)
            print(user_inputs)

            logging.info(f"USER INPUTS: {user_inputs}")            

            #df = utils.load_data(file_path=self.data_transformation_artifact.transformed_data_path)
            df = utils.load_data(file_path="/config/workspace/Input_Master_Files/transformed_dataset.xlsx")

            df["SP_USD"], df["Profit_USD"] = self.get_selling_price(df, "USD_CP", 3.75, 1.80, user_inputs['us_profit'])
            df["SP_CAD"], df["Profit_CAD"] = self.get_selling_price(df, "CAD_CP", 5.00, 1.00, user_inputs['cad_profit'])
            df["SP_AUS"], df["Profit_AUS"] = self.get_selling_price(df, "AUS_CP", 7.00, 1.80, user_inputs['aus_profit'])

            df["Min_SP_USD"], df["Min_Profit_USD"]  = self.get_min_selling_price(df, "USD_CP", 3.75, 1.80, user_inputs['us_min_profit'])
            df["Min_SP_CAD"], df["Min_Profit_CAD"]  = self.get_min_selling_price(df, "CAD_CP", 3.75, 1.80, user_inputs['cad_min_profit'])
            df["Min_SP_AUS"], df["Min_Profit_AUS"]  = self.get_min_selling_price(df, "AUS_CP", 3.75, 1.80, user_inputs['aus_min_profit'])

            df["Max_SP_USD"]  = df["SP_USD"] + 5
            df["Max_SP_CAD"]  = df["SP_CAD"] + 5
            df["Max_SP_AUS"]  = df["SP_AUS"] + 5

            utils.save_data(file_path= self.target_price_generator_config.target_sp_price_data_path , df=df)

            target_price_generator_artifact = artifact_entity.TargetPriceGeneratorArtifact(                    
                    target_sp_price_data_path = self.target_price_generator_config.target_sp_price_data_path)

            logging.info(f"Target Price Generator artifact: {target_price_generator_artifact}\n")
            return target_price_generator_artifact

        except Exception as e:
            raise CustomException(e, sys)
