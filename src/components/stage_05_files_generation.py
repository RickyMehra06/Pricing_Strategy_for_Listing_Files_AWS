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

class FilesGeneration:
    def __init__(self, 
                    files_generation_config: config_entity.FilesGenerationConfig,
                    target_price_generator_artifact: artifact_entity.TargetPriceGeneratorArtifact):
        try:
            logging.info(f"{'>>'*10} Stage-05 Files Generation Initiated {'<<'*10}")
            self.files_generation_config=files_generation_config
            self.target_price_generator_artifact=target_price_generator_artifact

            self.usd_fillz_columns = ['SKU_USD','ASIN10','product-id-type','SP_USD','item-condition','Quantity','add-delete',
                                    'will-ship-internationally','expedited-shipping', 'item-note']                               
            self.usd_auto_columns  = ['SKU_USD','Min_SP_USD','Max_SP_USD','country-code_USD','currency-code_USD', 
                                    'rule-name','rule-action']
            
            
            self.cad_fillz_columns = ['SKU_CAD','ASIN10','product-id-type','SP_CAD','item-condition','Quantity','add-delete',
                                    'will-ship-internationally','expedited-shipping', 'item-note']                                         
            self.cad_auto_columns  = ['SKU_CAD','Min_SP_CAD','Max_SP_CAD','country-code_CAD','currency-code_CAD',
                                    'rule-name','rule-action']
            
            
            self.aus_fillz_columns = ['SKU_AUS','ASIN10','product-id-type','SP_AUS','item-condition','Quantity','add-delete',
                                    'will-ship-internationally','expedited-shipping', 'item-note']                                   
            self.aus_auto_columns  = ['SKU_AUS', 'Min_SP_AUS', 'Max_SP_AUS', 'country-code_USD', 
                                    'currency-code_AUS', 'rule-name', 'rule-action']

            self.Output_Folder = r"Output_Folder/"

            
        except Exception as e:
            raise CustomException(e, sys)

    def initiate_files_generation(self)->artifact_entity.FilesGenerationArtifact:

        try:
            logging.info("Loading required_sp_price_data file.")
            #df = utils.load_data(file_path=self.price_generation_artifact.required_sp_price_data_path)
            data = utils.load_data(file_path="/config/workspace/Input_Master_Files/required_sp_price.xlsx")

            # Creating subset of dataset with the required columns to make fillz files and automated pricing fillz files.
            df=data[['ISBN','Quantity','Publisher','ASIN10','SP_USD','Min_SP_USD', 'Max_SP_USD', 
                    'SP_CAD','Min_SP_CAD','Max_SP_CAD', 'SP_AUS','Min_SP_AUS', 'Max_SP_AUS']]
            logging.info("Subset of dataset is created with required columns for creating fillz files.")

            # Creating SKU for the respective currency code
            df["SKU_USD"] = df["Publisher"]+"_USD_"+df["ISBN"].astype(str)
            df["SKU_CAD"] = df["Publisher"]+"_CAD_"+df["ISBN"].astype(str)
            df["SKU_AUS"] = df["Publisher"]+"_AUS_"+df["ISBN"].astype(str)
            logging.info("SKU created for all region as per currency code.")

            # Creating columns with constant values which are required for creating fillz files template
            df['product-id-type'] = 1
            df['item-condition'] = 11
            df['add-delete'] = 'a'
            df['will-ship-internationally'] = 'n'
            df['country-code_USD'] = 'US'
            df['currency-code_USD'] = 'USD'
            df['country-code_CAD'] = 'CA'
            df['currency-code_CAD'] = 'CAD'
            df['country-code_AUS'] = 'AU'
            df['currency-code_AUS'] = 'AUS'
            df['rule-name'] = 'lowest_0.02'
            df['rule-action'] = 'start'
            df['expedited-shipping'] = ''
            df['item-note'] = ''
            logging.info("Creating columns with constant values which are required for creating fillz files template.")

            # Creating subset of dataset as per the US region Fillz file template
            usd_df = df[self.usd_fillz_columns]
            usd_df = usd_df.rename(columns={"SKU_USD":"SKU", "ASIN10":"product-id", "SP_USD":"price", "Quantity":"quantity"}) 

            # Creating subset of dataset as per the US region automated pricing file template
            usd_auto = df[self.usd_auto_columns]
            usd_auto = usd_auto.rename(columns={'SKU_USD':'SKU', 'Min_SP_USD':'minimum-seller-allowed-price',
                                                'Max_SP_USD':'maximum-seller-allowed-price', 'country-code_USD':'country-code', 
                                                'currency-code_USD':'currency-code'})
            utils.save_data(file_path= self.files_generation_config.usd_fillz_file_path , df=usd_df)
            utils.save_data(file_path= self.files_generation_config.usd_auto_file_path , df =usd_auto)
            logging.info('USD Fillz and Automated pricing files have been generated successfully.')



            # Creating subset of dataset as per Canadian region Fillz file template
            cad_df = df[self.cad_fillz_columns]
            cad_df = cad_df.rename(columns={"SKU_CAD":"SKU", "ASIN10":"product-id", "SP_CAD":"price", "Quantity":"quantity"})
            
            # Creating subset of dataset as per Canadian region automated pricing file template
            cad_auto = df[self.cad_auto_columns]
            cad_auto = cad_auto.rename(columns={'SKU_CAD':'SKU', 'Min_SP_CAD':'minimum-seller-allowed-price',
                                                'Max_SP_CAD':'maximum-seller-allowed-price', 'country-code_CAD':'country-code', 
                                                'currency-code_CAD':'currency-code'})
            utils.save_data(file_path= self.files_generation_config.cad_fillz_file_path , df=cad_df)
            utils.save_data(file_path= self.files_generation_config.cad_auto_file_path , df =cad_auto)
            logging.info('CAD Fillz and Automated pricing files have been generated successfully.')



            # Creating subset of dataset as per Australian region Fillz file template
            aus_df = df[self.aus_fillz_columns]
            aus_df = aus_df.rename(columns={"SKU_AUS":"SKU", "ASIN10":"product-id", "SP_AUS":"price", "Quantity":"quantity"})
            
            # Creating subset of dataset as per Australian region automated pricing file template
            aus_auto = df[self.aus_auto_columns]
            aus_auto = aus_auto.rename(columns={'SKU_AUS':'SKU', 'Min_SP_AUS':'minimum-seller-allowed-price',
                                                'Max_SP_AUS':'maximum-seller-allowed-price', 'country-code_AUS':'country-code', 
                                                'currency-code_AUS':'currency-code'})
            utils.save_data(file_path= self.files_generation_config.aus_fillz_file_path , df=aus_df)
            utils.save_data(file_path= self.files_generation_config.aus_auto_file_path , df =aus_auto)
            logging.info('AUS Fillz and Automated pricing files have been generated successfully.')

            files_generation_artifact = artifact_entity.FilesGenerationArtifact(                    
                    usd_fillz_file_path = self.files_generation_config.usd_fillz_file_path,
                    usd_auto_file_path  = self.files_generation_config.usd_auto_file_path,
                    cad_fillz_file_path = self.files_generation_config.cad_fillz_file_path,
                    cad_auto_file_path  = self.files_generation_config.cad_auto_file_path,
                    aus_fillz_file_path = self.files_generation_config.aus_fillz_file_path,
                    aus_auto_file_path  = self.files_generation_config.aus_auto_file_path
                    )

            logging.info(f"Files Generation artifact: {files_generation_artifact}\n")
            return files_generation_artifact          


        except Exception as e:
            raise CustomException(e, sys)

    