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


class DataTransformation:
    def __init__(self, 
                    data_transformation_config: config_entity.DataTransformationConfig,
                    data_ingestion_artifact: artifact_entity.DataIngestionArtifact):
        try:
            logging.info(f"{'>>'*10} Stage-03 Data Transformation Initiated {'<<'*10}")
            self.data_transformation_config=data_transformation_config
            self.data_ingestion_artifact=data_ingestion_artifact
            
        except Exception as e:
            raise CustomException(e, sys)

    def drop_missing_values(self, df)->pd.DataFrame:
        try:
            invalid_df = df[df.isna().any(axis=1)]     
            invalid_df["remarks"] = "Missing values"
            invalid_df.reset_index(drop=True, inplace=True)

            missing_values_count = df.isnull().values.sum()   # Total numbers of missing values
            if missing_values_count > 0:
                df.dropna(axis=0, inplace=True)
                df.reset_index(drop=True, inplace=True)

                logging.info(f"There were {missing_values_count} missing values in {invalid_df.shape[0]} rows and are removed from the dataset.")
                return df, invalid_df

            logging.info(f"There is no missing value in the dataset.")   
            return df, invalid_df

        except Exception as e:
            raise CustomException(e, sys)

    def drop_Non13_ISBN(self, df, invalid_df):
        # ISBN length is of 13 digits    
        # This function removes the rows where ISBN length is not equal to 13
        try:
            df['ISBN'] = df['ISBN'].astype(str)
            df['ISBN'] = df['ISBN'].str.split(".").str[0]
            df['ISBN13_len'] = df['ISBN'].apply(len)
            #df['ISBN_len'] = [len(str(int(x))) if isinstance(x, int) else len(str(x)) for x in df['ISBN']]
       
            temp_invalid_df = df.loc[df['ISBN13_len'] != 13]
            if temp_invalid_df.shape[0]>0:
                df.drop(df[df.ISBN13_len != 13].index, inplace = True)
                df.drop(["ISBN13_len","Date"], axis = 1, inplace = True)
                df.reset_index(drop=True, inplace=True)

                temp_invalid_df['remarks'] = "Non 13 digit ISBN"
                invalid_df = pd.concat([invalid_df, temp_invalid_df], ignore_index=True)
                logging.info("{} ISBNs are not of exactly 13 digits and removed from listing." .format(temp_invalid_df.shape[0]))
                return df, invalid_df
            else:
                df.drop(["ISBN13_len","Date"], axis=1, inplace=True)
                df.reset_index(drop=True, inplace=True, )
                logging.info("All ISBNs are of exactly 13 digits !!\n")
                return df, invalid_df 

        except Exception as e:
            raise CustomException(e, sys)         

    def drop_currency_code(self, df, invalid_df):
        # function will drop rows whose currency code is not in currency_code_list
        try:            
            currency_code_list = ['INR', 'USD', 'CAD', 'EUR', 'GBP']      
            
            temp_invalid_df = df[~df['Currency'].isin(currency_code_list)]
            if temp_invalid_df.shape[0]>0:            
                df = df[df['Currency'].isin(currency_code_list)]
                df.reset_index(drop=True, inplace=True)

                temp_invalid_df['remarks'] = "Invalid currency code"
                invalid_df = pd.concat([invalid_df, temp_invalid_df], ignore_index=True)
                logging.info("{} number of rows have invalid currency code.".format(temp_invalid_df.shape[0]))
                return df, invalid_df
            else:
                logging.info("Currency codes are as per defined currency_code_list.")
                return df, invalid_df 

        except Exception as e:
            raise CustomException(e, sys)

    def drop_price(self, df, invalid_df):   
        # function will drop rows where price is less than or equals to zero
        try:    
            temp_invalid_df = df.loc[df['Price']<=0]
            if temp_invalid_df.shape[0]>0:                      
                df = df.loc[df['Price']>0]
                df.reset_index(drop=True, inplace=True)
                
                temp_invalid_df['remarks'] = "Price <= Zero"
                invalid_df = pd.concat([invalid_df, temp_invalid_df], ignore_index=True)                
                logging.info("{} number of rows have Price less than or equals to zero.".format(temp_invalid_df.shape[0]))
                return df, invalid_df
            else:
                logging.info("All prices are more than zero.")
                return df, invalid_df 

        except Exception as e:
            raise CustomException(e, sys)

    def drop_quantity(self, df, invalid_df):
        # function will drop rows where Quantity is less than or equals to zero
        try:        
            temp_invalid_df = df.loc[df['Quantity']<=0]
            if temp_invalid_df.shape[0]>0:                      
                data = df.loc[df['Quantity']>0]
                data.reset_index(drop=True, inplace=True)
                
                temp_invalid_df['remarks'] = "Quantity <= Zero"
                invalid_df = pd.concat([invalid_df, temp_invalid_df], ignore_index=True)
                
                logging.info("{} number of rows have quantity less than or equals to zero.".format(temp_invalid_df.shape[0]))
                return data, invalid_df
            else:
                logging.info("All Quantities are more than zero.")
                return data, invalid_df 

        except Exception as e:
            raise CustomException(e, sys)

    def drop_quantity_below_threshold(self, df):
        # function to drop quanities below threshold level as per respective publisher
        try:      
            df.drop(df[(df['Publisher'].isin(["Osprey","Paragon","Amex"])) & (df['Quantity']<=3)].index, inplace=True)
            df.drop(df[(df['Publisher'].isin(["IBBD","CP","Tson"])) & (df['Quantity']<=4)].index, inplace=True)
            df.drop(df[(df['Publisher'].isin(["Rappa","GBD","RBC","WBC"])) & (df['Quantity']<=5)].index, inplace=True)
            
            df.reset_index(drop=True, inplace=True)
            logging.info("Quantities dropped below threshold levels for the respective publisher.")
            return df

        except Exception as e:
            raise CustomException(e, sys)

    def make_quantity_bins(self, df):
        # function will convert quanties into buckets     
        max_quantity = df['Quantity'].max()
        bins = [1,10,25,50,100,max_quantity+1]
        group = [2,5,10,25,50]
        
        try:
            df['Quantity'] = pd.cut(df['Quantity'], bins, labels=group) # This will change the data type to "Category"           
            df['Quantity'] = df['Quantity'].astype(int)           
            df.reset_index(drop=True, inplace=True)
            logging.info("Quantities converted into respective bins")
            return df

        except Exception as e:
            raise CustomException(e, sys)

    
    def currency_conversion_as_per_GOC(self, df):
        # Converting Prices of other currencies into INR as per GOC rates.
        # GOC stands for 'Goods of committee' and their rates are used by Publishers for currency conversion.
        # This usually remains fixed throughtout the month.
        try:
            df.loc[df['Currency']=="GBP", 'INR_Price'] = df.loc[df['Currency']=="GBP", 'Price']*101.00   # GBP/INR Rate
            df.loc[df['Currency']=="EUR", 'INR_Price'] = df.loc[df['Currency']=="EUR", 'Price']*87.00    # EUR/INR Rate
            df.loc[df['Currency']=="USD", 'INR_Price'] = df.loc[df['Currency']=="USD", 'Price']*82.00    # USD/INR Rate
            df.loc[df['Currency']=="CAD", 'INR_Price'] = df.loc[df['Currency']=="CAD", 'Price']*62.00    # CAD/INR Rate
            df.loc[df['Currency']=="INR", 'INR_Price'] = df.loc[df['Currency']=="INR", 'Price']

            logging.info("Prices have been converted into INR as per GOC rates.")
            return df  

        except Exception as e:
            raise CustomException(e, sys)

    def get_discount(self, df):
        # Applying discounts on INR_Price as per given by respective publisher
        # This is the price at which books are purchased

        try:    
            # Applying 40% discount on publishers: 'Osprey', 'Paragon','Amex'
            df.loc[df['Publisher'].isin(['Osprey', 'Paragon','Amex']), 'Discounted_Price'] = df.loc[df['Publisher'].isin(['Osprey', 'Paragon','Amex']), 'INR_Price']*0.60
            
            # Applying 35% discount on publishers: 'GBD', 'IBBD', 'Tson'
            df.loc[df['Publisher'].isin(['GBD', 'IBBD', 'Tson']), 'Discounted_Price'] = df.loc[df['Publisher'].isin(['GBD', 'IBBD', 'Tson']), 'INR_Price']*0.65
            
            # Applying 30% discount on publishers: 'CP', 'WBC', 'RBC', 'Rappa'
            df.loc[df['Publisher'].isin(['CP', 'WBC', 'RBC', 'Rappa']), 'Discounted_Price'] = df.loc[df['Publisher'].isin(['CP', 'WBC', 'RBC', 'Rappa']), 'INR_Price']*0.70
            
            logging.info("Discounts have been apppied as per the Publishers.")
            return df  

        except Exception as e:
            raise CustomException(e, sys)
    
    def convert_price_into_target_currency(self, df, usd_rate, cad_rate, aus_rate):
        # function will convert the discounted cost price into respective target currency
        # usd_rate, cad_rate, aus_rate are to be taken from the user through HTML form    
        try:
            df["USD_Price"] = round((df["Discounted_Price"]/usd_rate),2)
            df["CAD_Price"] = round((df["Discounted_Price"]/cad_rate),2)
            df["AUS_Price"] = round((df["Discounted_Price"]/aus_rate),2)
            
            logging.info("Prices have been converted into respective target currency.")
            return df

        except Exception as e:
            raise CustomException(e, sys)
        
       
    def get_net_cost_price(self, df):
        # function is used to generate Net_Cost_Price(ncp) of each currency as per Disclaimer_status(_D or _ND)
        # Net_Cost_Price(ncp) = Cost_price + Shipping_charge + Packing_cost
        # There are two types of shipping charges: one for disclaimer(_D) and other for Non_Disclaimer(_ND)  
        try:                   
            packing_cost = 1.00             
            if df["Disclaimer"]=="Yes":
                df['USD_CP'] = df["USD_Price"] + df["SC_USD_D"] + packing_cost
                df['CAD_CP'] = df["CAD_Price"] + df["SC_CAD_D"] + packing_cost
                df['AUS_CP'] = df["AUS_Price"] + df["SC_AUS_D"] + packing_cost
            else:
                df['USD_CP'] = df["USD_Price"] + df["SC_USD_ND"] + packing_cost
                df['CAD_CP'] = df["CAD_Price"] + df["SC_CAD_ND"] + packing_cost
                df['AUS_CP'] = df["AUS_Price"] + df["SC_AUS_ND"] + packing_cost
        
            return df
        except Exception as e:
            raise CustomException(e, sys)     


    def initiate_data_transformation(self)->artifact_entity.DataTransformationArtifact:

        try:
            logging.info("-----Transforming dataset-----")
            #df = utils.load_data(file_path=self.data_ingestion_artifact.raw_data_file_path)
            #master_df = utils.load_data(file_path=self.data_ingestion_artifact.master_data_file_path)
            #shipping_df = utils.load_data(file_path=self.data_ingestion_artifact.shipping_charge_file_path)

            df = utils.load_data(file_path="/config/workspace/Input_Master_Files/raw_data_file.xlsx")
            master_df = utils.load_data(file_path="/config/workspace/Input_Master_Files/Master_File.xlsx")
            shipping_df = utils.load_data(file_path="/config/workspace/Input_Master_Files/Shipping_Charge.xlsx")
            
            df, invalid_df = self.drop_missing_values(df=df)
            df, invalid_df = self.drop_Non13_ISBN(df=df, invalid_df=invalid_df)
            df, invalid_df = self.drop_currency_code(df=df, invalid_df=invalid_df)
            df, invalid_df = self.drop_price(df=df, invalid_df=invalid_df)
            df, invalid_df = self.drop_quantity(df=df, invalid_df=invalid_df)          

            df = self.drop_quantity_below_threshold(df=df)
            df = self.make_quantity_bins(df=df)

            # Merging data with master dataset
            master_df["ISBN"]=master_df["ISBN"].astype(str)
            df = pd.merge(df, master_df[["ISBN", "ASIN10", "Weight", "Status", "Disclaimer"]], on='ISBN', how='left')
                                            
            df.drop(df[df["Weight"]>= 2.500].index, inplace=True)        # Dropping rows having Weights >=2.500 KG
            df.drop(df[df["Status"]== "Barred"].index, inplace=True)     # Dropping rows having Staus as "Barred"
            
            # Fetching rows where ASIN info are not available in the master dataset.
            df_without_asin = df[pd.isnull(df["ASIN10"])]    
            logging.info("{} ISBNs have no ASIN and weights available in the master file" .format(df_without_asin.shape[0]))
            
            # Fetching rows where ASIN info are available in the master dataset.
            df = df[pd.notnull(df["ASIN10"])]
            df.reset_index(drop=True, inplace=True)
            logging.info("Dataset is merged with the master dataset successfully.")
            
            df = self.currency_conversion_as_per_GOC(df=df)
            df = self.get_discount(df=df)

            logging.info(f"Using user's inputs given through flask api.")
            user_inputs = utils.load_object(file_path=user_inputs_path)
            logging.info(f"USD/INR: {user_inputs['usd/inr']} CAD/INR: {user_inputs['cad/inr']} AUS/INR: {user_inputs['aus/inr']}") 
            df = self.convert_price_into_target_currency(df, user_inputs['usd/inr'], user_inputs['cad/inr'], user_inputs['aus/inr'])
            
            # few values are reflecting like 3.000004
            df["Weight"] = pd.to_numeric(df["Weight"], errors ='coerce')
            df["Weight"] = round(df["Weight"], 1) 
            df = pd.merge(df, shipping_df, on='Weight', how='left')
            
            df = df.apply(lambda row: self.get_net_cost_price(row), axis=1)
            logging.info("Net cost price is calculated as per respective currency.")

            utils.save_data(file_path= self.data_transformation_config.invalid_data_path, df=invalid_df)
            utils.save_data(file_path= self.data_transformation_config.data_without_asin_path, df=df_without_asin)
            utils.save_data(file_path= self.data_transformation_config.transformed_data_path , df=df)

            data_transformation_artifact = artifact_entity.DataTransformationArtifact(                    
                    invalid_data_path = self.data_transformation_config.invalid_data_path,
                    data_without_asin_path = self.data_transformation_config.data_without_asin_path,
                    transformed_data_path = self.data_transformation_config.transformed_data_path,
                    )

            logging.info(f"Data transformation artifact: {data_transformation_artifact}\n")
            return data_transformation_artifact

        except Exception as e:
            raise CustomException(e, sys)

        