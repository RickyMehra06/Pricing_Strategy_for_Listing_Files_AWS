from src.exception import CustomException
from src.logger import logging
from src.entity import config_entity, artifact_entity
from src import utils
import os, sys
import pandas as pd
import numpy as np
from typing import Optional


class DataValidation:
    def __init__(self,
                    data_validation_config: config_entity.DataValidationConfig,
                    data_ingestion_artifact: artifact_entity.DataIngestionArtifact):
        try:
            logging.info(f"{'>>'*10} Stage- 02 Data Validation initiated {'<<'*10}")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact

            self.validation_error_dict = dict()

        except Exception as e:
            raise CustomException(e, sys)
    
    def validate_missing_column(self,current_df,base_df, report_key_name)->bool:
        try:
            # function will tell us all base features are available in the original dataset or not
            base_columns = base_df.columns
            current_columns = current_df.columns

            missing_columns_list = []
            for base_column in base_columns:
                if base_column not in current_columns:
                    logging.info(f"{base_column} is not available in the current data.")
                    missing_columns_list.append(base_column)

            if len(missing_columns_list)>0:
                self.validation_error_dict[report_key_name]=missing_columns_list
                logging.info(f"Dataset does not have all required columns, kindly review columns.")
                return False

            logging.info(f"All expected features are available in the dataset")
            return True

        except Exception as e:
            raise CustomException(e, sys)

    def validate_missing_values_with_threshold(self, df, report_key_name)->bool:
        try:
            threshold = self.data_validation_config.missing_threshold

            null_report = df.isna().sum()/df.shape[0]
            column_names_above_threshold = null_report[null_report>threshold].index

            if len(list(column_names_above_threshold))>0:
                logging.info(f"{list(column_names_above_threshold)} features have missing values greater than threshold-{threshold}.")
                self.validation_error_dict[report_key_name] = list(column_names_above_threshold)
                return False
           
            logging.info(f"Missing values is validated as per the defined threshold.")
            return True            

        except Exception as e:
            raise CustomException(e, sys)


    def validate_column_names_and_datatypes(self, current_df, base_df, report_key_name)->bool:
        try:
            # below code is used to check numbers of features are same in both base and original dataset.
            # If we use len(base_df.columns)==len(current_df.columns)
            # then only numbers of features can be checked not their names.
            invalid_columm_name_list = []
            for elem1, elem2 in zip(sorted(list(base_df.columns)), sorted(list(current_df.columns))):
                if elem1 != elem2:
                    invalid_columm_name_list.append(elem2)        
            if len(list(invalid_columm_name_list))>0:
                self.validation_error_dict[report_key_name]=invalid_columm_name_list
                logging.info(f"{invalid_columm_name_list} features are not as per the base inventory template.")
                return False        
            logging.info(f"All required features are available as per the base inventory template.")
    
            # validating features having same data types as the base inventory template
            invalid_datatypes_list = []
            for elem1, elem2 in zip(sorted(list(base_df.columns)), sorted(list(current_df.columns))):
                if base_df[elem1].dtypes != current_df[elem2].dtypes:
                    invalid_datatypes_list.append(elem2)
            
            if len(list(invalid_datatypes_list))>0:
                self.validation_error_dict[report_key_name]=invalid_datatypes_list
                logging.info(f"{invalid_datatypes_list} feature does not have data types as per the base inventory template.")
                return False
    
            logging.info(f"All features have datatypes as per the base inventory template.")        
            return True

        except Exception as e:
            raise CustomException(e, sys)    
    
    
    def initiate_data_validation(self)->artifact_entity.DataValidationArtifact:
        try:
            logging.info("Loading base inventory_template file for validation process.")
            base_df = pd.read_excel(self.data_validation_config.base_file_path)
            #current_df = pd.read_excel(self.data_ingestion_artifact.raw_data_file_path)
            current_df = pd.read_excel("/config/workspace/Input_Master_Files/raw_data_file.xlsx")


            validate_missing_column_status = self.validate_missing_column(current_df=current_df,base_df=base_df, 
                                                                    report_key_name="missing_column_within_raw_dataset")
            if validate_missing_column_status:
                logging.info("All base features are available in the original dataset.")



            validate_missing_values_with_threshold_status=self.validate_missing_values_with_threshold(
                                                                                                    df=current_df, 
                                                                                                    report_key_name="missing_values_with_threshold")

            if validate_missing_values_with_threshold_status:
                logging.info("All features have missing values below threshold level.")



            validate_column_names_and_datatypes_status=self.validate_column_names_and_datatypes(current_df=current_df, 
                                                                                                base_df=base_df, 
                                                                                                report_key_name="names_datatypes_within_raw_dataset")
            if validate_column_names_and_datatypes_status:
                logging.info("Original dataset is as per the defined base inventory template.")


            # Write validation report
            logging.info("Writing validation report in yaml file")
            utils.write_yaml_file(file_path= self.data_validation_config.report_file_path, data= self.validation_error_dict)

            # Validation Artifact
            data_validation_artifact = artifact_entity.DataValidationArtifact(self.data_validation_config.report_file_path)
            logging.info(f"Data validation artifact: {data_validation_artifact}\n")
           
            return data_validation_artifact

        except Exception as e:
            raise CustomException(e, sys)



