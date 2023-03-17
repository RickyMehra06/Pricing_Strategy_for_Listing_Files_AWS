from src.exception import CustomException
from src.logger import logging
import os, sys
from datetime import datetime


DATA_FILES_PATH              = "/config/workspace/Input_FIles"
MASTER_DATA_FILE_PATH        = "/config/workspace/Input_Master_Files/Master_File.xlsx"
SHIPPING_CHARGE_FILE_PATH    = "/config/workspace/Input_Master_Files/Shipping_Charge.xlsx"

DATABASE_NAME = "Amazon_listing_db"
COLLECTION_NAME ="Inventory_collection"
MASTER_DATA_COLLECTION_NAME = "Master_data_collection"
SHIPPING_CHARGE_COLLECTION_NAME = "Shipping_charge_collection"

FILE_NAME = "original_dataset.xlsx"
MASTER_FILE_NAME = "master_dataset.xlsx"
SHIPPING_CHARGE_FILE_NAME = "shipping_charge_dataset.xlsx"

INVALID_DATA_FILE_NAME = "invalid_dataset.xlsx"
DATA_WITHOUT_ASIN_FILE_NAME = "data_without_ASIN.xlsx"
TRANSFORMED_DATA_FILE_NAME = "transformed_dataset.xlsx"

TARGET_SP_PRICE_DATA_FILE_NAME = "target_sp_price.xlsx"

USD_FILLZ_FILE_NAME = "usd_fillz_file.xlsx"
USD_AUTO_FILE_NAME = "usd_auto_file.xlsx"
CAD_FILLZ_FILE_NAME = "cad_fillz_file.xlsx"
CAD_AUTO_FILE_NAME = "cad_auto_file.xlsx"
AUS_FILLZ_FILE_NAME = "aus_fillz_file.xlsx"
AUS_AUTO_FILE_NAME =  "aus_auto_file.xlsx"


class ListingPipelineConfig:
    
    def __init__(self):
        try: 
            self.artifact_dir = os.path.join(os.getcwd(), "Artifact", f"{datetime.now().strftime('%Y-%m-%d__%H-%M-%S')}")

        except Exception as e:
            raise CustomException(e,sys) 

class DataIngestionConfig:
    def __init__(self, listing_pipeline_config: ListingPipelineConfig):
        try:
            self.data_ingestion_dir = os.path.join(listing_pipeline_config.artifact_dir, "data_ingestion")

            self.raw_data_file_path = os.path.join(self.data_ingestion_dir, FILE_NAME)
            self.master_data_file_path = os.path.join(self.data_ingestion_dir, MASTER_FILE_NAME)
            self.shipping_charge_file_path = os.path.join(self.data_ingestion_dir, SHIPPING_CHARGE_FILE_NAME)

        except Exception as e:
            raise CustomException(e,sys) 

class DataValidationConfig:
     def __init__(self, listing_pipeline_config: ListingPipelineConfig):
        try:
            self.data_validation_dir = os.path.join(listing_pipeline_config.artifact_dir, "data_validation")
            
            #self.good_data_file_path = os.path.join(self.data_validation_dir, GOOD_DATA_FILE_NAME)
            #self.bad_data_file_path = os.path.join(self.data_validation_dir, BAD_DATA_FILE_NAME)
            self.report_file_path = os.path.join(self.data_validation_dir, "validation_report.yaml")

            self.base_file_path = "/config/workspace/Base_file/Inventory_template.xlsx"            
            self.missing_threshold:float = 0.3

        except Exception as e:
            raise CustomException(e, sys)

class DataTransformationConfig:
    def __init__(self, listing_pipeline_config: ListingPipelineConfig):
        try:        
            self.data_transformation_dir = os.path.join(listing_pipeline_config.artifact_dir, "data_transformation")
            self.invalid_data_path = os.path.join(self.data_transformation_dir, "Transformed", INVALID_DATA_FILE_NAME)
            self.data_without_asin_path = os.path.join(self.data_transformation_dir, "Transformed", DATA_WITHOUT_ASIN_FILE_NAME)     
            self.transformed_data_path = os.path.join(self.data_transformation_dir, "Transformed", TRANSFORMED_DATA_FILE_NAME)
            
        except Exception as e:
            raise CustomException(e, sys)

class TargetPriceGeneratorConfig:
    def __init__(self, listing_pipeline_config: ListingPipelineConfig):
        try:        
            self.target_price_generator_dir = os.path.join(listing_pipeline_config.artifact_dir, "target_sp_price")
            self.target_sp_price_data_path = os.path.join(self.target_price_generator_dir, "target_sp_price", TARGET_SP_PRICE_DATA_FILE_NAME)
            
        except Exception as e:
            raise CustomException(e, sys)

class FilesGenerationConfig:
    def __init__(self, listing_pipeline_config: ListingPipelineConfig):
        try:        
            self.files_generation_dir = os.path.join(listing_pipeline_config.artifact_dir, "Files_generated")
            self.usd_fillz_file_path = os.path.join(self.files_generation_dir, "Files_generated", USD_FILLZ_FILE_NAME)
            self.usd_auto_file_path = os.path.join(self.files_generation_dir, "Files_generated", USD_AUTO_FILE_NAME)
            self.cad_fillz_file_path = os.path.join(self.files_generation_dir, "Files_generated", CAD_FILLZ_FILE_NAME)
            self.cad_auto_file_path = os.path.join(self.files_generation_dir, "Files_generated", CAD_AUTO_FILE_NAME)
            self.aus_fillz_file_path = os.path.join(self.files_generation_dir, "Files_generated", AUS_FILLZ_FILE_NAME)
            self.aus_auto_file_path = os.path.join(self.files_generation_dir, "Files_generated", AUS_AUTO_FILE_NAME)

        except Exception as e:
            raise CustomException(e, sys)
        


