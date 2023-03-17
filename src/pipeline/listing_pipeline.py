from src.exception import CustomException
from src.logger import logging
import os, sys

from src.entity import config_entity

from src.components.stage_01_data_ingestion import DataIngestion
from src.components.stage_02_data_validation import DataValidation
from src.components.stage_03_data_transformation import DataTransformation
from src.components.stage_04_target_price_generator import TargetPriceGenerator
from src.components.stage_05_files_generation import FilesGeneration


def start_listing_pipeline():
    try:
        listing_pipeline_config = config_entity.ListingPipelineConfig()

        #data ingestion
        data_ingestion_config= config_entity.DataIngestionConfig(listing_pipeline_config= listing_pipeline_config)
        data_ingestion = DataIngestion(data_ingestion_config= data_ingestion_config)
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        
        # data vaidation
        data_validation_config = config_entity.DataValidationConfig(listing_pipeline_config= listing_pipeline_config)

        data_validation = DataValidation(data_validation_config, data_ingestion_artifact)
        data_validation_artifact = data_validation.initiate_data_validation()


        # Data Transformation
        data_transformation_config = config_entity.DataTransformationConfig(listing_pipeline_config= listing_pipeline_config)

        data_transformation = DataTransformation(data_transformation_config, data_ingestion_artifact)
        data_transformation_artifact= data_transformation.initiate_data_transformation()


        # Target Price Generation
        target_price_generator_config = config_entity.TargetPriceGeneratorConfig(listing_pipeline_config= listing_pipeline_config)
        
        target_price_generator = TargetPriceGenerator(target_price_generator_config, data_transformation_artifact)
        target_price_generator_artifact= target_price_generator.initiate_target_price_generator()

        # Files Generation
        files_generation_config = config_entity.FilesGenerationConfig(listing_pipeline_config= listing_pipeline_config)
        
        files_generation = FilesGeneration(files_generation_config, target_price_generator_artifact)
        files_generation_artifact= files_generation.initiate_files_generation()
        


    except Exception  as e:
        raise CustomException(e,sys) 