from src.pipeline.listing_pipeline import start_listing_pipeline
from src.exception import CustomException
import os, sys

if __name__=="__main__":
    try:
        start_point = start_listing_pipeline()
        print(start_point)

    except Exception as e:
       raise CustomException(e, sys)