from src.pipeline.listing_pipeline import start_listing_pipeline
from src.exception import CustomException
import os, sys
from src import utils

import numpy as np
import pandas as pd
import glob
from datetime import date

import pymongo

from flask import Flask, request, jsonify, url_for, render_template

data_keys = ['usd/inr','cad/inr','aus/inr','us_profit','cad_profit','aus_profit','us_min_profit','cad_min_profit','aus_min_profit']
USER_INPUTS_OBJECT_FILE_NAME= "User_Inputs.pkl"

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/fillz_api', methods=['POST'])

def fillz_api():
    try:
        data = [float(x) for x in request.form.values()]
        data = [round(x,2) for x in data]
        user_inputs_dict = dict(zip(data_keys, data))  

        user_inputs_dir = os.path.join(os.getcwd(), "User_Inputs_Api")
        user_inputs_object_path = os.path.join(user_inputs_dir, "Inputs", USER_INPUTS_OBJECT_FILE_NAME)
        utils.save_object(file_path=user_inputs_object_path, obj=user_inputs_dict)
        print("User inputs are saved successfully using pickle file.")

        start_point = start_listing_pipeline()
        print(start_point)

    except Exception as e:
        raise CustomException(e, sys)

    return render_template('home.html', output_data = "USD, CAD & AUS files are created!!")        
        

if __name__=="__main__":
    try:       
        #app.run(debug=True)
        #app.run(host="0.0.0.0")
        app.run(host="0.0.0.0", port=8080)

    except Exception as e:
       raise CustomException(e, sys)
   
   
