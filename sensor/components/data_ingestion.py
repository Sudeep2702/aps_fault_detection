from sensor import utils
from sensor.entity import config_entity

from sensor.entity import artifact_entity
from sensor.Exception import SensorException
from sensor.logger import logging
import os,sys
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split


class DataIngestion:

    def __init__(self,data_ingestion_config:config_entity.Datainjestionconfig):
        try:
            logging.info(f"{'>>'*20}Data Ingestion{'<<20*'} ")
            self.data_ingestion_config= data_ingestion_config
        except Exception as e :
            raise SensorException(e,sys)

    def initiate_data_ingestion(self)->artifact_entity.DatainjestionArtifact():
        try:
            logging.info(f'exporting data to pandas dataframe')
            #exporting collection data as pandas dataframe
            df:pd.Dataframe = utils.get_collection_as_dataframe(
                database_name=self.data_ingestion_config.database_name,
                collection_name = self.data_ingestion_config.collection_name)

            logging.info("save data into feature store")

             #replace na with NAN
            df.replace(to_replace= "na",value=np.NAN,axis = 1,inplace=True)
             #save data into feature store
            logging.info('Create the feature folder if not exists')
             #creating feature store folder
            feature_store_dir = os.path.dirname(self.data_ingestion_config.feature_store_file_path)
            os.makedirs(feature_store_dir,exist_ok=True)
            logging.info("saving the features into data_folders")
              #save df into feature store
            df.to_csv(path_or_buf= self.data_ingestion_config.feature_store_file_path,index = False,header= True)

            logging.info("splitting the data into train and test data_set")
            train_df,test_df = train_test_split(df,self.data_ingestion_config.test_size,random_state = 40)
   
            logging.info("create dataset directory folder if not available")
            #create dataset directory folder if not available
            dataset_dir = os.path.dirname(self.data_ingestion_config.train_file_path)
            os.makedirs(dataset_dir,exist_ok=True)

            logging.info("save the data into data folder")
            df.to_csv(path_or_buf= self.data_ingestion_config.train_file_path,index = False,header= True)
            df.to_csv(path_or_buf= self.data_ingestion_config.test_file_path,index = False,header= True)
   
            #prepare artifact 
            data_ingestion_artifact = artifact_entity.DatainjestionArtifact(
                feature_store_file_path=self.data_ingestion_config.feature_store_file_path,
                train_file_path=self.data_ingestion_config.train_file_path,
                test_file_path = self.data_ingestion_config.test_file_path)
            logging.info(f" Data ingestion artifact:{data_ingestion_artifact}")
            return data_ingestion_artifact

        except Exception as e :
            raise SensorException(error_message=e, error_detail=sys)






