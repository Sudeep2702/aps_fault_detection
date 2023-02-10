from sensor.logger import logging 
from sensor.utils import get_collection_as_dataframe
from sensor.Exception import SensorException
from sensor.entity.config_entity import Datainjestionconfig
from sensor.entity import config_entity
from sensor.components import data_ingestion
from sensor.components.data_ingestion import DataIngestion


import sys,os

if __name__ == "__main__":
     try :
          training_pipeline_config = config_entity.TrainingPipelineConfig()
          data_ingestion_config = Datainjestionconfig(training_pipeline_config)
          print(data_ingestion_config.to_dict())
          data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
          print(data_ingestion.initiate_data_ingestion)

     except Exception as e:
          raise SensorException(e,sys)
          print(e)  
     
