# Provide the mongodb localhost url to connect python to mongodb.
import pymongo
import pandas as pd
import json
Data_file_path ='/config/workspace/aps_failure_training_set1.csv'
client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")
Data_base_name = 'aps'
collection_name =  'sensor'
if __name__=='__main__':
    df = pd.read_csv(Data_file_path)
    print(f"Rows and columns :{df.shape}")
#convert data_frame into json so that we can dump these records in mongo db 
    df.reset_index(drop=True,inplace=True )
    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])
 #dumping into mongodb
    client[Data_base_name][collection_name].insert_many(json_record)
