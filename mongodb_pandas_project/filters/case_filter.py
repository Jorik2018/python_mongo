from datetime import datetime
import json
import os
from mongodb_pandas_project.backup_script import batching, file_exists, generate_csv, backup_dir, get_db, get_timestamp, to_int, to_string, to_hex
import pandas as pd
from bson import ObjectId
import time

from mongodb_pandas_project.storage import download, upload

limit = 10000
db = get_db()
dateformat = "%Y-%m-%d %H:%M:%S"

def filter():
    data_map = {}
    query = None
    date = None
    skip = 0
    if download(f'filtered_mongodb/limits.json', f'{backup_dir}/limits.json'):
        if(file_exists(f'{backup_dir}/limits.json')):
            with open(f'{backup_dir}/limits.json', "r") as file:
                try:
                    data_map = json.loads(file.read())
                    print(data_map)
                    if 'case' in data_map:
                        #date = datetime.strptime(data_map['case']['createDate'],dateformat)
                        skip = data_map['case']['$skip']
                        #query = json.dumps({"createDate": {"$gte": {"$date":date.isoformat()+"Z"}}})
                except (TypeError, ValueError) as e:
                    print(f"Error al analizar el contenido JSON: {e}")


    generate_csv('case', '_id,caseCode', notimed =True, query=query, sort = None, skip = skip, limit = limit)

    max_high_date = None#result[0]['maxDate'] if result else None

    tmp_file_path = f'{backup_dir}/case_tmp.csv'
    file_path = f'{backup_dir}/case.csv'
    filtered_path = f'{backup_dir}/filtered_mongodb/case.csv'
    nrows = 1000
    total = 0
    if os.path.exists(filtered_path):os.remove(filtered_path)
    while True:
        df = pd.read_csv(file_path,skiprows = range(1, 000), nrows = nrows)
        total = total + len(df)
        if df.empty:
            if(file_exists(filtered_path)):
                file = f'filtered_mongodb/case_filtered_skip_{skip}_limit_{limit}_{get_timestamp()}.csv'
                upload(filtered_path,file )
                print(file)
                df = pd.read_csv(filtered_path)
                #Si no existe algo que eliminar se incrementara el skip para seguir a la siguiente pagina
                #Si se eliminan documetos se repasara la pagina actual
                if limit == total: skip = skip + limit
                skip = skip - len(df)
                data_map['case'] = {'$skip':skip}
                with open(f'{backup_dir}/limits.json', 'w+') as file:
                    json.dump(data_map, file) 
                if len(df):
                    cursor = db['case'].find({
                        "_id": {"$in": [ObjectId(_id) for _id in df['_id'].unique()]}
                    }, {
                        "_id": 1,
                        "nameExternal": 1,
                        "contactInfo.numberDocument": 1,
                        "contactInfo.fullName": 1,
                        "companyInfo.code": 1,
                        "companyInfo.legalName": 1
                    })
                    mongo_df = pd.DataFrame(list(cursor))
                    print(mongo_df)
                    mongo_df['_id'] = mongo_df['_id'].apply(lambda x: str(x))
                    
                    df = df.merge(mongo_df, left_on='_id', right_on='_id', how='left')
                    df.to_csv(filtered_path, index=False)
                    upload(filtered_path,file )
                delete(df)
                upload(f'{backup_dir}/limits.json',f'filtered_mongodb/limits.json' )
            break
        print(f"===========| Chunk size = {len(df)} |============")
        df['_id'] = df['_id'].apply(lambda x: to_hex(x))
        df['caseCode'] = df['caseCode'].apply(lambda x: to_string(x))
        df['caseCodeInt'] = df['caseCode'].apply(lambda x: to_int(x))
        df['ObjectId'] = df['_id'].apply(lambda x: ObjectId(x))
        #print(df)
        filters = [
            filter_by_historycase,
            filter_by_provisionreport,
            filter_by_financing
        ]
        for filter_function in filters:
            start_time = time.time()  # Record start time
            print(f"Applying filter: {filter_function.__name__}")
            initial_len =len(df)
            df = filter_function(df)
            df = df[df['exist'] != True]
            end_time = time.time()  # Record the end time
            #print(df)
            minutes, seconds = divmod(end_time - start_time, 60)
            print(f"{initial_len} to {len(df)}.\t\tTime used by {filter_function.__name__}: {int(minutes)} minutes and {seconds:.2f} seconds")
            if len(df)==0: break
        #print(df['ObjectId'].tolist())
        os.makedirs(os.path.dirname(filtered_path), exist_ok=True)
        if os.path.exists(filtered_path):
            df[['_id', 'caseCode']].to_csv(filtered_path, mode='a', index=False, header=False)
        else:
            df[['_id', 'caseCode']].to_csv(filtered_path, index=False)
        batching(file_path,tmp_file_path,nrows)
        os.remove(file_path)
        os.rename(tmp_file_path, file_path)
        
def delete(df):
    print("Rows to delete:", len(df))
    if len(df)>0:
        df['_id'] = df['_id'].apply(lambda x: to_hex(x))
        #print(df['_id'].apply(lambda x: ObjectId(x)).tolist())
        #print("Number of rows:", len(df))
        result = db['case'].delete_many({"_id": {"$in": df['_id'].apply(lambda x: ObjectId(x)).tolist()}})
        print(f"Number of documents deleted: {result.deleted_count}")

def filter_by_provisionreport(df):#provisionReport
    id_list = df['caseCode'].tolist()
    query = [
        {"$match":{'caseCode': {'$in': id_list}}},
        {"$group": {"_id":  {"caseCode": "$caseCode"}}},
        {"$project": {"_id":0,"caseCode":"$_id.caseCode"}},
    ]
    result_list = list(db['provisionReport'].aggregate(query))
    id_list = list(set([item.get('caseCode', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['caseCode'].isin(id_list)
    return df

def filter_by_financing(df):#financing
    id_list = df['caseCodeInt'].tolist()
    query = [
        {"$match":{'caseCode': {'$in': id_list}}},
        {"$group": {"_id":  {"caseCode": "$caseCode"}}},
        {"$project": {"_id":0,"caseCode":"$_id.caseCode"}},
    ]
    result_list = list(db['financing'].aggregate(query))
    id_list = list(set([item.get('caseCode', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['caseCodeInt'].isin(id_list)
    return df

def filter_by_historycase(df):#historyCase
    id_list = df['_id'].tolist()
    query = [
        {"$match":{'idCase': {'$in': id_list}}},
        {"$group": {"_id":  {"caseCode": "$idCase"}}},
        {"$project": {"_id":0,"caseCode":"$_id.caseCode"}},
    ]
    result_list = list(db['historyCase'].aggregate(query))
    id_list = list(set([item.get('caseCode', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['_id'].isin(id_list)
    return df
