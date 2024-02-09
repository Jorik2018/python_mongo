from pymongo import MongoClient
import pandas as pd
import subprocess
import argparse
from datetime import datetime
from bson import ObjectId
from dotenv import load_dotenv
import os

load_dotenv()
uri = os.getenv('MONGODB_URI') 
database_name = os.getenv('MONGODB_NAME')
backup_dir = os.getenv('BACKUP_DIR','backup')
client = MongoClient(uri)
db = client[database_name]
backup_directory = 'path_to_backup_directory'
collections_to_backup = ['user','case']
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

def collections():
    collections = db.list_collection_names()
    print(f'Collections in the database "{database_name}":')
    for collection in collections:
        print(collection)

def generate_csv(collections = 'company', fields = False, notimed =False):
    if not collections: collections = 'company'
    if not fields: fields = '_id,username'
    if notimed:
        filename = f'{backup_dir}/{collections}.csv'
    else:
        filename = f'{backup_dir}/{collections}_{timestamp}.csv'
    print(filename)
    print(fields)
    
    subprocess.run(['mongoexport', '--uri', uri, '--db', database_name, '--type','csv','--out', filename,
                    '--collection', collections, '--fields', fields])
    
def generate_all_csv():
    generate_csv('user', '_id,username', notimed =True)
    df = pd.read_csv(f'{backup_dir}/user.csv')
    print(df)
    
    if False:
        print('filter by provisionReport')
        username_list = df['username'].tolist()
        query = [
            {"$match": {
                '$or': [
                    {'createUserCode': {'$in': username_list}},
                    {'userAssignedNumberDocument': {'$in': username_list}}
                ]
            }},
            {"$group": {"_id":  {"createUserCode": "$createUserCode", "userAssignedNumberDocument": "$userAssignedNumberDocument"}}},
            {"$project": {"_id":0,"createUserCode": "$_id.createUserCode", "userAssignedNumberDocument": "$_id.userAssignedNumberDocument"}},
        ]
        result_list = list(db['provisionReport'].aggregate(query))
        username_list = list(set([item.get('createUserCode', None) for item in result_list] + [item.get('userAssignedNumberDocument', None) for item in result_list]))
        df['exist'] = df['username'].isin(username_list)
        df = df[df['exist'] != True]
        print(df)
    df = filter_by_person(df) 
    df = filter_by_purchaseconfirmationreport(df)
    id_list = df['_id'].tolist()
    username_list = df['username'].tolist()


def filter_by_purchaseconfirmationreport(df):
    print('Filter by purchaseConfirmationReport')
    df['_id'] = df['_id'].apply(lambda x: str(x)[9:-1])
    id_list = df['_id'].tolist()
    username_list = df['username'].tolist()
    query = [
        {"$match": {
            '$or': [
                {'user.userId': {'$in': id_list}},
                {'user.userName': {'$in': username_list}}
            ]
        }},
        {"$group": {"_id":  {"userId": "$user.userId", "userName": "$user.userName"}}},
        {"$project": {"_id":0,"userId": "$_id.userId", "userName": "$_id.userName"}},
    ]
    result_list = list(db['purchaseConfirmationReport'].aggregate(query))
    df['exist'] = df['_id'].isin(list(set([item.get('userId', None) for item in result_list])))
    df = df[df['exist'] != True]
    print(df)
    return df

def filter_by_person(df):
    print('Filter by person')
    username_list = df['username'].tolist()
    query = [
        {"$match":{'code': {'$in': username_list}}},
        {"$project": {"_id":0,"code":1}},
    ]
    result_list = list(db['person'].aggregate(query))
    username_list = list(set([item.get('code', None) for item in result_list]))
    df['exist'] = df['username'].isin(username_list)
    df = df[df['exist'] != True]
    print(df)
    return df

def generate_backup(collections = False):
    if not collections: collections = 'company,case,company'
    backup_directory = f'{backup_dir}/{database_name}_{timestamp}_backup'
    collections = collections.split(',')
    print(collections)
    print(backup_directory)
    subprocess.run(['mongodump', '--uri', uri, '--db', database_name, '--out', backup_directory,'--collection']+collections_to_backup)
    

def restore_backup():
    restore_database_name = 'restored_database'
    subprocess.run(['mongorestore', '--uri', uri, '--db', restore_database_name, backup_directory])
    
def filter_collection():
    collection_name = 'user'
    collection = db[collection_name]
    cursor = collection.find().limit(10)
    data = list(cursor)
    df = pd.DataFrame(data)
    for index, row in df.iterrows():
        print(row.to_dict())
    
    

def main():
    parser = argparse.ArgumentParser(description='Export/Import MongoDB data to/from CSV file.')
    parser.add_argument('operation', choices=['export', 'import','collections','backup','csv'], help='Operation to perform: export or import')
    parser.add_argument('--collection', required=False, help='Collection name')
    parser.add_argument('--fields', required=False, help='Fields from collection to export in csv columns')

    args = parser.parse_args()

    if args.operation == 'collections':
        collections(args.collection)
    elif args.operation == 'backup':
        generate_backup(args.collection)
    elif args.operation == 'csv':
        generate_all_csv()
        #generate_csv(args.collection, args.fields)
    #elif args.operation == 'export':
    #    export_to_csv(args.uri, args.db, args.collection, args.csv)
    #elif args.operation == 'import':
    #    import_from_csv(args.uri, args.db, args.collection, args.csv)

if __name__ == '__main__':
    main()

