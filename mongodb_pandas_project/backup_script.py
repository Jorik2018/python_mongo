import pandas as pd
import subprocess
from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient
from dotenv import load_dotenv
import os
load_dotenv()


uri = os.getenv('MONGODB_URI') 
database_name = os.getenv('MONGODB_NAME')
backup_dir = os.getenv('BACKUP_DIR','backup')
db = None

def get_db():
    global db
    if db == None:
        client = MongoClient(uri)
        db = client[database_name]
    return db

def test():
    for key, value in os.environ.items():
        print(f"{key}: {value}")

def zip_folder(folder_path, zip_path):
    import zipfile
    import os
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, folder_path)
                zip_file.write(file_path, relative_path)


def get_timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def to_hex(value):
    try:
        if value.startswith("ObjectId("):
            return value[9:-1]
        else:
            return value
    except ValueError:
        return str(value)
    
def to_string(value):
    try:
        return str(int(value))
    except ValueError:
        return str(value)
    
def to_int(value):
    try:
        return int(value)
    except ValueError:
        return 0

def file_exists(file_path):
    try:
        with open(file_path, 'rb'):
            pass
        return True
    except FileNotFoundError:
        return False
    
def collections():
    db = get_db()
    collections = db.list_collection_names()
    print(f'Collections in the database "{database_name}":')
    for collection in collections:
        print(collection)
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
    if not collections: collections = 'company,user,case'
    folder_name = f'{database_name}_{get_timestamp()}_backup'
    backup_directory = f'{backup_dir}/{folder_name}'
    #collections = [['--collection',col] for col in collections.split(',')]
    #collections = [x for xs in collections for x in xs]
    for collection in collections.split(','):
        d = ['mongodump', '--uri', uri, '--db', database_name, '--gzip', '--out', backup_directory, '--collection',collection]
        print(d)
        try:
            subprocess.run(d)
        except subprocess.CalledProcessError as e:
            # Handle the error here
            print("Error:", e)
            return
    zip_folder(backup_directory,backup_directory+'.zip')
    from mongodb_pandas_project.storage import upload
    upload(backup_directory+'.zip', f"backup_mongodb/{folder_name}.zip")
    
def restore_backup(backup, restore_database_name = False):
    import glob
    if not restore_database_name: restore_database_name = 'restored_database'
    backup_directory = f'{backup_dir}/{backup}'

    bson_files = glob.glob(f"{backup_directory}/*.bson.gz")

    # Print the list of matching files
    print("Matching files:")
    for file_path in bson_files:
        collection = os.path.basename(file_path).split('.')[0]
        print(file_path)
        d = ['mongorestore', '--uri', uri,'--nsInclude',f'{restore_database_name}.{collection}','--gzip', '--archive', file_path]
        subprocess.run(d)
    
def generate_csv(collections = 'company', fields = False, notimed =False):
    if not collections: collections = 'company'
    if not fields: fields = '_id,username'
    if notimed:
        filename = f'{backup_dir}/{collections}.csv'
    else:
        filename = f'{backup_dir}/{collections}_{get_timestamp()}.csv'
    print(filename)
    print(fields)
    subprocess.run(['mongoexport', '--uri', uri, '--db', database_name, '--type','csv','--out', filename,
                    '--collection', collections, '--fields', fields])
    
def batching(input_file_path, output_file_path, skip_rows = 2000):
    chunk_size = 1000 
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    with open(output_file_path, 'w') as output_file:
        writer = None
        for chunk in pd.read_csv(input_file_path, chunksize=chunk_size, skiprows=range(1, skip_rows + 1)):
            if writer is None:
                chunk.to_csv(output_file, index=False, header=True, lineterminator='\n')
                writer = True
            else:
                chunk.to_csv(output_file, index=False, header=False, lineterminator='\n')