import os
from mongodb_pandas_project.backup_script import batching, file_exists, generate_csv, backup_dir, db, get_timestamp, to_int, to_string, to_hex
import pandas as pd
from bson import ObjectId
import time

from mongodb_pandas_project.storage import upload

def filter():
    generate_csv('user', '_id,username', notimed =True)
    tmp_file_path = f'{backup_dir}/user_tmp.csv'
    file_path = f'{backup_dir}/user.csv'
    filtered_path = f'{backup_dir}/filtered_mongodb/user.csv'
    nrows = 1000
    if os.path.exists(filtered_path):os.remove(filtered_path)
    while True:
        df = pd.read_csv(file_path,skiprows = range(1, 000), nrows = nrows)
        if df.empty:
            if(file_exists(filtered_path)):
                upload(filtered_path,f'filtered_mongodb/user_filtered_{get_timestamp()}.csv' )
                delete(pd.read_csv(filtered_path))
            break
        print(f"===========| Chunk size = {len(df)} |============")
        df['_id'] = df['_id'].apply(lambda x: to_hex(x))
        df['ObjectId'] = df['_id'].apply(lambda x: ObjectId(x))
        #print(df)
        filters = [
            filter_by_person,
            filter_by_provisionreport,
            filter_by_purchaseconfirmationreport,
            filter_by_receiptpayment,
            filter_by_rraareport,
            filter_by_shoppingcartdigital,
            filter_by_shoppingexperiencescore,
            filter_by_case,
            filter_by_filecertificationresponse,
            filter_by_grouppayment,
            filter_by_historycase,
            filter_by_netpromoterscore,
            filter_by_newuserreport,
            filter_by_notification,
            filter_by_onetimepassword
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
        os.makedirs(os.path.dirname(filtered_path), exist_ok=True)
        if os.path.exists(filtered_path):
            df[['_id', 'username']].to_csv(filtered_path, mode='a', index=False, header=False)
        else:
            df[['_id', 'username']].to_csv(filtered_path, index=False)
        batching(file_path,tmp_file_path,nrows)
        os.remove(file_path)
        os.rename(tmp_file_path, file_path)

def delete(df):
    print("Rows to delete:", len(df))
    if len(df)>0:
        df['_id'] = df['_id'].apply(lambda x: to_hex(x))
        #print(df['_id'].apply(lambda x: ObjectId(x)).tolist())
        result = db['user'].delete_many({"_id": {"$in": df['_id'].apply(lambda x: ObjectId(x)).tolist()}})
        print(f"Number of documents deleted: {result.deleted_count}")

def filter_by_person(df):
    id_list = df['username'].tolist()
    query = [
        {"$match":{'code': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$code"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['person'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['username'].isin(id_list)
    return df

def filter_by_provisionreport(df):
    #print('filter by provisionReport')
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
    #print(username_list)
    df['exist'] = df['username'].isin(username_list)
    return df

def filter_by_purchaseconfirmationreport(df):
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
    df['exist'] = (
        df['_id'].isin(list(set([item.get('userId', None) for item in result_list])))|
        df['username'].isin(list(set([item.get('userName', None) for item in result_list])))
    )
    return df

def filter_by_receiptpayment(df):
    username_list = df['username'].tolist()
    query = [
        {"$match":{'userCode': {'$in': username_list}}},
        {"$group": {"_id":  {"userCode": "$userCode"}}},
        {"$project": {"_id":0,"userCode":"$_id.userCode"}},
    ]
    result_list = list(db['receiptPayment'].aggregate(query))
    username_list = list(set([item.get('userCode', None) for item in result_list]))
    df['exist'] = df['username'].isin(username_list)
    df = df[df['exist'] != True]
    return df

def filter_by_rraareport(df):
    username_list = df['username'].tolist()
    query = [
        {"$match":{'codeUser': {'$in': username_list}}},
        {"$group": {"_id":  {"codeUser": "$codeUser"}}},
        {"$project": {"_id":0,"codeUser":"$_id.codeUser"}},
    ]
    result_list = list(db['rraaReport'].aggregate(query))
    username_list = list(set([item.get('codeUser', None) for item in result_list]))
    df['exist'] = df['username'].isin(username_list)
    return df

def filter_by_shoppingcartdigital(df):
    id_list = df['_id'].tolist()
    query = [
        {"$match":{'userId': {'$in': id_list}}},
        {"$group": {"_id":  {"userId": "$userId"}}},
        {"$project": {"_id":0,"userId":"$_id.userId"}},
    ]
    result_list = list(db['shoppingCartDigital'].aggregate(query))
    id_list = list(set([item.get('userId', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['_id'].isin(id_list)
    return df

def filter_by_shoppingexperiencescore(df):
    id_list = df['_id'].tolist()
    username_list = df['username'].tolist()
    query = [
        {"$match": {
            '$or': [
                {'idUser': {'$in': id_list}},
                {'personDataRequest.idUser': {'$in': id_list}},
                {'personDataRequest.code': {'$in': username_list}}
            ]
        }},
        {"$group": {"_id":  {"idUser": "idUser", "idUser2": "$personDataRequest.idUser", "code": "$personDataRequest.code"}}},
        {"$project": {"_id":0,"idUser": "$_id.idUser", "idUser2": "$_id.idUser2", "code": "$_id.code"}},
    ]
    result_list = list(db['shoppingExperienceScore'].aggregate(query))
    df['exist'] = (
        df['_id'].isin(list(set([item.get('idUser', None) for item in result_list])))|
        df['_id'].isin(list(set([item.get('idUser2', None) for item in result_list])))|
        df['username'].isin(list(set([item.get('code', None) for item in result_list])))
    )
    return df

def filter_by_case(df):
    id_list = df['_id'].tolist()
    username_list = df['username'].tolist()
    oid_list = df['ObjectId'].tolist()
    #print(id_list)
    query = [
        {"$match": {
            '$or': [
                {'attachment.createUser.idUser': {'$in': id_list}},
                {'attachment.createUser.codeUser': {'$in': username_list}},
                {'createUser.idUser': {'$in': id_list}},
                {'createUser.codeUser': {'$in': username_list}},
                {'contactInfo.numberDocument': {'$in': username_list}},
                {'userAssigned.$id': {'$in': oid_list}},
            ]
        }},
        {"$group": {"_id":  {
                            "idUser": "$attachment.createUser.idUser", 
                             "codeUser": "$attachment.createUser.codeUser", 
                             "idUser2": "$createUser.idUser",
                             "codeUser2": "$createUser.codeUser",
                             "numberDocument": "$contactInfo.numberDocument",
                             "oid": "$userAssigned.$id",
                             }}},
        {"$project": {"_id":0,
                      "idUser": "$_id.idUser", 
                      "codeUser": "$_id.codeUser",
                      "idUser2": "$_id.idUser2", 
                      "codeUser2": "$_id.codeUser2", 
                      "numberDocument": "$_id.numberDocument", 
                      "oid": "$_id.oid",
                      }},
    ]
    result_list = list(db['case'].aggregate(query))
    #print(result_list)
    df['exist'] = (
        df['_id'].isin([value for item in result_list for value in item.get('idUser', [])])|
        df['username'].isin([value for item in result_list for value in item.get('codeUser', [])])|
        df['_id'].isin(list(set([item.get('idUser2', None) for item in result_list])))|
        df['username'].isin(list(set([item.get('codeUser2', None) for item in result_list])))|
        df['username'].isin(list(set([item.get('numberDocument', None) for item in result_list])))|
        df['ObjectId'].isin(list(set([item.get('oid', None) for item in result_list])))
    )
    return df

def filter_by_filecertificationresponse(df):
    id_list = df['_id'].tolist()
    query = [
        {"$match":{'userCode': {'$in': id_list}}},
        {"$group": {"_id":  {"userCode": "$userCode"}}},
        {"$project": {"_id":0,"userCode":"$_id.userCode"}},
    ]
    result_list = list(db['fileCertificationResponse'].aggregate(query))
    id_list = list(set([item.get('userCode', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['_id'].isin(id_list)
    return df

def filter_by_grouppayment(df):#groupPayment
    id_list = df['username'].tolist()
    #print(id_list)
    query = [
        {"$project":{"code":{"$concat":["$createUser.typeDocument", "-", "$createUser.numberDocument"]}}},
        {"$match":{"code": {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$code"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['groupPayment'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['username'].isin(id_list)
    return df

def filter_by_historycase(df):
    id_list = df['username'].tolist()
    query = [
        {"$match":{'createBy.codeUser': {'$in': id_list}}},
        {"$group": {"_id":  {"codeUser": "$createBy.codeUser"}}},
        {"$project": {"_id":0,"codeUser":"$_id.codeUser"}},
    ]
    result_list = list(db['historyCase'].aggregate(query))
    id_list = list(set([item.get('codeUser', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['username'].isin(id_list)
    return df

def filter_by_netpromoterscore(df):
    id_list = df['_id'].tolist()
    username_list = df['username'].tolist()
    query = [
        {"$match": {
            '$or': [
                {'idUser': {'$in': id_list}},
                {'detailAnswer.personDataRequest.idUser': {'$in': id_list}},
                {'detailAnswer.personDataRequest.code': {'$in': username_list}}
            ]
        }},
        {"$group": {"_id":  {"idUser": "idUser", "idUser2": "$detailAnswer.personDataRequest.idUser",
                              "code": "$detailAnswer.personDataRequest.code"}}},
        {"$project": {"_id":0,"idUser": "$_id.idUser", "idUser2": "$_id.idUser2", "code": "$_id.code"}},
    ]
    result_list = list(db['netPromoterScore'].aggregate(query))
    #print(result_list)
    df['exist'] = (
        df['_id'].isin(list(set([item.get('idUser', None) for item in result_list])))|
        df['_id'].isin(list(set([item.get('idUser2', None) for item in result_list])))|
        df['username'].isin(list(set([item.get('code', None) for item in result_list])))
    )
    return df

def filter_by_newuserreport(df):
    id_list = df['username'].tolist()
    query = [
        {"$match":{'personCode': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$personCode"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['newUserReport'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['username'].isin(id_list)
    return df

def filter_by_notification(df):
    id_list = df['username'].tolist()
    query = [
        {"$match":{'userCode': {'$in': id_list}}},
        {"$group": {"_id":  {"userCode": "$userCode"}}},
        {"$project": {"_id":0,"userCode":"$_id.userCode"}},
    ]
    result_list = list(db['notification'].aggregate(query))
    id_list = list(set([item.get('userCode', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['username'].isin(id_list)
    return df

def filter_by_onetimepassword(df):
    id_list = df['username'].tolist()
    query = [
        {"$match":{'documentNumber': {'$in': id_list}}},#can be lowercase but this is case sensitive
        {"$group": {"_id":  {"code": "$documentNumber"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['oneTimePassword'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['username'].isin(id_list)
    return df


