import os
from mongodb_pandas_project.backup_script import batching, file_exists, generate_csv, backup_dir, db, get_timestamp, to_int, to_string, to_hex
import pandas as pd
from bson import ObjectId
import time

from mongodb_pandas_project.storage import upload

def filter():
    generate_csv('company', '_id,code', notimed =True)
    tmp_file_path = f'{backup_dir}/company_tmp.csv'
    file_path = f'{backup_dir}/company.csv'
    filtered_path = f'{backup_dir}/filtered_mongodb/company.csv'
    nrows = 1000
    if os.path.exists(filtered_path):os.remove(filtered_path)
    while True:
        df = pd.read_csv(file_path,skiprows = range(1, 000), nrows = nrows)
        if df.empty:
            if(file_exists(filtered_path)):
                upload(filtered_path,f'filtered_mongodb/company_filtered_{get_timestamp()}.csv' )
                delete(pd.read_csv(filtered_path))
            break
        print(f"===========| Chunk size = {len(df)} |============")
        df['_id'] = df['_id'].apply(lambda x: to_hex(x))
        df['ObjectId'] = df['_id'].apply(lambda x: ObjectId(x))
        df['ruc'] = df['code'].apply(lambda x: x.split('-')[1])
        df['exist'] = False
        #print(df)
        filters = [
            filter_by_onetimepassword,
            filter_by_provisionreport,
            filter_by_purchaseconfirmationreport,
            filter_by_purchaseorder,
            filter_by_purchasedamount,
            filter_by_receiptpayment,
            filter_by_rraareport,
            filter_by_shoppingcartdigital,
            filter_by_shoppingexperiencescore,
            filter_by_user,
            filter_by_userhistory,
            filter_by_case,
            filter_by_countaccess,
            filter_by_factory,
            filter_by_filecertificationresponse,
            filter_by_financialaccount,
            filter_by_financing,
            filter_by_grouppayment,
            filter_by_logdownloadgetreport,
            filter_by_loggetreport,
            filter_by_magneticmedia,
            filter_by_netpromoterscore,
            filter_by_newuserreport,
            filter_by_notification
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
            print(f"{initial_len} to {len(df)}. Time used by {filter_function.__name__}: {int(minutes)} minutes and {seconds:.2f} seconds")
        #print(df['ObjectId'].tolist())
        os.makedirs(os.path.dirname(filtered_path), exist_ok=True)
        if os.path.exists(filtered_path):
            df[['_id', 'code']].to_csv(filtered_path, mode='a', index=False, header=False)
        else:
            df[['_id', 'code']].to_csv(filtered_path, index=False)
        batching(file_path,tmp_file_path,nrows)
        os.remove(file_path)
        os.rename(tmp_file_path, file_path)

def delete(df):
    #print(df['_id'].apply(lambda x: ObjectId(x)).tolist())
    print("Rows to delete:", len(df))
    if len(df)>0:
        df['_id'] = df['_id'].apply(lambda x: to_hex(x))
        #result = db['company'].delete_many({"_id": {"$in": df['_id'].apply(lambda x: ObjectId(x)).tolist()}})
        #print(f"Number of documents deleted: {result.deleted_count}")

def filter_by_onetimepassword(df):
    id_list = df['code'].tolist()
    query = [
        {"$match":{'ruc': {'$in': id_list}}},#can be lowercase but this is case sensitive
        {"$group": {"_id":  {"code": "$ruc"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['oneTimePassword'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df

def filter_by_provisionreport(df):#provisionReport
    id_list = df['code'].tolist()
    query = [
        {"$match":{'companyInfoCode': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$companyInfoCode"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['provisionReport'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df

def filter_by_purchaseconfirmationreport(df):#purchaseConfirmationReport
    id_list = df['_id'].tolist()
    code_list = df['code'].tolist()
    #print(code_list)
    query = [
        {"$match": {
            '$or': [
                {'company.companyId': {'$in': id_list}},
                {'company.companyCode': {'$in': code_list}}
            ]
        }},
        {"$group": {"_id":  {"companyId": "$company.companyId", "code": "$company.companyCode"}}},
        {"$project": {"_id":0,"companyId": "$_id.companyId", "code": "$_id.code"}},
    ]
    result_list = list(db['purchaseConfirmationReport'].aggregate(query))
    #print(result_list)
    df['exist'] = (
        df['_id'].isin(list(set([item.get('companyId', None) for item in result_list])))|
        df['code'].isin(list(set([item.get('code', None) for item in result_list])))
    )
    return df

def filter_by_purchaseorder(df):#purchaseOrder
    id_list = df['_id'].tolist()
    code_list = df['code'].tolist()
    #print(code_list)
    query = [
        {"$match": {
            '$or': [
                {'company.companyId': {'$in': id_list}},
                {'company.companyCode': {'$in': code_list}}
            ]
        }},
        {"$group": {"_id":  {"companyId": "$company.companyId", "code": "$company.companyCode"}}},
        {"$project": {"_id":0,"companyId": "$_id.companyId", "code": "$_id.code"}},
    ]
    result_list = list(db['purchaseOrder'].aggregate(query))
    #print(result_list)
    df['exist'] = (
        df['_id'].isin(list(set([item.get('companyId', None) for item in result_list])))|
        df['code'].isin(list(set([item.get('code', None) for item in result_list])))
    )
    return df

def filter_by_purchasedamount(df):#purchasedAmount
    id_list = df['code'].tolist()
    query = [
        {"$match":{'companyCode': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$companyCode"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['purchasedAmount'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df

def filter_by_receiptpayment(df):#receiptPayment
    id_list = df['code'].tolist()
    query = [
        {"$match":{'companyCode': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$companyCode"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['receiptPayment'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df

def filter_by_rraareport(df):#rraaReport
    id_list = df['code'].tolist()
    query = [
        {"$match":{'portfolio.codeCompany': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$portfolio.codeCompany"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['rraaReport'].aggregate(query))
    #print(id_list)
    df['exist'] = df['code'].isin([value for item in result_list for value in item.get('code', [])])
    return df

def filter_by_shoppingcartdigital(df):#shoppingCartDigital
    id_list = df['_id'].tolist()
    query = [
        {"$match":{'companyId': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$companyId"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['shoppingCartDigital'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['_id'].isin(id_list)
    return df

def filter_by_shoppingexperiencescore(df):#shoppingExperienceScore
    id_list = df['_id'].tolist()
    query = [
        {"$match":{'idCompany': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$idCompany"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['shoppingExperienceScore'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['_id'].isin(id_list)
    return df

def filter_by_user(df):
    id_list = df['ObjectId'].tolist()
    query = [
        {"$match":{'portfolio.company.$id': {'$in': id_list}}},
        {"$group": {"_id":  {"oid": "$portfolio.company.$id"}}},
        {"$project": {"_id":0,"oid":"$_id.oid"}},
    ]
    result_list = list(db['user'].aggregate(query))
    #print(result_list)
    df['exist'] = df['ObjectId'].isin([value for item in result_list for value in item.get('oid', [])])
    return df

def filter_by_userhistory(df):#userHistory
    id_list = df['code'].tolist()
    query = [
        {"$match":{'companyCode': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$companyCode"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['userHistory'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df

def filter_by_case(df):
    id_list = df['_id'].tolist()
    code_list = df['code'].tolist()
    oid_list = df['ObjectId'].tolist()
    query = [
        {"$match": {
            '$or': [
                {'companyInfo.idCompany': {'$in': id_list}},
                {'companyInfo.code': {'$in': code_list}},
                {'company.$id': {'$in': oid_list}},
            ]
        }},
        {"$group": {"_id":  {
                            "idCompany": "$companyInfo.idCompany", 
                             "code": "$companyInfo.code", 
                             "oid": "$company.$id",
                             }}},
        {"$project": {"_id":0,
                      "idCompany": "$_id.idCompany", 
                      "code": "$_id.code",
                      "oid": "$_id.oid",
                      }},
    ]
    result_list = list(db['case'].aggregate(query))
    df['exist'] = (
        df['_id'].isin(list(set([item.get('idCompany', None) for item in result_list])))|
        df['code'].isin(list(set([item.get('code', None) for item in result_list])))|
        df['ObjectId'].isin(list(set([item.get('oid', None) for item in result_list])))
    )
    return df

def filter_by_countaccess(df):#countAccess
    id_list = df['code'].tolist()
    query = [
        {"$match":{'code': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$code"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['countAccess'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df

def filter_by_factory(df):
    id_list = df['code'].tolist()
    query = [
        {"$match":{'code': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$code"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['factory'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df

def filter_by_filecertificationresponse(df):#fileCertificationResponse
    id_list = df['_id'].tolist()
    query = [
        {"$match":{'companyCode': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$companyCode"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['fileCertificationResponse'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['_id'].isin(id_list)
    return df

def filter_by_financialaccount(df):#financialAccount
    id_list = df['code'].tolist()
    query = [
        {"$match":{'codeCompany': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$codeCompany"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['financialAccount'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df

def filter_by_financing(df):#financing
    ruc_list = df['ruc'].tolist()
    query = [
        {"$match":{'companyNumberDocument': {'$in': ruc_list}}},
        {"$group": {"_id":  {"companyTypeDocument": "$companyTypeDocument","code": "$companyNumberDocument"}}},
        {"$project": {"_id":0,"companyTypeDocument":"$_id.companyTypeDocument","code":"$_id.code"}},
    ]
    result_list = list(db['financing'].aggregate(query))
    id_list = list(set([(item.get('companyTypeDocument', '')+'-'+item.get('code', '')) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df

def filter_by_grouppayment(df):#groupPayment
    id_list = df['code'].tolist()
    query = [
        {"$match":{'code': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$code"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['groupPayment'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df

def filter_by_logdownloadgetreport(df):#logDownloadGetReport
    id_list = df['ruc'].tolist()
    query = [
        {"$match":{'companyCode': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$companyCode"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['logDownloadGetReport'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['ruc'].isin(id_list)
    return df

def filter_by_loggetreport(df):#logGetReport
    id_list = df['code'].tolist()
    query = [
        {"$match":{'companyCode': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$companyCode"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['logGetReport'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df

def filter_by_magneticmedia(df):#magneticMedia
    id_list = df['ruc'].tolist()
    query = [
        {"$match":{'ruc': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$ruc"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['magneticMedia'].aggregate(query))
    #print(result_list)
    df['exist'] = df['ruc'].isin(list(set([item.get('code', None) for item in result_list])))
    return df

def filter_by_netpromoterscore(df):#netPromoterScore
    id_list = df['_id'].tolist()
    query = [
        {"$match":{'idCompany': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$idCompany"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['netPromoterScore'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['_id'].isin(id_list)
    return df

def filter_by_newuserreport(df):#newUserReport
    id_list = df['code'].tolist()
    query = [
        {"$match":{'companyCode': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$companyCode"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['newUserReport'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df

def filter_by_notification(df):#notification
    id_list = df['code'].tolist()
    query = [
        {"$match":{'companyCode': {'$in': id_list}}},
        {"$group": {"_id":  {"code": "$companyCode"}}},
        {"$project": {"_id":0,"code":"$_id.code"}},
    ]
    result_list = list(db['notification'].aggregate(query))
    id_list = list(set([item.get('code', None) for item in result_list]))
    #print(id_list)
    df['exist'] = df['code'].isin(id_list)
    return df