import os
import boto3
import requests
import json
import sys
from datetime import date, timedelta

import logging

basePath = sys.argv[0]
# basePath = 'D:\OneDrive - Medkart Pharmacy Pvt. Ltd\Dhaval\Python_ipynb\Daily Snaps/'

logger = logging.getLogger('my_logger')
logging.basicConfig(filename=basePath+'logs/whatsapp.log',
                    filemode='w', level=logging.DEBUG)


def delete_all_objects_from_s3_folder():
    bucket_name = "snap-stores"
    s3_client = boto3.client("s3")

    response = s3_client.list_objects_v2(Bucket=bucket_name)

    try:
        files_in_folder = response["Contents"]
        files_to_delete = []
        for f in files_in_folder:
            files_to_delete.append({"Key": f["Key"]})

        response = s3_client.delete_objects(
            Bucket=bucket_name, Delete={"Objects": files_to_delete})

    except:
        print("Bucket Already Empty ")


try:
    delete_all_objects_from_s3_folder()

except:
    print('Done')
print('Reports Removed from s3 Bucket')

try:
    cwd = r"Store"
    files = os.listdir(cwd)

    # # ## Upload Stores Snaps
    for file in files:
        if file.endswith('.png'):
            s3 = boto3.client("s3")
            s3.upload_file(
                Filename=f"Store\\{file}",
                Bucket="snap-stores",
                Key=file,
                ExtraArgs={'ACL': 'public-read', 'ContentType': 'image/jpeg'})

    cwd = r"Emp"
    files = os.listdir(cwd)

    # Upload Employee Snaps
    for file in files:
        if file.endswith('.png'):
            s3 = boto3.client("s3")
            s3.upload_file(
                Filename=f"Emp\\{file}",
                Bucket="snap-stores",
                Key=file,
                ExtraArgs={'ACL': 'public-read', 'ContentType': 'image/jpeg'})

    print("Reports Upload Successs......!!!!!")

    import redshift_connector
    conn = redshift_connector.connect(
        host='local host',
        database='database',
        user='user',
        password='password'
    )

    import pandas as pd
    cursor: redshift_connector.Cursor = conn.cursor()
    cursor.execute(f"select * from store_master")
    result: pd.DataFrame = cursor.fetch_dataframe()
    storemaster = pd.DataFrame(result)
    storemaster = storemaster.loc[:, ['branch', 'storecode', 'asms', 'phonenumber']]
    storemaster['phonenumber'] = storemaster['phonenumber'].fillna(9081531610)
    storemaster['phonenumber'] = storemaster['phonenumber'].astype('str')
    storemaster['phonenumber'] = storemaster['phonenumber'].apply(lambda x :x.split('.')[0])

    d1 = dict(zip(storemaster['branch'], storemaster['phonenumber']))

    import pandas as pd
    cursor: redshift_connector.Cursor = conn.cursor()
    cursor.execute(f"select * from employeedata")
    result: pd.DataFrame = cursor.fetch_dataframe()
    Employee = pd.DataFrame(result)
    Employee = Employee.loc[:, ['empcode', 'wtspnumber']]
    Employee['wtspnumber'] = Employee['wtspnumber'].fillna(9081531610)
    Employee['wtspnumber'] = Employee['wtspnumber'].astype('str')
    Employee['wtspnumber'] = Employee['wtspnumber'].apply(lambda x :x.split('.')[0])

    d2 = dict(zip(Employee['empcode'], Employee['wtspnumber']))

    d2['107'] = d2.get('E107')

    d3 = d1 | d2

    all_snaps = list(d3.keys())

    for name in all_snaps:
        try:
            url = "api link"

            payload = json.dumps({
                "apiKey": "api key",
                "campaignName": "templatename",
                "destination": d3.get(name),
                "userName": d3.get(name),
                "media":
                {"url": f"s3 url/{name}.png",
                "filename": f"{name}.png"
                }})

            headers = {
                'Content-Type': 'application/json'
            }

            response = requests.request("POST", url, headers=headers, data=payload)
            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logging.warning("--------------------------------------------------")
            logging.warning(name)
        


except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    logging.warning("--------------------------------------------------")
    logging.error("Oops! An exception has occured:" + str(e))
    logging.error("Line Number:" + str(exc_tb.tb_lineno))
    logging.error("Exception TYPE:" + str(type(e)))
    logging.warning("--------------------------------------------------")
