
import os
import sys
import logging
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_column', None)
pd.set_option('display.max_row', None)

# basePath = sys.argv[1]
basePath = 'D:\OneDrive - Medkart Pharmacy Pvt. Ltd\Dhaval\Python_ipynb\Daily Snaps/'

logger = logging.getLogger('my_logger')
logging.basicConfig(filename=basePath+'logs/StoresReports.log',
                    filemode='w', level=logging.DEBUG)


try:
    folder_path = basePath + 'Store'
    folder_path1 = basePath + 'Emp'

    file_list = os.listdir(folder_path)

    for file_name in file_list:
        file_path = os.path.join(folder_path, file_name)

        if os.path.isfile(file_path):
            try:
                os.remove(file_path)  # Delete the file
            except Exception as e:
                print(f"Error deleting {file_name}: {e}")

    file_list = os.listdir(folder_path1)

    for file_name in file_list:
        file_path = os.path.join(folder_path1, file_name)

        if os.path.isfile(file_path):
            try:
                os.remove(file_path)  # Delete the file
            except Exception as e:
                print(f"Error deleting {file_name}: {e}")

    print('Reports Deleted from folder...!!!!!!!!!!')

    import redshift_connector
    conn = redshift_connector.connect(
        host='Local Server', 
        database='database',
        user='user id',
        password='password'
    )

    x = ((date.today()-timedelta(days=1)) -
         relativedelta(months=1)).strftime('%Y-%m') + '-01'
    y = ((date.today()-timedelta(days=1)) +
         relativedelta(months=1)).strftime('%Y-%m') + '-01'
    z = ((date.today()-timedelta(days=1)) +
         relativedelta(months=0)).strftime('%Y-%m') + '-01'

    cursor: redshift_connector.Cursor = conn.cursor()
    cursor.execute(
        f"select date, store, daytargetmultiplier from target_till_dec where date >='{x}' ")

    result: pd.DataFrame = cursor.fetch_dataframe()
    SaleTGT = pd.DataFrame(result)

    SaleTGT['daytargetmultiplier'] = SaleTGT['daytargetmultiplier'].astype(
        'float64').astype('int64')
    SaleTGT['date'] = pd.to_datetime(
        SaleTGT['date'], format='%Y-%m-%d').dt.date

    cursor.execute(
        f"select date, store, hdtarget, offertarget, otctarget, generictarget from offer_otc_target where date >='{x}' ")

    result: pd.DataFrame = cursor.fetch_dataframe()
    OFFER_OTC = pd.DataFrame(result)

    OFFER_OTC[['store', 'offertarget', 'otctarget', 'hdtarget', 'generictarget']] = OFFER_OTC[[
        'store', 'offertarget', 'otctarget', 'hdtarget', 'generictarget']].astype('float64').astype('int64')
    OFFER_OTC['date'] = pd.to_datetime(
        OFFER_OTC['date'], format='%Y-%m-%d').dt.date

    COCO_Targets = pd.merge(SaleTGT, OFFER_OTC, on=[
                            'date', 'store'], how='left')

    cursor.execute(
        f"select * from fofo_targets where date >='{x}' and date < '{y}'")
    
    import pandas as pd
    result: pd.DataFrame = cursor.fetch_dataframe()
    FOFO_Targets = pd.DataFrame(result)

    FOFO_Targets = FOFO_Targets.loc[:, [
        'date', 'store', 'sale_ext', 'offer_ext', 'otc ext']]
    FOFO_Targets.columns = ['date', 'store',
                            'daytargetmultiplier', 'offertarget', 'otctarget']
    FOFO_Targets['date'] = pd.to_datetime(
        FOFO_Targets['date'], format='%Y-%m-%d').dt.date

    Targets = pd.concat([COCO_Targets, FOFO_Targets])

    Targets['generictarget'] = Targets['generictarget'].fillna(0)
    Targets[['daytargetmultiplier', 'generictarget']] = Targets[[
        'daytargetmultiplier', 'generictarget']].astype('int64')

    cursor.execute(f"select * from sales_details where billdate >='{x}'")
    result: pd.DataFrame = cursor.fetch_dataframe()
    SS_Data = pd.DataFrame(result)
    SS_Data[['amount', 'alternatestorecode', 'billnumber', 'quantity', 'newcustbill', 'newcustsale', 'offersale', 'offerqty', 'brandedotcsale', 'gensale', 'genqty', 'otcsale', 'otcqty']] = SS_Data[[
        'amount', 'alternatestorecode', 'billnumber', 'quantity', 'newcustbill', 'newcustsale', 'offersale', 'offerqty', 'brandedotcsale', 'gensale', 'genqty', 'otcsale', 'otcqty']].astype('float64').astype('int64')

    Emp_Data = SS_Data.copy()
    SS_Data = SS_Data.groupby(['billdate', 'alternatestorecode']).agg({'amount': 'sum', 'quantity': 'sum', 'gensale': 'sum', 'newcustsale': 'sum',
                                                                       'billnumber': 'sum', 'newcustbill': 'sum', 'offerqty': 'sum', 'offersale': 'sum', 'otcqty': 'sum', 'otcsale': 'sum'}).reset_index()

    query = "SELECT * FROM offer_otc_sale WHERE billdate LIKE '" + \
        '-'.join(x.split('-')[:-1]) + "%'"
    cursor.execute(query)
    result: pd.DataFrame = cursor.fetch_dataframe()
    OfferOTCSale1 = pd.DataFrame(result)

    query = "SELECT * FROM offer_otc_sale WHERE billdate LIKE '" + \
        '-'.join(z.split('-')[:-1]) + "%'"
    cursor.execute(query)
    result: pd.DataFrame = cursor.fetch_dataframe()
    OfferOTCSale2 = pd.DataFrame(result)

    OfferOTCSale = pd.concat([OfferOTCSale1, OfferOTCSale2])

    OfferOTCSale['productcode'] = OfferOTCSale['productcode'].astype('int64')
    OfferOTCSale = OfferOTCSale[OfferOTCSale['productcode'].isin(
        [19346, 19345, 19344, 19343, 19233])]
    OfferOTCSale['alternatestorecode'] = OfferOTCSale['alternatestorecode'].astype(
        'int64')
    OfferOTCSale = OfferOTCSale.fillna(0)

    OfferOTCSale = OfferOTCSale.groupby(['billdate', 'alternatestorecode', 'productname']).agg({
        'quantity': 'sum'}).unstack().reset_index()

    OfferOTCSale.columns = ['billdate', 'alternatestorecode', 'DREAM REST BY HELSE TAB 1X30',	'HAIR BOOST BY HELSE TAB 1X30',
                            'JOINT FIT BY HELSE TAB 1X30',	'OPTI MAN BY HELSE TAB 1X30', 'OPTI WOMAN BY HELSE TAB 1X30']

    OfferOTCSale = OfferOTCSale.fillna(0)

    OfferOTCSale['billdate'] = pd.to_datetime(
        OfferOTCSale['billdate'], format='%Y-%m-%d').dt.date
    OfferOTCSale['alternatestorecode'] = OfferOTCSale['alternatestorecode'].astype(
        'int64')

    SS_Data = SS_Data.merge(
        OfferOTCSale, on=['billdate', 'alternatestorecode'], how='left')
    SS_Data = SS_Data.fillna(0)

    OfferOTCSale[['DREAM REST BY HELSE TAB 1X30',	'HAIR BOOST BY HELSE TAB 1X30',	'JOINT FIT BY HELSE TAB 1X30',	'OPTI MAN BY HELSE TAB 1X30', 'OPTI WOMAN BY HELSE TAB 1X30']] = OfferOTCSale[[
        'DREAM REST BY HELSE TAB 1X30',	'HAIR BOOST BY HELSE TAB 1X30',	'JOINT FIT BY HELSE TAB 1X30',	'OPTI MAN BY HELSE TAB 1X30', 'OPTI WOMAN BY HELSE TAB 1X30']].astype('int64')

    StoreData = Targets.merge(SS_Data, left_on=['date', 'store'], right_on=[
                              'billdate', 'alternatestorecode']).drop(['billdate', 'alternatestorecode'], 1)

    YestData = StoreData[StoreData['date'] ==
                         date.today()-timedelta(days=1)].reset_index(drop=True)

    MTDData = StoreData[pd.to_datetime(
        StoreData['date'], format='%Y-%m-%d').dt.month == (date.today()-timedelta(days=1)).month]
    MTDData = MTDData.groupby('store').agg({'daytargetmultiplier': 'sum', 'offertarget': 'sum', 'otctarget': 'sum', 'amount': 'sum', 'quantity': 'sum', 'gensale': 'sum', 'newcustsale': 'sum', 'billnumber': 'sum', 'newcustbill': 'sum', 'offerqty': 'sum', 'offersale': 'sum',
                                            'otcqty': 'sum', 'otcsale': 'sum', 'DREAM REST BY HELSE TAB 1X30': 'sum',	'HAIR BOOST BY HELSE TAB 1X30': 'sum',	'JOINT FIT BY HELSE TAB 1X30': 'sum',	'OPTI MAN BY HELSE TAB 1X30': 'sum', 'OPTI WOMAN BY HELSE TAB 1X30': 'sum'}).reset_index()

    LMTDData = StoreData[pd.to_datetime(StoreData['date'], format='%Y-%m-%d').dt.month == (
        (date.today()-timedelta(days=1))-relativedelta(months=1)).month]
    LMTDData = LMTDData[pd.to_datetime(StoreData['date'], format='%Y-%m-%d').dt.day <= (
        (date.today()-timedelta(days=1))-relativedelta(months=1)).day]
    LMTDData = LMTDData.groupby('store').agg({'daytargetmultiplier': 'sum', 'offertarget': 'sum', 'otctarget': 'sum', 'amount': 'sum', 'quantity': 'sum', 'gensale': 'sum', 'newcustsale': 'sum', 'billnumber': 'sum', 'newcustbill': 'sum', 'offerqty': 'sum',
                                              'offersale': 'sum', 'otcqty': 'sum', 'otcsale': 'sum', 'DREAM REST BY HELSE TAB 1X30': 'sum',	'HAIR BOOST BY HELSE TAB 1X30': 'sum',	'JOINT FIT BY HELSE TAB 1X30': 'sum',	'OPTI MAN BY HELSE TAB 1X30': 'sum', 'OPTI WOMAN BY HELSE TAB 1X30': 'sum'}).reset_index()

    SalesData = YestData.merge(
        MTDData, on='store', how='left', suffixes=(None, '_MTD'))
    SalesData = SalesData.merge(
        LMTDData, on='store', how='left', suffixes=(None, '_LMTD'))
    SalesData = SalesData.fillna(0)

    cols = SalesData.columns
    for col in cols:
        try:
            SalesData[col] = SalesData[col].astype('int64')
        except:
            SalesData[col] = SalesData[col]

    SalesData['AOV'] = (SalesData['amount'] /
                        SalesData['billnumber']).astype('int64')
    SalesData['AOV_MTD'] = round(
        (SalesData['amount_MTD']/SalesData['billnumber_MTD']), 0)
    SalesData['AOV_LMTD'] = round(
        (SalesData['amount_LMTD']/SalesData['billnumber_LMTD']), 0)

    SalesData['Achievement'] = round(
        (SalesData['amount']/SalesData['daytargetmultiplier'])*100, 2)
    SalesData['Achievement_MTD'] = round(
        (SalesData['amount_MTD']/SalesData['daytargetmultiplier_MTD'])*100, 2)
    SalesData['Achievement_LMTD'] = round(
        (SalesData['amount_LMTD']/SalesData['daytargetmultiplier_LMTD'])*100, 2)

    SalesData['NewCustContri'] = round(
        (SalesData['newcustsale']/SalesData['amount'])*100, 2)
    SalesData['NewCustContri_MTD'] = round(
        (SalesData['newcustsale_MTD']/SalesData['amount_MTD'])*100, 2)
    SalesData['NewCustContri_LMTD'] = round(
        (SalesData['newcustsale_LMTD']/SalesData['amount_LMTD'])*100, 2)

    SalesData['offerContri'] = round(
        (SalesData['offersale']/SalesData['amount'])*100, 2)
    SalesData['offerContri_MTD'] = round(
        (SalesData['offersale_MTD']/SalesData['amount_MTD'])*100, 2)
    SalesData['offerContri_LMTD'] = round(
        (SalesData['offersale_LMTD']/SalesData['amount_LMTD'])*100, 2)

    SalesData['OTCContri'] = round(
        (SalesData['otcsale']/SalesData['amount'])*100, 2)
    SalesData['OTCContri_MTD'] = round(
        (SalesData['otcsale_MTD']/SalesData['amount_MTD'])*100, 2)
    SalesData['OTCContri_LMTD'] = round(
        (SalesData['otcsale_LMTD']/SalesData['amount_LMTD'])*100, 2)

    SalesData['SalesDelta'] = round(
        SalesData['daytargetmultiplier']-SalesData['amount'], 0)
    SalesData['SalesDelta_MTD'] = round(
        SalesData['daytargetmultiplier_MTD']-SalesData['amount_MTD'], 0)
    SalesData['SalesDelta_LMTD'] = round(
        SalesData['daytargetmultiplier_LMTD']-SalesData['amount_LMTD'], 0)

    SalesData['ASP'] = round(SalesData['amount']/SalesData['quantity'], 2)
    SalesData['ASP_MTD'] = round(
        SalesData['amount_MTD']/SalesData['quantity_MTD'], 2)
    SalesData['ASP_LMTD'] = round(
        SalesData['amount_LMTD']/SalesData['quantity_LMTD'], 2)

    SalesData['BasketSize'] = round(
        SalesData['quantity']/SalesData['billnumber'], 2)
    SalesData['BasketSize_MTD'] = round(
        (SalesData['quantity_MTD']/SalesData['billnumber_MTD']), 2)
    SalesData['BasketSize_LMTD'] = round(
        (SalesData['quantity_LMTD']/SalesData['billnumber_LMTD']), 2)

    SalesData['GenericContri'] = round(
        (SalesData['gensale']/SalesData['amount'])*100, 2)
    SalesData['GenericContri_MTD'] = round(
        (SalesData['gensale_MTD']/SalesData['amount_MTD'])*100, 2)
    SalesData['GenericContri_LMTD'] = round(
        (SalesData['gensale_LMTD']/SalesData['amount_LMTD'])*100, 2)

    SalesData = SalesData[SalesData['amount'] > 0]
    SS_Data = SS_Data.fillna(0)

    cursor.execute(f"select * from store_master")
    result: pd.DataFrame = cursor.fetch_dataframe()
    storemaster = pd.DataFrame(result)
    storemaster = storemaster.loc[:, ['branch', 'storecode', 'asms']]
    storemaster['storecode'] = storemaster['storecode'].astype('int64')

    SalesData = SalesData.merge(
        storemaster, left_on='store', right_on='storecode', how='left').drop('storecode', 1)

    SalesData['date'] = SalesData['date'].apply(
        lambda x: x.strftime("%d-%B-%Y"))

    SalesData = SalesData[SalesData['amount'] > 0]
    SalesData = SalesData.fillna(0)
    SalesData[['DREAM REST BY HELSE TAB 1X30',	'HAIR BOOST BY HELSE TAB 1X30',	'JOINT FIT BY HELSE TAB 1X30',	'OPTI MAN BY HELSE TAB 1X30', 'OPTI WOMAN BY HELSE TAB 1X30']] = SalesData[[
        'DREAM REST BY HELSE TAB 1X30',	'HAIR BOOST BY HELSE TAB 1X30',	'JOINT FIT BY HELSE TAB 1X30',	'OPTI MAN BY HELSE TAB 1X30', 'OPTI WOMAN BY HELSE TAB 1X30']].astype('int64')

    SalesData[['DREAM REST BY HELSE TAB 1X30_MTD',	'HAIR BOOST BY HELSE TAB 1X30_MTD',	'JOINT FIT BY HELSE TAB 1X30_MTD',	'OPTI MAN BY HELSE TAB 1X30_MTD', 'OPTI WOMAN BY HELSE TAB 1X30_MTD']] = SalesData[[
        'DREAM REST BY HELSE TAB 1X30_MTD',	'HAIR BOOST BY HELSE TAB 1X30_MTD',	'JOINT FIT BY HELSE TAB 1X30_MTD',	'OPTI MAN BY HELSE TAB 1X30_MTD', 'OPTI WOMAN BY HELSE TAB 1X30_MTD']].astype('int64')
    SalesData.columns = [x.replace(" ", "_") for x in SalesData]
    from babel.numbers import format_currency

    def format_currency_without_decimal(number, currency_code, locale='en_US'):
        formatted_currency = format_currency(
            number, currency_code, format="#,##0", locale=locale)
        formatted_currency_without_decimal = formatted_currency.split(
            '.')[0]  # Remove the decimal part
        return formatted_currency_without_decimal

    SalesData['amount'] = SalesData['amount'].apply(
        lambda x: format_currency_without_decimal(x, 'INR', locale='en_IN').replace(u'\xa0', u' '))
    SalesData['amount_MTD'] = SalesData['amount_MTD'].apply(
        lambda x: format_currency_without_decimal(x, 'INR', locale='en_IN').replace(u'\xa0', u' '))
    SalesData['amount_LMTD'] = SalesData['amount_LMTD'].apply(
        lambda x: format_currency_without_decimal(x, 'INR', locale='en_IN').replace(u'\xa0', u' '))

    cols = SalesData.columns

    for col in cols:
        if ('Sales' in col) or ('Delta' in col) or ('Sale' in col) or ('sale' in col) or ('multiplier' in col):
            SalesData[col] = SalesData[col].apply(lambda x: format_currency_without_decimal(
                x, 'INR', locale='en_IN').replace(u'\xa0', u' '))
        else:
            SalesData[col] = SalesData[col]

    from jinja2 import Template

    template_file_path = 'store.html'
    with open(template_file_path, 'r', encoding='utf-8') as file:
        template_content = file.read()

    # Load the Jinja2 template
    template = Template(template_content)

    from html2image import Html2Image
    for row in SalesData.values:
        hti = Html2Image()
        hti.size = (712, 1200)
        employee_data = dict(zip(SalesData.columns, row))
        rendered_html = template.render(**employee_data)
        hti.output_path = 'Store'
        hti.screenshot(rendered_html, save_as=f"{row[85]}.png")

    Emp_Data['empcode'] = Emp_Data['username'].apply(lambda x: x.split('_')[0])
    Emp_Data = Emp_Data[Emp_Data['alternatestorecode'] < 100]
    Emp_Data['empcode'] = Emp_Data['empcode'].astype('str')

    cursor.execute(f"select * from employeedata")
    import pandas as pd
    result: pd.DataFrame = cursor.fetch_dataframe()
    EmpData = pd.DataFrame(result)
    EmpData = EmpData.drop_duplicates(subset='empcode')
    EmpData = EmpData.loc[:, ['empcode', 'empname', 'wtspnumber']]

    EmpData['empcode'] = EmpData['empcode'].replace('E107', '107')
    Emp_Data['empcode'] = Emp_Data['empcode'].astype('str')

    YestEmpData = Emp_Data[Emp_Data['billdate']
                           == date.today()-timedelta(days=1)]
    YestEmpData = YestEmpData.groupby(['billdate', 'alternatestorecode', 'empcode']).agg(
        {'amount': 'sum', 'billnumber': 'sum', 'quantity': 'sum', 'offerqty': 'sum', 'offersale': 'sum', 'gensale': 'sum', 'otcqty': 'sum', 'otcsale': 'sum'}).reset_index()

    MTDEmpData = Emp_Data[pd.to_datetime(
        Emp_Data['billdate'], format='%Y-%m-%d').dt.month == (date.today()-timedelta(days=1)).month]
    MTDEmpData = MTDEmpData.groupby(['empcode']).agg({'amount': 'sum', 'billnumber': 'sum', 'quantity': 'sum',
                                                      'offerqty': 'sum', 'offersale': 'sum', 'gensale': 'sum', 'otcqty': 'sum', 'otcsale': 'sum'}).reset_index()

    LMTDEmpData = Emp_Data[pd.to_datetime(Emp_Data['billdate'], format='%Y-%m-%d').dt.month == (
        (date.today()-timedelta(days=1))-relativedelta(months=1)).month]
    LMTDEmpData = LMTDEmpData[pd.to_datetime(
        LMTDEmpData['billdate'], format='%Y-%m-%d').dt.day <= (date.today()-timedelta(days=1)).day]
    LMTDEmpData = LMTDEmpData.groupby(['empcode']).agg({'amount': 'sum', 'billnumber': 'sum', 'quantity': 'sum',
                                                        'offerqty': 'sum', 'offersale': 'sum', 'gensale': 'sum', 'otcqty': 'sum', 'otcsale': 'sum'}).reset_index()

    EmployeeData = YestEmpData.merge(
        MTDEmpData, on=['empcode'], how='left', suffixes=(None, '_MTD'))
    EmployeeData = EmployeeData.merge(
        LMTDEmpData, on=['empcode'], how='left', suffixes=(None, '_LMTD'))

    EmployeeData = EmployeeData.fillna(0)
    cols = EmployeeData.columns
    for col in cols:
        try:
            EmployeeData[col] = EmployeeData[col].astype('int64')
        except:
            EmployeeData[col] = EmployeeData[col]

    EmployeeData['AOV'] = round(
        EmployeeData['amount']/EmployeeData['billnumber'], 0)
    EmployeeData['AOV_MTD'] = round(
        EmployeeData['amount_MTD']/EmployeeData['billnumber_MTD'], 0)
    EmployeeData['AOV_LMTD'] = round(
        EmployeeData['amount_LMTD']/EmployeeData['billnumber_LMTD'], 0)

    EmployeeData['BasketSize'] = round(
        EmployeeData['quantity']/EmployeeData['billnumber'], 2)
    EmployeeData['BasketSize_MTD'] = round(
        EmployeeData['quantity_MTD']/EmployeeData['billnumber_MTD'], 2)
    EmployeeData['BasketSize_LMTD'] = round(
        EmployeeData['quantity_LMTD']/EmployeeData['billnumber_LMTD'], 2)

    EmployeeData['ASP'] = round(
        EmployeeData['amount']/EmployeeData['quantity'], 2)
    EmployeeData['ASP_MTD'] = round(
        EmployeeData['amount_MTD']/EmployeeData['quantity_MTD'], 2)
    EmployeeData['ASP_LMTD'] = round(
        EmployeeData['amount_LMTD']/EmployeeData['quantity_LMTD'], 2)

    EmployeeData['empcode'] = EmployeeData['empcode'].astype('int64')
    EmpData['empcode'] = EmpData['empcode'].astype('int64')
    EmpData1 = EmployeeData.merge(EmpData, on='empcode', how='left')

    EmpData2 = EmpData1.merge(
        storemaster, left_on='alternatestorecode', right_on='storecode').drop('storecode', 1)
    EmpData2 = EmpData2.fillna(0)
    print(EmpData2.head())

    cols = EmpData2.columns
    for col in cols:
        try:
            EmpData2[col] = EmpData2[col].astype('int64')
        except:
            EmpData2[col] = EmpData2[col]

    cols = EmpData2.columns
    for col in cols:
        if ('Sales' in col) or ('Delta' in col) or ('Sale' in col) or ('sale' in col) or ('multiplier' in col) or ('amount' in col):
            EmpData2[col] = EmpData2[col].apply(
                lambda x: format_currency_without_decimal(x, 'INR', locale='en_IN')).replace()
        else:
            EmpData2[col] = EmpData2[col]

    

    from jinja2 import Template
    template_file_path = 'Emp.html'
    with open(template_file_path, 'r', encoding='utf-8') as file:
        template_content = file.read()

    template = Template(template_content)

    for row in EmpData2.values:
        hti = Html2Image()
        hti.size = (712, 1200)
        employee_data = dict(zip(EmpData2.columns, row))
        rendered_html = template.render(**employee_data)
        hti.output_path = 'Emp'
        hti.screenshot(rendered_html, save_as=f"{row[2]}.png")

except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    logging.warning("--------------------------------------------------")
    logging.error("Oops! An exception has occured:" + str(e))
    logging.error("Line Number:" + str(exc_tb.tb_lineno))
    logging.error("Exception TYPE:" + str(type(e)))
    logging.warning("--------------------------------------------------")
