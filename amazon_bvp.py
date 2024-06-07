import requests
import pandas as pd
import json
import os, shutil

# storing the data in google cloud
from google.cloud import storage

import requests
import zipfile
from urllib.parse import urlparse

# retrieve credentials for Bayer companies
data = pd.read_json('../amazon_ads_credentials.json')
data['index']=data.loc[:, 'company']
data = data.set_index('index')
credentialsObject = data.loc['bayer'] # get Bayer credentials object
credentials=dict(credentialsObject) # create credentials object for consumption

# GCS credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='../bayer-ch-ecommerce-282069d49dcf.json'

# Marketplace
marketplaceId = 'ATVPDKIKX0DER'
manager_account_id ='amzn1.ads1.ma1.5p1pdvxb23zbcka8jyfnsrz12'

# local and cloud storage locations
download_location = './download'
prepared_location = './prepared'
gcs_storage_bucket = 'ch_ecommerce_global_storage'
gcs_storage_path = 'amazon_media/bvp'

# ADVERTISING API - OAUTH RELATED REQUESTS

# Call this function to obtain a Refresh Token. To get the information needed to call this, please see the accompanying
# setup process PDF document.

def getRefreshTokenViaCode(code, allowed_origin, client_id, client_secret):
    """
    Refresh Token Request\n
    [ONE-TIME]\n
    This will be used only once to obtain a valid refresh token\n

    :param code: A one-time use access code that's valid for 5 minutes after visiting https://www.amazon.com/ap/oa?client_id=<CLIENT_ID>&scope=advertising::campaign_management&response_type=code&redirect_uri=<ALLOWED_ORIGIN_URI>
    :param allowed_origin: The origin that has been added to the security profile (including any trailing slashes if necessary)
    :param client_id: The Login-with-Amazon (LWA) Client ID.
    :param client_secret: The Client Secret on the LWA security profile.
    :return: None, but prints the response data with the following format. \n
    { \n
        "access_token": "Atza|...", \n
        "refresh_token": "Atzr|...", \n
        "token_type": "bearer", \n
        "expires_in": 3600 \n
    }
    """
    url = "https://api.amazon.com/auth/o2/token"
    payload = {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': allowed_origin,
        'client_id': client_id,
        'client_secret': client_secret
    }
    response = requests.post(url, data=payload)
    r_json = response.json()
    return r_json["refresh_token"]

# Call this function as needed to get a new Access Token using your Refresh Token

def getAccessTokenViaRefreshToken(refresh_token, client_id, client_secret):
    """
    Access Token Request\n
    [REPEATED]\n
    This will be used as often as needed to make calls to the
    Advertising API\n\n

    :param refresh_token:
    :param client_id: The Login-with-Amazon (LWA) Client ID.
    :param client_secret: The Client Secret on the LWA security profile.
    :return: None, but prints the response data with the following format. \n
        { \n
            "access_token": "Atza|...", \n
            "refresh_token": "Atzr|...", \n
            "token_type": "bearer", \n
            "expires_in": 3600 \n
        } \n
    """
    url = "https://api.amazon.com/auth/o2/token"
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret
    }
    response = requests.post(url, data=payload)
    
    r_json = response.json()
    return r_json["access_token"]
    
# ADVERTISING API - BRAND BENCHMARKS API RELATED CALLS

# This API will return all most recent report types

def getLatestReportMetadata(company_code, access_token, client_id, manager_account_id):
    """
    Get the latest reports across all available report types.

    :param company_code: The company code corresponding to the company / marketplace.
    :param access_token: The oauth access token obtained from using the refresh token.
    :param client_id: The user's LWA client id
    :param manager_account_id: The advertiser's manager account ID. The requesting user must have been invited to join this.
    :return: None, but prints the response data with the following format.  \n
    { \n
	    "nextToken": null, \n
	    "reportsMetadata": [ \n
            { \n
                "advertiserId": "PNG_US", \n
                "indexDate": "2024-03-22", \n
                "obfuscatedMarketplaceId": "ATVPDKIKX0DER", \n
                "reportType": "EXCEL" \n
            }, \n
            { \n
                "advertiserId": "PNG_US", \n
                "indexDate": "2024-03-22", \n
                "obfuscatedMarketplaceId": "ATVPDKIKX0DER", \n
                "reportType": "ZIP_BRAND" \n
            }, \n
            { \n
                "advertiserId": "PNG_US", \n
                "indexDate": "2024-3-17", \n
                "obfuscatedMarketplaceId": "ATVPDKIKX0DER", \n
                "reportType": "ZIP_ASIN" \n
            } \n
	    ] \n
    } \n
    """
    url = f'https://advertising-api.amazon.com/insights/brandView/advertisers/{company_code}/allReportMetadata'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Amazon-Advertising-API-ClientId': client_id,
        'Amazon-Advertising-API-Manager-Account': manager_account_id
    }
    response = requests.get(url, headers=headers)
    print(response.text)
    r_json = response.json()
    return r_json

# This API will return the latest report

def getReport(company_code, report_type, index_date, access_token, client_id, manager_account_id):
    """
    Get the downloadable report URL which allows the report to be streamed from S3.

    :param company_code: The company code corresponding to the company / marketplace.
    :param report_type: All possible report types. One of [EXCEL, ZIP_BRAND, ZIP_ASIN]
    :param index_date: The index date of the report (e.g. YYYY-MM-DD)
    :param access_token: The oauth access token obtained from using the refresh token.
    :param client_id: The user's LWA client id
    :return: None, but prints the response data with the following format. \n
    { \n
	    "downloadLink": "https://..." \n
    } \n
    """
    url = f'https://advertising-api.amazon.com/insights/brandView/advertisers/{company_code}/reports/{report_type}/indexDates/{index_date}'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Amazon-Advertising-API-ClientId': client_id,
        'Amazon-Advertising-API-Manager-Account': manager_account_id
    }
    response = requests.get(url, headers=headers)
    # print(response.text)
    r_json = response.json()
    return r_json["downloadLink"]
    
# file processing functions
def extract_filename_from_url(url):
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    return filename

def download_file(url, dest_folder=download_location):
    filename = extract_filename_from_url(url)
    local_filepath = os.path.join(dest_folder, filename)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filepath, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filepath

def is_zip_file(filepath):
    return zipfile.is_zipfile(filepath)

def is_excel_file(filepath):
    return filepath.lower().endswith(('.xls', '.xlsx'))

def unpack_zip_file(zip_filepath, dest_folder=download_location):
    unpacked_files = []
    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        zip_ref.extractall(dest_folder)
        unpacked_files = zip_ref.namelist()
    return [os.path.join(dest_folder, name) for name in unpacked_files]

# GOogle Cloud Storage Upload
def upload_to_bucket(date_to_store, file_type, local_filename):
    ''' Upload data to bucket '''
    # get actual filename from local_filename
    filename = local_filename.split('/').pop()
    # define gcs_storage_path from parameters
    gcs_storage_path_for_upload = gcs_storage_path + '/'+ file_type + '/' + date_to_store + '/'+filename
    
    # initiate storage client for GCS
    storage_client = storage.Client()

    # select the bucket
    bucket = storage_client.get_bucket(gcs_storage_bucket)

    # get the location
    blob = bucket.blob(gcs_storage_path_for_upload)
    
    # upload the file
    blob.upload_from_filename(local_filename)
    
    return None

# Function to transform the dataframe into a database-friendly structure
def transform_dataframe(df,static_columns):
    # Identify the columns that will remain as identifiers and those that will be unpivoted
    id_vars = df.columns[:static_columns].tolist()  # Assuming the first n columns are identifiers
    value_vars = df.columns[static_columns:]  # Remaining columns are metrics
    
    # Melt the dataframe
    melted_df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name="Date", value_name="Value")
    
    # Split the "Date" column into "Period Identifier" and "Period Value"
    melted_df[['Period Identifier', 'Period Value']] = melted_df['Date'].str.split(': ', expand=True)
    
    # Drop the original "Date" column
    melted_df = melted_df.drop(columns=['Date'])
    
    return melted_df

def run_excel_processing(local_excel_uri, availability_date):
    '''
    Inside Excel file there are four (4) sheets containing melded data (values are distributed horizontaly and require transposing it verticaly)
    BAYER_US_Amazon-Brand-View-Pro_2024-06-07.xlsx
     +---- Sheets to extract, meld and deliver
        -- "Category Sales Share", "GV and Conversion", "Share of Voice", "Subscribe & Save"
    
    '''
    file_path = local_excel_uri
    # Load the Excel file
    xls = pd.ExcelFile(file_path)
    
    # extract index_date from file name
    # index_date = file_path.split('_').pop().split('.')[0]
    # or pick it up from higher level process
    index_date = availability_date

    # Define sheets to extract
    sheets_to_extract = ["Category Sales Share", "GV and Conversion", "Share of Voice", "Subscribe & Save"]

    # Load the specified sheets into dataframes
    dataframes = {sheet: xls.parse(sheet) for sheet in sheets_to_extract}
    
    # Transform each dataframe
    transformed_dataframes = {sheet: transform_dataframe(df,11) for sheet, df in dataframes.items()}
    
    # Save the final dataframes as CSV files and store to GCS
    for sheet, df in transformed_dataframes.items():
        # create local path and file name
        # use file name to add folder structure
        file_path_and_name = prepared_location + '/'+sheet.replace(' ', '_').lower()+'.csv'
        df.to_csv(file_path_and_name, index=False)
        # publish to the right GCS place
        # add dt= to index data to be used for hive partitioning on GCS
        upload_to_bucket('dt='+index_date, sheet.replace(' ', '_').lower(), file_path_and_name)

def run_ziped_files_processing(file_uris, extensions,availability_date):
    # print('Processing files: ',file_uris,extensions,availability_date)
    '''
    There are two zip files, each containing two CSV files
    BAYER_US_Amazon-Brand-View-Pro_2024-06-02.zip
     +--- BAYER_US 2024-06-02 ASIN Movement Report.csv  ++ just copy to GCS
       +- BAYER_US 2024-06-02 ASIN Grain Report.csv     ++ already melded file, requires parsing of Period field to split between time identifier and time value
     +--- BAYER_US ASIN Hierarchy.csv                   ++ just copy to GCS
       +- metrics.csv                                   ++ requires melding. First 11 fields are static, others are Period fields requiring melding and parsing  
    
    '''
    for filename in file_uris:
        if filename.split('/').pop()=='BAYER_US ASIN Hierarchy.csv':
            upload_to_bucket('dt='+availability_date,'asin_hierarchy',filename)
        
        if filename.split('/').pop()=='BAYER_US '+availability_date+' ASIN Movement Report.csv':
            upload_to_bucket('dt='+availability_date,'asin_movement',filename)
        
        if filename.split('/').pop()=='BAYER_US '+availability_date+' ASIN Grain Report.csv':
            # load csv to dataframe
            grain_report = pd.read_csv(filename)
            # split the value to period identifier and period value
            grain_report[['Period Identifier', 'Period Value']] = grain_report['period'].str.split(': ', expand=True)

            # Drop the original "period" column
            grain_report = grain_report.drop(columns=['period'])
            
            # create csv and place it in local file for bucket upload
            file_path_and_name = prepared_location+'/asin_grain_report.csv'
            grain_report.to_csv(file_path_and_name, index=False)
            # publish to the right GCS place
            # add dt= to index data to be used for hive partitioning on GCS
            upload_to_bucket('dt='+availability_date, 'grain_report', file_path_and_name)
            
        if filename.split('/').pop()=='metrics.csv':
            # load csv to dataframe
            metrics_report = pd.read_csv(filename)
            metrics_report = transform_dataframe(metrics_report,11)
            
            # create csv and place it in local file for bucket upload
            file_path_and_name = prepared_location+'/metrics.csv'
            metrics_report.to_csv(file_path_and_name, index=False)
            # publish to the right GCS place
            # add dt= to index data to be used for hive partitioning on GCS
            upload_to_bucket('dt='+availability_date, 'metrics', file_path_and_name)

def process_file(url,availability_date):
    '''
    Whatever we get as URL to download we have to be sure should we unzip it or do nothing 
    '''
    downloaded_file = download_file(url)
    file_uris = []
    extensions = []

    if is_excel_file(downloaded_file):
        file_uris.append(downloaded_file)
        extensions.append(os.path.splitext(downloaded_file)[1])
        # in case of Excel file, there needs to happen specific sheet extraction, transformation and storage localy and sent to GCS
        run_excel_processing(downloaded_file,availability_date)
    elif is_zip_file(downloaded_file):
        file_uris = unpack_zip_file(downloaded_file)
        extensions = [os.path.splitext(file)[1] for file in file_uris]
        os.remove(downloaded_file)  # Remove the downloaded zip file
        # after extraction of ZIP files, run the processing
        run_ziped_files_processing(file_uris, extensions,availability_date)
    else:
        file_uris.append(downloaded_file)
        extensions.append(os.path.splitext(downloaded_file)[1])

    return None

def process_report(advertiserId, indexDate, obfuscatedMarketplaceId, reportType,ACCESS_TOKEN, CLIENT_ID, MANAGER_ACCOUNT_ID):
    url_to_download = getReport(obfuscatedMarketplaceId, reportType, indexDate, ACCESS_TOKEN, CLIENT_ID, MANAGER_ACCOUNT_ID)
    # lets see what kind of URL we got
    # print(url_to_download)
    # get the file and perform magic
    process_file(url_to_download,indexDate)
    return None

def remove_folder_content(folder_name):
    ''' Perform cleanup of folders '''
    for filename in os.listdir(folder_name):
        file_path = os.path.join(folder_name, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

if __name__ == '__main__':

    COMPANY_CODE = marketplaceId
    REPORT_TYPE = ''
    INDEX_DATE = ''
    CLIENT_ID = credentials['CLIENT_ID']
    CLIENT_SECRET = credentials['CLIENT_SECRET']
    REFRESH_TOKEN = credentials['REFRESH_TOKEN']
    # Access token must be updated every hour as needed
    ACCESS_TOKEN = getAccessTokenViaRefreshToken(REFRESH_TOKEN, CLIENT_ID, CLIENT_SECRET)

    # This value is used for authorization. Must be granted access to the Manager Account for
    # this advertiser.
    MANAGER_ACCOUNT_ID = manager_account_id
    
    # pull latest and available reports 
    AVAILABLE_REPORTS = getLatestReportMetadata(COMPANY_CODE, ACCESS_TOKEN, CLIENT_ID, MANAGER_ACCOUNT_ID)['reportsMetadata']
    
    # loop through available reports and download to local download_location
    for report in AVAILABLE_REPORTS:
        advertiserId = report["advertiserId"]
        indexDate = report["indexDate"]
        obfuscatedMarketplaceId = report["obfuscatedMarketplaceId"]
        reportType = report["reportType"]
        process_report(advertiserId, indexDate, obfuscatedMarketplaceId, reportType, ACCESS_TOKEN, CLIENT_ID, MANAGER_ACCOUNT_ID)

    # clean download and prepared folder
    remove_folder_content(download_location)
    remove_folder_content(prepared_location)
