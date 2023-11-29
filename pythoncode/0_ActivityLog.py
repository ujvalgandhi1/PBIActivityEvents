import json, requests, pandas as pd, msal
from datetime import date, timedelta
from azure.identity import ClientSecretCredential, InteractiveBrowserCredential, UsernamePasswordCredential
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, when
from pyspark.sql.types import StructType, StructField, StringType

from pyspark.sql.functions import col, from_json, lit, when, concat
from pyspark.sql.types import StructType, StructField, StringType
import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants

#Get yesterdays date and convert to string
activityDate = date.today() - timedelta(days=1)
activityDate = activityDate.strftime("%Y-%m-%d")

kv_name = "<enter your key vault name>.vault.azure.net"
linked_service = "<Create a linked service to your Key Vault - enter that name here>"
secret_name = "<enter your client secret from the Key Vault here that holds the client secret>"

session = SparkSession.builder.getOrCreate()
token_library = (session._jvm.com.microsoft.azure.synapse.
                             tokenlibrary.TokenLibrary)
client_secret = token_library.getSecret(
                kv_name, secret_name, linked_service)

tenant_id = '<enter your tenant id - you can get this from the overview page of your app registration>'
client_id = '<enter your client id - you can get this from the overview page of your app registration>'
scope = 'https://analysis.windows.net/powerbi/api/.default'
authority_url = "https://login.microsoftonline.com/<enter your tenant id>"

client_secret_credential_class = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
access_token_class = client_secret_credential_class.get_token(scope)
token_string = access_token_class.token
access_token = token_string

header = {'Authorization': f'Bearer {access_token}'}

#Set Power BI REST API to get Activities for yesterday
url = "https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime='" + activityDate + "T00:00:00'&endDateTime='" + activityDate + "T23:59:59'"

#Use MSAL to grab token
app = msal.ConfidentialClientApplication(client_id, authority=authority_url, client_credential=client_secret)
result = app.acquire_token_for_client(scopes=scope)


#Get latest Power BI Activities
if 'access_token' in result:
    access_token = result['access_token']
    header = {'Content-Type':'application/json', 'Authorization':f'Bearer {access_token}'}
    api_call = requests.get(url=url, headers=header)
    

    #Specify empty Dataframe with all columns
    column_names = ['Id', 'RecordType', 'CreationTime', 'Operation', 'OrganizationId', 'UserType', 'UserKey', 'Workload', 'UserId', 
                    'ClientIP', 'UserAgent', 'Activity', 'IsSuccess', 'RequestId', 'ActivityId', 'ItemName', 'WorkSpaceName', 
                    'DatasetName', 'ReportName', 'WorkspaceId', 'ObjectId', 'DatasetId', 'ReportId', 'ReportType', 
                    'DistributionMethod', 'ConsumptionMethod']
    df = pd.DataFrame(columns=column_names)

    #Set continuation URL
    contUrl = api_call.json()['continuationUri']
    
    #Get all Activities for first hour, save to dataframe (df1) and append to empty created df
    result = api_call.json()['activityEventEntities']
    df1 = pd.DataFrame(result)
    pd.concat([df, df1])


    #Call Continuation URL as long as results get one back to get all activities through the day
    while contUrl is not None:        
        api_call_cont = requests.get(url=contUrl, headers=header)
        contUrl = api_call_cont.json()['continuationUri']
        result = api_call_cont.json()['activityEventEntities']
        df2 = pd.DataFrame(result)
        df = pd.concat([df, df2])
    
    #Set ID as Index of df
    df = df.set_index('Id')

df = df.astype(str)

spark = SparkSession.builder.appName("example").getOrCreate()
pd.DataFrame.iteritems = pd.DataFrame.items
activitydf = spark.createDataFrame(df)