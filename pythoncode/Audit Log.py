#!/usr/bin/env python
# coding: utf-8

# ## Audit Log
# 
# 
# 

# In[ ]:


#Activity Log - audit events
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

kv_name = "kysynuq2e3ujz.vault.azure.net"
linked_service = "Az_KeyVault"
secret_name = "PBIRESTAPICLIENTSECRET"

session = SparkSession.builder.getOrCreate()
token_library = (session._jvm.com.microsoft.azure.synapse.
                             tokenlibrary.TokenLibrary)
client_secret = token_library.getSecret(
                kv_name, secret_name, linked_service)

tenant_id = '<Enter your Tenant ID>'
client_id = '<Enter the client ID from your App Registration>'
scope = 'https://analysis.windows.net/powerbi/api/.default'
authority_url = "https://login.microsoftonline.com/<enter your tenant id>"

client_secret_credential_class = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
access_token_class = client_secret_credential_class.get_token(scope)
token_string = access_token_class.token
access_token = token_string

header = {'Authorization': f'Bearer {access_token}'}

#Set Power BI REST API to get Activities for today
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
    column_names = ['Id', 'RecordType', 'CreationTime', 'Operation', 'OrganizationId', 'UserType', 'UserKey', 'Workload', 
                    'UserId', 'ClientIP', 'UserAgent', 'Activity', 'IsSuccess', 'RequestId', 'ActivityId', 'ItemName', 
                    'WorkSpaceName', 'DatasetName', 'ReportName', 'WorkspaceId', 'ObjectId', 'DatasetId', 'ReportId', 'ReportType', 
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


# In[2]:


#Datasets
base_url = 'https://api.powerbi.com/v1.0/myorg/'
header = {'Authorization': f'Bearer {access_token}'}

url = 'admin/datasets'

# HTTP GET Request
datasets = requests.get(base_url + url, headers=header)

datasets = json.loads(datasets.content)

# Concatenates all of the results into a single dataframe
datasetsdict = pd.concat([pd.json_normalize(x) for x in datasets['value']])

#pd.dataflowsdict.iteritems = pd.dataflowsdict.items
datasetsdict.columns = datasetsdict.columns.str.strip()
pd.DataFrame.iteritems = pd.DataFrame.items
df = spark.createDataFrame(datasetsdict)

getdatasetsadmin = df


# In[3]:


#Reports
base_url = 'https://api.powerbi.com/v1.0/myorg/'
header = {'Authorization': f'Bearer {access_token}'}

url = 'admin/reports'

# HTTP GET Request
reports = requests.get(base_url + url, headers=header)

reports = json.loads(reports.content)

# Concatenates all of the results into a single dataframe
reportsdict = pd.concat([pd.json_normalize(x) for x in reports['value']])

#pd.dataflowsdict.iteritems = pd.dataflowsdict.items
reportsdict.columns = reportsdict.columns.str.strip()
pd.DataFrame.iteritems = pd.DataFrame.items
df = spark.createDataFrame(reportsdict)

getreportsadmin = df


# In[4]:


#Capacity
base_url = 'https://api.powerbi.com/v1.0/myorg/'
header = {'Authorization': f'Bearer {access_token}'}

url = 'admin/capacities'

# HTTP GET Request
capacities = requests.get(base_url + url, headers=header)

capacities = json.loads(capacities.content)

# Concatenates all of the results into a single dataframe
capacitiesdict = pd.concat([pd.json_normalize(x) for x in capacities['value']])

#pd.dataflowsdict.iteritems = pd.dataflowsdict.items
capacitiesdict.columns = capacitiesdict.columns.str.strip()
pd.DataFrame.iteritems = pd.DataFrame.items
df = spark.createDataFrame(capacitiesdict)

getcapacitiesadmin = df


# In[23]:


#Combine elements together
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants


# Rename columns and select necessary columns from getdatasetsadmin
getdatasetsadmin1 = getdatasetsadmin.withColumnRenamed("id", "DatasetId") \
    .withColumnRenamed("name", "DatasetName") \
    .withColumnRenamed("description", "DatasetDescription") \
    .withColumnRenamed("workspaceId", "datasetWorkspaceId") \
    .select("DatasetId", "DatasetName", "datasetWorkspaceId", "isRefreshable",
            "isEffectiveIdentityRolesRequired", "isInPlaceSharingEnabled",
            "datasetworkspaceId", "configuredBy", "createdDate", "webUrl")


# Left join activitydf with getdatasetsadmin
result_df = activitydf.join(getdatasetsadmin1, activitydf.DatasetId == getdatasetsadmin1.DatasetId, "left") \
    .drop(getdatasetsadmin1.DatasetId) \
    .withColumnRenamed("DatasetName", "name") \
    .withColumnRenamed("datasetWorkspaceId", "workspaceId_dataset") \
    .drop("datasetworkspaceId")


# Left join activitydf with getreportsadmin
result_df = result_df.join(getreportsadmin, activitydf.ReportId == getreportsadmin.id, "left") \
    .drop(getreportsadmin.id) \
    .withColumnRenamed("reportType", "ReportType") \
    .drop("id", "embedUrl", "originalReportObjectId", "appId")

#Convert Spark Frame to required columns
selected_columns = ["RecordType", "CreationTime", "Operation", "OrganizationId", "UserType", "UserKey", "Workload", "UserId", "ClientIP",
                  "Activity", "RequestId", "ItemName", "WorkSpaceName", "ReportId", "CapacityId", "capacityName", "DataConnectivityMode",
                  "ArtifactId", "ArtifactName", "LastRefreshTime", "ArtifactKind", "isRefreshable", "configuredBy", "sku",
                  "state", "capacityUserAccessRight", "region"
                  ]	

# Check if each column exists and replace missing columns with NULL
result_df = result_df.select(
    *[col(col_name) if col_name in result_df.columns else lit(None).cast(StringType()).alias(col_name) for col_name in selected_columns]
).orderBy(*selected_columns)

#Make everything as String
column_names = result_df.columns

for column_name in column_names:
    result_df = result_df.withColumn(column_name, col(column_name).cast("string"))


# In[22]:


#Append into SQL Pool Table
#result_df.printSchema()
#spark.read.synapsesql("<Enter the 3 part SQL Pool table - SQL Pool Name.,schema name.tableName").limit(0).printSchema()

#Write to a SQL Pool Table
#Create a staging directory in your Azure Data Lake
(result_df.write
 .option(Constants.SERVER, "<Enter the Dedicated SQL Endpoint from the Synapse Overview page>")
 .option(Constants.TEMP_FOLDER, "<Enter the staging directory path>")
 .mode("append")
 .synapsesql("<Enter the 3 part SQL Pool table - SQL Pool Name.,schema name.tableName>"))


