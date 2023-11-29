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