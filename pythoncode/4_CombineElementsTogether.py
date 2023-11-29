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
