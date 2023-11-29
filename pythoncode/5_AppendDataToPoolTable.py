#Write to a SQL Pool Table
(result_df.write
 .option(Constants.SERVER, "prvsyn-cd4fzlhv.sql.azuresynapse.net")
 .option(Constants.TEMP_FOLDER, "abfss://samples@saprvsynraw3ag3v.dfs.core.windows.net/AccountSamples/DOE/Hanford/PBIDashboards/staging/")
 .mode("append")
 .synapsesql("artdedicatedsqlpool01.pbis.PBIMonitoring_AuditLog"))