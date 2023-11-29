# PBIActivityEvents
Using Spark to track PowerBI Activity Events
Have you ever wondered what goes on behind the scenes when users interact with your PowerBI reports? The PowerBI REST API opens a door to a wealth of activity data that can be harnessed to gain insights into user behavior and system performance.
In this blog post, we'll explore the exciting possibilities of using Apache Spark to tap into the PowerBI REST API, enabling you to track and analyze activity events seamlessly. By the end, you'll be equipped to create a comprehensive PowerBI dashboard that gives you a real-time pulse on your PowerBI environment.
Discover how this integration not only empowers you to monitor user interactions but also opens avenues for optimizing report performance, ensuring data security, and making informed decisions for your PowerBI deployment.
This will guide you through the process of connecting Spark to the PowerBI REST API, retrieving detailed activity logs, and transforming the data into actionable insights. Buckle up for a journey into the world of real-time analytics and data-driven decision-making.
Ready to supercharge your PowerBI monitoring? Let's dive in!

**Set up**
1.	Azure Synapse Analytics (Or Fabric Data Science)
a.	Spark Notebook
b.	Synapse Dedicated SQL Pool (or Fabric Managed Table/Datawarehouse table) You can write the data out to any relational database using some code modifications}
2.	Azure Key Vault {Slight modification in code if you are using Fabric}
3.	PowerBI 

![Notional Architecture](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/PBIRESTNotionalArchitecture.jpg)?raw=true)

 
**Set up components (if you are using Synapse Analytics)
Note : You are going to need admin permissions (or request an admin) to do some of the set up components**

**PowerBI REST API set up**
We are using an App Registration for access to the PowerBI REST API
Use Entra to create a new App Registration. Fill in the details as per your specifications
![Entra App Registration](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/0_AppRegistration.png?raw=true)
 
Once you have created the App Registration, navigate to the “Certificates and Secrets” section and create a new client secret
**Note : Keep the value (it is client secret) and the client id in a secure place. Once the screen displays the value, it will not display it again in case you navigate away. If that happens, you have to delete this secret and create a new one. **
![Client Secret](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/1_AppRegistrationSecret.png)

Once you have the secret created, we have to add some API Permissions (This might need a PowerBI admin)
![Base API Permissions](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/2_AppRegistrationAPIPermissions.png)

![Entra App Registration-3](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/3_AppRegistration.png?raw=true)

![API App Registration](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/3_AppRegistrationAPIPermissions_1.png)
 
Once you have this piece ready (and potentially added the App Registration to an Entra Group), have your PowerBI Admin follow these steps
https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#step-3---enable-the-power-bi-service-admin-settings

https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#step-4---add-the-service-principal-to-your-workspace
These two steps will allow the new App Registration to access the PowerBI APIs via the REST call. 

**Azure Key Vault**
Use an existing Key Vault or create a new one – this is required so that you don’t have the client secret (created earlier) in open format in your Spark Notebook.
To add a secret to the vault, follow the steps:
1.	Navigate to your new key vault in the Azure portal
2.	On the Key Vault settings pages, select Secrets.
3.	Select on Generate/Import.
4.	On the Create a secret screen choose the following values:
•	Upload options: Manual.
•	Name: Type a name for the secret. The secret name must be unique within a Key Vault. The name must be a 1-127 character string, starting with a letter and containing only 0-9, a-z, A-Z, and -. 
•	Value: Type the value for the secret. This is the value you saved from the earlier step when you created the App Registration and got a secret value.
•	Leave the other values to their defaults. Select Create.

**Azure Synapse Analytics**
Once you have the first two parts done, we are ready to move to Synapse Analytics (or Fabric)
The notional architecture outlined above uses a Synapse Analytics Spark Notebook with a Dedicated SQL Pool to hold the data. This can be modified to have a Fabric notebook with a Managed Table/Datawarehouse table with some code modifications

 
**Spark Notebook**
Navigate to the Notebooks under your Synapse Notebook and select an existing Spark Pool (or provision a new Spark Pool) and use PySpark as your language
Give a name to your Spark Notebook and click “Publish All” to commit your changes
The code modules are all available https://github.com/ujvalgandhi1/PBIActivityEvents/tree/main/pythoncode
The entire end to end code is available for download HERE
The entire code is represented in code snippets via screenshots here in case you want to follow along step by step. 
Note : Please take care of code indentations – they might have moved and might introduce errors. 

![FirstCode-A](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/2_FirstCode_a.png)

![FirstCode-B](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/2_FirstCode_b.png)

![FirstCode-C](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/2_FirstCode_c.png)

![FirstCode-D](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/2_FirstCode_d.png)

![SecondCode](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/2_SecondCode.png)

![ThirdCode](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/2_ThirdCode.png)

![FourthCode](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/2_FourthCode.png)

![FifthCode-A](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/2_FifthCode_a.png)

![FifthCode-B](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/2_FifthCode_b.png)

Our final data frame is result_df which has all the values we need for final reporting/analysis.
The code can be scheduled to run on a trigger for the previous day’s activity log. Because of this, we have to append the data into a Synapse Dedicated SQL Pool (or any relational database of your choice)
Use an existing Synapse Dedicated SQL Pool or create a small (DW100) one 
Run the create table script found at https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/sql/SQLPool_CreateTableScript.txt
in your pool to get a table of your choice.
Once you get your table created, you have to incorporate the final Spark Code that takes the Spark Dataframe (result_df) and appends the data to the SQL Pool table

![SixthCode](https://github.com/ujvalgandhi1/PBIActivityEvents/blob/main/images/2_SixthCode.png)

