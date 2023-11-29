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

 
Set up components (if you are using Synapse Analytics)
Note : You are going to need admin permissions (or request an admin) to do some of the set up components
PowerBI REST API set up
We are using an App Registration for access to the PowerBI REST API
Use Entra to create a new App Registration. Fill in the details as per your specifications
 
 
Once you have created the App Registration, navigate to the “Certificates and Secrets” section and create a new client secret
Note : Keep the value (it is client secret) and the client id in a secure place. Once the screen displays the value, it will not display it again in case you navigate away. If that happens, you have to delete this secret and create a new one. 
 
Once you have the secret created, we have to add some API Permissions (This might need a PowerBI admin)
 
 
 
Once you have this piece ready (and potentially added the App Registration to an Entra Group), have your PowerBI Admin follow these steps
https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#step-3---enable-the-power-bi-service-admin-settings

https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#step-4---add-the-service-principal-to-your-workspace
These two steps will allow the new App Registration to access the PowerBI APIs via the REST call. 
