# atlax360-hi-exercise

Python exercise for ATLAX 360 IT hiring processes


PRE-REQUISITES
--------------

You must to have installed:

Python 3.6.x
<br>
PIP 21.3.x
<br>
Docker 20.10.x




DOCKER IMAGE
------------

In order to install our SQL Server image, you will need to run this command:

```
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=cmgYB2Zr4NJra2gRtGyjypag" -p 1433:1433 fbertos/mssql-atlax360-hi:2.0
```

Our Docker image can be found here:
https://hub.docker.com/r/fbertos/mssql-atlax360-hi

Once this is done, a new SQL server instance will be running locally on port 1433 (please make sure this port is available previously)


The SQL Server database contains two tables:

Customer: this table will store our list of customers with their ID and their Name.

Item: this table will store our items with their ID and their Version. There can be many Versions for the same ID, but only the last one will be the active one.
      Also, the DeletedFlag column will tell us if the Item is Deleted.
```
+-----------------------------------+
+ ItemId | VersionNbr | DeletedFlag +
+      1 |          1 |           0 +
+      1 |          2 |           0 + 
+      1 |          3 |           1 + => This version is the only one active for Item 1, but it is deleted
+-----------------------------------+
```



PYODBC
------

Python pyodbc library is needed, please use PIP to install it and follow these guide:
https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017




EXERCISE
--------

Our tables are growing up quickly and we have now some performance issues on the system. We decided to solve the problem with some adjustments:

1. Create indexes on these tables to increase the performance

2. Create a Python command line process in order to move our data from SQL Server into a CSV file to be loaded in a Datawarehouse


In order to achieve that, please:

1. Create any index you decide it is needed to increase queries performance

2. Please finish the extract method of our DBExtract class with the code needed to export the following data from SQL Server into a CSV file:
  - ItemId
  - ItemDocumentNbr
  - CustomerName
  - CreateDate (format YYYY-MM-dd HH:mm:ss)
  - UpdateDate (format YYYY-MM-dd HH:mm:ss)

Please notice we just want active and non deleted items. 

3. We want also a column in the excel with the ItemSource calculated like:
   - ItemSource: Local if the CustomerName starts with 99
                 External in any other case

4. Please use Python pandas or petl libraries to extract the data.

5. The resulting CSV file should be in UTF-8 format and compressed with GZIP

6. Please use ; as field terminator, CRLF as line terminator and " as quote field character

7. Each applier will need to deliver:
  - DML sentences with any T-SQL CREATE INDEX sentence concluded
  - A new public GIT repository for the Python solution provided

