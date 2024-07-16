Snowflake Assessment
1. How will you use to change the warehouse for workload processing to a warehouse named ‘COMPUTE_WH_XL’?
Ans : USE WAREHOUSE COMPUTE_WH_XL;
2. Consider a table vehicle_inventory that stores vehicle information of all vehicles in your dealership. The table has only one VARIANT column called vehicle_data which stores information in JSON format. The data is given below:
{
“date_of_arrival”: “2021-04-28”,
“supplier_name”: “Hillside Honda”,
“contact_person”: {
“name”: “Derek Larssen”,
“phone”: “8423459854”
},
“vehicle”: [
{
“make”: “Honda”,
“model”: “Civic”,
“variant”: “GLX”,
“year”: “2020”
}
]
}
What is the command to retrieve supplier_name?
Ans : SELECT vehicle_data:supplier_name::string AS supplier_name
FROM vehicle_inventory;
3. From a terminal window, how to start SnowSQL from the command prompt ? And write the steps to load the data from local folder into a Snowflake table usin three types of internal stages.
Ans : To Start SnowSQL open command prompt and type
sysdm.cpl
snowsql –version
snowsql -a qjhtlxv-ajaykrishnar(userpath)
username
password
Then to Load data from local folder to Snowflake
User Stage
CREATE OR REPLACE STAGE user_stage;
COPY INTO my_table 
FROM @user_stage/data_files 
FILE_FORMAT = ‘csv_format’ / ‘Json_format’ ;
For Account Stage
COPY INTO my_table 
FROM @ACCOUNT_STAGE/data_files 
FILE_FORMAT = ‘csv_format’ / ‘Json_format’ ;
For Share Stage
COPY INTO my_table 
FROM @share_stage/data_files 
FILE_FORMAT = ‘csv_format’ / ‘Json_format’ ;






4. Create an X-Small warehouse named xf_tuts_wh using the CREATE WAREHOUSE command with below options 
a) Size with x-small
b) which can be automatically suspended after 10 mins
c) setup how to automatically resume the warehouse
d) Warehouse should be suspended once after created
Ans : Create Warehouse xf_tuts_wh
With
Warehouse Size = x-small
Autosuspend = 600
Autoresume = TRUE
Initially_Suspended = True;

5. A CSV file ‘customer.csv’ consists of 1 or more records, with 1 or more fields in each record, and sometimes a header record. Records and fields in each file are separated by delimiters. How will
Load the file into snowflake table ?
Step 1 : Creating table
Create TABLE AJ_Snowflake(empid int,empname varchar(100),salary float);
Step2 : Create Stage
Create or Replace STAGE my_csv_stage;
Step3 : Upload the file to stage using snowsql
snowsql -a <account_name> -u <username> -d <database_name> -s <schema_name> -q "PUT file://path/to/SF.csv @my_csv_stage;"
Step 4: Copy data into table
Copy Into AJ_Snowflake
From @my_csv_stage/SF.csv
File_Format = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

6. Write the commands to disable < auto-suspend > option for a virtual warehouse
Ans : ALTER WAREHOUSE AJ_WH SET AUTO_SUSPEND = NULL;

7. What is the command to concat the column named 'EMPLOYEE' between two % signs ? 
Ans : SELECT CONCAT('%', EMPLOYEE, '%') AS NEW_EMPLOYEE
FROM AJ_Snowflake;
8. You have stored the below JSON in a table named car_sales as a variant column

{
  "customer": [
    {
      "address": "San Francisco, CA",
      "name": "Joyce Ridgely",
      "phone": "16504378889"
    }
  ],
  "date": "2017-04-28",
  "dealership": "Valley View Auto Sales",
  "salesperson": {
    "id": "55",
    "name": "Frank Beasley"
  },
  "vehicle": [
    {
      "extras": [
        "ext warranty",
        "paint protection"
      ],
      "make": "Honda",
      "model": "Civic",
      "price": "20275",
      "year": "2017"
    }
  ]
}
How will you query the table to get the dealership data?
Ans : SELECT car_data:dealership::string AS dealership_data
FROM car_sales;

9. A medium size warehouse runs in Auto-scale mode for 3 hours with a resize from Medium (4 servers per cluster) to Large (8 servers per cluster). Warehouse is resized from Medium to Large at 1:30 hours, Cluster 1 runs continuously, Cluster 2 runs continuously for the 2nd and 3rd hours, Cluster 3 runs for 15 minutes in the 3rd hour. How many total credits will be consumed
Ans : 9 Credits

10. What is the command to check status of snowpipe?
Ans : Show Pipes;

11. What are the different methods of getting/accessing/querying data from Time travel , Assume the table name is 'CUSTOMER' and please write the command for each method.
Ans : 1.By Using Timestamp 
SELECT *
FROM CUSTOMER
AT (TIMESTAMP = '2024-07-15 12:00:00');

SELECT *
FROM CUSTOMER
Before (TIMESTAMP = '2024-07-15 12:00:00');

2.By Using Query ID
SELECT * FROM CUSTOMER AT (Query ID = '12345abcd-1234-abcd-1234-abcdef123456');

12. If comma is defined as column delimiter in file "employee.csv" and if we get extra comma in the data how to handle this scenario?
Ans : By Specifying the file format exactly as CSV and using NULL IF
CREATE OR REPLACE FILE FORMAT csv_format 

TYPE = 'CSV' FIELD_DELIMITER = ',' 
NULL_IF = ('');

13. What is the command to read data directly from S3 bucket/External/Internal Stage
Ans : SELECT * FROM @stage/file.csv (FILE_FORMAT = 'csv_format');

14. Lets assume we have table with name 'products' which contains duplicate rows. How will delete the duplicate rows ?
Ans : Using CTE
WITH duplicates AS ( SELECT empid, name, email,
 ROW_NUMBER() OVER (PARTITION BY empid, name, email ORDER BY empid) 
AS row_num FROM employees
)
DELETE FROM duplicates WHERE row_num > 1;

15. How is data unloaded out of Snowflake?
Ans : Step 1: First creating an stage
CREATE OR REPLACE STAGE SFStage
stage URL = 's3://bucket/path'
FILE_FORMAT = ( 
TYPE = 'CSV' 
FIELD_DELIMITER = ',' 
SKIP_HEADER = 1 );
Step 2 : Now Use Copy Into command to Unload data
COPY INTO @SFStage/unload/employee_data
FROM employee_data
FILE_FORMAT = ‘csv_format’
Overwrite = False;




