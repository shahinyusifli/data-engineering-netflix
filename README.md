# data-engineering-netflix
 I've developed a data engineering solution for Inbank's take-home task. I designed a normalized data model and multi-layer architecture for efficient data warehousing. In addition, I created ELT pipelines using Airflow, Python, and PostgreSQL to handle data flows into a PostgreSQL data warehouse layers. Furthermore, I formulated and executed SQL queries to fulfill the specific reporting requirements. All SQL queries and User Defined Functions can be found under the DDL folder.
## Installation and Run in Local
Clone the repo from GitHub by running:
```
 $ git clone https://github.com/shahinyusifli/data-engineering-netflix.git
```
Start the Docker container in detached mode inside the project folder
```
 $ docker-compose up -d  
```
Make a new database called 'netflix_dw' and create tables using queries located in the SQL folder. Once the database and tables are created, save credentials like dbname, user, password, host, and port. These credentials will be needed to establish connections in the Admin panel.

Airflow menu:
![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/connection.png)

After adding the record and confirming that the List Connection works, you can proceed to run all the DAGs. Simply go to the DAGs section and initiate all the pipelines with a single click.

## Tasks and Solutions
### Task 1
Construct a normalized data model for the dataset provided.
### Solution
I have created a normalized data model meeting the requirements of each normal form. 
- I would like to start the 1NF(first normal form). There are not any row order to convey information and we can not see repeated groups. Also, we had a unique primary key. But we can see there is some violation of mixing data types within the "Plan Duration" column which saves "1 Month" data and "1" and "Month" belong to integer and string or char datatypes respectively. Therefore, I prefer to transform this data and eliminate the "Month" value, we can represent years and months with just numbers. 
- There is not any violation of the 2NF(second normal form). All non-key attributes of columns have a logical relation with the "User ID" key attribute.
- According to the requirements of the 3NF(third normal form), there should not be any transient dependency between non-key attributes. But we can notice this dependency in the description section of columns in the task pdf. , we can understand the transient dependency between the "Revenue" and "Subscription Type" columns with this statement "Monthly Revenue: Fee receivable for the given subscription type". It means "Revenue" depending on the User with the User's Subscription Type. In other words, we can describe it as {User ID} → {Subscription Type} → {Revenue}. To meet the requirements of the third normal form, I have created a new table that consists of ID, Subscription, Plan_Duration, and Revenue columns. 
- BCNF(Boyce Codd's normal form) can be beneficial for eliminating future data inconsistency problems. We can ensure the quality of queries by implementing a super key concept. I have created a Device table for this purpose.


After implementing these normal forms, I have created 3 layers for data loading and transformation. In the first layer, the bronze layer is created for storing raw data. It is also represented as a data lake. 


![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/netflix_dw_bronze.png)

In this way, I have created a silver layer for transforming data such as formatting date columns and eliminating rows according to age>110 condition. In the silver layer, transformation data according to quality checks, modeling raw data, and eliminating outlier data are aimed. 

![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/netflix_dw_silver.png)

Finally, the gold layer represents data that is suitable for future data analytics or data science projects. Selected multi-layer architecture is beneficial for fast responses to data linage issues.
![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/schema_netflix_dw.png)


### Task 2
Load the data into the data model you constructed and into DB engines like MySQL/SQLite/Postgres or some free online one using the ELT approach (Extra points if you use Python and SQL in combination to achieve this task). How would you make such loading repeatable on a regular basis?

### Solution
I have selected PostgreSQL, Airflow, and Python to accomplish this task. PostgreSQL was selected because it has strong performance in selecting big amounts of data which is the most critical for data warehouses. Also, I have selected Airflow because I can achieve repeatable loading by using the advantages of Python. I can achieve repeatable loading on a regular basis with start_date and schedule_interval of DAG properties which we can define to execute pipelines in monthly, weekly, daily, etc. time intervals. In total, I have created 7 data pipelines. I would like to give a short description of each of them.

- First (bronze) layer pipeline: Extract raw data from a CSV file.
![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/bronze_pipeline.png)

- Second (silver) layer pipeline: In this pipeline, some transformations are done such as formatting date data and eliminating outliers. Also, data modeling is done by splitting different types in the same column. 
![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/silver_pipeline.png)

- Fact sales pipeline: Some values such as subscription types and devices are mapped. Also, User Defined Functions are used in PostgreSQL for this purpose. All functions belong to the gold layer or schema and can be found inside the DDL folder.
![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/sales_fact_table_pipeline.png)

- Populate date dimension: This pipeline consists of one task which is responsible for generating a calendar table in 2 years range. Also, a common table expression (CTE) is used to define the start and end dates for this range.
![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/calendar_pipeline.png)

- Subscription pipeline: Extract and Load variables from the Subscription Type, Revenue, and Plan Duration columns of the netflix table of the silver layer to the Subscription Dimension of the gold layer. 
![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/subscription_pipeline.png)

- account pipeline: Load user-sensitive PII data from the silver layer table to account dimension of the gold layer. We can find loading transformed user id, join date, age, gender, and country data to target dimension.
![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/users_pipeline.png)

- device pipeline: Load mapped and transformed device values from the silver layer table to the target dimension.
![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/device_pipeline.png)

### Task 3
What control measures could be deployed here to ensure the correctness of data? Do you notice something odd about the dataset provided?
### Solutions 
There are some control measures that can be used for the correctness of data
- Control measure for "Plan Duration": As I mentioned "Plan Duration" can violate the first normal form. Therefore, it is transformed inside subscription_pipeline.
- Control measure for "Ages": There are some outlined data such as 107 and 904 in the "Ages" column of the dataset. 107 can be acceptable but 904 is not. Therefore, rows are deleted if the age is more than 110. This transformation is done in the silver layer.

Also, there is some anomaly in the dataset. I think each type of subscription should have a constant Revenue value but range of Revenue values is listed from 10 to 15 for each subscription. It may happen during the generation of data.

### Task 4
Write queries for answering the following questions:
- a.The most profitable country for Netflix.

 Query:
 ```sql
SELECT
    gda.country AS "Country",
    SUM(gds.revenue) AS "Profit"
FROM
    gold.dim_account gda
JOIN
    gold.fct_sales gfs ON gda.id = gfs.account_id
JOIN
    gold.dim_subscription gds ON gfs.subscription_id = gds.id
GROUP BY
    gda.country
ORDER BY
    "Profit" DESC
limit 1;
```
Result:

![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/result_of_first_task.png)
- b. The most popular packages per country.

It can be defined with number of users.

 Query:
 ``` sql
 WITH SubscriptionCounts AS (
    SELECT
        da.country,
        ds.subscription_type AS "Subscription type",
        COUNT(fs.account_id) AS "Number of users"
    FROM
        gold.fct_sales fs
    JOIN
        gold.dim_subscription ds ON fs.subscription_id = ds.id
    JOIN
        gold.dim_account da ON fs.account_id = da.id
    GROUP BY
        da.country, ds.subscription_type
),
RankedSubscriptions AS (
    SELECT
        country,
        "Subscription type",
        "Number of users",
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY "Number of users" DESC) AS rn
    FROM
        SubscriptionCounts
)
SELECT
    country,
    "Subscription type",
    "Number of users"
FROM
    RankedSubscriptions
WHERE
    rn = 1;

```
Result: 

![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/result_of_second_task.png)
- c. Which country has the potential for improving earnings if Netflix starts charging subscribers an additional fee for sharing Netflix households outside of their own? 

This involves examining profits generated by multiple households linked to a single contract. For two selected households, their profits could be doubled to if we change the policy to sharing users. We can see the result with this SQL query:
 ``` sql
SELECT
    da.country as "Country",
    SUM(CASE WHEN fs.household_profile_ind >= 1 THEN ds.revenue ELSE 0 END) as "Profit of more than 1 household",
    SUM(CASE WHEN fs.household_profile_ind = 1 THEN ds.revenue ELSE 0 END) as "Profit of 1 household",
    SUM(CASE WHEN fs.household_profile_ind >= 1 THEN ds.revenue ELSE 0 END) * 2 as "Earnable profit"
FROM
    gold.fct_sales fs
JOIN
    gold.dim_account da ON fs.account_id = da.id
JOIN
    gold.dim_subscription ds ON fs.subscription_id = ds.id
GROUP BY
    da.country
ORDER BY "Earnable profit" DESC
LIMIT 1;
 ```
 Result:

 ![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/Images/result_of_third_task.png)
- d. A report showing the popularity of Movies and Series in different customer segments and the device used to consume, across the different markets the company operates in.

 Query:
``` sql
SELECT
    acc.country as "Country",
    acc.gender as "Gender",
    dd.device as "Device",
    SUM(fs.movies_watched) as "Number of watched movies",
    SUM(fs.series_watched) as "Number of watched series"
FROM
    gold.dim_account acc
JOIN
    gold.fct_sales fs ON acc.id = fs.account_id
JOIN
    gold.dim_device dd ON fs.device_id = dd.id
GROUP BY
    acc.country, acc.gender, dd.device
ORDER BY
    acc.country, acc.gender, dd.device;
 
```
Result: Result can be find in "Results_of_query_4" folder 