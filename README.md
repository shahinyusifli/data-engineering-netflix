# data-engineering-netflix
 I've developed a data engineering solution for Inbank's take-home task. I designed normalized data model for efficient data warehousing. In addition, I created ETL pipelines using Airflow to handle data flow into a PostgreSQL data warehouse. Furthermore, I formulated and executed SQL queries to fulfill the specific reporting requirements.
### Task 1
Construct a normalized data model for the dataset provided.
### Solution
I have created a normalized data model meeting the requirements of each normal form. 
- I would like to start the first normal form. There are not any row order to convey information and we can not see repeated groups. Also, we had a unique primary key. But we can see there is some violation of mixing data types within the "Plan Duration" column which saves "1 Month" data and "1" and "Month" belong to integer and string or char datatypes. Therefore, I prefer to transform this data and eliminate the "Month" value, we can represent years and months with just numbers. 
- There is not any violation of the second normal form. All non-key attributes of columns have logical relation with the "User ID" key attribute.
- According to the requirements of the third normal form, there should not be any transient dependency between non-key attributes. But we can notice this dependency in the description section of columns in the task pdf. Especially, we can understand the transient dependency between the "Revenue" and "Subscription Type" columns with this statement "Monthly Revenue: Fee receivable for the given subscription type". It means "Revenue" depending on the User with the User's Subscription Type. In other words, we can describe it as {User ID} → {Subscription Type} → {Revenue}. For meeting the requirements of the third normal form, I have created a new table that consists of ID, Subscription, and Revenue columns. 
- Boyce Codd's normal form can be beneficial for eliminating future data inconsistency problems. We can ensure the quality of queues by implementing a super key concept. I have created new tables for the Device, Country, and Gender columns. 
After implementing these normal forms, I have to decide to implement data warehousing concepts because queries in Task 4 look like queries for the data analytics process. I have created relevant dimensions and fact tables considering all normal forms, final schema can be found below:
![alt text](https://github.com/shahinyusifli/data-engineering-netflix/blob/main/schema_netflix_dw.png)


### Task 2
Load the data into the data model you constructed and into DB engines like MySQL/SQLite/Postgres or some free online one using the ELT approach (Extra points if you use Python and SQL in combination to achieve this task). How would you make such loading repeatable on a regular basis?

### Solution
I have selected PostgreSQL, Airflow, and Python to accomplish this task. PostgreSQL was selected because it has strong performance in selecting big amounts of data which is the most critical for data warehouses. Also, I have selected Airflow because I can achieve repeatable loading by using the advantages of Python. In total, I have created 7 ETL pipelines. I would like to give a short description of each of them.
- country_pipeline: Extract and Load unique variables from the Country column of the dataset to the Country Dimension of the data warehouse
- device_pipeline: Extract and Load unique variables from the Device column of the dataset to the Country Dimension of the data warehouse
- fact_sales_pipeline: Extract, Transform, and Load data from User_ID, Subscription Type, Country, Device, and Last Payment Date column of the dataset to the Sales Fact table. Also, all columns are mapped to foreign keys.
- gender_pipeline: Extract and Load gender variables from the Gender column of the dataset to the Gender Dimension of the data warehouse
- populate_time_dimension:
- subscription_pipeline: Extract and Load variables from the Subscription Type, Revenue, and Plan Duration columns of the dataset to the Subscription Dimension of the data warehouse
- user_pipeline: Extract User related information such as User ID, Join Date, Country, Age, Gender, Active Profiles, Household Profile Ind, Movies Watched, Series Watched to User Dimension. Also, Gender data is transformed with ID of Gender Dimension

### Task 3
What control measures could be deployed here to ensure the correctness of data? Do you notice something odd about the dataset provided?
### Answer 
There are some control measures that can be used for the correctness of data
- Control measure for "Plan Duration": As I mentioned "Plan Duration" can violate the first normal form. Therefore, it is transformed into subscription_pipeline.
- Control measure for "Ages": There are some outlined data such as 107 and 904 in the "Ages" column of the dataset. 107 can be acceptable but 904 is not. Therefore, rows are deleted if the age is more than 110. This transformation is done in user_pipeline and fact_sales_pipeline.
There is some anomaly in the dataset. I think each type of subscription should have a different Revenue value but it is listed from 10 to 15. It may happen during the generation of data.


### Task 4
Write queries for answering the following questions:
- a.The most profitable country for Netflix.
 Query:
- b. The most popular packages per country.
 Query:
- c. Which country has the potential for improving earnings if Netflix starts charging subscribers an additional fee for sharing Netflix households outside of their own?
 Query:
- d. A report showing the popularity of Movies and Series in different customer segments and the device used to consume, across the different markets the company operates in.
 Query:
