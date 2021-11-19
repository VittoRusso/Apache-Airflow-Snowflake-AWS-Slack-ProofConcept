## Apache-Airflow-Snowflake-AWS-Slack-ProofConcept

# Introduction

As a proof of concept the main objective of this project is to simulate a real world use case in which we can perform real data engineers task that could be performed on a daily basis.

We are going to simulate that for a given dataset (obtained from kaggle at: https://www.kaggle.com/megelon/meetup) we can extract CSV files hosted on a blob storage on the cloud, in this case a S3 AWS bucket, to then perform the ingestion of the data into a SQL database aslo hosted on the cloud (Snowflake). Once the data has been ingested into staging tables we can perform 2 main task, one to perform transformations of that data into views that can be used by a Data Analyst and 2 provide an infrastructure of near realtime data updating with means of a STREAM and MERGE functions of SQL/Snowflake.

All of the previous tasks are meant to be automated using Apache Airflow.

# In detail explication

As mentioned before, we used the Meetup dataset which contains 9 CSV files of relevant information about the events, groups and members registered in Meetup. There are auxiliary files such as topics, cities, places and categories that contextualize the information in the 3 main files Events, Members and Groups.

To simulate a realistic environment the dataset is mounted on an S3 bucket account where the information should be extracted and uploaded to Snowflake.

Thinking for future steps a typical snowflake data architecture was used a 3 schema architecture: INGESTION (STAGE), CURATION, CALCULATION.

<img src="/Pictures/Pic1.jpg" height="400" alt="Screenshot"/> 
