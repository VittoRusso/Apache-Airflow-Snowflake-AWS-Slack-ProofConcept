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

The 9 CSV files are mounted in an S3 bucket in AWS, each in a subfolder where it will be possible to add future files to be extracted en masse. The 3 data schemas of INGESTION, CURATION and CALCULATION are created. In turn, an external S3 stage and a file format in snowflake is created to read the CSV files from S3.

<img src="/Pictures/Pic2.png" height="400" alt="Screenshot"/> 

<img src="/Pictures/Pic3.png" height="400" alt="Screenshot"/> 

Using the snowflake compute warehouse and the COPY INTO function it is possible to read from the INGESTION schema to the table of that data in CURATION with the following query.

<img src="/Pictures/Pic4.png" height="400" alt="Screenshot"/> 

<img src="/Pictures/Pic4_1.jpg" height="400" alt="Screenshot"/> 

The first transformation task was to join with JOINS two main tables of event and group to create a table ready to create a dashboard for business intelligence with relevant data and combined in a single table.

For the second task, the purpose was to create a streamline of any modified data into the main destination table with millions of rows, this is so that we dont need to reupload huge files in GB but instead, only the data that changed. 

The second made use of a MERGE and STREAM function from Snowflake. The combination of these two functions has the ability to listen to a source table, take only the newly inserted, updated or deleted data and perform their respective insertion into a target table. This capability has the potential to simplify updates of tables with millions of records. For this exercise the MEMBERS table containing millions of records was used to demonstrate the simplified capability of keeping this table up to date.

For the query to create the table of the first transofrmation, a WHERE clause was used to eliminate events that did not have at least one attendance accepted.

<img src="/Pictures/Pic5.jpg" height="400" alt="Screenshot"/> 

For the second transformation it is necessary to create a target table "CURATION.SOURCE_MEMBERS", create a STREAM that channels only the changes made on the source table "CURATION.MEMBERS_STREAM" and a target table "CURATION.MEMBERS".

<img src="/Pictures/Pic6.jpg" alt="Screenshot"/>

And finally, the MERGE statement to import those changes to the target table.

<img src="/Pictures/Pic7.jpg" alt="Screenshot"/>

For the last step, using Snowflake's OPERATOR in Apache Airflow, nodes were created to create a pipeline that, in a recurrent way, every 15 minutes will perform the transformations in point 2. To decouple Airflow from the computation, in snowflake Stored Procedures were created to be called from the DAGs and create the pipelines.

A DAG was created for each transform of the previous point.

A DAG for the reading from S3 of the CSV files, the creation of tables in the CURATION schema and the creation of the transformed table between events and groups.

<img src="/Pictures/Pic8.jpg" alt="Screenshot"/>

<img src="/Pictures/Pic9.jpg" alt="Screenshot"/>

You can see the Stored Procedures call from the Snowflake logs.

<img src="/Pictures/Pic10.jpg" alt="Screenshot"/>

And a second DAG, this last DAG simulates the insertion of a data to the source table of the MERGE/STREAM as if it were an event of creation of a new member of MEETUP, this later calls the stored procedure that executes the merge to the final destination table.

<img src="/Pictures/Pic11.jpg" alt="Screenshot"/>

The data can be evidenced by entering it in the STREAM table and then in the target table.

<img src="/Pictures/Pic12.jpg" alt="Screenshot"/>

<img src="/Pictures/Pic13.jpg" alt="Screenshot"/>

As an extra exercise of the test we used a last functionality of Apache Airflow, the slack alerts. In order to have a good understanding of the flows and their states, it can be a very important feature to bring success or failure messages directly to collaborative tools where several developers and stakeholders participate.

The slack operator functionality allows to create nodes in apache airflow that send messages to channels and people in slack.

In this case, we add an operator node at the end of the second DAG that notifies a user that the DAG was executed and a new member is added to the MEMBERS table.

For that we create the node in python and add it at the end of the pipe line.

<img src="/Pictures/Pic14.jpg" alt="Screenshot"/>
<img src="/Pictures/Pic15.jpg" alt="Screenshot"/>
<img src="/Pictures/Pic16.jpg" alt="Screenshot"/>

