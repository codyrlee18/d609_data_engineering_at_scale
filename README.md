# d609_data_engineering_at_scale

### Project Summary

In this project, I ingested sensor and customer data from the STEDI Step Trainer system into AWS using S3 and AWS Glue. I cleaned, joined, and transformed the data through multiple trusted and curated zones, ensuring that only records with proper research consent were included. The final curated datasets were prepared in a data lakehouse architecture, enabling Data Scientists to use them for training machine learning models.

- All of the DDL scripts for the tables I manually created in Glue Console from the JSON Data are in the "DDL Scripts" folder. 
- All of the python scripts generated from the Glue Jobs I created through nodes in AWS Glue Studio are in the "Glue Jobs Scripts" folder. 
- All screenshots demonstrating correct row counts for each of the tables created and adherence to the project suggestions are in the “Screenshots of Athena Queries” folder.

Throughout the project we used the STEDI Step Trainer raw data to curate a solution that will help the team to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy was a primary consideration in how we cleaned and transformed the data. The work done in these folders is meant to represent the sanitized customer data that will allow Data Scientists to train the learning model. 

#### References
In addition to studying the course material throughout the Udacity Nanodegree, I used generative AI tools (such as ChatGPT) to assist with debugging Glue job logic, optimizing SQL queries, interpreting project requirements, and navigating AWS Glue Studio. This support helped ensure accurate row counts, proper filtering for research consent, and alignment with both core and stand-out project expectations.

