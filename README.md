# d609_data_engineering_at_scale

### Project Summary

In this project, I ingested sensor and customer data from the STEDI Step Trainer system into AWS using S3 and AWS Glue. I cleaned, joined, and transformed the data through multiple trusted and curated zones, ensuring that only records with proper research consent were included. The final curated datasets were prepared in a data lakehouse architecture, enabling Data Scientists to use them for training machine learning models.

- All of the DDL scripts for the tables I manually created in Glue Console from the JSON Data are in the "DDL Scripts" folder. 
- All of the python scripts generated from the Glue Jobs I created through nodes in AWS Glue Studio are in the "Glue Jobs Scripts" folder. 
- All screenshots demonstrating correct row counts for each of the tables created and adherence to the project suggestions are in the “Screenshots of Athena Queries” folder.


#### References
I used GenAI for assisting with debugging Glue job logic, optimizing SQL queries, interpreting project requirements, and navigating AWS Glue Studio throughout the project. The guidance helped ensure accuracy in row counts, proper filtering for research consent, and compliance with the project suggestions.
