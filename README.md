# cassandra-noshowappts

Due to my experience in the healthcare industry and my desire to learn new databases, I decided to create a project that pulls No-Show Appointment data into Apache Cassandra, a database I had never worked with before. The Apache Cassandra database was built on a Docker container and some data manipulation was performed as an attempt to make the information more user-friendly. Shown below is the outline of the project, and a deeper read into my approach can be found on this Medium article: https://medium.com/swlh/building-a-python-data-pipeline-to-apache-cassandra-on-a-docker-container-fc757fbfafdd

## Project Outline

`data` folder: contains a spreadsheet of no-show appointment data obtained from Kaggle.com

`docker-compose.yaml`: yaml file containing the configuration for the Docker container which the Cassandra database lives on

`etl_utilities`: I generally pull together commonly-used Python methods into a utilities directory for the purpose of being reused by other Python scripts and reducing redundant code. For this project, since I'm only using one method for connecting to an external source, I placed it within a single file rather than a directory itself.

`no_show_etl.py`: This file contains the methods that pull the data out of the CSV file into a Pandas dataframe, performs data cleanup/manipulation on the data set, and then inserts the data into the Cassandra database.

## Real-World Adjustments and Best Practices

Due to the small scope of this project, not all of the best practices or real-world implementations were provided. Shown below are some adjustments I'd make for a real-world project:

- Batch inserts: one of the best practices for inserting a large dataset into a database is to perform the insert within batches, rather than all at once or row-by-row.
- Within an AWS environment, I'd create a datalake environment within S3 bucket(s) and stage the CSV file within the appropriate directory for data consumption. That way if the data were to be accidentally overwritten within the database, the original copy can still be found within the S3 bucket.
