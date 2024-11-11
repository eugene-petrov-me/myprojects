# Data pipeline project

## Project overview

This project focuses on practicing skills that are required to building a data pipeline automation script in Python. 

The pipeline cleans, transforms and loads subscriber data into codemycade database and a csv file in production folder. 
In the data cleaning process we ensure the data does not contain any duplicates and missing values prior to any tables being joined.
In the transformation stage we unpack data into separate columns (where relevant) and join tables to create a holistic view for analysts.
Finally, the combined and cleaned data is loaded into a table in a SQLite3 database and a csv file.

## Project folder structure
```
subscriber-pipeline-starter-kit/
|--dev
| |--codemycode_updated.db
| |--codemycode.db
| |--data_pipeline.py
| |--script.sh
|--prod
| |--students_data.csv
|--log
| |--data_pipeline.log
| |--version.txt
| |--last_modified_time.txt
|--README.md 

```
## How to use
The entire pipeline can be executed using a bash script, which will run the Python script.
> ./data_pipeline.sh

Optionally, in bash settings an alias can be created to execute the script.
> alias="run_pipeline" "./data_pipeline.sh" 

## Python implementation details
In order to complete this project, the following Python modules have been useful: 
* Pandas
* SQLite3
* Unittest
* Logging

