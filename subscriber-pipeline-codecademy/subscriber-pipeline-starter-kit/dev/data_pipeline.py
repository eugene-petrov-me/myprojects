import sqlite3
import pandas as pd
import json
import logging
import unittest
import os
from datetime import datetime

# Create a logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    filename='data_pipeline.log', 
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %H:%M:%S'
)

# Function to check if the database has been updates since the last run
def read_last_modified_time():
    try:
        with open('last_modified_time.txt', 'r') as f:
            timestamp_str = f.read().strip()
            if timestamp_str:
                return datetime.fromisoformat(timestamp_str)
            else:
                return None
    except FileNotFoundError:
        logger.warning("last_modified_time.txt not found. Creating")
        return None

# Function to write last modified time to a file
def write_last_modified_time(modified_time):
    with open('last_modified_time.txt', 'w') as f:
        logger.info(f"last_modified_time.txt has been updated: {modified_time.isoformat()}")
        f.write(modified_time.isoformat())

# Function to check if the database has been updated
def is_database_updated(db_file):
    if not os.path.exists(db_file):
        logger.error(f"Database file {db_file} not found.")
        return False

    try:
        file_info = os.stat(db_file)
        file_modified_time = file_info.st_mtime
        current_modified_time = datetime.fromtimestamp(file_modified_time)
        logger.debug(f'Current modified time is {current_modified_time}')
        
        last_modified_time = read_last_modified_time()
        logger.debug(f'Last modified time is {last_modified_time}')
        if last_modified_time is None or current_modified_time > last_modified_time:
            write_last_modified_time(current_modified_time)
            return True
        return False
    except Exception as e:
        logger.error(f"Error checking database modification time: {e}")
        return False

# Function to convert to json
def to_json(row, field):
    try:
        return json.loads(row).get(field, None)
    except (json.JSONDecodeError, TypeError):
        logger.error(f"Version {version}: Failed to parse JSON in row: {row}")
        return None

# Function to read version from a file
def read_version():
    try:
        with open('version.txt', 'r') as file:
            version = int(file.read().strip())
            return version
    except FileNotFoundError:
        logger.warning("version.txt not found. Starting at version 1.")
        return 1  # Default version

# Function to write updated version to a file
def write_version(version):
    with open('version.txt', 'w') as file:
        file.write(f"{version}")

# Function to keep track of versions
def change_version():
    global version
    version += 1
    logger.info(f"version is updated to {version}")
    write_version(version)

# Connection to database and data processing unit
try:  
    db_file = 'cademycode_updated.db'
    last_modified_time = read_last_modified_time()
    version = read_version()
    change_version()
    with sqlite3.connect(db_file) as con:
        logger.info(f"Version {version}: Connected to the database")
        cur = con.cursor()
        
        if is_database_updated(db_file):
            logger.info(f"Version {version}: Database is updated, executing the pipeline")
            
            # Read sql into a dataframe
            students_query = """SELECT * FROM cademycode_students"""
            df_students = pd.read_sql_query(students_query, con)
          
            df_students_duplicates_count = df_students.duplicated().sum()
            if df_students_duplicates_count > 0:
                logger.warning(f"Version {version}: cademycode_students table contains {df_students_duplicates_count} duplicates")
                df_students = df_students.drop_duplicates()

            # Unpack contact_info column into separate columns for address and email
            df_students['mailing_address'] = df_students['contact_info'].apply(lambda x: to_json(x, 'mailing_address'))
            df_students['email'] = df_students['contact_info'].apply(lambda x: to_json(x, 'email'))
            df_students = df_students.drop(columns= {'contact_info'})

            # Reorder colums logically and rename
            df_students = df_students[["uuid", "name", "dob", "sex", "mailing_address", "email", "job_id", "num_course_taken", "current_career_path_id", "time_spent_hrs"]]
            df_students = df_students.rename(columns={"current_career_path_id":"career_path_id"})

            # Fill in empty values
            df_students['job_id'] = df_students['job_id'].fillna(0)
            df_students['num_course_taken'] = df_students['num_course_taken'].fillna(0)
            df_students['career_path_id'] = df_students['career_path_id'].fillna(0)
            df_students['time_spent_hrs'] = df_students['time_spent_hrs'].fillna(0)

            # Change data types for numerical columns
            df_students = df_students.astype({'job_id': 'float64', 'num_course_taken': 'float64', 'career_path_id': 'float64', 'time_spent_hrs': 'float64'})
            df_students = df_students.astype({'job_id': 'int64', 'num_course_taken': 'int64', 'career_path_id': 'int64'})

            # read sql into a dataframe and change data_types
            courses_query = """SELECT * FROM cademycode_courses"""
            df_courses = pd.read_sql_query(courses_query, con)
          
            df_courses_duplicates_count = df_courses.duplicated().sum()
            if df_courses_duplicates_count > 0:
                logger.warning(f"Version {version}: cademycode_courses table contains {df_courses_duplicates_count} duplicates")
                df_courses = df_courses.drop_duplicates()
            
            df_courses = df_courses.astype({'career_path_id': 'int64', 'hours_to_complete': 'int64'})

            # read sql into a dataframe and change data_types
            jobs_query = """SELECT * FROM cademycode_student_jobs"""
            df_jobs = pd.read_sql_query(jobs_query, con)
          
            df_jobs_duplicates_count = df_jobs.duplicated().sum()
            if df_jobs_duplicates_count > 0:
                logger.warning(f"Version {version}: cademycode_job table contains {df_jobs_duplicates_count} duplicates")
                df_jobs = df_jobs.drop_duplicates()
          
            df_jobs = df_jobs.astype({'job_id': 'int64', 'avg_salary': 'int64'})

            df_student_row_count = df_students.shape[0]
            logger.info(f"Version {version}: Number of line before the join: {df_student_row_count}")

            # joining students data with job and courses
            df_merged = pd.merge(df_students, df_courses,  how='left', left_on="career_path_id", right_on="career_path_id")
            df_final = pd.merge(df_merged, df_jobs, how="left", left_on="job_id", right_on="job_id")

            df_student_data_row_count = df_final.shape[0]
            logger.info(f"Version {version}: Number of lines after the join: {df_student_data_row_count}")

            df_final["job_category"] = df_final["job_category"].fillna('Unknown')
            df_final["avg_salary"] = df_final["avg_salary"].fillna(0)
            df_final["career_path_name"] = df_final["career_path_name"].fillna('Unknown')
            df_final["hours_to_complete"] = df_final["hours_to_complete"].fillna(0)

            # checking if the table doesn't contain duplicated student as a result of the join
            if df_student_row_count == df_student_data_row_count:
                logger.info(f"Version {version}: Number of students remains the same after the join")
            else: 
                logger.error(f"Version {version}: Number of students changes after the join")

            # if hours spent equals or more than needed hours to complete a path - then true
            df_final['_completed_path'] = (df_final['time_spent_hrs'] > df_final['hours_to_complete']).fillna(False)

            # load the final table into the database and export as a csv file
            db_table = "students_data"
            df_final.to_sql(name=db_table, con=con, if_exists="replace")
            logger.info(f"Version {version}: Updating the DB table - {db_table}")

            file_path = "students_data.csv"
            df_final.to_csv(file_path, index=False)
            logger.info(f"Version {version}: Exporting file to {file_path}")
        else: 
            logger.info(f"Version {version}: Database has no updates")

except Exception as e:
      logger.error(f"Version {version}: Error running the pipeline: {e}")
      raise

class TestDataPipeline(unittest.TestCase):
    def test_number_of_rows(self):
        self.assertEqual(df_student_row_count, df_student_data_row_count, "The number of rows changed during transformation")
    
    def test_no_null_values(self):
        self.assertFalse(df_final.isnull().values.any(), "There are null values in the final table")

if __name__ == '__main__':
    unittest.main()