#!/usr/bin/env python3

import pymysql
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import re
import json
import logging
import salt.client
import salt.config

# Initialize a Salt client
salt_client = salt.client.LocalClient()

# Retrieve 'sle_config' pillar data
sle_config_pillar_data = salt_client.cmd('127.0.0.1', 'pillar.item', ['sle_config'])
if '127.0.0.1' in sle_config_pillar_data:
    sle_config_data = sle_config_pillar_data.get('127.0.0.1', {}).get('sle_config', {})
        
    confluence_username = sle_config_data.get('username', '')
    confluence_password = sle_config_data.get('password', '')
    confluence_url     = sle_config_data.get('confluence_url', '')
    db_host = sle_config_data.get('db_host', '')
    db_user = sle_config_data.get('db_user', '')
    db_name = sle_config_data.get('db_name', '')
    db_password = sle_config_data.get('db_password', '')
    new_db_host = sle_config_data.get('new_db_host', '')
    new_db_user = sle_config_data.get('new_db_user', '')
    new_db_password = sle_config_data.get('new_db_password', '')
    new_db_name = sle_config_data.get('new_db_name', '')

else:
    print("'sle_config' not found in pillar data.")

#########################################################################################################################################
#Name:
#    get_bugs_count
#
#Parameters:
#    - confluence_username (str): The username to authenticate with Confluence.
#    - confluence_password (str): The password associated with the provided Confluence username.
#
#Description:
#    Fetches the bug count for various milestones (versions) from a Confluence page. 
#    The function queries a predefined `confluence_url` using the provided `confluence_username` and `confluence_password` to authenticate.
#    The data fetched is expected to be in JSON format with a structure that contains the bug count for each milestone.
#Returns:
#    - dict: A dictionary containing the bug counts for the various milestones. 
#    In case of an HTTP error or any other exception, appropriate error messages are logged, and an empty dictionary is returned.
#
#Exceptions:
#    - If there's an HTTP error while fetching data from Confluence, it logs the error message with the prefix "HTTP error occurred:".
#    - For any other exceptions, it logs the error message with the prefix "Error occurred:".
#########################################################################################################################################
def get_bugs_count(confluence_username, confluence_password):
    try:
        response = requests.get(confluence_url, auth=(confluence_username, confluence_password))
        response.raise_for_status()

        if response.status_code == 200:
            page_data = response.json()
            io_field = page_data['body']['view']['value']

            def get_beta_bugs_count(version):
                pattern = f'{version} Total Bugs =(\d+)'
                match = re.search(pattern, io_field)
                if match:
                    return int(match.group(1))
                return 0
            # Extract the counts for different versions
            bug_counts = {
                'beta1': get_beta_bugs_count('Beta1'),
                'beta2': get_beta_bugs_count('Beta2'),
                'beta3': get_beta_bugs_count('Beta3'),
                'publicbeta': get_beta_bugs_count('PublicBeta'),
                'rc1': get_beta_bugs_count('RC1'),
                'publicrc': get_beta_bugs_count('PublicRC'),
                'gmc': get_beta_bugs_count('GMC')
            }

            return bug_counts

    except requests.exceptions.HTTPError as err:
        logging.error(f"HTTP error occurred: {err}")
    except Exception as err:
        logging.error(f"Error occurred: {err}")

    return {}

# Configure the logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#########################################################################################################################################
#Name:
#is_milestone_present
#
#Parameters:
#
#   - new_db_host (str): Hostname for the database.
#   - new_db_user (str): Database user's name.
#   - new_db_password (str): Password for the database user.
#   - new_db_name (str): Name of the database.
#   - milestone_version (str): Version of the milestone to check.
#
#Description:
#   - Establishes a connection to the specified database and checks if the provided milestone_version exists in the VirtPerfData table.
#       If an error occurs during the process, an error message is logged.
#
#Returns:
#
#   - True if the milestone_version is present in the table; False otherwise.
#Exceptions:
#   - Logs any database-related errors encountered during execution.
#########################################################################################################################################
def is_milestone_present(new_db_host, new_db_user, new_db_password, new_db_name, milestone_version):
    global new_connection
    new_connection = None  # Initialize the connection to None

    try:
        # Connect to the new database
        new_connection = pymysql.connect(host=new_db_host, user=new_db_user, password=new_db_password, db=new_db_name)

        with new_connection.cursor() as new_cursor:
            # Check if the milestone version already exists in the table
            query = "SELECT COUNT(*) FROM perfData WHERE mileStone_Version = %s"
            new_cursor.execute(query, (milestone_version,))
            count = new_cursor.fetchone()[0]
            
            return count > 0

    except pymysql.Error as e:
        logging.error(f"Error while checking milestone presence: {e}")
        return False

    finally:
        # Close the new database connection
        if new_connection:
            new_connection.close()
        logging.info("Database connection closed.")

#########################################################################################################################################
#Name:
#   insert_status_counts
#
#Description:
#   - Connects to two databases, retrieves test statuses from the old one, processes the results, and then inserts aggregated test 
#     data into the new database.
#
#Parameters: None
#
#Returns: None
#
#Key Operations:
#
#Establishes connections to both the old and new databases.
#Executes a SQL query on the old database to gather test statuses.
#Processes the query results to compute test count summaries.
#Fetches bug counts from Confluence.
#Checks if a given milestone version already exists in the new database.
#If the milestone version is new, inserts test count data into the new database.
#Closes both database connections.
#Exceptions:
#   - Logs any errors encountered during execution.
#
#Dependencies:
#   - Requires the pymysql, logging, and external functions get_bugs_count and is_milestone_present.
#########################################################################################################################################

def insert_status_counts():
    connection = None
    new_connection = None
    try:
        #Connecting to old & new databases
        connection = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name)
        new_connection = pymysql.connect(host=new_db_host, user=new_db_user, password=new_db_password, db=new_db_name)

        cursor = connection.cursor()
        cursor.execute(f"USE {db_name}")

        sql_query = """
           SELECT q_build, status, COUNT(*) AS count
           FROM report_view
           WHERE q_role_name = 'performance'
              AND q_release = 'SLES-15-SP5'
              AND status IN ('pass', 'fail')
              AND q_build IN ('Beta1', 'Beta2', 'Beta3', 'PublicBeta', 'RC1', 'PublicRC', 'GMC')
           GROUP BY q_build, status;
        """
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        build_data = {}
        
        logging.info("Processing query results.")
        for row in rows:
            q_build, status, count = row
            if q_build not in build_data:
                build_data[q_build] = {'pass': 0, 'fail': 0}
            build_data[q_build][status.lower()] = count

        bug_counts = get_bugs_count(confluence_username,confluence_password)
        new_cursor = new_connection.cursor()

        for build, counts in build_data.items():
            no_tests_pass = counts.get('pass', 0)
            no_tests_fail = counts.get('fail', 0)
            no_tests_total = no_tests_pass + no_tests_fail
            no_tests_bug = bug_counts.get(build.lower(), 0)
            mileStone_Version = build
            execution_date = datetime.now()
            
            if is_milestone_present(new_db_host, new_db_user, new_db_password, new_db_name, mileStone_Version):
                logging.warning(f"Milestone version {mileStone_Version} already present. Skipping insertion.")
                continue
            
            new_cursor.execute("""
                INSERT INTO perfData(no_tests_total, no_tests_pass, no_tests_fail, no_tests_bug, mileStone_Version, execution_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (no_tests_total, no_tests_pass, no_tests_fail, no_tests_bug, mileStone_Version, execution_date))

        new_connection.commit()

    except pymysql.Error as e:
        logging.error(f"An error occurred: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Closing database connections.")
        if connection:
            connection.close()
        if new_connection:
            new_connection.close()

# Call the function
insert_status_counts()
