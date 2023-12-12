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

salt_client = salt.client.LocalClient()

sle_config_pillar_data = salt_client.cmd('127.0.0.1', 'pillar.item', ['sle_config'])
if '127.0.0.1' in sle_config_pillar_data:
    sle_config_data = sle_config_pillar_data.get('127.0.0.1', {}).get('sle_config', {})

        # Access the 'username' value from 'sle_config'
    confluence_username = sle_config_data.get('username', '')
    confluence_password = sle_config_data.get('password', '')
    confluence_url     = sle_config_data.get('rt_confluence_url', '')
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

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

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
                'rc1': get_beta_bugs_count('RC1'),
                'rc2': get_beta_bugs_count('RC2'),
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

def is_milestone_present(new_db_host, new_db_user, new_db_password, new_db_name, milestone_version):
    global new_connection
    new_connection = None  # Initialize the connection to None

    try:
        # Connect to the new database
        new_connection = pymysql.connect(host=new_db_host, user=new_db_user, password=new_db_password, db=new_db_name)
        logging.info("Connected to the new database successfully.")

        with new_connection.cursor() as new_cursor:
            # Check if the milestone version already exists in the table
            query = "SELECT COUNT(*) FROM RealTimeData WHERE mileStone_Version = %s"
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

def insert_status_counts():
    connection = None
    new_connection = None
    try:
        logging.info("Connecting to the old database.")
        connection = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name)

        logging.info("Connecting to the new database.")
        new_connection = pymysql.connect(host=new_db_host, user=new_db_user, password=new_db_password, db=new_db_name)

        logging.info("Executing SQL query on old database.")
        cursor = connection.cursor()
        cursor.execute("""
            SELECT q_build, status, COUNT(*) AS count 
            FROM report_view 
            WHERE q_role_name = 'RealTime' 
                AND status IN ('pass', 'fail') 
                AND q_build IN ('beta1', 'RC1', 'RC2', 'GMC')
            GROUP BY q_build, status;
        """)
        rows = cursor.fetchall()
        build_data = {}

        logging.info("Processing query results.")
        for row in rows:
            q_build, status, count = row
            if q_build not in build_data:
                build_data[q_build] = {'pass': 0, 'fail': 0}
            build_data[q_build][status.lower()] = count

        logging.info("Fetching bug counts from Confluence.")
        bug_counts = get_bugs_count(confluence_username,confluence_password)
        new_cursor = new_connection.cursor()

        for build, counts in build_data.items():
            no_tests_pass = counts.get('pass', 0)
            no_tests_fail = counts.get('fail', 0)
            no_tests_total = no_tests_pass + no_tests_fail
            no_tests_bug = bug_counts.get(build.lower(), 0)
            mileStone_Version = build
            execution_date = datetime.now()
            
            logging.info(f"Checking if milestone version {mileStone_Version} is present.")
            if is_milestone_present(new_db_host, new_db_user, new_db_password, new_db_name, mileStone_Version):
                logging.warning(f"Milestone version {mileStone_Version} already present. Skipping insertion.")
                continue
            
            logging.info(f"Inserting data for milestone version {mileStone_Version}.")
            new_cursor.execute("""
                INSERT INTO RealTimeData(no_tests_total, no_tests_pass, no_tests_fail, no_tests_bug, mileStone_Version, execution_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (no_tests_total, no_tests_pass, no_tests_fail, no_tests_bug, mileStone_Version, execution_date))

        logging.info("Committing transaction to the new database.")
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
