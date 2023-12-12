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
    confluence_url     = sle_config_data.get('alp_confluence_url', '')
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
                'build2.1': get_beta_bugs_count('Build2.1'),
                'build2.4': get_beta_bugs_count('Build2.4'),
                'build4.1': get_beta_bugs_count('Build4.1'),
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
            query = "SELECT COUNT(*) FROM ALPData WHERE mileStone_Version = %s"
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

        releases_and_builds = [
            {'release': 'ALP_Micro', 'builds': ['Build4.1']},
            {'release': 'ALP_Dolomite1.0', 'builds': ['Build2.1', 'Build2.4']}
        ]

        logging.info("Executing SQL query on old database.")
        cursor = connection.cursor()
        new_cursor = new_connection.cursor()

        build_data = {}
        
        for item in releases_and_builds:
            q_release = item['release']
            q_builds = item['builds']

            for q_build in q_builds:
                cursor.execute("""
                    SELECT status, COUNT(*) AS count 
                    FROM report_view 
                    WHERE q_role_name = 'ALP' 
                        AND status IN ('pass', 'fail') 
                        AND q_release = %s
                        AND q_build = %s
                    GROUP BY status;
                """, [q_release, q_build])

                rows = cursor.fetchall()

                for row in rows:
                    status, count = row
                    if q_build not in build_data:
                        build_data[q_build] = {'pass': 0, 'fail': 0}
                    build_data[q_build][status.lower()] = count
        
        logging.info("Fetching bug counts from Confluence.")
        bug_counts = get_bugs_count(confluence_username,confluence_password)

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
                INSERT INTO ALPData(no_tests_total, no_tests_pass, no_tests_fail, no_tests_bug, mileStone_Version, execution_date)
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
