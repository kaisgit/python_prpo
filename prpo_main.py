#!/bin/env python3
import json
import pymysql.cursors
import csv
import datetime
from datetime import datetime
from pytz import timezone
from collections import defaultdict
#import pandas as pd
import sys
import logging
from logging.handlers import RotatingFileHandler
import configparser
import pathlib
import itertools
from pprint import pprint
from operator import itemgetter
import time
import gnupg
import io
import os
import socket

import smtplib
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

####################################################################################################
# prpo_main.py
####################################################################################################

######### CONFIGS
config_dir = '/etc/prpo/'
config_file = config_dir + 'config.ini'
try:
    file = open(config_file)
    config = configparser.ConfigParser()
    config.read(config_file)
    try:
        pr_data_path = config['DATA']['pr_data_path']
        po_data_path = config['DATA']['po_data_path']
        prpo_log_file = config['LOG']['prpo_log_file']
        loglevel = config['LOG_HANDLER_LEVEL']['loglevel']
        from_addr = config['EMAIL']['from']
        to_addr = config['EMAIL']['to']
        reply_to = config['EMAIL']['replyto']
        from_addr_err = config['EMAIL_ON_ERRORS_UPON_EXIT']['from']
        to_addr_err = config['EMAIL_ON_ERRORS_UPON_EXIT']['to']
        reply_to_err = config['EMAIL_ON_ERRORS_UPON_EXIT']['replyto']

        try:
            pathlib.Path(pr_data_path).mkdir(exist_ok=True)
            pathlib.Path(po_data_path).mkdir(exist_ok=True)
            try:
                logging.basicConfig(filename=prpo_log_file, format='%(asctime)s %(levelname)-10s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
                logging.getLogger().setLevel(loglevel)
                msginfo = 'Configuration settings read successfully...'
                logging.info(msginfo)
            except IOError as ioerr:
                #msginfo = '*ERROR* Unable to write to log file. Please check permissions. ' + str(ioerr)
                #logging.error(msginfo)
                sys.exit(1)
        except Exception as dir_e:
            #msginfo = '*ERROR* Unable to create directory ' + str(dir_e)
            #logging.error(msginfo)
            sys.exit(1)
    except Exception as e:
        #msginfo = '*ERROR*\n' + str(e) + ' doesnot exist or is not set.\n'
        #logging.error(msginfo)
        sys.exit(1)
except Exception as err:
    #msginfo = '*ERROR* File or directory not found!\n' + str(err)
    #logging.error(msginfo)
    sys.exit(1)


######### GPG CREDENTIALS
msginfo = 'Reading environment GPG credentials...'
logging.info(msginfo)

try:
    gpg_pass = {}
    gpg_pass['PR']  = os.environ['PRPO_GPG_PR']
    gpg_pass['PO']  = os.environ['PRPO_GPG_PO']
except Exception as ospgperr:
    errmsg = '*ERROR* Environment variable ' + str(ospgperr) + ' is not defined.\n'
    logging.critical(errmsg)
    sys.exit(1)

######### DATABASE CONFIGS
msginfo = 'Reading environment DB credentials...\n'
logging.info(msginfo)

try:
    db_host         = os.environ['PRPO_DB_HOST']
    db_user         = os.environ['PRPO_DB_USERNAME']
    db_password     = os.environ['PRPO_DB_PASSWORD']
    db_name         = os.environ['PRPO_DB_NAME']
    db_host2        = os.environ['PRPO2_DB_HOST']
    db_user2         = os.environ['PRPO2_DB_USERNAME']
    db_password2     = os.environ['PRPO2_DB_PASSWORD']
    db_name2         = os.environ['PRPO2_DB_NAME']
except Exception as osdberr:
    errmsg = '*ERROR* Environment variable ' + str(osdberr) + ' is not defined.\n'
    logging.critical(errmsg)
    sys.exit(1)

####################################################################################################
# Setting database configuration
####################################################################################################
try:
    conn = pymysql.connect(host=db_host,
                       user=db_user,
                       passwd=db_password,
                       db=db_name,
                       charset='latin1',  # utf8mb4
                       cursorclass=pymysql.cursors.DictCursor)

    conn2 = pymysql.connect(host=db_host2,
                       user=db_user2,
                       passwd=db_password2,
                       db=db_name2,
                       charset='latin1',  # utf8mb4
                       cursorclass=pymysql.cursors.DictCursor)
except Exception as dberr:
    errmsg = '*DATABASE CONNECTION ERROR* Unable to connect to the database...Please check the connection settings.\n' + str(dberr)
    logging.critical(errmsg)
    sys.exit(1)


def processing_PR(conn, conn2, input_text_datafile, file_name):
    log, products, sites, current_invalid_PR = {}, {}, {}, []
    current_raw_PR_IDs, current_PR_IDs = [], []
    sitecode_not_found = aqid_not_found = equipment_id = equipment_db_id = ''
    total_no_recs = raw_rows_inserted = raw_rows_updated = rows_updated_with_rectype_c = recs_with_null_aqid = 0
    rows_inserted = rows_updated = rows_deleted = 0
    invalid_prod_sites_rows_inserted = invalid_prod_sites_rows_updated = 0
    invalid_kpr_rel_rows_inserted = invalid_kpr_rel_rows_updated  = 0
    valid_prod_sites_and_kpr_rel_deleted = 0

    logging.info('Begin PR processing at ' + datetime.now().strftime('%Y-%m-%d %X'))
    log['loaded_date_time'] = datetime.now().strftime("%Y-%m-%d %X")

    db_col_mapping = { 'pr_nbr': 'PURCHASE_REQUISITION_NBR',
                       'pr_line_nbr': 'PURCH_REQ_LINE_NBR',
                       'pr_status_desc': 'PR_STATUS_DESCRIPTION',
                       'company_code': 'COMPANY_CODE',
                       'currency_code': 'CURRENCY_CODE',
                       'request_date': 'REQUEST_DATE',
                       'material_description': 'MATERIAL_DESCRIPTION',
                       'total_amount': 'TOTAL_AMOUNT',
                       'price_per_unit': 'PRICE_PER_UNIT',
                       'quantity': 'QUANTITY',
                       'model_number': 'MODEL_NUMBER',
                       'aq_id': 'AQ_ID',
                       'project_nbr': 'PROJECT_NBR',
                       'spend_type_code': 'SPEND_TYPE_CODE',
                       'functional_location_code': 'FUNCTIONAL_LOCATION_CODE',
                       'spend_type_description': 'SPEND_TYPE_DESCRIPTION',
                       'functional_location_description': 'FUNCTIONAL_LOCATION_DESCRIPTION',
                       'destination': 'DESTINATION',
                       'vendor_number': 'VENDOR_NUMBER',
                       'vendor_name': 'VENDOR_NAME',
                       'initiator': 'INITIATOR',
                       'cost_center_nbr': 'COST_CENTER_NBR',
                       'record_type': 'RECORD_TYPE' }

    ####################################################################################################
    # Logging.
    # Exception occured: must be str, not PosixPath. (cast filename to str())
    logging.info('Opening datafile: /var/pr/data/' + str(file_name))
    log['file_name'] = pr_data_path + '/data/' + str(file_name)
    log['decrypted_data_loc'] = pr_data_path + '/tmp/'
    logging.info('Reading datafile.' + '\n')

    ##################################################################################
    # strip() to remove all leading or trailing blank spaces before the fieldnames from the datafiles.
    # Console: Exception occured: name 'col' is not defined. Mismatched column names.
    ##################################################################################
    headers = input_text_datafile.fieldnames
    if not headers:
        logging.error('Datafile is empty. Please check the datafile and try again.')
        open(pr_data_path + '/status/' + str(file_name) + '.processed', 'a').close()
        return
    else:
        headers = [x.strip(' ') for x in headers]
        for k,v in db_col_mapping.items():
            if v not in headers:
                logging.error(v + ' Headers do not match. Please check the datafile.')
                sys.exit(1)

    logging.info('Getting products from gh_product_codes...')
    products = get_product_id(conn)

    logging.info('Getting site_id from gh_sites...')
    sites = get_site_id(conn)

    logging.info('Getting valid products and sites...')
    valid_product_sites = check_valid_product_and_site(conn)

    logging.info('Getting AQ_ID with no KPR Release...')
    invalid_aqid_kpr_release = check_invalid_aqid_with_kpr_release(conn)

    ####################################################################################################
    logging.info('(RAW) Getting current pr_nbr, pr_line_nbr from conveyor gh_eapproval_pr_line_item...')
    ####################################################################################################
    try:
        with conn2.cursor() as cursor:
            sql = "SELECT concat(pr_nbr, ':', pr_line_nbr) as pr_id FROM gh_eapproval_pr_line_item"
            cursor.execute(sql)
            result = cursor.fetchall()

            for row in result:
                current_raw_PR_IDs.append(row['pr_id'])
    except Exception as err:
        errmsg = "*(RAW) ERROR* %s while getting current records from conveyor gh_eapproval_pr_line_item...", err
        logging.critical(errmsg)
        raise

    ####################################################################################################
    logging.info('Getting pr_nbr, pr_line_nbr from gh_eapproval_pr_line_item...')
    ####################################################################################################
    try:
        with conn.cursor() as cursor:
            sql = "SELECT concat(pr_nbr, ':', pr_line_nbr) as pr_id FROM gh_eapproval_pr_line_item"
            cursor.execute(sql)
            result = cursor.fetchall()
    
            for row in result:
                current_PR_IDs.append(row['pr_id'])
    
            ####################################################################################################
            # Getting current rec from gh_pr_invalid_data
            logging.info('Getting current invalid recs from gh_pr_invalid_data...')
            ####################################################################################################
            sql = "SELECT pr_nbr, pr_line_nbr, aq_id, gh_product_id, gh_site_id FROM gh_pr_invalid_data"
            cursor.execute(sql)
            result = cursor.fetchall()
    
            for row in result:
                current_invalid_PR.append({'pr_nbr': row['pr_nbr'],
                                           'pr_line_nbr': row['pr_line_nbr']})

        ####################################################################################################
        # Defining SQLs
        ####################################################################################################
        new_pr_INSERT_SQL = """
            INSERT INTO gh_eapproval_pr_line_item (pr_nbr, pr_line_nbr, pr_status_desc, company_code, currency_code, request_date, material_description, total_amount, price_per_unit, quantity, model_number, aq_id, project_nbr, spend_type_code, functional_location_code, spend_type_description, functional_location_description, destination, vendor_number, vendor_name, initiator, cost_center_nbr, record_type, gh_product_id, gh_site_id, gh_equipment_id, gh_equipment_db_id, created)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        new_raw_pr_INSERT_SQL = """
            INSERT INTO gh_eapproval_pr_line_item (pr_nbr, pr_line_nbr, pr_status_desc, company_code, currency_code, request_date, material_description, total_amount, price_per_unit, quantity, model_number, aq_id, project_nbr, spend_type_code, functional_location_code, spend_type_description, functional_location_description, destination, vendor_number, vendor_name, initiator, cost_center_nbr, record_type, gh_product_id, gh_site_id, gh_equipment_id, gh_equipment_db_id, gh_product_name, gh_site, created)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        pr_UPDATE_SQL = """
            UPDATE gh_eapproval_pr_line_item
            SET
                pr_status_desc = %s,
                company_code = %s,
                currency_code = %s,
                request_date = %s,
                material_description = %s,
                total_amount = %s,
                price_per_unit = %s,
                quantity = %s,
                model_number = %s,
                aq_id = %s,
                project_nbr = %s,
                spend_type_code = %s,
                functional_location_code = %s,
                spend_type_description = %s,
                functional_location_description = %s,
                destination = %s,
                vendor_number = %s,
                vendor_name = %s,
                initiator = %s,
                cost_center_nbr = %s,
                record_type = %s,
                gh_product_id = %s,
                gh_site_id = %s,
                gh_equipment_id = %s,
                gh_equipment_db_id = %s,
                updated = %s,
                status_id = %s
             WHERE
                pr_nbr = %s
                AND pr_line_nbr = %s"""

        pr_raw_UPDATE_SQL = """
            UPDATE gh_eapproval_pr_line_item
            SET
                pr_status_desc = %s,
                company_code = %s,
                currency_code = %s,
                request_date = %s,
                material_description = %s,
                total_amount = %s,
                price_per_unit = %s,
                quantity = %s,
                model_number = %s,
                aq_id = %s,
                project_nbr = %s,
                spend_type_code = %s,
                functional_location_code = %s,
                spend_type_description = %s,
                functional_location_description = %s,
                destination = %s,
                vendor_number = %s,
                vendor_name = %s,
                initiator = %s,
                cost_center_nbr = %s,
                record_type = %s,
                gh_product_id = %s,
                gh_site_id = %s,
                gh_equipment_id = %s,
                gh_equipment_db_id = %s,
                gh_product_name = %s,
                gh_site = %s,
                updated = %s
             WHERE
                pr_nbr = %s
                AND pr_line_nbr = %s"""
    
        filename_by_pr_po_rec = """
            INSERT INTO gh_pr_po_file (pr_nbr, pr_line_nbr, file_name, created)
            VALUES (%s, %s, %s, %s)"""

        pr_status_UPDATE_SQL = """
            UPDATE gh_eapproval_pr_line_item
            SET
                record_type = %s, updated = %s, status_id = %s
            WHERE 
                pr_nbr = %s
                AND pr_line_nbr = %s"""

        pr_invalid_product_site_aqid_INSERT_SQL = """
            INSERT INTO gh_pr_invalid_data (pr_nbr, pr_line_nbr, aq_id, gh_product_id, gh_site_id, gh_equipment_id, gh_equipment_db_id, category, description, updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        pr_invalid_product_site_aqid_UPDATE_SQL = """
            UPDATE gh_pr_invalid_data
            SET
                aq_id = %s,
                gh_product_id = %s,
                gh_site_id = %s,
                gh_equipment_id = %s,
                gh_equipment_db_id = %s,
                description = %s,
                status_id = %s,
                updated = %s
            WHERE
                pr_nbr = %s
                and pr_line_nbr = %s"""

        pr_invalid_product_site_aqid_DELETE_SQL = """
            UPDATE gh_pr_invalid_data
            SET
                updated = %s, status_id = %s
            WHERE
                pr_nbr = %s
                and pr_line_nbr = %s"""

        ###########################################################
        # Looping thru each record from the main datafile
        # date format from datafile is: yyyy-mm-dd, any other format will cause this conversion to fail.
        # datetime.strptime(row['PO_DATE'], '%m/%d/%y').strftime('%Y-%m-%d')
        # datetime.now().strftime("%Y-%m-%d %X")	# the %X is shorthand for 00:00:00
        ###########################################################
        now = datetime.now().strftime("%Y-%m-%d %X")
        #now = datetime.now().strftime("%Y-%m-%d 00:00:00")
        spend_type_desc = ['EQ','RETRO','AOU']

        for row in input_text_datafile:
            product_sites = {}
            record_not_matched_flag = ''
            total_no_recs += 1
            pr_id = row['PURCHASE_REQUISITION_NBR'] + ':' + row['PURCH_REQ_LINE_NBR']

            ###########################################################
            product_id = product_name = None
            gh_product = products.get(row['PROJECT_NBR'])
            if gh_product:
                product_id = gh_product['product_id']
                product_name = gh_product['product_name']

            ###########################################################
            site_id = site = None
            gh_site = sites.get(row['FUNCTIONAL_LOCATION_CODE'])
            if gh_site:
                site_id = gh_site['site_id']
                site = gh_site.get('site')		# for populating gh_site in raw table and in email report.
            else:
                if (row['FUNCTIONAL_LOCATION_CODE'] == 'LOCN-TBD1'):
                    site_id = 0
                elif (row['FUNCTIONAL_LOCATION_CODE'] == ''):
                    sitecode_not_found += row['FUNCTIONAL_LOCATION_CODE'] + " (" + row['PURCHASE_REQUISITION_NBR'] + '-' + row['PURCH_REQ_LINE_NBR'] + ")\n"
                else:
                    sitecode_not_found += row['FUNCTIONAL_LOCATION_CODE'] + " (" + row['PURCHASE_REQUISITION_NBR'] + '-' + row['PURCH_REQ_LINE_NBR'] + ")\n"

            #print('(PR) FUNCTIONAL_LOCATION_CODE: ', row['FUNCTIONAL_LOCATION_CODE'], ' , site_id: ', site_id, ', site: ', site)

            ###########################################################
            equipment_id = equipment_db_id = None
            if row['AQ_ID']:
                aq_id = (row['AQ_ID'] + "-R") if (row['SPEND_TYPE_DESCRIPTION'] == 'RETRO' and row['AQ_ID'][-2:] != '-R') else row['AQ_ID']

                aqid_equipment = get_equipment(conn, parse_aqid(aq_id))

                if (aqid_equipment):
                    equipment_id = aqid_equipment[0]
                    equipment_db_id = aqid_equipment[1]
            else:
                recs_with_null_aqid += 1

            ####################################################################################################
            # BEGIN save full PR and PO data to Tableau DB for data validation
            # ERRORS: KeyError: 'T205380101'
            #         During handling of the above exception, another exception occurred:
            #         TypeError: not all arguments converted during string formatting
            # SOL: row['PROJECT_NBR'] doesnot exist in products{} , set to default valiue 'None'
            ####################################################################################################
            if conn2.open:
                try:
                    with conn2.cursor() as cursor:
                        if ( (row['RECORD_TYPE'] == 'I') or (row['RECORD_TYPE'] == 'U') or (row['RECORD_TYPE'] == 'D') ):
                            data = (row['PURCHASE_REQUISITION_NBR'], row['PURCH_REQ_LINE_NBR'], file_name, now)
                            cursor.execute(filename_by_pr_po_rec, data)
                            conn2.commit()

                            if pr_id not in current_raw_PR_IDs:
                                data = (row['PURCHASE_REQUISITION_NBR'],
                                        row['PURCH_REQ_LINE_NBR'],
                                        row['PR_STATUS_DESCRIPTION'],
                                        row['COMPANY_CODE'],
                                        row['CURRENCY_CODE'],
                                        row['REQUEST_DATE'],
                                        row['MATERIAL_DESCRIPTION'],
                                        row['TOTAL_AMOUNT'],
                                        row['PRICE_PER_UNIT'],
                                        row['QUANTITY'],
                                        row['MODEL_NUMBER'],
                                        row['AQ_ID'],
                                        row['PROJECT_NBR'],
                                        row['SPEND_TYPE_CODE'],
                                        row['FUNCTIONAL_LOCATION_CODE'],
                                        row['SPEND_TYPE_DESCRIPTION'],
                                        row['FUNCTIONAL_LOCATION_DESCRIPTION'],
                                        row['DESTINATION'],
                                        row['VENDOR_NUMBER'],
                                        row['VENDOR_NAME'],
                                        row['INITIATOR'],
                                        row['COST_CENTER_NBR'],
                                        row['RECORD_TYPE'],
                                        product_id,
                                        site_id,
                                        equipment_id,
                                        equipment_db_id,
                                        product_name,
                                        site,
                                        now)
                                cursor.execute(new_raw_pr_INSERT_SQL, data)
                                raw_rows_inserted += cursor.rowcount
                                conn2.commit()
                            else:
                                data = (row['PR_STATUS_DESCRIPTION'],
                                        row['COMPANY_CODE'],
                                        row['CURRENCY_CODE'],
                                        row['REQUEST_DATE'],
                                        row['MATERIAL_DESCRIPTION'],
                                        row['TOTAL_AMOUNT'],
                                        row['PRICE_PER_UNIT'],
                                        row['QUANTITY'],
                                        row['MODEL_NUMBER'],
                                        row['AQ_ID'],
                                        row['PROJECT_NBR'],
                                        row['SPEND_TYPE_CODE'],
                                        row['FUNCTIONAL_LOCATION_CODE'],
                                        row['SPEND_TYPE_DESCRIPTION'],
                                        row['FUNCTIONAL_LOCATION_DESCRIPTION'],
                                        row['DESTINATION'],
                                        row['VENDOR_NUMBER'],
                                        row['VENDOR_NAME'],
                                        row['INITIATOR'],
                                        row['COST_CENTER_NBR'],
                                        row['RECORD_TYPE'],
                                        product_id,
                                        site_id,
                                        equipment_id,
                                        equipment_db_id,
                                        product_name,
                                        site,
                                        now,
                                        row['PURCHASE_REQUISITION_NBR'],
                                        row['PURCH_REQ_LINE_NBR'])
                                cursor.execute(pr_raw_UPDATE_SQL, data)
                                raw_rows_updated += cursor.rowcount
                                conn2.commit()
                except Exception as err:
                    logging.critical('*ERROR* while inserting or updating raw PR data to conveyor database. ', err)
                    send_email_notification_on_errors()
                    raise
            else:
                conn2.ping(reconnect=True)

            ####################################################################################################
            # Main Logic
            ####################################################################################################
            if product_id:
                if row['SPEND_TYPE_DESCRIPTION'] in spend_type_desc:
                    if site_id != None:
                        if (equipment_id and equipment_db_id):
                            try:
                                with conn.cursor() as cursor:
                                    ####################################################################################################
                                    # The record types:
                                    #	“I” for new PR
                                    #	“U” for changed PR (OLD. No longer used.)
                                    #	“D” for deleted/cancelled PR.
                                    #
                                    # * For 'I' and 'U', insert new record into database, if a record exists in database, then update.
                                    #   By default, status_id is set to '1' for newly inserted record
                                    #   For 'U', after an update, leave status_id as is
                                    #   For 'D', after marked as delete, update status_id to 0
                                    # 
                                    # ERROR: Exception occured: not all arguments converted during string formatting
                                    # SOLUTION: SOL: check for correct number of arguments %s.
                                    # ERROR:  Warning: (1264, "Out of range value for column 'xxxx_date' at row 1")
                                    # SOLUTION: incorrect date format. Must be yyyy-mm-dd
                                    ####################################################################################################
                                    if (row['RECORD_TYPE'] == 'I') or (row['RECORD_TYPE'] == 'U'):
                                        ####################################################################################################
                                        # Checking if the record in the datafile is already in the database.
                                        ####################################################################################################
                                        if pr_id not in current_PR_IDs:
                                            data = (row['PURCHASE_REQUISITION_NBR'],
                                                    row['PURCH_REQ_LINE_NBR'],
                                                    row['PR_STATUS_DESCRIPTION'],
                                                    row['COMPANY_CODE'],
                                                    row['CURRENCY_CODE'],
                                                    row['REQUEST_DATE'],
                                                    row['MATERIAL_DESCRIPTION'],
                                                    row['TOTAL_AMOUNT'],
                                                    row['PRICE_PER_UNIT'],
                                                    row['QUANTITY'],
                                                    row['MODEL_NUMBER'],
                                                    row['AQ_ID'],
                                                    row['PROJECT_NBR'],
                                                    row['SPEND_TYPE_CODE'],
                                                    row['FUNCTIONAL_LOCATION_CODE'],
                                                    row['SPEND_TYPE_DESCRIPTION'],
                                                    row['FUNCTIONAL_LOCATION_DESCRIPTION'],
                                                    row['DESTINATION'],
                                                    row['VENDOR_NUMBER'],
                                                    row['VENDOR_NAME'],
                                                    row['INITIATOR'],
                                                    row['COST_CENTER_NBR'],
                                                    row['RECORD_TYPE'],
                                                    product_id,
                                                    site_id,
                                                    equipment_id,
                                                    equipment_db_id,
                                                    now)
                                            cursor.execute(new_pr_INSERT_SQL, data)
                                            rows_inserted += cursor.rowcount
                                            conn.commit()
                                        else:
                                            data = (row['PR_STATUS_DESCRIPTION'],
                                                    row['COMPANY_CODE'],
                                                    row['CURRENCY_CODE'],
                                                    row['REQUEST_DATE'],
                                                    row['MATERIAL_DESCRIPTION'],
                                                    row['TOTAL_AMOUNT'],
                                                    row['PRICE_PER_UNIT'],
                                                    row['QUANTITY'],
                                                    row['MODEL_NUMBER'],
                                                    row['AQ_ID'],
                                                    row['PROJECT_NBR'],
                                                    row['SPEND_TYPE_CODE'],
                                                    row['FUNCTIONAL_LOCATION_CODE'],
                                                    row['SPEND_TYPE_DESCRIPTION'],
                                                    row['FUNCTIONAL_LOCATION_DESCRIPTION'],
                                                    row['DESTINATION'],
                                                    row['VENDOR_NUMBER'],
                                                    row['VENDOR_NAME'],
                                                    row['INITIATOR'],
                                                    row['COST_CENTER_NBR'],
                                                    row['RECORD_TYPE'],
                                                    product_id,
                                                    site_id,
                                                    equipment_id,
                                                    equipment_db_id,
                                                    now,
                                                    1,
                                                    row['PURCHASE_REQUISITION_NBR'],
                                                    row['PURCH_REQ_LINE_NBR'])
                                            cursor.execute(pr_UPDATE_SQL, data)
                                            rows_updated += cursor.rowcount
                                            conn.commit()
                                    elif (row['RECORD_TYPE'] == 'D'):
                                        data = (row['RECORD_TYPE'], now, 0, row['PURCHASE_REQUISITION_NBR'], row['PURCH_REQ_LINE_NBR'])
                                        cursor.execute(pr_status_UPDATE_SQL, data)
                                        rows_deleted += cursor.rowcount
                                        conn.commit()

                                    ####################################################################
                                    # BEGIN checking for invalid records
                                    ####################################################################
                                    product_sites = {'product_id': product_id,
                                                     'site_id': site_id}

                                    aqid_kpr_release = {'product_id': product_id,
                                                        'site_id': site_id,
                                                        'equipment_id': equipment_id,
                                                        'equipment_db_id': equipment_db_id}

                                    # cast current_pr.pr_line_nbr to integer because current_invalid_PR.pr_line_nbr is integer.
                                    current_pr = {'pr_nbr': row['PURCHASE_REQUISITION_NBR'],
                                                  'pr_line_nbr': int(row['PURCH_REQ_LINE_NBR'])}

                                    ####################################################################
                                    # Is Functional Location a valid site for a given product?
                                    # if not valid, then mark as invalid product site.
                                    ####################################################################
                                    if product_sites not in valid_product_sites:
                                        category = 'Invalid product and site'
                                        description = row['PROJECT_NBR'] + " and " + row['FUNCTIONAL_LOCATION_CODE'] + " are not valid MP site in ghDS."
                                        send_email_invalid_pr_recs.append([
                                            row['PURCHASE_REQUISITION_NBR'],
                                            row['PURCH_REQ_LINE_NBR'],
                                            '',
                                            '',
                                            aq_id,
                                            product_name,
                                            site,
                                            category,
                                            description])

                                        if current_pr in current_invalid_PR:
                                            #print('UPDATE: ', current_pr)
                                            data = (aq_id,
                                                    product_id,
                                                    site_id,
                                                    equipment_id,
                                                    equipment_db_id,
                                                    description,
                                                    '1',
                                                    now,
                                                    row['PURCHASE_REQUISITION_NBR'],
                                                    row['PURCH_REQ_LINE_NBR'])
                                            cursor.execute(pr_invalid_product_site_aqid_UPDATE_SQL, data)
                                            invalid_prod_sites_rows_updated += cursor.rowcount
                                            conn.commit()
                                        else:
                                            #print('INSERT: ', current_pr)
                                            data = (row['PURCHASE_REQUISITION_NBR'],
                                                    row['PURCH_REQ_LINE_NBR'],
                                                    aq_id,
                                                    product_id,
                                                    site_id,
                                                    equipment_id,
                                                    equipment_db_id,
                                                    category,
                                                    description,
                                                    now)
                                            cursor.execute(pr_invalid_product_site_aqid_INSERT_SQL, data)
                                            invalid_prod_sites_rows_inserted += cursor.rowcount
                                            conn.commit()
                                    elif aqid_kpr_release in invalid_aqid_kpr_release:
                                        ####################################################################
                                        # Does AQID have KPR release? AQID has KPR release if (qty > 0)
                                        ####################################################################
                                            category = 'No KPR release for AQ_ID'
                                            description = 'No KPR release for: ' + str(aq_id)
                                            send_email_invalid_pr_recs.append([
                                                row['PURCHASE_REQUISITION_NBR'],
                                                row['PURCH_REQ_LINE_NBR'],
                                                '',
                                                '',
                                                aq_id,
                                                product_name,
                                                site,
                                                category,
                                                description])

                                            if current_pr in current_invalid_PR:
                                                #print('UPDATE: ', current_pr)
                                                data = (aq_id,
                                                        product_id,
                                                        site_id,
                                                        equipment_id,
                                                        equipment_db_id,
                                                        description,
                                                        '1',
                                                        now,
                                                        row['PURCHASE_REQUISITION_NBR'],
                                                        row['PURCH_REQ_LINE_NBR'])
                                                cursor.execute(pr_invalid_product_site_aqid_UPDATE_SQL, data)
                                                invalid_kpr_rel_rows_updated += cursor.rowcount
                                                conn.commit()
                                            else:
                                                #print('INSERT: ', current_pr)
                                                data = (row['PURCHASE_REQUISITION_NBR'],
                                                       row['PURCH_REQ_LINE_NBR'],
                                                       aq_id,
                                                       product_id,
                                                       site_id,
                                                       equipment_id,
                                                       equipment_db_id,
                                                       category,
                                                       description,
                                                       now)
                                                cursor.execute(pr_invalid_product_site_aqid_INSERT_SQL, data)
                                                invalid_kpr_rel_rows_inserted += cursor.rowcount
                                                conn.commit()
                                    else:
                                        if current_pr in current_invalid_PR:
                                            #print('TO DELETE: ', current_pr)
                                            data = (now,
                                                    0,
                                                    row['PURCHASE_REQUISITION_NBR'],
                                                    row['PURCH_REQ_LINE_NBR'])
                                            cursor.execute(pr_invalid_product_site_aqid_DELETE_SQL, data)
                                            valid_prod_sites_and_kpr_rel_deleted += cursor.rowcount
                                            conn.commit()
                            except Exception as sqlprerr:
                                # Write to the /status/.error and log files.
                                erfile = open(pr_data_path + '/status/' + file_name + '.error', 'a')
                                erfile.write('*PR SQL ERROR* Got error {!r}, errno is {}\n'.format(sqlprerr, sqlprerr.args[0]))

                                logging.critical('*PR SQL ERROR* Got error {!r}, errno is {}'.format(sqlprerr, sqlprerr.args[0]))
                                raise
                        else:
                            # AQ_ID
                            # logs AQ_ID that is not found in ghDS. Null/empty AQ_ID not print to log.
                            if (row['AQ_ID']):
                                logging.info('aq_id: ' + aq_id + ' (' + row['PURCHASE_REQUISITION_NBR'] + '-' + row['PURCH_REQ_LINE_NBR'] + ') not found. Unable to map.')
                                aqid_not_found += aq_id + " (" + row['PURCHASE_REQUISITION_NBR'] + '-' + row['PURCH_REQ_LINE_NBR'] + ")\n"
                            record_not_matched_flag = 1
                    else:
                        # FUNCTIONAL_LOCATION_CODE
                        record_not_matched_flag = 1
                else:
                    # SPEND_TYPE_DESCRIPTION
                    record_not_matched_flag = 1
            else:
                # PROJECT_NBR
                record_not_matched_flag = 1

            if (record_not_matched_flag):
                if pr_id in current_PR_IDs:
                    try:
                        with conn.cursor() as cursor:
                            data = ('C', now, 0, row['PURCHASE_REQUISITION_NBR'], row['PURCH_REQ_LINE_NBR'])
                            cursor.execute(pr_status_UPDATE_SQL, data)
                            rows_updated_with_rectype_c += cursor.rowcount
                            conn.commit()
                    except Exception as sqlerr:
                            # Write to the /status/.error and log files.
                            erfile = open(pr_data_path + '/status/' + file_name + '.error', 'a')
                            erfile.write('*pr_status_UPDATE_SQL ERROR* Got error {!r}, errno is {}\n'.format(sqlprerr, sqlprerr.args[0]))
                            logging.critical('*pr_status_UPDATE_SQL ERROR* Got error {!r}, errno is {}'.format(sqlerr, sqlerr.args[0]))
                            raise


        log['num_recs_loaded'] = total_no_recs
        log['num_new_recs_added'] = rows_inserted
        log['num_recs_with_issues'] = recs_with_null_aqid
        log['num_recs_ignored'] = (total_no_recs - rows_inserted - recs_with_null_aqid)

        if sitecode_not_found:
            logging.info("\nSAP Site Code not found in gh_sites:")
            logging.info(sitecode_not_found)

        logging.info("(RAW) No of PR recs loaded: " + str(total_no_recs))
        logging.info("(RAW) No of PR recs inserted: " + str(raw_rows_inserted))
        logging.info("(RAW) No of PR recs updated: " + str(raw_rows_updated))

        logging.info("(INVALID sites for products) No of recs inserted: " + str(invalid_prod_sites_rows_inserted))
        logging.info("(INVALID sites for products) No of recs updated: " + str(invalid_prod_sites_rows_updated))
        logging.info("(INVALID AQID with no KPR Release) No of recs inserted: " + str(invalid_kpr_rel_rows_inserted))
        logging.info("(INVALID AQID with no KPR Release) No of recs updated: " + str(invalid_kpr_rel_rows_updated))
        logging.info("(VALID) No of recs deleted: " + str(valid_prod_sites_and_kpr_rel_deleted))

        logging.info("No of PR recs loaded: " + str(total_no_recs))
        logging.info("No of PR recs inserted: " + str(rows_inserted))
        logging.info("No of PR recs updated: " + str(rows_updated))
        logging.info("No of PR recs updated to Record Type C: " + str(rows_updated_with_rectype_c))
        logging.info("No of PR recs deleted: " + str(rows_deleted))
        logging.info("No of PR recs with empty aq_id: " + str(recs_with_null_aqid))
        logging.info("No of PR recs ignored: " + str(total_no_recs - rows_inserted - recs_with_null_aqid))
        logging.info('Ending script for PR at ' + datetime.now().strftime("%Y-%m-%d %X"))
        logging.info('===================================================\n\n')

        ####################################################################################################
        # Log status to database.
        # 1. Exception occured: 'PosixPath' object has no attribute 'translate'
        #    log['file_name'] = PosixPath('datafile/PR-Extractor-04-07-2018-004620.txt')
        #       Solution: cast into str()
        # 2. Warning: (1265, "Data truncated for column 'description' at row 1")
        #       Solution: Change text to mediumtext for Description. Only needed for full data load. Change back to text for incremental load.
        ####################################################################################################
        description = "SAP Site Code not found in gh_sites:\n" + sitecode_not_found[:32000] + "\naq_id not found in gh_bom_equipment:\n" + aqid_not_found[:32000]
        try:
            with conn.cursor() as cursor:
                sql = "INSERT INTO gh_pr_po_file_process_log (loaded_date_time, file_name, file_saved_to, num_recs_loaded, num_new_recs_added, num_recs_ignored, num_recs_with_issues, description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                data = (log['loaded_date_time'], log['file_name'], log['decrypted_data_loc'], log['num_recs_loaded'], log['num_new_recs_added'], log['num_recs_ignored'], log['num_recs_with_issues'], description)
                cursor.execute(sql, data)
                conn.commit()

                if (cursor.rowcount):
                    open(pr_data_path + '/status/' + file_name + '.processed', 'a').close()
        except Exception as ex:
            errmsg = "ERROR: %s while inserting into gh_pr_po_file_process_log in processing_PR()." % format(ex)
            logging.critical(errmsg)
            raise

    except Exception as ex:
        errmsg = "Exception occured: %s while running processing_PR(). Returning to main." % format(ex)
        logging.critical(errmsg)
        raise

def processing_PO(conn, conn2, input_text_datafile, file_name):
    log, products, sites, current_invalid_PO = {}, {}, {}, []
    current_raw_PO_IDs, current_PO_IDs = [], []
    sitecode_not_found = aqid_not_found = ""
    total_no_recs = raw_rows_inserted = rows_inserted = rows_updated = raw_rows_updated = rows_deleted = rows_updated_with_rectype_c = recs_with_issues = recs_with_null_aqid = 0
    invalid_prod_sites_rows_inserted = invalid_prod_sites_rows_updated = 0
    invalid_kpr_rel_rows_inserted = invalid_kpr_rel_rows_updated  = 0
    valid_prod_sites_and_kpr_rel_deleted = 0

    logging.info('Begin processing PO at ' + datetime.now().strftime('%Y-%m-%d %X'))
    log['loaded_date_time'] = datetime.now().strftime('%Y-%m-%d %X')

    ####################################################################################################
    # Mapping: Database columns <=> column header from text datafile.
    # Validating column headers of gh_sap_po_line_item table with text datafile.
    ####################################################################################################
    db_col_mapping = {'po_nbr': 'PURCHASE_ORDER_NUMBER',
                      'po_line_nbr': 'PURCH_ORDER_LINE_NBR',
                      'material_description': 'MATERIAL_DESCRIPTION',
                      'company_code': 'COMPANY_CODE',
                      'tracking_no': 'TRACKING_NO',
                      'po_quantity': 'PO_QUANTITY',
                      'net_price': 'NET_PRICE',
                      'net_value': 'NET_VALUE',
                      'effective_value': 'EFFECTIVE_VALUE',
                      'pr_nbr': 'EAPPROVAL_PR_NBR',
                      'pr_line_nbr': 'EAPPROVAL_PR_LINE_NBR',
                      'initiator': 'NAME_OF_INITIATOR',
                      'vendor_name': 'VENDOR_NAME',
                      'functional_location': 'FUNCTIONAL_LOCATION',
                      'project_nbr': 'PROJECT_NBR',
                      'aq_id': 'AQID',
                      'po_status': 'PO_STATUS',
                      'spend_type_code': 'SPEND_TYPE_CODE',
                      'spend_type_description': 'SPEND_TYPE_DESC',
                      'currency': 'CURRENCY',
                      'order_unit': 'ORDER_UNIT',
                      'order_price_unit': 'ORDER_PRICE_UNIT',
                      'gr_quantity': 'GR_QUANTITY',
                      'record_type': 'RECORD_TYPE',
                      'po_date': 'PO_DATE',
                      'po_item_deleted_flag': 'PO_ITEM_DELETED_FLAG' }

    ####################################################################################################
    # Opening and reading text datafile.
    # Exception occured: invalid file: PosixPath('file.txt')
    # Exception occured: must be str, not PosixPath. (cast filename to str())
    logging.info('Opening datafile: /var/po/data/' + str(file_name))
    log['file_name'] = po_data_path + '/data/' + str(file_name)
    log['decrypted_data_loc'] = po_data_path + '/tmp/'
    logging.info('Reading datafile.' + '\n')

    ###########################################################
    # Mapping column headers of text datafile
    # strip() to remove all leading or trailing blank spaces before the fieldnames from the datafiles.
    ###########################################################
    headers = input_text_datafile.fieldnames
    if not headers:
        logging.error('Datafile is empty. Please check the datafile and try again.')
        open(po_data_path + '/status/' + str(file_name) + '.processed', 'a').close()
        return
    else:
        headers = [x.strip(' ') for x in headers]
        for k,v in db_col_mapping.items():
            if v not in headers:
                logging.error(v + ' Headers do not match. Please check the datafile.')
                sys.exit(1)

    logging.info('Getting products from gh_product_codes...')
    products = get_product_id(conn)

    logging.info('Getting site_id from gh_sites...')
    sites = get_site_id(conn)

    logging.info('Getting valid products and sites...')
    valid_product_sites = check_valid_product_and_site(conn)

    logging.info('Getting AQ_ID with no KPR Release...')
    invalid_aqid_kpr_release = check_invalid_aqid_with_kpr_release(conn)

    ####################################################################################################
    logging.info('(RAW) Getting current po_nbr, po_line_nbr from conveyor gh_sap_po_line_item...')
    ####################################################################################################
    try:
        with conn2.cursor() as cursor:
            sql = "SELECT concat(po_nbr, ':', po_line_nbr) as po_id FROM gh_sap_po_line_item"
            cursor.execute(sql)
            result = cursor.fetchall()

            for row in result:
                current_raw_PO_IDs.append(row['po_id'])
    except Exception as err:
        errmsg = "*(RAW) ERROR* %s while getting current records from conveyor gh_sap_po_line_item...", err
        logging.critical(errmsg)
        raise

    ####################################################################################################
    # Getting current PO ids from database.
    logging.info('Getting po_nbr, po_line_nbr from gh_sap_po_line_item...')
    ####################################################################################################
    try:
        with conn.cursor() as cursor:
            sql = "SELECT concat(po_nbr, ':', po_line_nbr) as po_id FROM gh_sap_po_line_item"
            cursor.execute(sql)
            result = cursor.fetchall()

            for row in result:
                current_PO_IDs.append(row['po_id'])

            ####################################################################################################
            # Getting current rec from gh_po_invalid_data
            logging.info('Getting current invalid recs from gh_po_invalid_data...')
            ####################################################################################################
            sql = "SELECT pr_nbr, pr_line_nbr, po_nbr, po_line_nbr, aq_id, gh_product_id, gh_site_id FROM gh_po_invalid_data"
            cursor.execute(sql)
            result = cursor.fetchall()
    
            for row in result:
                current_invalid_PO.append({'po_nbr': row['po_nbr'],
                                           'po_line_nbr': row['po_line_nbr']})

	    ####################################################################################################
	    # Defining sqls
	    ####################################################################################################
        new_po_INSERT_SQL = """
            INSERT INTO gh_sap_po_line_item ( po_nbr, po_line_nbr, material_description, company_code, tracking_no, po_quantity, net_price, net_value, effective_value, pr_nbr, pr_line_nbr, initiator, vendor_name, functional_location, project_nbr, aq_id, po_status, spend_type_code, spend_type_description, currency, order_unit, order_price_unit, gr_quantity, record_type, po_date, po_item_deleted_flag, gh_product_id, gh_site_id, gh_equipment_id, gh_equipment_db_id, created, status_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        new_raw_po_INSERT_SQL = """
            INSERT INTO gh_sap_po_line_item ( po_nbr, po_line_nbr, material_description, company_code, tracking_no, po_quantity, net_price, net_value, effective_value, pr_nbr, pr_line_nbr, initiator, vendor_name, functional_location, project_nbr, aq_id, po_status, spend_type_code, spend_type_description, currency, order_unit, order_price_unit, gr_quantity, record_type, po_date, po_item_deleted_flag, gh_product_id, gh_site_id, gh_equipment_id, gh_equipment_db_id, gh_product_name, gh_site, created, status_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        po_UPDATE_SQL = """
            UPDATE gh_sap_po_line_item
            SET
                 material_description = %s,
                 company_code = %s,
                 tracking_no = %s,
                 po_quantity = %s,
                 net_price = %s,
                 net_value = %s,
                 effective_value = %s,
                 pr_nbr = %s,
                 pr_line_nbr = %s,
                 initiator = %s,
                 vendor_name = %s,
                 functional_location = %s,
                 project_nbr = %s,
                 aq_id = %s,
                 po_status = %s,
                 spend_type_code = %s,
                 spend_type_description = %s,
                 currency = %s,
                 order_unit = %s,
                 order_price_unit = %s,
                 gr_quantity = %s,
                 record_type = %s,
                 po_date = %s,
                 po_item_deleted_flag = %s,
                 gh_product_id = %s,
                 gh_site_id = %s,
                 gh_equipment_id = %s,
                 gh_equipment_db_id = %s,
                 updated = %s,
                 status_id = %s
              WHERE
                 po_nbr = %s
                 AND po_line_nbr = %s"""

        po_raw_UPDATE_SQL = """
            UPDATE gh_sap_po_line_item
            SET
                 material_description = %s,
                 company_code = %s,
                 tracking_no = %s,
                 po_quantity = %s,
                 net_price = %s,
                 net_value = %s,
                 effective_value = %s,
                 pr_nbr = %s,
                 pr_line_nbr = %s,
                 initiator = %s,
                 vendor_name = %s,
                 functional_location = %s,
                 project_nbr = %s,
                 aq_id = %s,
                 po_status = %s,
                 spend_type_code = %s,
                 spend_type_description = %s,
                 currency = %s,
                 order_unit = %s,
                 order_price_unit = %s,
                 gr_quantity = %s,
                 record_type = %s,
                 po_date = %s,
                 po_item_deleted_flag = %s,
                 gh_product_id = %s,
                 gh_site_id = %s,
                 gh_equipment_id = %s,
                 gh_equipment_db_id = %s,
                 gh_product_name = %s,
                 gh_site = %s,
                 updated = %s,
                 status_id = %s
              WHERE
                 po_nbr = %s
                 AND po_line_nbr = %s"""

        filename_by_pr_po_rec = """
            INSERT INTO gh_pr_po_file (po_nbr, po_line_nbr, pr_nbr, pr_line_nbr, file_name, created)
            VALUES (%s, %s, %s, %s, %s, %s)"""

        po_status_UPDATE_SQL = """
             UPDATE gh_sap_po_line_item
             SET
                 record_type = %s, updated = %s, status_id = %s
             WHERE 
                 po_nbr = %s
                 AND po_line_nbr = %s"""

        po_invalid_product_site_aqid_INSERT_SQL = """
            INSERT INTO gh_po_invalid_data (pr_nbr, pr_line_nbr, po_nbr, po_line_nbr, aq_id, gh_product_id, gh_site_id, gh_equipment_id, gh_equipment_db_id, category, description, updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        po_invalid_product_site_aqid_UPDATE_SQL = """
            UPDATE gh_po_invalid_data
            SET
                pr_nbr = %s,
                pr_line_nbr = %s,
                aq_id = %s,
                gh_product_id = %s,
                gh_site_id = %s,
                gh_equipment_id = %s,
                gh_equipment_db_id = %s,
                description = %s,
                status_id = %s,
                updated = %s
            WHERE
                po_nbr = %s
                and po_line_nbr = %s"""

        po_invalid_product_site_aqid_DELETE_SQL = """
            UPDATE gh_po_invalid_data
            SET
                updated = %s, status_id = %s
            WHERE
                po_nbr = %s
                and po_line_nbr = %s"""

        ###########################################################
        # Looping thru each record from the datafile
        ###########################################################
        now = datetime.now().strftime('%Y-%m-%d %X')
        #now = datetime.now().strftime('%Y-%m-%d 00:00:00')
        spend_type_desc = ['EQ','RETRO','AOU']

        for row in input_text_datafile:
            record_not_matched_flag = ''
            total_no_recs += 1
            po_id = row['PURCHASE_ORDER_NUMBER'] + ':' + row['PURCH_ORDER_LINE_NBR']

            ###########################################################
            product_id = product_name = None
            gh_product = products.get(row['PROJECT_NBR'])
            if gh_product:
                product_id = gh_product['product_id']
                product_name = gh_product['product_name']

            ###########################################################
            site_id = site = None
            gh_site = sites.get(row['FUNCTIONAL_LOCATION'])
            if gh_site:
                site_id = gh_site['site_id']
                site = gh_site.get('site')      # for populating gh_site in raw table and in email report.
            else:
                if (row['FUNCTIONAL_LOCATION'] == 'LOCN-TBD1'):
                    site_id = 0
                elif (row['FUNCTIONAL_LOCATION'] == ''):
                    sitecode_not_found += row['FUNCTIONAL_LOCATION'] + " (" + row['PURCHASE_ORDER_NUMBER'] + '-' + row['PURCH_ORDER_LINE_NBR'] + ")\n"
                else:
                    sitecode_not_found += row['FUNCTIONAL_LOCATION'] + " (" + row['PURCHASE_ORDER_NUMBER'] + '-' + row['PURCH_ORDER_LINE_NBR'] + ")\n"

            #print('(PO) FUNCTIONAL_LOCATION: ', row['FUNCTIONAL_LOCATION'], ' , site_id: ', site_id, ', site: ', site)

            ###########################################################
            equipment_id = equipment_db_id = None
            if row['AQID']:
                aq_id = (row['AQID'] + "-R") if (row['SPEND_TYPE_DESC'] == 'RETRO' and row['AQID'][-2:] != '-R') else row['AQID']

                aqid_equipment = get_equipment(conn, parse_aqid(aq_id))
                if (aqid_equipment):
                    equipment_id = aqid_equipment[0]
                    equipment_db_id = aqid_equipment[1]
            else:
                recs_with_null_aqid += 1

            ####################################################################################################
            # Storing RAW PO data in a separate database
            ####################################################################################################
            if conn2.open:
                try:
                    with conn2.cursor() as cursor:
                        if ( (row['RECORD_TYPE'] == 'I') or (row['RECORD_TYPE'] == 'U') or (row['RECORD_TYPE'] == 'D') ):
                            data = (row['PURCHASE_ORDER_NUMBER'], row['PURCH_ORDER_LINE_NBR'], row['EAPPROVAL_PR_NBR'], row['EAPPROVAL_PR_LINE_NBR'], file_name, now)
                            cursor.execute(filename_by_pr_po_rec, data)
                            conn2.commit()

                            if po_id not in current_raw_PO_IDs:
                                data = (row['PURCHASE_ORDER_NUMBER'],
                                        row['PURCH_ORDER_LINE_NBR'],
                                        row['MATERIAL_DESCRIPTION'],
                                        row['COMPANY_CODE'],
                                        row['TRACKING_NO'],
                                        row['PO_QUANTITY'],
                                        row['NET_PRICE'],
                                        row['NET_VALUE'],
                                        row['EFFECTIVE_VALUE'],
                                        row['EAPPROVAL_PR_NBR'],
                                        row['EAPPROVAL_PR_LINE_NBR'],
                                        row['NAME_OF_INITIATOR'],
                                        row['VENDOR_NAME'],
                                        row['FUNCTIONAL_LOCATION'],
                                        row['PROJECT_NBR'],
                                        row['AQID'],
                                        row['PO_STATUS'],
                                        row['SPEND_TYPE_CODE'],
                                        row['SPEND_TYPE_DESC'],
                                        row['CURRENCY'],
                                        row['ORDER_UNIT'],
                                        row['ORDER_PRICE_UNIT'],
                                        row['GR_QUANTITY'],
                                        row['RECORD_TYPE'],
                                        row['PO_DATE'],
                                        row['PO_ITEM_DELETED_FLAG'],
                                        product_id,
                                        site_id,
                                        equipment_id,
                                        equipment_db_id,
                                        product_name,
                                        site,
                                        now,
                                        1)
                                cursor.execute(new_raw_po_INSERT_SQL, data)
                                raw_rows_inserted += cursor.rowcount
                                conn2.commit()
                            else:
                                data = (row['MATERIAL_DESCRIPTION'],
                                        row['COMPANY_CODE'],
                                        row['TRACKING_NO'],
                                        row['PO_QUANTITY'],
                                        row['NET_PRICE'],
                                        row['NET_VALUE'],
                                        row['EFFECTIVE_VALUE'],
                                        row['EAPPROVAL_PR_NBR'],
                                        row['EAPPROVAL_PR_LINE_NBR'],
                                        row['NAME_OF_INITIATOR'],
                                        row['VENDOR_NAME'],
                                        row['FUNCTIONAL_LOCATION'],
                                        row['PROJECT_NBR'],
                                        row['AQID'],
                                        row['PO_STATUS'],
                                        row['SPEND_TYPE_CODE'],
                                        row['SPEND_TYPE_DESC'],
                                        row['CURRENCY'],
                                        row['ORDER_UNIT'],
                                        row['ORDER_PRICE_UNIT'],
                                        row['GR_QUANTITY'],
                                        row['RECORD_TYPE'],
                                        row['PO_DATE'],
                                        row['PO_ITEM_DELETED_FLAG'],
                                        product_id,
                                        site_id,
                                        equipment_id,
                                        equipment_db_id,
                                        product_name,
                                        site,
                                        now,
                                        1,
                                        row['PURCHASE_ORDER_NUMBER'],
                                        row['PURCH_ORDER_LINE_NBR'])
                                cursor.execute(po_raw_UPDATE_SQL, data)
                                raw_rows_updated += cursor.rowcount
                                conn2.commit()
                except Exception as err:
                    logging.critical('*ERROR* while inserting or updating raw PO data to conveyor database. ', err)
                    raise
            else:
                conn2.ping(reconnect=True)

            ####################################################################################################
            # Main Logic
            ####################################################################################################
            if product_id:
                if row['SPEND_TYPE_DESC'] in spend_type_desc:
                    if site_id != None:
                        if (equipment_id and equipment_db_id):
                            try:
                                with conn.cursor() as cursor:
                                    if (row['RECORD_TYPE'] == 'I') or (row['RECORD_TYPE'] == 'U'):
                                        if po_id not in current_PO_IDs:
                                            data = (row['PURCHASE_ORDER_NUMBER'],
                                                    row['PURCH_ORDER_LINE_NBR'],
                                                    row['MATERIAL_DESCRIPTION'],
                                                    row['COMPANY_CODE'],
                                                    row['TRACKING_NO'],
                                                    row['PO_QUANTITY'],
                                                    row['NET_PRICE'],
                                                    row['NET_VALUE'],
                                                    row['EFFECTIVE_VALUE'],
                                                    row['EAPPROVAL_PR_NBR'],
                                                    row['EAPPROVAL_PR_LINE_NBR'],
                                                    row['NAME_OF_INITIATOR'],
                                                    row['VENDOR_NAME'],
                                                    row['FUNCTIONAL_LOCATION'],
                                                    row['PROJECT_NBR'],
                                                    row['AQID'],
                                                    row['PO_STATUS'],
                                                    row['SPEND_TYPE_CODE'],
                                                    row['SPEND_TYPE_DESC'],
                                                    row['CURRENCY'],
                                                    row['ORDER_UNIT'],
                                                    row['ORDER_PRICE_UNIT'],
                                                    row['GR_QUANTITY'],
                                                    row['RECORD_TYPE'],
                                                    row['PO_DATE'],
                                                    row['PO_ITEM_DELETED_FLAG'],
                                                    product_id,
                                                    site_id,
                                                    equipment_id,
                                                    equipment_db_id,
                                                    now,
                                                    1)
                                            cursor.execute(new_po_INSERT_SQL, data)
                                            rows_inserted += cursor.rowcount
                                            conn.commit()
                                        else:
                                            ####################################################################################################
                                            #logger.info('Updating PO rec: ' + row['PURCHASE_ORDER_NUMBER'] + '-' + row['PURCH_ORDER_LINE_NBR'])
                                            # UPDATE records from table.
                                            ####################################################################################################
                                            data = (row['MATERIAL_DESCRIPTION'],
                                                    row['COMPANY_CODE'],
                                                    row['TRACKING_NO'],
                                                    row['PO_QUANTITY'],
                                                    row['NET_PRICE'],
                                                    row['NET_VALUE'],
                                                    row['EFFECTIVE_VALUE'],
                                                    row['EAPPROVAL_PR_NBR'],
                                                    row['EAPPROVAL_PR_LINE_NBR'],
                                                    row['NAME_OF_INITIATOR'],
                                                    row['VENDOR_NAME'],
                                                    row['FUNCTIONAL_LOCATION'],
                                                    row['PROJECT_NBR'],
                                                    row['AQID'],
                                                    row['PO_STATUS'],
                                                    row['SPEND_TYPE_CODE'],
                                                    row['SPEND_TYPE_DESC'],
                                                    row['CURRENCY'],
                                                    row['ORDER_UNIT'],
                                                    row['ORDER_PRICE_UNIT'],
                                                    row['GR_QUANTITY'],
                                                    row['RECORD_TYPE'],
                                                    row['PO_DATE'],
                                                    row['PO_ITEM_DELETED_FLAG'],
                                                    product_id,
                                                    site_id,
                                                    equipment_id,
                                                    equipment_db_id,
                                                    now,
                                                    1,
                                                    row['PURCHASE_ORDER_NUMBER'],
                                                    row['PURCH_ORDER_LINE_NBR'])
                                            cursor.execute(po_UPDATE_SQL, data)
                                            rows_updated += cursor.rowcount
                                            conn.commit()
                                    elif (row['RECORD_TYPE'] == 'D'):
                                        if po_id in current_PO_IDs:
                                            data = (row['RECORD_TYPE'], now, 0, row['PURCHASE_ORDER_NUMBER'], row['PURCH_ORDER_LINE_NBR'])
                                            cursor.execute(po_status_UPDATE_SQL, data)
                                            rows_deleted += cursor.rowcount
                                            conn.commit()
                                        else:
                                            data = (row['PURCHASE_ORDER_NUMBER'],
                                                    row['PURCH_ORDER_LINE_NBR'],
                                                    row['MATERIAL_DESCRIPTION'],
                                                    row['COMPANY_CODE'],
                                                    row['TRACKING_NO'],
                                                    row['PO_QUANTITY'],
                                                    row['NET_PRICE'],
                                                    row['NET_VALUE'],
                                                    row['EFFECTIVE_VALUE'],
                                                    row['EAPPROVAL_PR_NBR'],
                                                    row['EAPPROVAL_PR_LINE_NBR'],
                                                    row['NAME_OF_INITIATOR'],
                                                    row['VENDOR_NAME'],
                                                    row['FUNCTIONAL_LOCATION'],
                                                    row['PROJECT_NBR'],
                                                    row['AQID'],
                                                    row['PO_STATUS'],
                                                    row['SPEND_TYPE_CODE'],
                                                    row['SPEND_TYPE_DESC'],
                                                    row['CURRENCY'],
                                                    row['ORDER_UNIT'],
                                                    row['ORDER_PRICE_UNIT'],
                                                    row['GR_QUANTITY'],
                                                    row['RECORD_TYPE'],
                                                    row['PO_DATE'],
                                                    row['PO_ITEM_DELETED_FLAG'],
                                                    product_id,
                                                    site_id,
                                                    equipment_id,
                                                    equipment_db_id,
                                                    now,
                                                    0)
                                            cursor.execute(new_po_INSERT_SQL, data)
                                            rows_deleted += cursor.rowcount
                                            conn.commit()

                                    ####################################################################
                                    # BEGIN checking for invalid records
                                    ####################################################################
                                    product_sites = {'product_id': product_id,
                                                     'site_id': site_id}

                                    aqid_kpr_release = {'product_id': product_id,
                                                        'site_id': site_id,
                                                        'equipment_id': equipment_id,
                                                        'equipment_db_id': equipment_db_id}

                                    # cast current_po.pr_line_nbr to integer because current_invalid_PR.pr_line_nbr is integer.
                                    current_po = {'po_nbr': row['PURCHASE_ORDER_NUMBER'],
                                                  'po_line_nbr': int(row['PURCH_ORDER_LINE_NBR'])}

                                    ####################################################################
                                    # Is Functional Location a valid site for a given product?
                                    # if not valid, then mark as invalid product site.
                                    ####################################################################
                                    if product_sites not in valid_product_sites:
                                        category = 'Invalid product and site'
                                        description = row['PROJECT_NBR'] + " and " + row['FUNCTIONAL_LOCATION'] + " are not valid MP site in ghDS."
                                        send_email_invalid_po_recs.append([
                                            row['EAPPROVAL_PR_NBR'],
                                            row['EAPPROVAL_PR_LINE_NBR'],
                                            row['PURCHASE_ORDER_NUMBER'],
                                            row['PURCH_ORDER_LINE_NBR'],
                                            aq_id,
                                            product_name,
                                            site,
                                            category,
                                            description])
                                        if current_po in current_invalid_PO:
                                            #print('UPDATE: ', current_po)
                                            data = (row['EAPPROVAL_PR_NBR'],
                                                    row['EAPPROVAL_PR_LINE_NBR'],
                                                    aq_id,
                                                    product_id,
                                                    site_id,
                                                    equipment_id,
                                                    equipment_db_id,
                                                    description,
                                                    '1',
                                                    now,
                                                    row['PURCHASE_ORDER_NUMBER'],
                                                    row['PURCH_ORDER_LINE_NBR'])
                                            cursor.execute(po_invalid_product_site_aqid_UPDATE_SQL, data)
                                            invalid_prod_sites_rows_updated += cursor.rowcount
                                            conn.commit()
                                        else:
                                            #print('INSERT: ', current_po)
                                            data = (row['EAPPROVAL_PR_NBR'],
                                                    row['EAPPROVAL_PR_LINE_NBR'],
                                                    row['PURCHASE_ORDER_NUMBER'],
                                                    row['PURCH_ORDER_LINE_NBR'],
                                                    aq_id,
                                                    product_id,
                                                    site_id,
                                                    equipment_id,
                                                    equipment_db_id,
                                                    category,
                                                    description,
                                                    now)
                                            cursor.execute(po_invalid_product_site_aqid_INSERT_SQL, data)
                                            invalid_prod_sites_rows_inserted += cursor.rowcount
                                            conn.commit()
                                    elif aqid_kpr_release in invalid_aqid_kpr_release:
                                        ####################################################################
                                        # Does AQID have KPR release? AQID has KPR release if (qty > 0)
                                        ####################################################################
                                            category = 'No KPR release for AQ_ID'
                                            description = 'No KPR release for: ' + str(aq_id)
                                            send_email_invalid_po_recs.append([
                                                row['EAPPROVAL_PR_NBR'],
                                                row['EAPPROVAL_PR_LINE_NBR'],
                                                row['PURCHASE_ORDER_NUMBER'],
                                                row['PURCH_ORDER_LINE_NBR'],
                                                aq_id,
                                                product_name,
                                                site,
                                                category,
                                                description])
                                            if current_po in current_invalid_PO:
                                                #print('UPDATE: ', current_po)
                                                data = (row['EAPPROVAL_PR_NBR'],
                                                        row['EAPPROVAL_PR_LINE_NBR'],
                                                        aq_id,
                                                        product_id,
                                                        site_id,
                                                        equipment_id,
                                                        equipment_db_id,
                                                        description,
                                                        '1',
                                                        now,
                                                        row['PURCHASE_ORDER_NUMBER'],
                                                        row['PURCH_ORDER_LINE_NBR'])
                                                cursor.execute(po_invalid_product_site_aqid_UPDATE_SQL, data)
                                                invalid_kpr_rel_rows_updated += cursor.rowcount
                                                conn.commit()
                                            else:
                                                #print('INSERT: ', current_po)
                                                data = (row['EAPPROVAL_PR_NBR'],
                                                       row['EAPPROVAL_PR_LINE_NBR'],
                                                       row['PURCHASE_ORDER_NUMBER'],
                                                       row['PURCH_ORDER_LINE_NBR'],
                                                       aq_id,
                                                       product_id,
                                                       site_id,
                                                       equipment_id,
                                                       equipment_db_id,
                                                       category,
                                                       description,
                                                       now)
                                                cursor.execute(po_invalid_product_site_aqid_INSERT_SQL, data)
                                                invalid_kpr_rel_rows_inserted += cursor.rowcount
                                                conn.commit()
                                    else:
                                        if current_po in current_invalid_PO:
                                            #print('TO DELETE: ', current_po)
                                            data = (now,
                                                    0,
                                                    row['PURCHASE_ORDER_NUMBER'],
                                                    row['PURCH_ORDER_LINE_NBR'])
                                            cursor.execute(po_invalid_product_site_aqid_DELETE_SQL, data)
                                            valid_prod_sites_and_kpr_rel_deleted += cursor.rowcount
                                            conn.commit()

                            except Exception as sqlpoerr:
                                # Write to the .error file
                                erfile = open(po_data_path + '/status/' + file_name + '.error', 'a')
                                erfile.write('*PO SQL ERROR* Got error {!r}, errno is {}\n'.format(sqlpoerr, sqlpoerr.args[0]))

                                # prints to the log file
                                logging.critical('*PO SQL ERROR* Got error {!r}, errno is {}'.format(sqlpoerr, sqlpoerr.args[0]))
                                raise
                        else:
                            # AQID
                            # logs AQID that is not found in ghDS. Null/empty AQID not print to log.
                            if (row['AQID']):
                                logging.info('aq_id: ' + aq_id + ' (' + row['PURCHASE_ORDER_NUMBER'] + '-' + row['PURCH_ORDER_LINE_NBR'] + ') not found. Unable to map.')
                                aqid_not_found += aq_id + " (" + row['PURCHASE_ORDER_NUMBER'] + '-' + row['PURCH_ORDER_LINE_NBR'] + ")\n"
                            record_not_matched_flag = 1
                    else:
                        # FUNCTIONAL_LOCATION
                        record_not_matched_flag = 1
                else:
                    # SPEND_TYPE_DESCRIPTION
                    record_not_matched_flag = 1
            else:
                # PROJECT_NBR
                record_not_matched_flag = 1

            if (record_not_matched_flag):
                if po_id in current_PO_IDs:
                    try:
                        with conn.cursor() as cursor:
                            data = ('C', now, 0, row['PURCHASE_ORDER_NUMBER'], row['PURCH_ORDER_LINE_NBR'])
                            cursor.execute(po_status_UPDATE_SQL, data)
                            rows_updated_with_rectype_c += cursor.rowcount
                            conn.commit()
                    except Exception as sqlerr:
                            # Write to the /status/.error and log files.
                            erfile = open(po_data_path + '/status/' + file_name + '.error', 'a')
                            erfile.write('*po_status_UPDATE_SQL ERROR* Got error {!r}, errno is {}\n'.format(sqlprerr, sqlprerr.args[0]))

                            logging.critical('*po_status_UPDATE_SQL ERROR* Got error {!r}, errno is {}'.format(sqlerr, sqlerr.args[0]))
                            raise

        log['num_recs_loaded'] = total_no_recs
        log['num_new_recs_added'] = rows_inserted
        log['num_recs_with_issues'] = recs_with_issues
        log['num_recs_ignored'] = (total_no_recs - rows_inserted - recs_with_issues)

        if sitecode_not_found:
            logging.info("\nSAP Site Code not found in gh_sites:")
            logging.info(sitecode_not_found)
    
        logging.info("(RAW) No of PO recs loaded: " + str(total_no_recs))
        logging.info("(RAW) No of PO recs inserted: " + str(raw_rows_inserted))
        logging.info("(RAW) No of PO recs updated: " + str(raw_rows_updated))

        logging.info("(INVALID sites for products) No of recs inserted: " + str(invalid_prod_sites_rows_inserted))
        logging.info("(INVALID sites for products) No of recs updated: " + str(invalid_prod_sites_rows_updated))
        logging.info("(INVALID AQID with no KPR Release) No of recs inserted: " + str(invalid_kpr_rel_rows_inserted))
        logging.info("(INVALID AQID with no KPR Release) No of recs updated: " + str(invalid_kpr_rel_rows_updated))
        logging.info("(VALID) No of recs deleted: " + str(valid_prod_sites_and_kpr_rel_deleted))

        logging.info("No of PO recs loaded: " + str(total_no_recs))
        logging.info("No of PO recs inserted: " + str(rows_inserted))
        logging.info("No of PO recs updated: " + str(rows_updated))
        logging.info("No of PR recs updated to Record Type C: " + str(rows_updated_with_rectype_c))
        logging.info("No of PO recs deleted: " + str(rows_deleted))
        logging.info("No of PO recs with issues: " + str(recs_with_issues))
        logging.info("No of PO recs ignored: " + str(total_no_recs - rows_inserted - recs_with_issues))
        logging.info('Ending script for PO at ' + datetime.now().strftime('%Y-%m-%d %X'))
        logging.info('===================================================\n\n')

        ####################################################################################################
        # Log status to database.
        ####################################################################################################
        description = "SAP Site Code not found in gh_sites:\n" + sitecode_not_found[:32000] + "\naq_id not found in gh_bom_equipment:\n" + aqid_not_found[:32000]
        try:
            with conn.cursor() as cursor:
                sql = "INSERT INTO gh_pr_po_file_process_log (loaded_date_time, file_name, file_saved_to, num_recs_loaded, num_new_recs_added, num_recs_ignored, num_recs_with_issues, description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                data = (log['loaded_date_time'], str(log['file_name']), log['decrypted_data_loc'], log['num_recs_loaded'], log['num_new_recs_added'], log['num_recs_ignored'], log['num_recs_with_issues'], description)
                cursor.execute(sql, data)
                conn.commit()

                if (cursor.rowcount):
                    open(po_data_path + '/status/' + file_name + '.processed', 'a').close()
        except Exception as ex:
            errmsg = "ERROR: %s while inserting into gh_pr_po_file_process_log in processing_PO()." % format(ex)
            logging.critical(errmsg)
            raise

    except Exception as ex:
        errmsg = "Exception occured: %s while running processing_PO(). Returning to main." % format(ex)
        logging.critical(errmsg)
        raise

def processing_prpo_consolidation(conn):
    prs, pos = [], []

    ####################################################################################################
    # Logging
    ####################################################################################################
    #logger = logging.getLogger('prpolog')
    #logger.setLevel(logging.DEBUG)
    #handler = RotatingFileHandler(prpo_log_file)
    #logger.addHandler(handler)

    logging.info('Begin consolidation at ' + datetime.now().strftime('%Y-%m-%d %X'))

    ####################################################################################################
    # initialize
    #fmt = "%Y-%m-%d %H:%M:%S %Z%z"
    ####################################################################################################
    fmt = "%Y-%m-%d %H:%M:%S"
    now_utc = datetime.now(timezone('UTC'))                 # print(now_utc.strftime(fmt))
    now_pst = now_utc.astimezone(timezone('US/Pacific'))    # print(now_pst.strftime(fmt))
    #logging.info('UTC now: ' + now_utc.strftime(fmt))
    
    pr_active_SELECT_SQL = """
            SELECT pr_nbr, pr_line_nbr, quantity, gh_product_id, gh_site_id, gh_equipment_id, gh_equipment_db_id
            FROM
                gh_eapproval_pr_line_item pr INNER JOIN gh_product_prpo_automation au ON (pr.gh_product_id = au.product_id)
            WHERE
                pr.pr_status_desc not in ('Cancelled', 'Rejected', 'New Requisition (Initiator)')
                AND pr.status_id = 1
                AND au.end_date >= %s
                AND au.status_id = 1
            ORDER BY
                gh_product_id, gh_site_id, gh_equipment_id, gh_equipment_db_id"""
    
    pr_in_po_SELECT_SQL = """
            SELECT pr_nbr, pr_line_nbr
            FROM gh_sap_po_line_item
            WHERE
                 pr_nbr = %s"""
    
    po_active_SELECT_SQL = """
            SELECT gh_product_id as product_id, gh_site_id as site_id, gh_equipment_id as equip_id, gh_equipment_db_id as equip_db_id, CAST(sum(po_quantity) as SIGNED) po_qty
            FROM
                gh_sap_po_line_item po inner join gh_product_prpo_automation au on (po.gh_product_id = au.product_id)
            WHERE
                 po.status_id = 1
                 AND au.end_date >= %s 
                 AND au.status_id = 1
            GROUP BY gh_product_id, gh_site_id, gh_equipment_id, gh_equipment_db_id"""
    
    prpo_in_equip_summary_SELECT_SQL = """
            SELECT *
            FROM gh_pr_po_equipment_summary
            WHERE
                 product_id = %s
                 AND site_id = %s
                 AND equipment_id = %s
                 AND equipment_db_id = %s"""
    
    insert_new_prpo_to_equip_summary_INSERT_SQL = """
            INSERT INTO gh_pr_po_equipment_summary (product_id, site_id, equipment_id, equipment_db_id, pr_new_requisition_qty, po_valid_qty, created)
            VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    
    update_equip_summary_UPDATE_SQL = """
            UPDATE gh_pr_po_equipment_summary
            SET pr_new_requisition_qty = %s, po_valid_qty = %s, updated = %s
            WHERE
                 product_id = %s and site_id = %s and equipment_id = %s and equipment_db_id = %s"""
    
    try:
        with conn.cursor() as cursor:
            ####################################################################################################
            # Getting active PR data.
            ####################################################################################################
            #logger.info('Getting active PR data...')
            cursor.execute(pr_active_SELECT_SQL, now_utc)
            result = cursor.fetchall()
    
            for row in result:
                ####################################################################################################
                # Checks if PR rec is in PO, if PR is NOT in PO, sum PR's qty.
                ####################################################################################################
                #data = (row['pr_nbr'], row['pr_line_nbr'])
                data = (row['pr_nbr'])
                cursor.execute(pr_in_po_SELECT_SQL, data)
                result = cursor.fetchone()
    
                if (cursor.rowcount == 0):
                    prs.append({'product_id':row['gh_product_id'], 'site_id':row['gh_site_id'], 'equip_id':row['gh_equipment_id'], 'equip_db_id':row['gh_equipment_db_id'], 'pr_qty':int(row['quantity'])})
    
            prs = [list(b) for _, b in itertools.groupby(prs, key=lambda x:[x['product_id'], x['site_id'], x['equip_id'], x['equip_db_id']])]
            prs = [prs_sum_qty(i) for i in prs]

            #logging.info('PR data:')
            #for rec in prs:
                #logging.info(rec)
    
            ##############################################################################################################################################
            # Getting active PO data and quantity
            # To prevent po_qty to return as, Decimal('####'), we need to cast it in the sql query: CAST(sum(po_quantity) as SIGNED) po_qty
            ##############################################################################################################################################
            #logger.info('Getting active PO data...')
            cursor.execute(po_active_SELECT_SQL, now_utc)
            result = cursor.fetchall()
    
            pos = result
            #logging.info('PO data:')
            #for rec in pos:
                #logging.info(rec)
    
            #####################################################################################################################
            # Map PR to PO.
            # If PR's and PO's 'product_id','site_id','equip_id','equip_db_id' contain same values, then combine into one record.
            # If PO's 'product_id','site_id','equip_id','equip_db_id' doesnot match with PR's, set PO's qty to 0.
            #####################################################################################################################
            logging.info('Merging PR and PO.')
            data = defaultdict(dict)
            f = itemgetter('product_id','site_id','equip_id','equip_db_id')
            for d in [*prs, *pos]:
                data[f(d)].update(d)
    
            prpo = list(data.values())
            #for rec in prpo:
                #logging.info(rec)
    
            logging.info('Inserting or Updating...')
            for row in prpo:
                if 'pr_qty' not in row:
                    row['pr_qty'] = 0
                if 'po_qty' not in row:
                    row['po_qty'] = 0
    
                #logger.info("(product_id: " + str(row['product_id']) + ", site_id: " + str(row['site_id']) + ", equip_id: " + str(row['equip_id']) + ", equip_db_id: " + str(row['equip_db_id']) + ", pr_qty: " +  str(row['pr_qty']) + ", po_qty: " + str(row['po_qty']) + ")")
    
            ####################################################################################################
            # Check if gh_product_id, gh_site_id, gh_equipment_id, gh_equipment_db_id are in gh_pr_po_equipment_summary
            ####################################################################################################
            now = datetime.now().strftime('%Y-%m-%d 00:00:00')
            rows_inserted = rows_updated = 0
    
            for row in prpo:
                data = (row['product_id'], row['site_id'], row['equip_id'], row['equip_db_id'])
                cursor.execute(prpo_in_equip_summary_SELECT_SQL, data)
                result = cursor.fetchone()
    
                ####################################################################################################
                # Insert new record into gh_pr_po_equipment_summary for new record or Update existing records.
                ####################################################################################################
                if not result:
                    data = (row['product_id'], row['site_id'], row['equip_id'], row['equip_db_id'], row['pr_qty'], row['po_qty'], now)
                    cursor.execute(insert_new_prpo_to_equip_summary_INSERT_SQL, data)
                    rows_inserted += cursor.rowcount
                    #logger.info("Rows Inserted: {}" . format(rows_inserted))
                    conn.commit()
                else:
                    data = (row['pr_qty'], row['po_qty'], now, row['product_id'], row['site_id'], row['equip_id'], row['equip_db_id'])
                    cursor.execute(update_equip_summary_UPDATE_SQL, data)
                    rows_updated += cursor.rowcount
                    #logger.info("Rows Updated: {}" . format(rows_updated))
                    conn.commit()
    
        logging.info('Consolidation ended at ' + datetime.now().strftime('%Y-%m-%d %X'))
        logging.info('===================================================\n\n')
    
    except Exception as ex:
        errmsg = "Consolidation Error. Exception occured: %s. Returning to main." % format(ex)
        logging.critical(errmsg)
        raise
    

####################################################################################################
# Filter data based on PROJECT_NBR (Tcodes)
####################################################################################################
def get_product_id(conn):
    products = {}
    get_prod_id_sql = "SELECT pc.product_id, pc.product_code, product_name FROM gh_product_codes pc inner join gh_product p on (pc.product_id = p.product_id) where pc.status_id = 1"
    try:
        with conn.cursor() as cursor:
            cursor.execute(get_prod_id_sql)
            result = cursor.fetchall()

            for row in result:
                products[row['product_code']] = row

            return products
    except Exception as sqlerr:
        logging.critical('*ERROR* get_prod_id_sql in get_product_id()', sqlerr)
        raise

####################################################################################################
# Filter data based on FUNCTIONAL_LOCATION_CODE => site_id
####################################################################################################
def get_site_id(conn):
    sites = {}
    get_site_id_sql = "SELECT site_id, site, site_sap_code FROM gh_sites where site_sap_code is not null and site_sap_code != '' and status_id > 0"
    try:
        with conn.cursor() as cursor:
            cursor.execute(get_site_id_sql)
            result = cursor.fetchall()

            for row in result:
                sites[row['site_sap_code']] = row

            #sites['LOCN-TBD1'] = {'site_id': 0}
            #sites[''] = {'site_id': 0} 

            return sites
    except Exception as sqlerr:
        logging.critical('*ERROR* get_site_id_sql in get_site_id() ', sqlerr)
        raise

####################################################################################################
# split aq_id
####################################################################################################
def parse_aqid(aq_id):
    items = aq_id.split('-')
    apple_equipment_id = items[0]
    apple_equipment_version = items[1]
    if len(items) == 3:
        apple_equipment_type = items[2]
    elif len(items) == 2:
        apple_equipment_type = ''

    return apple_equipment_id, apple_equipment_version, apple_equipment_type

####################################################################################################
# get equipment_id, equipment_db_id from aq_id
####################################################################################################
def get_equipment(conn, aq_id):
    equipment_SELECT_SQL = """
            SELECT
                equipment_id,
                db_id as equipment_db_id,
                apple_equipment_id,
                apple_equipment_version,
                apple_equipment_type,
                if(apple_equipment_id is null, '', concat(LPAD(apple_equipment_id,5,0),'-',LPAD(gh_bom_equipment.apple_equipment_version,2,0),if(ifnull(gh_bom_equipment.apple_equipment_type,'')='' ,'', concat('-', gh_bom_equipment.apple_equipment_type) ) )) as apple_equipment_uid
            FROM
                gh_bom_equipment
            WHERE
                apple_equipment_id = %s
                and apple_equipment_version = %s
                and ifnull(apple_equipment_type,'') = %s"""

    try:
        with conn.cursor() as cursor:
            ####################################################################################################
            # Exception occured: 'NoneType' object is not subscriptable
            # Add an 'if' condition to prevent the query returns zero record with this error message:
            # Exception occured: must be str, not NoneType while running processing_PR()
            # aq_id is empty or null
            ####################################################################################################
            cursor.execute(equipment_SELECT_SQL, aq_id)
            result = cursor.fetchone()
            if cursor.rowcount == 1:
                equipment_id = result['equipment_id']
                equipment_db_id = result['equipment_db_id']

                return equipment_id, equipment_db_id
    except Exception as sqlerr:
        logging.critical('ERROR: equipment_SELECT_SQL in get_equipment() ', sqlerr)
        raise

####################################################################################################
# check for valid product and site
####################################################################################################
def check_valid_product_and_site(conn):
    valid_product_site = {}

    valid_product_site_SELECT_SQL = """
        SELECT m.product_id,
               m.site_id,
               s.site_sap_code,
               p.product_name
        FROM gh_model m
             inner join gh_product_prpo_automation a on (m.product_id = a.product_id)
             inner join gh_sites s on (m.site_id = s.site_id)
             inner join gh_product p on (m.product_id = p.product_id)
        WHERE m.product_stage = 138
              and m.product_sub_stage = ''
              and m.scenario = 450
              and m.status_id = 1"""

    try:
        with conn.cursor() as cursor:
            cursor.execute(valid_product_site_SELECT_SQL)

            valid_product_site = [{'product_id': row['product_id'], 'site_id': row['site_id']} for row in cursor.fetchall()]

            #result = cursor.fetchall()
            #for row in result:
                #valid_product_site = [{'product_id': row['product_id'], 'site_id': row['site_id']}]

            return valid_product_site

    except Exception as sqlerr:
        logging.critical('ERROR: valid_product_site_SELECT_SQL in check_valid_product_and_site() ', sqlerr)
        raise

####################################################################################################
# Does AQID has KPR release (qty>0)
####################################################################################################
def check_invalid_aqid_with_kpr_release(conn):
    invalid_aqid_has_kpr_release = []

    invalid_aqid_has_kpr_release_SELECT_SQL = """
        SELECT m_rel.product_id, m_rel.site_id, mpe_rel.equipment_id, mpe_rel.equipment_db_id, sum(ifnull(rel.release_qty,0)) as sum_release_qty
        FROM gh_model m_rel
            inner join gh_product_prpo_automation a on (m_rel.product_id = a.product_id)
            inner join gh_mpintent_equipment mpe_rel on (m_rel.model_id = mpe_rel.model_id and m_rel.db_id = mpe_rel.model_db_id)
            inner join gh_mpintent_equipment_release rel on (mpe_rel.mpintent_equipment_id = rel.mpintent_equipment_id and mpe_rel.mpintent_equipment_db_id = rel.mpintent_equipment_db_id)
        WHERE
            m_rel.product_stage = 138
            and m_rel.product_sub_stage = ''
            and m_rel.scenario = 450
            and m_rel.status_id = 1
            and a.status_id = 1
            and mpe_rel.status_id = 1
            and rel.status_id = 1
        GROUP BY product_id, site_id, equipment_id, equipment_db_id
        HAVING sum_release_qty = 0"""

    try:
        with conn.cursor() as cursor:
            cursor.execute(invalid_aqid_has_kpr_release_SELECT_SQL)
            result = cursor.fetchall()

            for row in result:
                invalid_aqid_has_kpr_release.append({'product_id':row['product_id'],
                                                   'site_id':row['site_id'],
                                                   'equipment_id':row['equipment_id'],
                                                   'equipment_db_id':row['equipment_db_id']
                                                   })
            return invalid_aqid_has_kpr_release

    except Exception as sqlerr:
        logging.critical('ERROR: invalid_aqid_has_kpr_release_SELECT_SQL in check_invalid_aqid_with_kpr_release() ', sqlerr)
        raise

##################################################
# Sum the QTY based on product_id, site_id, equip_id, equip_db_id
# "Unpacking Generalizations" works in python v3.5+. Had to rewrite because it doesn't work on v3.4.
# new_d = [list(b) for _, b in itertools.groupby(prs, key=lambda x:[x['product_id'], x['site_id'], x['equip_id'], x['equip_db_id']])]
# prs = [{**i[0], **{'pr_qty':sum(b['pr_qty'] for b in i)}} for i in new_d]
##################################################
def prs_sum_qty(prs_list):
    summary = prs_list[0].copy()
    summary['pr_qty'] = sum(b['pr_qty'] for b in prs_list)
    return summary

####################################################################################################
# Send email
####################################################################################################
def send_email(invalid_recs, report_type, filename):
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Reply-to'] = reply_to
    msg['Subject'] = "Invalid " + report_type + " Report"

    filename = filename[5:]
    # Create the body of the message (a plain-text and an HTML version).
    #text = "Hi!\nHow are you?\n"
    html = """\
    <html>
      <head>
      <style>
      table {
          border-collapse: collapse;
      }
      table, td, th {
          border: 1px solid black;
      }
      </style>
      <body style="background:#cfcfcf 100%; margin:0; padding:0">
      <center style="background:#cfcfcf 100% 0; margin=0; padding:0">
    <br />
    <br />
    <div style="border-radius:15px;background:#ffffff;width:1000px;font-family:Lucida Grande, Geneva, Verdana, Arial, Helvetica, sans-serif; font-size:12px; color:rgb(121,121,121); line-height:17px;" align="left" >
    <div style="padding:30px 30px 50px 30px;">
    <div style="text-align:right;padding:15px;font-size:120%" ><span style="color: #9BB3E4;" style="display:block"> &#xF8FF; <span style="font-weight:normal;text-decoration:none;color:#9BB3E4">ghDS</span></span></div>
    """

    html += """Invalid """ + report_type + """ Records from """ + filename + "<br /><br />"""
    html += """<table border='1' cellpadding='3' cellspacing='2'>
               <tr>
               <td>PR #</td>
               <td>PR LINE #</td>
               <td>PO #</td>
               <td>PO LINE #</td>
               <td>AQ ID</td>
               <td>PRODUCT NAME</td>
               <td>SITE</td>
               <td>CATEGORY</td>
               <td>DESCRIPTION</td>
               </tr>\n"""

    for rec in invalid_recs:
        html += """<tr><td>""" + str(rec[0]) + """</td>
                   <td>""" + str(rec[1]) + """</td>
                   <td>""" + str(rec[2]) + """</td>
                   <td>""" + str(rec[3]) + """</td>
                   <td>""" + str(rec[4]) + """</td>
                   <td>""" + str(rec[5]) + """</td>
                   <td>""" + str(rec[6]) + """</td>
                   <td>""" + str(rec[7]) + """</td>
                   <td>""" + str(rec[8]) + """</td>
                   </tr>\n"""
    html += """\
        </table>
         <p>- ghDS PRPO Automation Support</p>
    </div>
    </div>
    </div>
    <div style="width:1000;font-family:Lucida Grande, Geneva, Verdana, Arial, Helvetica, sans-serif; font-size:10px; color:rgb(121,121,121); line-height:10px;" align="center" >
    <p>Apple Need to Know Confidential</p>
    <p>Radar : <a href="rdar://new/problem/component=GroundhogDS&version=All" style="font-weight:normal;text-decoration:none;color:#9BB3E4"> GroundhogDS POAutomation | ALL</a></p>
    <p>Email : <a href="mailto:groundhogPOAutomation@group.apple.com" style="font-weight:normal;text-decoration:none;color:#9BB3E4" >ghDS PRPO Automation Support (groundhogPOAutomation@group.apple.com) </a></p>
    <p>Wiki :<a href="http://hwtewiki.apple.com/display/ghds/Home" style="font-weight:normal;text-decoration:none;color:#9BB3E4">http://hwtewiki.apple.com/display/ghds/Home</a></p>
    </div>
    </center>
      </body>
    </html>
    """

    # Record the MIME types of both parts - text/plain and text/html.
    #part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
    
    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    #msg.attach(part1)
    msg.attach(part2)
    
    try:
        # Send the message via local SMTP server.
        s = smtplib.SMTP('relay.apple.com')
        # sendmail function takes 3 arguments: sender's address, recipient's address and message to send - sent as one string.
        s.sendmail(from_addr, to_addr, msg.as_string())
    except Exception as emailerr:
        logging.critical('*ERROR* sending email in send_email(). ', emailerr)
        sys.exit(1)
    else:
        logging.info("Email sent successfully.\n\n")
        s.quit()

####################################################################################################
# Send email on errors and exit
####################################################################################################
def send_email_notification_on_errors():
    msg = MIMEMultipart()
    msg['From'] = from_addr_err
    msg['To'] = to_addr_err
    msg['Reply-to'] = reply_to_err
    msg['Subject'] = "*ERROR* PRPO - Critical or Fatal Error"

    hostname = socket.gethostname()

    # Create the body of the message (a plain-text and an HTML version).
    #text = "Hi!\nHow are you?\n"
    html = """\
    <html>
      <head>
      <body style="background:#cfcfcf 100%; margin:0; padding:0">
      <center style="background:#cfcfcf 100% 0; margin=0; padding:0">
    <br />
    <br />
    <div style="border-radius:15px;background:#ffffff;width:1000px;font-family:Lucida Grande, Geneva, Verdana, Arial, Helvetica, sans-serif; font-size:12px; color:rgb(121,121,121); line-height:17px;" align="left" >
    <div style="padding:30px 30px 50px 30px;">
    <div style="text-align:right;padding:15px;font-size:120%" ><span style="color: #9BB3E4;" style="display:block"> &#xF8FF; <span style="font-weight:normal;text-decoration:none;color:#9BB3E4">ghDS</span></span></div>

        The PRPO script on """ + hostname + """ has encountered a critical or fatal error, and has been terminated. Please check the log file for error messages.
         <p>- ghDS PRPO Automation Support</p>
    </div>
    </div>
    </div>
    <div style="width:1000;font-family:Lucida Grande, Geneva, Verdana, Arial, Helvetica, sans-serif; font-size:10px; color:rgb(121,121,121); line-height:10px;" align="center" >
    <p>Apple Need to Know Confidential</p>
    <p>Radar : <a href="rdar://new/problem/component=GroundhogDS&version=All" style="font-weight:normal;text-decoration:none;color:#9BB3E4"> GroundhogDS POAutomation | ALL</a></p>
    <p>Email : <a href="mailto:groundhogPOAutomation@group.apple.com" style="font-weight:normal;text-decoration:none;color:#9BB3E4" >ghDS PRPO Automation Support (groundhogPOAutomation@group.apple.com) </a></p>
    <p>Wiki :<a href="http://hwtewiki.apple.com/display/ghds/Home" style="font-weight:normal;text-decoration:none;color:#9BB3E4">http://hwtewiki.apple.com/display/ghds/Home</a></p>
    </div>
    </center>
      </body>
    </html>
    """

    # Record the MIME types of both parts - text/plain and text/html.
    #part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
    
    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    #msg.attach(part1)
    msg.attach(part2)
    
    try:
        # Send the message via local SMTP server.
        s = smtplib.SMTP('relay.apple.com')
        # sendmail function takes 3 arguments: sender's address, recipient's address and message to send - sent as one string.
        s.sendmail(from_addr_err, to_addr_err, msg.as_string())
    except Exception as emailerr:
        logging.critical('*ERROR* sending email in send_email_notification_on_errors(). ', emailerr)
        sys.exit(1)
    else:
        logging.info("Email sent successfully.\n\n")
        s.quit()

###########################################################################
# prpo_main.py
# loop thru the PR and PO datafiles in /var/pr/ or /var/po/ directory.
# PR -> processing_PR()
# PO -> processing_PO()
#       processing_prpo_consolidation()
###########################################################################

gpg = gnupg.GPG()

while True:
    try:
        logging.info('Checking database connections...')
        if (conn.open and conn2.open):
            logging.info('Database connections: park and conveyor are open...')

            datatypes = ['PR', 'PO']
            num_files = 0

            try:
                for datatype in datatypes:
                    filepath = pr_data_path if datatype == 'PR' else po_data_path

                    for getfile in sorted(pathlib.Path(filepath + '/status').glob('*.fetched')):
                        send_email_invalid_pr_recs, send_email_invalid_po_recs = [], []

                        index = getfile.name.index(".")
                        filename = getfile.name[:index]

                        if not ((os.path.isfile(filepath + '/status/' + filename + '.processed')) or (os.path.isfile(filepath + '/status/' + filename + '.error'))):
                            msginfo = 'Starting ' + datatype + ' at ' + datetime.now().strftime('%Y-%m-%d %X')
                            logging.info(msginfo)

                            with open(filepath + '/data/' + filename + '.txt.asc','rb') as f:

                                msginfo = 'Decrypting: ' + filepath + '/data/' + filename + '.txt.asc'
                                logging.info(msginfo)

                                data = gpg.decrypt_file(f, passphrase=gpg_pass[datatype])

                                result = csv.DictReader(io.StringIO(str(data)), delimiter='\t')

                                if datatype == 'PR':
                                    msginfo = 'Processing: ' + filepath + '/data/' + filename + '.txt.asc\n'
                                    logging.info(msginfo)

                                    processing_PR(conn, conn2, result, filename)
                                else:
                                    msginfo = 'Processing: ' + filepath + '/data/' + filename + '.txt.asc\n'
                                    logging.info(msginfo)

                                    processing_PO(conn, conn2, result, filename)

                                num_files += 1

                            if send_email_invalid_pr_recs:
                                logging.info('Begin sending email for invalid %s reports...' %datatype)
                                send_email(send_email_invalid_pr_recs, datatype, filename)
                            elif send_email_invalid_po_recs:
                                logging.info('Begin sending email for invalid %s reports...' %datatype)
                                send_email(send_email_invalid_po_recs, datatype, filename)


                if num_files > 0:
                    msginfo = 'Number of files processed: ' + str(num_files)
                    logging.info(msginfo)

                    msginfo = 'Processing consolidation...'
                    logging.info(msginfo)

                    processing_prpo_consolidation(conn)
                else:
                    msginfo = 'Number of PR/PO file(s) processed: ' + str(num_files)
                    logging.info(msginfo)

                msginfo = 'Sleeping...'
                logging.info(msginfo)

                time.sleep(600) # 300 secs = 5 minutes
            except Exception as mainerr:
                errmsg = 'In main. *ERROR* ' + str(mainerr)
                logging.critical(errmsg)
                send_email_notification_on_errors()
                if (conn.open and conn2.open):
                    sys.exit(1)
                else:
                    time.sleep(60)  # 60 secs = 1 minutes
                    conn.ping(reconnect=True)
                    conn2.ping(reconnect=True)
        else:
            time.sleep(60)  # 60 secs = 1 minutes
            logging.info('Lost database connections. Reconnecting...')
            conn.ping(reconnect=True)
            conn2.ping(reconnect=True)

    except Exception as dberr:
        errmsg = '*ERROR MAKING DATABASE CONNECTION* Please check database login credentials.\n' + str(dberr)
        logging.critical(errmsg)
        send_email_notification_on_errors()
        sys.exit(1)
