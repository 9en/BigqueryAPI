#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import configparser
import re
import sys
import time
import datetime
import importlib
from google.cloud import bigquery

class GCP:
    '''
    Description::
        Bigqueryの操作を行う
    :param filepath:
        設定ファイルが保存されているディレクトリパスを指定する
            * config.ini(必須)
            * query.sql
            * schema.py
    :param yyyy_mm_dd
        パーティションを指定するときの日付（YYYY-MM-DD）
    '''
    def __init__(self, filepath, yyyy_mm_dd):
        self.FILEPATH = filepath
        cfgparser = configparser.ConfigParser()
        cfgparser.optionxform = str
        cfgparser.read('/'.join([self.FILEPATH,'config.ini']), 'UTF-8')
        self.TABLE_NAME = cfgparser.get('bigquery', 'tablename')
        self.DATASET = cfgparser.get('bigquery', 'dataset')
        self.PROJECT = cfgparser.get('bigquery', 'gcp_project')
        self.ALLOW_LARGE_RESULTS = cfgparser.get('bigquery', 'allow_large_results')
        self.USE_LEGACY_SQL = cfgparser.get('bigquery', 'use_legacy_sql')
        self.WRITE_DISPOSITION = cfgparser.get('bigquery', 'write_disposition')
        self.YYYY_MM_DD = yyyy_mm_dd
        self.YYYYMMDD = yyyy_mm_dd.replace('-', '')
        self.client = bigquery.Client.from_service_account_json(cfgparser.get('bigquery', 'secret_file'), project=self.PROJECT)
        self.dataset_ref = self.client.dataset(str(self.DATASET))
        self.wait_time = 1 * 60
        self.query_params = [
                bigquery.ScalarQueryParameter('YYYYMMDD', 'STRING', self.YYYYMMDD),
                bigquery.ScalarQueryParameter('YYYY_MM_DD', 'STRING', self.YYYY_MM_DD)
                ]

    def read_schema(self):
        return importlib.import_module('.'.join([self.FILEPATH ,'schema'])).schema

    def read_query(self):
        filename = self.FILEPATH + '/query.sql'
        query = re.sub('\n', " ",re.sub('--.*\n', "\n", open(filename, 'r', encoding='utf-8').read()))
        query = query.replace('${YYYY_MM_DD}', self.YYYY_MM_DD)
        query = query.replace('${YYYYMMDD}', self.YYYYMMDD)
        return query

    def read_param_query(self, param, table_name, type, days_ago):
        if 'partitiontime' in (type.strip().lower()):
            query = 'SELECT ' + param +  ' FROM `table` WHERE _PARTITIONTIME = TIMESTAMP_SUB(TIMESTAMP("${YYYY_MM_DD}"), INTERVAL ' + str(days_ago) + ' - 1 DAY)'
        else:
            query = 'SELECT ' + param +  ' FROM `table_*` WHERE _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(DATE("${YYYY_MM_DD}"), INTERVAL ' + str(days_ago) + ' - 1 DAY))'
        query = query.replace('${YYYY_MM_DD}', self.YYYY_MM_DD)
        query = query.replace('table', re.sub('_$',''   ,table_name))
        return query

    def create_table(self):
        schema = self.read_schema()
        table_ref = self.dataset_ref.table(self.TABLE_NAME)
        table = bigquery.Table(table_ref, schema=schema)
        table.partitioning_type = 'DAY'
        table = self.client.create_table(table)

    def exists_table(self):
        from google.cloud.exceptions import NotFound
        try:
            self.client.get_table(self.dataset_ref.table(self.TABLE_NAME))
            return True
        except NotFound:
            return False

    def wait_condition(self, query, config):
        total_byte = total_byte_old = 0
        while total_byte == 0 or total_byte != total_byte_old:
            query_job = self.client.query(query, job_config=config)
            total_byte_old = total_byte
            total_byte = query_job.total_bytes_processed
            if total_byte == 0 or total_byte != total_byte_old:
                 time.sleep(self.wait_time)

    def run_dry(self, table_name, option):
        query = self.read_param_query(' * ' ,table_name, option['type'], option['days_ago'])
        config = self.set_config(sys._getframe().f_code.co_name)
        self.wait_condition(query, config)

    def set_config(self, run_type):
        table_ref = self.dataset_ref.table(self.TABLE_NAME + '$' + self.YYYYMMDD)
        if run_type == 'run_query':
            config = bigquery.QueryJobConfig()
            config.use_legacy_sql = self.USE_LEGACY_SQL
            config.allow_large_results = self.ALLOW_LARGE_RESULTS
            config.write_disposition = self.WRITE_DISPOSITION
            config.query_parameters = self.query_params
            config.destination = table_ref
        elif run_type == 'run_dry':
            config = bigquery.QueryJobConfig()
            config.use_legacy_sql = self.USE_LEGACY_SQL
            config.dry_run = True
            config.use_query_cache = False
        elif run_type == 'run_count_query':
            config = bigquery.QueryJobConfig()
            config.use_legacy_sql = self.USE_LEGACY_SQL
        elif run_type == 'load_data':
            config = bigquery.LoadJobConfig()
            config.write_disposition = self.WRITE_DISPOSITION
            config.field_delimiter = '\t'
            config.max_bad_records = 20
        elif run_type == 'download_data':
            config = bigquery.QueryJobConfig()
            config.use_legacy_sql = self.USE_LEGACY_SQL
            config.query_parameters = self.query_params
        return config

    def run_query(self):
        config = self.set_config(sys._getframe().f_code.co_name)
        query = self.read_query()
        self.client.query(query, job_config=config).result()

    def run_count_query(self):
        config = self.set_config(sys._getframe().f_code.co_name)
        table_name = '.'.join([self.PROJECT, str(self.DATASET), self.TABLE_NAME])
        query = self.read_param_query('COUNT(1)' ,table_name, 'partitiontime', 1)
        rows = self.client.query(query, job_config=config).result()
        for row in rows:
            if int(row[0]) == 0:
                print(result_count_0)

    def download_data(self):
        config = self.set_config(sys._getframe().f_code.co_name)
        query = self.read_query()
        rows = self.client.query(query, job_config=config).result()
        with open(os.getcwd() + '/data.tsv', 'w') as f:
            for row in rows:
                f.write('\t'.join(map(str, list(row))) + '\n')

    def load_data(self):
        config = self.set_config(sys._getframe().f_code.co_name)
        table_ref = self.dataset_ref.table(self.TABLE_NAME + '$' + self.YYYYMMDD)
        with open(os.getcwd() + '/data.tsv', 'rb') as f:
            job = self.client.load_table_from_file(file_obj=f, destination=table_ref, job_config=config)
        try:
            job.result()
        except:
            raise RuntimeError(job.errors)

    def bq_wait(self):
        if 'table_wait' in digdag.env.params:
            table_wait = digdag.env.params['table_wait']
            if table_wait:
                for table_name, option in table_wait.items():
                    self.run_dry(table_name, option)

    def bq_load(self):
        '''
        Description::
            データをBigqueryにロードする
        Config::
            * config.ini
            * query.sql
            * schema.py(新規テーブル作成の場合)
        Input::
            data.tsv

        Sample::
        >>> import BigqueryAPI
        >>> BQ = BigqueryAPI.GCP('sample', '2019-03-25')
        >>> BQ.bq_load()
        '''
        if not self.exists_table():
            self.create_table()
        self.load_data()

    def bq_download(self):
        '''
        Description::
            クエリを実行結果をダウンロードする
        Config::
            * config.ini
            * query.sql
        Output::
            data.tsv

        Sample::
        >>> import BigqueryAPI
        >>> BQ = BigqueryAPI.GCP('sample', '2019-03-25')
        >>> BQ.bq_download()
        '''
        self.download_data()

    def bq(self):
        '''
        Description::
            クエリを実行して結果をBigqueryに保存する
        Config::
            * config.ini
            * query.sql
            * schema.py(新規テーブル作成の場合)

        Sample::
        >>> import BigqueryAPI
        >>> BQ = BigqueryAPI.GCP('sample', '2019-03-25')
        >>> BQ.bq()
        '''
        if not self.exists_table():
            self.create_table()
        self.run_query()


