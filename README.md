# Overview
Bigqueryでよく操作することをパッケージ

# Requirements

* Python (>= 3.4)

# Setting up
```
pip install git+https://github.com/9en/BigqueryAPI
```

# Usage
## bq()
* Description:
    * クエリを実行して結果をBigqueryに保存する
* Config:
    * config.ini
    * query.sql
    * schema.py(新規テーブル作成の場合)

```
Sample::
>>> import BigqueryAPI
>>> BQ = BigqueryAPI.GCP('sample', '2019-03-25')
>>> BQ.bq()
```


## bq_download()
* Description:
    * クエリを実行結果をダウンロードする
* Config:
    * config.ini
    * query.sql
* Output:
    * data.tsv

```
Sample::
>>> import BigqueryAPI
>>> BQ = BigqueryAPI.GCP('sample', '2019-03-25')
>>> BQ.bq_download()
```


## bq()
* Description:
    * データをBigqueryにロードする
* Config:
    * config.ini
    * query.sql
    * schema.py(新規テーブル作成の場合)
* Input:
    * data.tsv

```
Sample::
>>> import BigqueryAPI
>>> BQ = BigqueryAPI.GCP('sample', '2019-03-25')
>>> BQ.bq_load()
```

# 設定ファイル
## config.ini
※サンプル
```
[bigquery]
secret_file: secret.json # ここはフルパスを記入
gcp_project: test
dataset: test
tablename: test
allow_large_results: True
use_legacy_sql: False
write_disposition: WRITE_TRUNCATE
```

## query.sql
SQLを書く

## schema.py
※サンプル
```
from google.cloud import bigquery

schema = [
    bigquery.SchemaField('dt', 'DATE', description='日付'),
]```

