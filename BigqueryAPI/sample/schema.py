from google.cloud import bigquery

schema = [
    bigquery.SchemaField('dt', 'DATE', description='日付'),
]
