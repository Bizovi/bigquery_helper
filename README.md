# BigQuery python helper package

Automates some common tasks which are not so easily achieved with the original `google-cloud-bigquery` client.

* Write **tables** and **views** from queries.
* Delete and Backup tables to `google storage` when replacing them.
* Download huge amounts of data into memory (`pandas.DataFrame`). Fast.

### Installation

1. Create a virtual environment: `virtualenv my-venv`. It will have installed `pip`, `setuptools` and `wheel`.
2. Install from github: `pip install git+https://github.com/bizovi/bigquery_helper`
3. Check installation: `python>> from bigquery_helper import helper`


### Getting Started
**Authentification** in the Google Cloud Platform can be a bit daunting when automating workflows which depend on multiple services. I found the following approach to work for most scenarios. So, you'll need a:

1. Service account and service key
2. Necessary and sufficient rights: for BigQuey and Google Storage gs://.

```python
import google.auth
import os

## Define constants ---------------------------------------------
SERVICE_KEY = '<Service_Key>.json'

scope = ['https://www.googleapis.com/auth/bigquery',
         'https://www.googleapis.com/auth/cloud-platform',
         'https://www.googleapis.com/auth/devstorage.read_write']

## auth with default service keys --------------------------------
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_KEY
credentials, project = google.auth.default()

if credentials.requires_scopes:
    credentials = credentials.with_scopes(scope)
```


**When** to use the bigquery client and when to use the `helper`:

If you want to download a small output of the query in the memory, use the client. Note that you might need to install the `pip install google-cloud-bigquery[pandas]`.

```python
 from google.cloud import biguery

 client = bigquery.Client()
 query  = """select count(*) as nr_rows
        from `bigquery-public-data.samples.natality`"""
 df_nat = client.query(query).to_dataframe()
 df_nat.head()
```

If the output of the query is greater than ~30k rows, use the helper. It expects a `gs://bucket/directory` *(TBD: it throws an error if used without a directory, upside: you get your buckets clean)* and a name for the temporary BQ table where to store the downloaded table.

```python
from bigquery_helper import helper

query  = """select count(*) as nr_rows
       from `bigquery-public-data.samples.natality`"""

df = helper.query_to_pandas(query,
    project_id=PROJECT_ID,
    dataset=DATASET,
    dest_table="Recommendations_Temp", # a temporary table to store results
    bucket=BUCKET, # bucket where to download the csv
    bucket_dir="recommendations", # directory where to download the csv
    if_exists='replace', # if the temp table exists
    block=True # wait for the query to finish
    )
```

Writing views should be easy, but it involves quite a bit of work in the client. The helper writes tables and views easily:

```python
res = helper.query_to_table(query,
    project_id=PROJECT_ID,
    dataset=DATASET,
    dest_table="SOME_TABLE",
    if_exists='replace',
    block=True)

res = helper.create_view(view_sql=query,
    project=PROJECT_ID,
    dataset_id=DATASET,
    view_name="SOME_VIEW",
    update=True)
```

For using the functions for less common use-cases, see the docs.
