from google.cloud import bigquery
from google.cloud import storage
import google.auth
import os
import datetime
import pandas as pd
from io import BytesIO



def clear_table(project_id, dataset, table, delete_rows=False):
    '''Delete table or all the rows from a BQ table
    Arguments:
    ---------
    project_id: str
        Google's BigQuery project id
    dataset: str
        Name of the dataset in which to store the table
    table: str
        Name of the table to download
    delete_rows: bool
        Whether to delete rows or the whole table (False)
    '''
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(dataset).table(table)

    if delete_rows == False:
        client.delete_table(table_ref)
        print('Table {}:{} deleted.'.format(dataset, table))
    else:
        dml = '''delete from `{}.{}.{}`
                 where 1=1;'''.format(project_id, dataset, table)
        job_config.destination = table_ref
        query_job = client.query(dml, location='US',
            job_config=job_config)
        print('Rows from table {}:{} deleted'.format(dataset, table))



def backup_table(project_id, dataset, table, bucket, directory):
    '''Export a table in a .csv format in gs://
    Arguments:
    ---------
    project_id: str
        Google's BigQuery project id
    dataset: str
        Name of the dataset in which to store the table
    table: str
        Name of the table to download
    bucket: str
        Name of the bucket where to save the csv
    directory: str
        Name of directory inside the bucket

    Returns:
    ---------
    (directory, csv_name): str
        Location of the saved file in gs://
    '''
    current_ts = str(datetime.datetime.utcnow())
    csv_name = '{}_{}.csv'.format(table, current_ts)
    destination_uri = 'gs://{}/{}/{}'.format(
        bucket, directory, csv_name)

    client = bigquery.Client()
    dataset_ref = client.dataset(dataset, project=project_id)
    table_ref = dataset_ref.table(table)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location='US')

    extract_job.result()  # wait for the job to finish
    assert extract_job.state == "DONE"

    print('Exported {}:{}.{} to {}'.format(
        project_id, dataset, table, destination_uri)
        )
    return (directory, csv_name)



def query_to_table(query, project_id, dataset, dest_table,
                   if_exists='fail', block=True):
    '''Submit a query job which writes a query to a table
    Arguments:
    ---------
    query: str
        StandardSQL query to be stored in a table
    project_id: str
        Google's BigQuery project id
    dataset: str
        Name of the dataset in which to store the table
    dest_table: str
        Name of the destination table
    if_exists : str (default: 'fail')
        append  - Specifies that rows may be appended to an existing table
        fail    - Specifies that the output table must be empty
        replace - Specifies that write should replace a table
    block: bool
        Specifies whether to wait for job or not

    Returns:
    ---------
    job: google.cloud.bigquery.job.QueryJob
        Returns the inserted QueryJob
    '''
    client = bigquery.Client()  # keep it generic for cross-projects
    job_config = bigquery.QueryJobConfig()

    # Set the destination table
    table_ref = client.dataset(dataset).table(dest_table)
    job_config.destination = table_ref
    job_config.use_legacy_sql = False
    job_config.allow_large_results = True
    job_config.create_disposition = "CREATE_IF_NEEDED"

    if if_exists == 'replace':
        job_config.write_disposition = "WRITE_TRUNCATE"

    # Start the query, passing in the extra configuration.
    try:
        query_job = client.query(query, location='US', job_config=job_config)
        # wait for the query to finish
        if block:
            query_job.result()
        print('Query results loaded to table {}'.format(table_ref.path))
        return query_job
    except Exception as err:
        print(err)
        return False


def create_view(view_sql, project, dataset_id, view_name, update=False):
    '''Create a view from a query. Update it if needed
    Arguments:
    ---------
    view_sql: str
        StandardSQL view definition (query)
    project: str
        Google's BigQuery project id
    dataset_id: str
        Name of the dataset in which to store the view
    view_name: str
        Name of the view
    update: bool, (default: False)
        Whether to update the view definition
    '''
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_id)
    table = dataset.table(view_name)

    if update == True:
        table_list = [x.table_id for x in list(bigquery_client.list_tables(dataset))]
        if view_name in table_list:
            res = clear_table(
                project_id=project,
                dataset=dataset_id,
                table=view_name,
                delete_rows=False)
        try:
            view = bigquery.Table(table)
            view.view_query = view_sql
            view.view_use_legacy_sql = False
            bigquery_client.create_table(view)
            return True
        except Exception as err:
            print(err)
            return False
    else:
        try:
            view = bigquery.Table(table)
            view.view_query = view_sql
            view.view_use_legacy_sql = False
            bigquery_client.create_table(view)
            return True
        except Exception as err:
            print(err)
            return False


def query_to_gs(query, project_id, dataset, dest_table, bucket, bucket_dir,
                if_exists='replace', block=True):
    '''Download data from a query to google storage. Returns the name of
    the file and location in storage
    Arguments:
    ---------
    query: str
        StandardSQL query to be stored in a table
    project_id: str
        Google's BigQuery project id
    dataset: str
        Name of the dataset in which to store the table
    dest_table: str
        Name of the destination table
    bucket: str
        gs:// bucket to which the file is saved
    bucket_dir: str
        The directory inside a bucket
    if_exists : str (default: 'fail')
        append  - Specifies that rows may be appended to an existing table
        fail    - Specifies that the output table must be empty
        replace - Specifies that write should replace a table
    block: bool
        Specifies whether to wait for job or not

    Returns:
    ---------
    (path, filename): str
        Path and filename of the saved object
    '''
    _ = query_to_table(
            query=query,
            dataset=dataset,
            dest_table=dest_table,
            project_id=project_id,
            if_exists=if_exists,
            block=block)

    gs_name = backup_table(
            project_id=project_id,
            dataset=dataset,
            table=dest_table,
            bucket=bucket,
            directory=bucket_dir)

    _ = clear_table(
            project_id=project_id,
            dataset=dataset,
            table=dest_table)

    return gs_name


def gs_to_pandas(project_id, bucket, path):
    """Download gs:// file as a DataFrame in bulk
    Arguments:
    ----------
    project_id: str
        Google's gs:// project id
    bucket: str
        Name of the gs:// in which table is stored
    path: str
        The path including the filename downloads/some_file.csv

    Returns:
    ----------
    df: DataFrame
        The table in a pd.DataFrame Format
    """
    client = storage.Client(project_id)
    bkt = client.get_bucket(bucket)
    blob = bkt.blob(path)
    content = blob.download_as_string()
    df = pd.read_csv(BytesIO(content))

    return df



def query_to_pandas(query, project_id, dataset, dest_table, bucket, bucket_dir,
                    if_exists='replace', block=True):
    '''Download data from a query to google storage. Returns the name of
    the file and location in storage
    Arguments:
    ---------
    query: str
        StandardSQL query to be stored in a table
    project_id: str
        Google's BigQuery project id
    dataset: str
        Name of the dataset in which to store the table
    dest_table: str
        Name of the destination table
    bucket: str
        gs:// bucket to which the file is saved
    bucket_dir: str
        The directory inside a bucket
    if_exists : str (default: 'fail')
        append  - Specifies that rows may be appended to an existing table
        fail    - Specifies that the output table must be empty
        replace - Specifies that write should replace a table
    block: bool, (default: False)
        Specifies whether to wait for job or not

    Returns:
    ---------
    df: DataFrame
        pandas' DataFrame in memory
    '''
    res = query_to_gs(
        query=query,
        project_id=project_id,
        dataset=dataset,
        dest_table=dest_table,
        bucket=bucket,
        bucket_dir=bucket_dir,
        if_exists=if_exists,
        block=block)

    path = "{}/{}".format(bucket_dir, res[1])

    df = gs_to_pandas(
        project_id=project_id,
        bucket=bucket,
        path=path)

    return df
