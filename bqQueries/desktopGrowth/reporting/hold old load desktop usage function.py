def load_desktop_usage_data(data, load_project, load_dataset_id, load_table_name, next_load_date):
    '''
    Loads dataset previously read into a permanent bigquery table
    :param load_project: Project to write dataset to
    :param load_dataset_id: Dataset that has table to be read
    :param read_table_name: Name of table to be read
    :param next_load_date: Desired date
    :return: data for loading into table
        '''

    # Change credentials to marketing analytics credentials
    # TODO: Change path from local to environment variable
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics' \
                                                   '/App Engine - moz-mktg-prod-001/moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'

    # Set dates required for loading new data
    next_load_date = datetime.strftime(next_load_date, '%Y%m%d')
    logging.info(f'{job_name}: Starting load for next load date: {next_load_date}')
    client = bigquery.Client(project=load_project)
    load_dataset_id = load_dataset_id
    load_table_name = load_table_name
    load_table_suffix = next_load_date
    load_table_id = f'{load_table_name.lower()}_{load_table_suffix}'

    # Configure load job
    dataset_ref = client.dataset(load_dataset_id)
    table_ref = dataset_ref.table(load_table_id)
    load_job_config = bigquery.QueryJobConfig()  # load job call
    load_job_config.schema = [
        bigquery.SchemaField('submission', 'DATE'),
        bigquery.SchemaField('country', 'STRING'),
        bigquery.SchemaField('source', 'STRING'),
        bigquery.SchemaField('medium', 'STRING'),
        bigquery.SchemaField('campaign', 'STRING'),
        bigquery.SchemaField('content', 'STRING'),
        bigquery.SchemaField('distribution_id', 'STRING'),
        bigquery.SchemaField('dau', 'INTEGER'),
        bigquery.SchemaField('mau', 'INTEGER'),
        bigquery.SchemaField('installs', 'INTEGER')
    ]  # Define schema
    load_job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='submission',
    )
    load_job_config.write_disposition = 'WRITE_TRUNCATE'  # Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    load_job_config.destination = table_ref

    # Run Load Job
    query_job = client.query(
        data,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location='US',
        job_config=load_job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    logging.info(f'{job_name}: Query results loaded to table {table_ref.path}')

    return None