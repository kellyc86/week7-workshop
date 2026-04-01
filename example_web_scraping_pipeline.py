from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime
from datetime import timedelta
import logging


log = logging.getLogger(__name__)


# =============================================================================
# 1. Set up the main configurations of the dag
# =============================================================================

default_args = {
    'start_date': datetime(2022, 2, 14),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'email_on_failure': False,
    'email_on_retry': False,
    'url_to_scrape': Variable.get("example_web_scraping_pipeline", deserialize_json=True)['url_to_scrape'],
    'aws_conn_id': "aws_default",
    'bucket_name': Variable.get("example_web_scraping_pipeline", deserialize_json=True)['bucket_name'],
    's3_key': Variable.get("example_web_scraping_pipeline", deserialize_json=True)['s3_key'],
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('example_web_scraping_pipeline',
          description='Example of a web scraping pipeline that save the outout to a csv file in S3',
          schedule_interval='@weekly',
          catchup=False,
          default_args=default_args,
          max_active_runs=1)

# =============================================================================
# 2. Define different functions
# =============================================================================

def web_scraping_function(**kwargs):

    # Libraries used in a task in Airflow need to be imported inside the function:
    # based on: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code

    # Import packages
    import requests
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin

    # Specify url
    url = kwargs['url_to_scrape']
    base_url = urljoin(url, "/").rstrip("/")

    log.info('Going to scrape data from {0}'.format(url))

    # Package the request, send the request and catch the response: r
    r = requests.get(url)

    # Extracts the response as html: html_doc
    html_doc = r.text

    # create a BeautifulSoup object from the HTML: soup
    soup = BeautifulSoup(html_doc)

    # Find all 'h2' tags (which define hyperlinks): h_tags
    h2_tags = soup.find_all('h2')

    scraped_data = []
    log.info('Going to scrape data from website')

    # Iterate over all the h2 tags found to extract the link and title of the blog posts
    for h2 in h2_tags:
        try:
            if "blog/" in h2.a.get("href"):
                scraped_data.append({"title_post":h2.text, 
                                    "link_post":base_url + h2.a.get("href")})
        except AttributeError as e:
            # Adding the handling of errors
            log.info('Tag was not found')
            continue

    log.info('Finished scraping the data')

    return scraped_data

def s3_save_file_func(**kwargs):

    # Libraries used in a task in Airflow need to be imported inside the function:
    # based on: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code

    import pandas as pd
    import io

    bucket_name = kwargs['bucket_name']
    key = kwargs['s3_key']

    # You can check the documentation of the S3Hook to see the different methods available to interact with S3: 
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/6.0.0/_api/airflow/providers/amazon/aws/hooks/s3/index.html
    s3 = S3Hook(kwargs['aws_conn_id'])

    # Get the task instance
    task_instance = kwargs['ti']

    # Get the output of the bash task
    scraped_data_previous_task = task_instance.xcom_pull(task_ids="web_scraping_task")

    log.info(f'xcom from web_scraping_task:{scraped_data_previous_task}')

    # Load the list of dictionaries with the scraped data from the previous task into a pandas dataframe
    log.info('Loading scraped data into pandas dataframe')
    df = pd.DataFrame.from_dict(scraped_data_previous_task)

    log.info(f'Saving scraped data to {key}')

    # Prepare the file to send to s3
    csv_buffer = io.StringIO()

    # Save the dataframe to a in-memory text stream
    df.to_csv(csv_buffer, index=False)

    # The getvalue() method gets the entire content of the file at once as a string
    data = csv_buffer.getvalue()

    print("Saving CSV file")

    # Write the file to S3 bucket in specific path defined in key
    # replace=True to replace the file if it already exists in the s3 path in the bucket
    # encoding='utf-8' to avoid encoding issues once the string is loaded into s3 as a binary file
    s3.load_string(data, key, bucket_name, replace=True, encoding='utf-8')


    log.info('Finished saving the scraped data to s3')


# =============================================================================
# 3. Set up the main configurations of the dag
# =============================================================================

web_scraping_task = PythonOperator(
    task_id='web_scraping_task',
    provide_context=True,
    python_callable=web_scraping_function,
    op_kwargs=default_args,
    dag=dag,

)

save_scraped_data_to_s3_task = PythonOperator(
    task_id='save_scraped_data_to_s3_task',
    provide_context=True,
    python_callable=s3_save_file_func,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,

)

# =============================================================================
# 4. Indicating the order of the tasks
# =============================================================================

web_scraping_task >> save_scraped_data_to_s3_task