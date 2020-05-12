from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import tweepy
from tweepy.error import TweepError, RateLimitError
from pandas import pandas as pd
from airflow.operators.python_operator import PythonOperator

dict_key = [{
            "consumer_key":"xxxxxxxxxxxxxxxx",
            "consumer_sec" : "xxxxxxxxxxxxxxx",
            "acc_token" : "xxxxxxxxxxxxxxxxxx",
            "acc_sec" : "xxxxxxxxxxxxxxxxxxxxxx",
        },
        ]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['email@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Twitter_Flow_Crawling',
    default_args=default_args,
    description='A Flow data Twitter',
    schedule_interval=timedelta(days=1),
)

        
        
def get_auth():
    code_key = 0
    auth = tweepy.OAuthHandler(dict_key[code_key]["consumer_key"], dict_key[code_key]["consumer_sec"])
    auth.set_access_token(dict_key[code_key]["acc_token"], dict_key[code_key]["acc_sec"])
    api = tweepy.API(auth, wait_on_rate_limit=True) 
    return api

def get_data_search(**kwargs):
    data = []
    query = "jokowi"
    api = get_auth()
    cursor = tweepy.Cursor(api.search, result_type='mixed', q=query, rpp=200, include_entities=True).items()
    c = 0
    for x in cursor:
        c = c + 1
        print(x)
        data.append(x)
        if c == 100:
            break
    return data

def parse_data(**context):
    all_data = []
    value = context['task_instance'].xcom_pull(task_ids='crawling_data')
    for tweet in value:
        print(tweet)
        dict_line = {
                    "createdAt": str(tweet.created_at),
                    "twitId": tweet.id,
                    "twitContent": str(tweet.text.encode('ascii', 'ignore').decode("ascii")),
                    "userId": tweet.user.id,
                    "userName": tweet.user.screen_name, #tweet.user.name.encode('ascii', 'ignore').decode("ascii"),
                    "location": tweet.user.location,
                    "retweet": tweet.retweet_count,
                    "like": tweet.favorite_count,
                    "userImg": tweet.user.profile_image_url_https,
                    "tag": "",
                    "type": "",
                    "profile": tweet.user.profile_image_url_https,
                    "screen_name":tweet.user.screen_name,
                    "description":tweet.user.description,
                    "url":tweet.user.url,
                    "followers_count":tweet.user.followers_count,
                    "friends_count":tweet.user.friends_count,
                    "protected":tweet.user.protected,
                    "listed_count":tweet.user.listed_count,
                    "created_at":str(tweet.user.created_at),
                    "verified":tweet.user.verified,
                    "statuses_count":tweet.user.statuses_count,
                }
        all_data.append(dict_line)
    return all_data

def save_data(**context):
    value = context['task_instance'].xcom_pull(task_ids='parsing_data')
    Ad = pd.DataFrame(value)
    Ad.to_csv("marwan.csv")

t1 = PythonOperator(
        task_id='crawling_data',
        python_callable=get_data_search,
        # provide_context=True,
        dag=dag)

t2 = PythonOperator(
        task_id='parsing_data',
        python_callable=parse_data,
        provide_context=True,
        dag=dag)
t3 = PythonOperator(
        task_id='save_data',
        python_callable=save_data,
        provide_context=True,
        dag=dag)

t1 >> t2 >> t3


        