import datetime
import os
import requests
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    DateTime,
    Float,
    bindparam,
)
from tabulate import tabulate


os.environ["no_proxy"] = "*"

CONNECTION_STRING = "mysql://Airflow:1@localhost/spark"
ENGINE = None
METADATA = None


@dag(
    dag_id="graduate_work",
    schedule="@once",
    start_date=pendulum.datetime(2024, 4, 4, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)


def Reestr_analityc():  

    @task(task_id="generate_reestr_table")
    def generate_reestr_table(**kwargs):
        ti = kwargs["ti"]

        global ENGINE
        global METADATA

        if ENGINE is None:
            ENGINE = create_engine(CONNECTION_STRING)

        df = pd.read_sql_query("select Project_name, Date_start, days_count, Quantity, Total_cost from reestr_proektov_work WHERE Date_start BETWEEN '2024-03-01' AND '2024-03-31' LIMIT 30", ENGINE)

        ti.xcom_push(
            key="reestr_table",
            value=df.to_markdown(index=False),
        )

    send_reestr_message_telegram_task = TelegramOperator(
        task_id="send_reestr_message_telegram",
        telegram_conn_id="telegram_default",
        token="6637949636:AAEyorna86B6BJficJAVhT51pbLzYI5_oHk",
        chat_id="1065793816",
        text="Сводная таблица задач за март"
        + "\n```code"
        + "\n{{ ti.xcom_pull(task_ids=['generate_reestr_table'], key='reestr_table')[0] }}"
        + "\n```",        
        telegram_kwargs={
            "parse_mode": "Markdown",
        },
    )
    
    @task(task_id="generate_revenue")
    def generate_revenue(**kwargs):
        ti = kwargs["ti"]

        global ENGINE
        global METADATA

        if ENGINE is None:
            ENGINE = create_engine(CONNECTION_STRING)

        revenue = pd.read_sql_query("SELECT SUM(Total_cost) FROM reestr_proektov_work WHERE Date_start BETWEEN '2024-03-01' AND '2024-03-31'", ENGINE)

        ti.xcom_push(
            key="revenue",
            value=revenue.to_markdown(index=False),
        )
        
    @task(task_id="count_quantity")
    def count_quantity(**kwargs):
        ti = kwargs["ti"]

        global ENGINE
        global METADATA

        if ENGINE is None:
            ENGINE = create_engine(CONNECTION_STRING)

        count_quantity = pd.read_sql_query("SELECT COUNT(Quantity) FROM reestr_proektov_work WHERE Date_start BETWEEN '2024-03-01' AND '2024-03-31'", ENGINE)

        ti.xcom_push(
            key="count_quantity",
            value=count_quantity.to_markdown(index=False),
        )
        
    @task(task_id="average_revenue")
    def average_revenue(**kwargs):
        ti = kwargs["ti"]

        global ENGINE
        global METADATA

        if ENGINE is None:
            ENGINE = create_engine(CONNECTION_STRING)

        average_revenue = pd.read_sql_query("SELECT SUM(Total_cost) / COUNT(Quantity) AS average_revenue FROM reestr_proektov_work WHERE Date_start BETWEEN '2024-03-01' AND '2024-03-31'", ENGINE)

        ti.xcom_push(
            key="average_revenue",
            value=average_revenue.to_markdown(index=False),
        )
        
    @task(task_id="average_time")
    def average_time(**kwargs):
        ti = kwargs["ti"]

        global ENGINE
        global METADATA

        if ENGINE is None:
            ENGINE = create_engine(CONNECTION_STRING)

        average_time = pd.read_sql_query("SELECT SUM(days_count) / COUNT(Quantity) AS average_time FROM reestr_proektov_work WHERE Date_start BETWEEN '2024-03-01' AND '2024-03-31'", ENGINE)

        ti.xcom_push(
            key="average_time",
            value=average_time.to_markdown(index=False),
        )
        
    @task(task_id="productivity")
    def productivity(**kwargs):
        ti = kwargs["ti"]

        global ENGINE
        global METADATA

        if ENGINE is None:
            ENGINE = create_engine(CONNECTION_STRING)

        productivity = pd.read_sql_query("SELECT SUM(Total_cost) / SUM(days_count) AS productivity FROM reestr_proektov_work WHERE Date_start BETWEEN '2024-03-01' AND '2024-03-31'", ENGINE)

        ti.xcom_push(
            key="productivity",
            value=productivity.to_markdown(index=False),
        )
        
    send_analytic_telegram_task = TelegramOperator(
        task_id="send_analytic_message_telegram",
        telegram_conn_id="telegram_default",
        token="6637949636:AAEyorna86B6BJficJAVhT51pbLzYI5_oHk",
        chat_id="1065793816",
        text="Основные показатели за месяц"
        + "\nВыручка (рублей)"        
        + "\n```code"
        + "\n{{ ti.xcom_pull(task_ids=['generate_revenue'], key='revenue')[0] }}"
        + "\n```"
        + "\nКоличество задач (шт)"        
        + "\n```code"
        + "\n{{ ti.xcom_pull(task_ids=['count_quantity'], key='count_quantity')[0] }}"
        + "\n```"
        + "\nСредний чек (рублей)"        
        + "\n```code"
        + "\n{{ ti.xcom_pull(task_ids=['average_revenue'], key='average_revenue')[0] }}"
        + "\n```"
        + "\nСреднее время выполнения задачи (дней)"        
        + "\n```code"
        + "\n{{ ti.xcom_pull(task_ids=['average_time'], key='average_time')[0] }}"
        + "\n```"
        + "\nСредняя производительность (рублей в день)"        
        + "\n```code"
        + "\n{{ ti.xcom_pull(task_ids=['productivity'], key='productivity')[0] }}"
        + "\n```",
        telegram_kwargs={
            "parse_mode": "Markdown",
        },
    )

    (        
        generate_reestr_table() >> send_reestr_message_telegram_task,
        generate_revenue() >> count_quantity() >> average_revenue() >> average_time() >> productivity() >> send_analytic_telegram_task,
    )


dag = Reestr_analityc()