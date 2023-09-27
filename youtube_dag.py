{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 23,
   "id": "d87c1a7b-6edd-4887-859b-251785139193",
   "metadata": {},
   "outputs": [],
   "source": [
    "from datetime import timedelta\n",
    "from airflow import DAG\n",
    "#from airflow.operators.python.PythonOperator import PythonOperator\n",
    "from airflow.operators.python import PythonOperator\n",
    "#from airflow.utils.data import days_ago\n",
    "from airflow.utils.dates import days_ago\n",
    "from datetime import datetime"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 29,
   "id": "1020730b-3b20-4253-946e-d698c08d84be",
   "metadata": {},
   "outputs": [],
   "source": [
    "from youtube_etl import get_youtube_comments"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 30,
   "id": "112d8f17-1f1e-4bed-b443-930501ea04d4",
   "metadata": {},
   "outputs": [],
   "source": [
    "default_args={\n",
    "    'owner':'airflow',\n",
    "    'depends_on_past': False,\n",
    "    'start_date':datetime(2020,11,8),\n",
    "    'email':['airflow@example.com'],\n",
    "    'email_on_failure':False,\n",
    "    'email_on_retry':False,\n",
    "    'retires':1,\n",
    "    'retry_delay':timedelta(minutes=1)\n",
    "}"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 31,
   "id": "5ea880c6-63c1-4849-9cbc-47e08f1e24f0",
   "metadata": {},
   "outputs": [],
   "source": [
    "dag=DAG(\n",
    "    'youtube_dag',\n",
    "    default_args=default_args,\n",
    "    description='My first etl code'\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 33,
   "id": "af77358a-f1a2-4c23-b640-c4ff191ee569",
   "metadata": {},
   "outputs": [],
   "source": [
    "run_etl=PythonOperator(\n",
    "    task_id='complete_youtube_etl',\n",
    "    python_callable=get_youtube_comments,\n",
    "    dag=dag,\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 34,
   "id": "65db2fd8-5817-4e72-8a48-5fc691238c2f",
   "metadata": {},
   "outputs": [],
   "source": [
    "run_etl"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
