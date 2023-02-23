FROM apache/airflow:latest
ENV JSON_KEY_BQ='credentials.json'
COPY requirements.txt /requirements.txt

USER root
RUN sudo apt-get update
RUN sudo apt-get -y install gcc
RUN sudo apt-get install -y libgdal-dev g++ --no-install-recommends 
RUN sudo apt-get clean -y

USER ${AIRFLOW_UID}

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
RUN pip install protobuf==3.19.0