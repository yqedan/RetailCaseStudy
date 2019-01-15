#!/bin/bash

# update apt
sudo apt-get update

# install java
sudo apt-get install -y default-jdk

# python 3 installs
sudo curl https://bootstrap.pypa.io/get-pip.py | sudo python3
sudo pip install boto3
sudo pip install --upgrade keyrings.alt
sudo pip install --upgrade snowflake-connector-python

# spark install
wget https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
tar -xvf spark-2.4.0-bin-hadoop2.7.tgz
rm spark-2.4.0-bin-hadoop2.7.tgz
mv spark-2.4.0-bin-hadoop2.7 spark
sudo mv spark /usr/lib

# edit the bashrc file (adds some env variables and edits path)
cp ~/.bashrc ~/.oldbashrc
echo 'export SPARK_HOME=/usr/lib/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=python3' >> ~/.bashrc
. ~/.bashrc

# MySQL (for food mart table)
sudo apt-get install -y mysql-server
sudo service mysql start
sudo mysql << EOF
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password by 'root';
flush privileges;
EOF
mysql -u root -proot << EOF
CREATE DATABASE food_mart;
EOF
wget http://pentaho.dlpage.phi-integration.com/mondrian/mysql-foodmart-database/foodmart_mysql.tar.gz
tar -xvf foodmart_mysql.tar.gz
rm foodmart_mysql.tar.gz
mysql -u root -proot food_mart < foodmart_mysql.sql
rm foodmart_mysql.sql

# postgres (for airflow db usage)
sudo apt-get install -y postgresql postgresql-contrib
sudo service postgresql start
sudo -u postgres psql -c "CREATE USER airflow WITH PASSWORD 'airflow';"
sudo -u postgres createdb airflow

# airflow
sudo AIRFLOW_GPL_UNIDECODE=yes pip install apache-airflow[postgres]

# update airflow config to use postgres instead of sqlite
airflow initdb # creates the file structure and config file
cp ~/airflow/airflow.cfg ~/airflow/oldairflow.cfg
sed -i 's?sql_alchemy_conn = sqlite:////home/'$USER'/airflow/airflow.db?sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow?' ~/airflow/airflow.cfg
sed -i 's/executor = SequentialExecutor/executor = LocalExecutor/' ~/airflow/airflow.cfg
rm ~/airflow/airflow.db
airflow initdb

printf '\nInstallation Complete\n\n\a'
