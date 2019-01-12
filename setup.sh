#!/bin/bash

# install java (takes a long time so get it out of the way first!)
sudo apt-get update
sudo apt-get install -y default-jdk

# python 2.7 and pip (optional but you will need to use pip3 for airflow install if you skip)
sudo apt install -y python2.7
sudo apt install -y python-pip
sudo curl https://bootstrap.pypa.io/get-pip.py | sudo python

# spark install
wget http://ftp.wayne.edu/apache/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
tar -xvf spark-2.4.0-bin-hadoop2.7.tgz
rm spark-2.4.0-bin-hadoop2.7.tgz
mv spark-2.4.0-bin-hadoop2.7 spark
sudo mv spark /usr/lib

# edit the .bashrc (dont set the env for PYSPARK_PYTHON if you are going to run pyspark with python 2.7)
cp ~/.bashrc ~/.oldbashrc
echo 'export JAVA_HOME=/usr/lib/jvm/default-java
export SPARK_HOME=/usr/lib/spark
export PATH=$PATH:$JAVA_HOME:$SPARK_HOME/bin
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

# postgres (for airflow db usage)
sudo apt-get install -y postgresql postgresql-contrib
sudo service postgresql start
sudo -u postgres psql -c "CREATE USER airflow WITH PASSWORD 'airflow';"
sudo -u postgres createdb airflow

# airflow
sudo apt-get install -y libmysqlclient-dev
sudo apt-get install -y libssl-dev
sudo apt-get install -y libkrb5-dev
sudo apt-get install -y libsasl2-dev
sudo AIRFLOW_GPL_UNIDECODE=yes pip install apache-airflow
pip install psycopg2-binary

# update airflow config to use postgres instead of sqlite
airflow initdb
cp ~/airflow/airflow.cfg ~/airflow/oldairflow.cfg
sed -i 's?sql_alchemy_conn = sqlite:////home/'$USER'/airflow/airflow.db?sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow?' airflow/airflow.cfg
sed -i 's/executor = SequentialExecutor/executor = LocalExecutor/' airflow/airflow.cfg
rm airflow/airflow.db
airflow initdb

# needed for the foolowing python 2 or 3 installs (snowflake connector)
sudo apt-get install -y libssl-dev libffi-dev

# python 2.7 installs
pip install boto3
sudo ~/.local/bin/pip install --upgrade snowflake-connector-python

# python 3 installs (airflow works across python 2 or 3 but not boto3 or snowflake connector)
sudo apt install python3-pip
sudo curl https://bootstrap.pypa.io/get-pip.py | sudo python3
pip3 install boto3
sudo pip3 install --upgrade keyrings.alt
sudo pip3 install --upgrade snowflake-connector-python

printf '\n\n\nInstallation Complete\n\n\n'
