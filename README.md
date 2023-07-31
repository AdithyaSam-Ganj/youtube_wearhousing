# YouTube Data Harvesting and Warehousing using Python, SQL, MongoDB, and Streamlit

###Introduction

YouTube Data Harvesting and Warehousing is a project that aims to allow users to access and analyze data from multiple YouTube channels. The project utilizes SQL, MongoDB, and Streamlit to create a user-friendly application that allows users to retrieve, store, and query YouTube channel and video data.


###Project Overview

The YouTube Data Harvesting and Warehousing project consists of the following components:

Streamlit Application: A user-friendly UI built using Streamlit library, allowing users to interact with the application and perform data retrieval and analysis tasks.
YouTube API Integration: Integration with the YouTube API to fetch channel and video data based on the provided channel ID.
MongoDB Data Lake: Storage of the retrieved data in a MongoDB database, providing a flexible and scalable solution for storing unstructured and semi-structured data.
SQL Data Warehouse: Migration of data from the data lake to a SQL database, allowing for efficient querying and analysis using SQL queries.
Data Visualization: Presentation of retrieved data using Streamlit's data visualization features, enabling users to analyze the data through charts and graphs.

###Guide for Users and Developers 

1. Tools Install
Jupyter notebook.
Python 3.11
MySQL.
MongoDB.
Youtube API key.

3. Requirement Libraries to Install
pip install google-api-python-client, pymongo, mysql-connector-python, sqlalchemy, pymysql, pymysql, pandas, numpy, plotly-express, streamlit.
( pip install google-api-python-client pymongo mysql-connector-python sqlalchemy pymysql pandas numpy plotly-express streamlit )

4. Import Libraries
    Youtube API libraries
    
    import googleapiclient.discovery
    from googleapiclient.discovery import build
    File handling libraries
    
    import json
    import re
    MongoDB
    
    import pymongo
   
    SQL libraries
    
    import mysql.connector
    import sqlalchemy
    from sqlalchemy import create_engine
    import pymysql
    pandas, numpy
    
    import pandas as pd
    import numpy as np
   
    Dashboard libraries
    
    import streamlit as st
    import plotly.express as px
   
4. E T L Process
    a) Extract data
    Extract the particular youtube channel data by using the youtube channel id provided by the user, with the help of the youtube API developer console. The user has to use his own API key. 
    b) Process and Transform the data
    After the extraction process, takes the extraction data and transform it into JSON format.
    c) Load data
    After the transformation process, the JSON format data is stored in the MongoDB database.
   d) There is an option to migrate the data to MySQL database from the MongoDB database in the Channel Analytics tab.
   Please note that you have to change the password from 'root' to your root user password in the source code if necessary. 

6. E D A Process and Framework
  a) Access MySQL DB
  Create a connection to the MySQL server and access the specified MySQL DataBase by using pymysql library and access tables.
  b) Filter the data



###References

Streamlit Documentation: https://docs.streamlit.io/
YouTube API Documentation: https://developers.google.com/youtube
MongoDB Documentation: https://docs.mongodb.com/
SQLAlchemy Documentation: https://docs.sqlalchemy.org/
Python Documentation: https://docs.python.org/
Matplotlib Documentation: https://matplotlib.org/
  Filter and process the collected data from the tables depending on the given requirements by using SQL queries and transform the processed data into a DataFrame format.
  c) Visualization
  Finally, create a Dashboard by using Streamlit and give dropdown options on the Dashboard to the user and select a question from that menu to analyse the data and show the output in Dataframe Table and Bar chart.
