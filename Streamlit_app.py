# ==================================================       /     IMPORT LIBRARY    /      =================================================== #

# [Youtube API libraries]
import googleapiclient.discovery
from googleapiclient.discovery import build

# [File handling libraries]
import json
import re

# [MongoDB]
import pymongo

# [SQL libraries]
#import mysql.connector
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql

# [pandas, numpy]
import pandas as pd
import numpy as np

# [Dash board libraries]
import streamlit as st
import plotly.express as px

import streamlit as st 

# ===================================================== / All necessary function for extracting data from youtube and storing it in dictionary file / ========= # 

# Function to check if the youtube id is vaild 
def check_valid_ids(youtube,channel_ids):
    try :
        try:
            channel_request = youtube.channels().list(
                part = 'snippet,statistics,contentDetails',
                id = channel_ids)
            
            channel_response = channel_request.execute()

            if 'items' not in channel_response:
                st.write(f"Invalid channel id: {channel_id}")
                st.error("Enter the correct 11-digit **channel_id**")
                return None
                    
        except HttpError as e: 
            st.error('Server error (or) Check your internet connection (or) Please Try again after a few minutes', icon='🚨')
            st.write('An error occurred: %s' % e)
            return None
            
    except:
        st.write('You have exceeded your YouTube API quota. Please try again tomorrow.')

## Function to get Channel Statistics 
## Function to get Channel Statistics 
def get_channel_stats(youtube,channel_ids):

    request = youtube.channels().list(
                part = 'snippet,contentDetails,statistics',
                id = channel_ids)
    response  = request.execute()

    all_data = []
    
    for i in range(len(response['items'])):
        data = dict(Channel_Name = response['items'][i]['snippet']['title'],
                    Subscriber_count = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_video_count = response['items'][i]['statistics']['videoCount'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Channel_description = response['items'][0]['snippet']['description'],
                    Channel_Id = channel_ids
                   )
        all_data.append(data)    
    
    return all_data   

## Function to get video ids from playlist 
def get_video_ids(youtube, playlist_ids):

    request = youtube.playlistItems().list(
                    part = 'contentDetails',
                    playlistId = playlist_ids,
                    maxResults = 50)
    response = request.execute()

    video_ids = []

    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True
    
    while more_pages: 
        if next_page_token is None:
            more_pages = False 
        else:
            request = youtube.playlistItems().list(
                            part = 'contentDetails',
                            playlistId = playlist_ids,
                            maxResults = 50,
                            pageToken = next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')
    return video_ids 

## Function to get max_comment number of comments for a video 
def get_video_comments(youtube, video_id, max_comments):
    
    request = youtube.commentThreads().list(
        part='snippet',
        maxResults=max_comments,
        textFormat="plainText",
        videoId=video_id)
    response = request.execute()

    comments = []
    
    for i in range(len(response['items'])):
        data = dict(Comment_Id = response['items'][i]['id'],
                    Comment_Text = response['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'],
                    Author_name =  response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    PublishedAt = response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'] )
        comments.append(data)
    
    return comments

## Define a function to convert duration
def convert_duration(duration):
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(regex, duration)
    if not match:
        return '00:00:00'
    hours, minutes, seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))

def get_video_details(youtube, video_ids):

    all_video_stats =[]
    for i in range(0,len(video_ids),50):
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id = ','.join(video_ids[i:i+50]))
        response = request.execute()     

        k = i 
        for video in response['items']:

            try : 
                comments_list = get_video_comments(youtube, video_ids[k],2)
            except: 
                comments_list = None 
            
            video_duration = convert_duration(video['contentDetails']['duration'])
            
            video_stats = dict(Video_id = video_ids[k],
                               Title = video['snippet']['title'],
                               Published_date = video['snippet']['publishedAt'],
                               Views=video['statistics']['viewCount'],
                               Likes=video['statistics']['likeCount'],
                               CommentsCount = video['statistics'].get('commentCount'),
                               Video_Description = video['snippet']['description'],
                               #tags = video['snippet'].get('tags'),
                               favouritecount = video['statistics']['favoriteCount'],
                               duration = video_duration,
                               caption_status = video['contentDetails']['caption'],
                               comments = comments_list)
            all_video_stats.append(video_stats)
            k += 1
            
    return all_video_stats 
    

#==================================================  / Streamlit Dashboard and main function / ===========================================================

st.title('Youtube Data Harvesting and Analysis')

add_sidebar = st.sidebar.selectbox(
    "Welcome to Youtube Data Analysis APP ",
    ("Harvest Channel Data", "Channel Data Analytics")
)


if add_sidebar == "Harvest Channel Data":
    default_channel = 'UCZxt6ue2cndNbJc3uwnQSPg'
    st.header('Data Collection Zone')
    st.write('Welcome to the data Collection Zone.')
    st.write('Please enter the channel ID of your desired Youtube Channel ID in box provided below.')
    
    cannel_id = st.text_input('Enter Channel ID')
    st.write('Enter Your API Key')
    api_key = st.text_input('Enter API Key')
    mongodb_get = st.button('Get Data and Store it in MongoDB Data Lake')
    #api_key = ''
    #channel_id = default_channel
    youtube = build('youtube','v3',developerKey = api_key)
    
    if mongodb_get == True:
        st.write(channel_id)

        check_valid_ids(youtube,channel_id)
        channel_statistics = get_channel_stats(youtube,channel_id) 
        playlist_id = channel_statistics[0]['Playlist_id']        
        video_ids = get_video_ids(youtube,playlist_id)
        video_details = get_video_details(youtube,video_ids)
        channel_data = dict(Channel_name = channel_statistics[0]['Channel_Name'],
                    Channel_statistics = channel_statistics,
                    Video_deatils = video_details)
        st.write(channel_data)

 # =================================================   /   MongoDB connection and storing the collected data   /    ========================================= #

        # create a client instance of MongoDB
        client = pymongo.MongoClient('mongodb://localhost:27017/')
        
        # create a database or use existing one
        mydb = client['GUVI']
        
        # create a collection
        collection = mydb['Youtube_data']

        # insert or update data in the collection
        upload = collection.replace_one({'_id': channel_id}, channel_data, upsert=True)

        # print the result of the insertion operation
        st.write(f"Updated document id: {upload.upserted_id if upload.upserted_id else upload.modified_count}")

        # Close the connection
        client.close()


#=========================================  Analytics Data Thingy ===============================================================================
if add_sidebar == "Channel Data Analytics":
    st.header('Channel Data Analysis')
    st.write('(Note : Please run the "Harvest Channel Data" Section Before Performing any Analysis Here.)')
    # create a client instance of MongoDB
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    
    # create a database or use existing one
    mydb = client['GUVI']
    
    # create a collection
    collection = mydb['Youtube_data']

    # Collect all document names and give them
    document_names = []
    for document in collection.find():
        document_names.append(document['Channel_name'])
    document_name = st.selectbox('**Select Channel name**', options = document_names, key='document_names')
    result = collection.find_one({"Channel_name": document_name})
    st.write(result)
    st.session_state['result'] = result
    
    client.close()

    st.write('''Click Below to Migrate MongoD database to MYSQL database''')
    Migrate = st.button('**Migrate to MySQL**')

    if Migrate:
        result = st.session_state['result']
        # ----------------------------- Data conversion --------------------- #
        # Channel data json to df
        channel_details = {
            "Channel_Name": result['Channel_name'],
            "Channel_Id": result['_id'],
            "Video_Count": result['Channel_statistics'][0]['Total_video_count'],
            "Subscriber_Count": result['Channel_statistics'][0]['Subscriber_count'],
            "Channel_Views": result['Channel_statistics'][0]['Views'],
            "Channel_Description": result['Channel_statistics'][0]['Channel_description'],
            "Playlist_Id": result['Channel_statistics'][0]['Playlist_id']
            }
        channel_df = pd.DataFrame.from_dict(channel_details, orient='index').T
        
        # playlist data json to df
        playlist = {"Channel_Id": result['_id'],
                        "Playlist_Id": result['Channel_statistics'][0]['Playlist_id']
                        }
        playlist_df = pd.DataFrame.from_dict(playlist, orient='index').T

        # video data json to df
        video_details_list = []
        for i in range(1,len(result['Video_deatils'])):
            video = {
                'Playlist_Id':result['Channel_statistics'][0]['Playlist_id'],
                'Video_Id': result['Video_deatils'][i]['Video_id'],
                'Video_Name': result['Video_deatils'][i]['Title'],
                'Video_Description': result['Video_deatils'][i]['Video_Description'],
                'Published_date': result['Video_deatils'][i]['Published_date'],
                'View_Count': result['Video_deatils'][i]['Views'],
                'Like_Count': result['Video_deatils'][i]['Likes'],
                'Favorite_Count': result['Video_deatils'][i]['favouritecount'],
                'Comment_Count': result['Video_deatils'][i]['CommentsCount'],
                'Duration': result['Video_deatils'][i]['duration'],
                'Caption_Status': result['Video_deatils'][i]['caption_status'],
                }
            video_details_list.append(video)
        video_df = pd.DataFrame(video_details_list)

        # Comment data json to df
        Comment_list = []
        for i in range(1,len(result['Video_deatils'])):
            
            comments = result['Video_deatils'][i]['comments']
            
            if comments == None or comments == []:
                pass
            else:
                for j in comments:
                    Comment_details = {
                    'Video_Id': result['Video_deatils'][i]['Video_id'],
                    'Comment_Id': j['Comment_Id'],
                    'Comment_Text': j['Comment_Text'],
                    'Comment_Author': j['Author_name'],
                    'Comment_Published_date': j['PublishedAt'],
                    }
                    Comment_list.append(Comment_details)
        Comments_df = pd.DataFrame(Comment_list)

# Connect to the MySQL server

        # Creating connection object
        connect = mysql.connector.connect(
            host = 'localhost',
            user = "root",
            password = "root"
        )

        # Create a new database and use
        mycursor = connect.cursor()
        mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")

        # Close the cursor and database connection
        mycursor.close()
        connect.close()
        st.write(connect)

        # Connect to the new created database
        engine = create_engine('mysql+mysqlconnector://root:root@localhost/youtube_db',  pool_pre_ping=True)

        
        # Use pandas to insert the DataFrames data to the SQL Database -> table1
        # Channel data to SQL
        channel_df.to_sql('channel', engine, if_exists='append', index=False,
                        dtype = {"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                                "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                "Video_Count": sqlalchemy.types.INT,
                                "Subscriber_Count": sqlalchemy.types.BigInteger,
                                "Channel_Views": sqlalchemy.types.BigInteger,
                                "Channel_Description": sqlalchemy.types.TEXT,
                                "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),})
        

        # Playlist data to SQL
        playlist_df.to_sql('playlist', engine, if_exists='append', index=False,
                        dtype = {"Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                    "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),})
        
        # Commend data to SQL
        Comments_df.to_sql('comments', engine, if_exists='append', index=False,
                        dtype = {'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                                'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                                'Comment_Text': sqlalchemy.types.TEXT,
                                'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                                'Comment_Published_date': sqlalchemy.types.String(length=50),})


        # Video data to SQL
        video_df = video_df[['Playlist_Id','Video_Id','Video_Name','Video_Description','Published_date','Like_Count',
                             'Favorite_Count','Duration','Comment_Count','View_Count']]
        st.write(video_df)
        video_df.to_sql('video', engine, if_exists='append', index=False,
                    dtype = {'Playlist_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Video_id': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Name': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Description': sqlalchemy.types.VARCHAR(length=225),
                            'Published_date': sqlalchemy.types.String(length=50),
                            'Like_Count': sqlalchemy.types.BigInteger,
                            'Favorite_Count': sqlalchemy.types.INT,
                            'Duration': sqlalchemy.types.VARCHAR(length=1024),
                             'Comment_Count': sqlalchemy.types.INT,
                            'View_Count': sqlalchemy.types.BigInteger,})
       
        video_df.to_sql('video', engine, if_exists='append', index=False,
                    dtype = {'Playlist_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Video_id': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Name': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Description': sqlalchemy.types.VARCHAR(length=max),
                            'Published_date': sqlalchemy.types.String(length=50),
                            'View_Count': sqlalchemy.types.BigInteger,
                            'Like_Count': sqlalchemy.types.BigInteger,
                            'Favorite_Count': sqlalchemy.types.INT,
                            'Comment_Count': sqlalchemy.types.INT,
                            'Duration': sqlalchemy.types.VARCHAR(length=1024),
                            'Caption_Status': sqlalchemy.types.VARCHAR(length=225),}) 
        st.write('Successfully Migrted Data')

# ====================================================   /     Channel Analysis zone     /   ================================================= #
        
    # Check available channel data
    Check_channel = st.checkbox('**Check available channel data for analysis**')
    
    if Check_channel:
       # Create database connection
        engine = create_engine('mysql+mysqlconnector://root:root@localhost/youtube_db', echo=False)
        # Execute SQL query to retrieve channel names
        query = "SELECT Channel_Name FROM channel;"
        results = pd.read_sql(query, engine)
        # Get channel names as a list
        channel_names_fromsql = list(results['Channel_Name'])
        # Create a DataFrame from the list and reset the index to start from 1
        df_at_sql = pd.DataFrame(channel_names_fromsql, columns=['Available channel data']).reset_index(drop=True)
        # Show dataframe
        st.dataframe(df_at_sql)

        
            # Selectbox creation
        question_tosql = st.selectbox('**Select your Question**',
        ('1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'), key = 'collection_question')
        
        # Creat a connection to SQL
        connect_for_question = pymysql.connect(host='localhost', user='root', password='root', db='youtube_db')
        cursor = connect_for_question.cursor()
        
        # Q1
        if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
            cursor.execute("SELECT channel.Channel_Name, video.Video_Name FROM channel JOIN playlist JOIN video ON channel.Channel_Id = playlist.Channel_Id AND playlist.Playlist_Id = video.Playlist_Id;")
            result_1 = cursor.fetchall()
            df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
            df1.index += 1
            st.dataframe(df1)
        
        # Q2
        elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
        
            col1,col2 = st.columns(2)
            with col1:
                cursor.execute("SELECT Channel_Name, Video_Count FROM channel ORDER BY Video_Count DESC;")
                result_2 = cursor.fetchall()
                df2 = pd.DataFrame(result_2,columns=['Channel Name','Video Count']).reset_index(drop=True)
                df2.index += 1
                st.dataframe(df2)
        
            with col2:
                fig_vc = px.bar(df2, y='Video Count', x='Channel Name', text_auto='.2s', title="Most number of videos", )
                fig_vc.update_traces(textfont_size=16,marker_color='#E6064A')
                fig_vc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
                st.plotly_chart(fig_vc,use_container_width=True)
        
        # Q3
        elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':
        
            col1,col2 = st.columns(2)
            with col1:
                cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.View_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.View_Count DESC LIMIT 10;")
                result_3 = cursor.fetchall()
                df3 = pd.DataFrame(result_3,columns=['Channel Name', 'Video Name', 'View count']).reset_index(drop=True)
                df3.index += 1
                st.dataframe(df3)
        
            with col2:
                fig_topvc = px.bar(df3, y='View count', x='Video Name', text_auto='.2s', title="Top 10 most viewed videos")
                fig_topvc.update_traces(textfont_size=16,marker_color='#E6064A')
                fig_topvc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
                st.plotly_chart(fig_topvc,use_container_width=True)
        
        # Q4 
        elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
            cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id;")
            result_4 = cursor.fetchall()
            df4 = pd.DataFrame(result_4,columns=['Channel Name', 'Video Name', 'Comment count']).reset_index(drop=True)
            df4.index += 1
            st.dataframe(df4)
        
        # Q5
        elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Like_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Like_Count DESC;")
            result_5= cursor.fetchall()
            df5 = pd.DataFrame(result_5,columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
            df5.index += 1
            st.dataframe(df5)
        
        # Q6
        elif question_tosql == '6. What is the total number of likes for each video, and what are their corresponding video names?':
            st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
            cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Like_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Like_Count DESC;")
            result_6= cursor.fetchall()
            df6 = pd.DataFrame(result_6,columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
            df6.index += 1
            st.dataframe(df6)
        
        # Q7
        elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        
            col1, col2 = st.columns(2)
            with col1:
                cursor.execute("SELECT Channel_Name, Channel_Views FROM channel ORDER BY Channel_Views DESC;")
                result_7= cursor.fetchall()
                df7 = pd.DataFrame(result_7,columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
                df7.index += 1
                st.dataframe(df7)
            
            with col2:
                fig_topview = px.bar(df7, y='Total number of views', x='Channel Name', text_auto='.2s', title="Total number of views", )
                fig_topview.update_traces(textfont_size=16,marker_color='#E6064A')
                fig_topview.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
                st.plotly_chart(fig_topview,use_container_width=True)
        
        # Q8
        elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
            cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Published_date FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id  WHERE EXTRACT(YEAR FROM Published_date) = 2022;")
            result_8= cursor.fetchall()
            df8 = pd.DataFrame(result_8,columns=['Channel Name','Video Name', 'Year 2022 only']).reset_index(drop=True)
            df8.index += 1
            st.dataframe(df8)
        
        # Q9
        elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            cursor.execute("SELECT channel.Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.Duration)))), '%H:%i:%s') AS duration  FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id GROUP by Channel_Name ORDER BY duration DESC ;")
            result_9= cursor.fetchall()
            df9 = pd.DataFrame(result_9,columns=['Channel Name','Average duration of videos (HH:MM:SS)']).reset_index(drop=True)
            df9.index += 1
            st.dataframe(df9)
        
        # Q10
        elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            cursor.execute("SELECT channel.Channel_Name, Distinct(video.Video_Name), video.Comment_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Comment_Count DESC;")
            result_10= cursor.fetchall()
            df10 = pd.DataFrame(result_10,columns=['Channel Name','Video Name', 'Number of comments']).reset_index(drop=True)
            df10.index += 1
            st.dataframe(df10)
        
        # SQL DB connection close
        connect_for_question.close()
        
        # ===============================================   /   COMPLETED   /   ====================================================================== #
