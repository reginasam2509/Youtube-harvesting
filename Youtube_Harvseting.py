import pandas as pd
import sys
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build
from sqlalchemy import create_engine
from PIL import Image
from bson import ObjectId
# added comment
icon = Image.open("D:\Youtube_logo.png")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing | BY Sam",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by *Sam*"""})

with st.sidebar:
    selected=option_menu( None,["Get_Data"],
    #selected = option_menu(None, ["Home","Extract & Transform","View"], 
                           icons=["house-door-fill","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}})
    

import pymongo
import urllib.parse

username = "Onepiece"
password = "Onepiece@2022"
hostname = "cluster0.vmwchla.mongodb.net"
database_name = "Guvi_Project"

# Escape the username and password-
escaped_username = urllib.parse.quote_plus(username)
escaped_password = urllib.parse.quote_plus(password)

# Create the MongoDB connection URI
uri = f"mongodb+srv://{escaped_username}:{escaped_password}@{hostname}/{database_name}?retryWrites=true&w=majority"

# Connect to MongoDB
client = pymongo.MongoClient(uri)
db=client.get_database('Guvi_Project')

#Connect with mysql database
host1='localhost' 
port1=3306
user1='root'
password1='regina'
database1='guvi_project'

connection_string = f'mysql+pymysql://{user1}:{password1}@{host1}:{port1}/{database1}'
engine=create_engine(connection_string)

api_key = "AIzaSyACSTsBWF3HtKQIpU4YqCcoL7bDvjVr8CY"
youtube = build('youtube','v3',developerKey=api_key)

# channel_dataframe = None
# playlist_dataframe = None
# videos_dataframe = None
# comments_info_dataframe = None

#Function to get channel details
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id).execute()
    for i in range(len(response['items'])):
        data = dict(Channel_id = channel_id,
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    PublishedAt=response['items'][i]['snippet']['publishedAt']
                    #'viewcount':item['statistics']['viewCount']
                    #Country = response['items'][i]['snippet'].get('country').
                    )

        ch_data.append(data)
    return ch_data

#Function to get Playlist
def get_playlist_info(channel_id):
    playlist_request=youtube.channels().list(
         part='contentDetails',
         id=channel_id
    ) 
    playlist_response=playlist_request.execute()
    if playlist_response.get('items'):
        playlist_info=playlist_response['items'][0]
        playlist_id=playlist_info["contentDetails"]["relatedPlaylists"]["uploads"]
        return playlist_id

# FUNCTION TO GET VIDEO IDS  
def get_channel_video(playlist_details):
    video_ids=[]
    playlist_request=youtube.playlistItems().list(
        part='snippet,contentDetails',
        playlistId=playlist_details,
        maxResults=50
    )
    playlist_response=playlist_request.execute()
    for item in playlist_response['items']:
        video_ids.append(item['contentDetails']['videoId'])
    next_page_token=playlist_response.get('nextPageToken')
    while next_page_token is not None:
        playlist_request=youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_details,
            maxResults=50,
            pageToken=next_page_token
        )
        playlist_response=playlist_request.execute()
        for item in playlist_response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page_token=playlist_response.get('nextPageToken')
    return video_ids

# ch_details=get_channel_details('UCqEcU3uiycJPSc8vp4lBqdQ')
# playlist_details=get_playlist_info('UCqEcU3uiycJPSc8vp4lBqdQ')
# v_ids=get_channel_video(playlist_details)
# print(playlist_details)
# print(len(v_ids))


# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids,playlist_details):
    video_stats = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(#Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Playlist_id=playlist_details,
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats

# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=10,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name
if selected == 'Get_Data':
    st.write("### Enter YouTube Channel_ID below :")
    ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')
    if ch_id and st.button("Get_Data"):
        ch_details = get_channel_details(ch_id)
        st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
        st.table(ch_details)
if ch_id and st.button("Upload To MongoDB"):
    collection1=db['channel_details']
    result=collection1.find_one({'Channel_id':ch_id})
    if result:
        st.sidebar.write('Already Present in MongoDB')
    else:
        with st.spinner('Please Wait for int...'):
            ch_details=get_channel_details(ch_id)
            playlist_details=get_playlist_info(ch_id)
            v_ids=get_channel_video(playlist_details)
            vid_details=get_video_details(v_ids,playlist_details)
            def comments():
                com_d = []
                for i in v_ids:
                    com_d+= get_comments_details(i)
                return com_d
            comm_details = comments()

            collections1 = db.channel_details
            collections1.insert_many(ch_details)

            collections2 = db.video_details
            collections2.insert_many(vid_details)

            collections3 = db.comments_details
            collections3.insert_many(comm_details)
            st.success("Upload to MogoDB successful !!")


if st.button("Convert To Dataframe"):
    # global channel_dataframe
    # global playlist_dataframe
    # global videos_dataframe
    # global comments_info_dataframe
    st.markdown("### Select a channel to begin Transformation to SQL")
    ch_names = channel_names()
    user_input = st.selectbox("Select Channel", options=ch_names)
    print(user_input)
    channel_info = db.channel_details.find_one({'Channel_name': user_input})
    print(channel_info)
    #channel_id=channel_info['Channel_id']
    playlist_id=channel_info['Playlist_id']
    channel_dataframe = pd.DataFrame(channel_info, index=[0], columns=[col for col in channel_info if col != '_id'])
    print(channel_dataframe)
    st.title("Channel_Details")
    st.dataframe(channel_dataframe)
        
    #Convert data types if needed
    channel_dataframe['Channel_id'] = channel_dataframe['Channel_id'].astype(str)
    channel_dataframe['Channel_name'] = channel_dataframe['Channel_name'].astype(str)
    channel_dataframe['Playlist_id'] = channel_dataframe['Playlist_id'].astype(str)
    channel_dataframe['Subscribers'] = channel_dataframe['Subscribers'].astype(int)
    channel_dataframe['Views'] = channel_dataframe['Views'].astype(int)
    channel_dataframe['Total_videos'] = channel_dataframe['Total_videos'].astype(int)
    channel_dataframe['Description']=channel_dataframe['Description'].apply(lambda x: x[:200]  if len(x) > 200 else x)
    channel_dataframe['PublishedAt']=pd.to_datetime(channel_dataframe['PublishedAt']).dt.date
        
    # Insert DataFrame into MySQL table
    #channel_dataframe.to_sql('Channel', con=engine, if_exists='append', index=False)
    
    #Create table
    Playlist_info = {'Playlist_id': channel_info['Playlist_id'], 'Channel_id': channel_info['Channel_id']}
    
    #Create Dataframe into MYSql Table
    Playlist_dataframe = pd.DataFrame(Playlist_info,index=[0],columns=[col for col in Playlist_info])
    
    st.title("Playlist_Details")
    st.dataframe(Playlist_dataframe)
    
    #Insert Dataframe into Mysql Table
    Playlist_dataframe['Channel_id'] = Playlist_dataframe['Channel_id'].astype(str)
    Playlist_dataframe['Playlist_id'] = Playlist_dataframe['Playlist_id'].astype(str)
    #Playlist_dataframe.to_sql('playlist', con=engine, if_exists='append',index=False)
 
    #Convert into video_dataframe   
    playlist_id=''.join(playlist_id)
    st.write("To convert into Video_Details")
    videos_info = db.video_details.find({'Playlist_id': playlist_id})
    videos_data = list(videos_info)  # Convert the Cursor to a list of dictionaries
    #len(videos_data) > 0:
    #print("inside in for loop")
    video_ids = [video['Video_id'] for video in videos_data]
    #Separate Key
    columns = list(videos_data[0].keys()) if videos_data else []
    columns = [col for col in columns if col != '_id']
    videos_dataframe = pd.DataFrame(videos_data, columns=columns)

    st.title("Video_Details")
    st.dataframe(videos_dataframe)

    #Data Preprocessing For Video Details
    videos_dataframe['Channel_id']=videos_dataframe['Channel_id'].astype(str)
    #videos_dataframe['Channel_name']=videos_dataframe['Channel_name'].astype(str)
    videos_dataframe['Video_id']=videos_dataframe['Video_id'].astype(str)
    videos_dataframe['Playlist_id']=videos_dataframe['Playlist_id'].astype(str)
    videos_dataframe['Title'] = videos_dataframe['Title'].apply(lambda x: x.lower())
    videos_dataframe['Tags'] =videos_dataframe['Tags'].apply(lambda x: ','.join(x) if x is not None else 'no tags')
    videos_dataframe['Thumbnail'] =videos_dataframe['Thumbnail'].apply(lambda x: x.split('/')[-1])
    videos_dataframe['Description'] = videos_dataframe['Description'].apply(lambda x: x[:200]  if len(x) > 200 else x)
    videos_dataframe['Published_date'] = pd.to_datetime(videos_dataframe['Published_date']).dt.date
    videos_dataframe['Duration'] = videos_dataframe['Duration'].apply(lambda x: pd.to_timedelta(x).total_seconds() if isinstance(x, str) else 0)
    videos_dataframe['Views'] = videos_dataframe['Views'].astype(int)
    videos_dataframe['Likes'] = videos_dataframe['Likes'].fillna(0).astype(int)
    videos_dataframe['Comments'] =videos_dataframe['Comments'].fillna(0).astype(int)
    videos_dataframe['Favorite_count'] =videos_dataframe['Favorite_count'].fillna(0).astype(int)
    videos_dataframe['Definition'] = videos_dataframe['Definition'].apply(lambda x: x.lower() if isinstance(x, str) else 'no defintion')
    videos_dataframe['Caption_status'] =videos_dataframe['Caption_status'].apply(lambda x: x.lower() if isinstance(x, str) else 'no caption')
    #videos_dataframe.to_sql('video',con=engine,if_exists='append', index=False)

    #Conver into comments_dataframe
    comment_details_info=[]
    for video_id in videos_dataframe['Video_id']:
        comments_info=db.comments_details.find({'Video_id':video_id})
        comment_details_info.extend(comments_info)
        #comments_info_dataframe=pd.DataFrame(comment_details_info)

    comments_info_dataframe=pd.DataFrame(comment_details_info, columns=[col for col in comment_details_info[0] if col != '_id'])
    print(comments_info_dataframe)
    st.title("Comment_Details")
    st.dataframe(comments_info_dataframe)

    #Data Preprocessing For Comment Details
    comments_info_dataframe['Comment_id']=comments_info_dataframe['Comment_id'].astype(str)
    comments_info_dataframe['Video_id']=comments_info_dataframe['Video_id'].astype(str)
    comments_info_dataframe['Comment_text'] = ''.join(e for e in comments_info_dataframe['Comment_text'] if e.isalnum() or e.isspace())
    comments_info_dataframe['Comment_author']=comments_info_dataframe['Comment_author'].astype(str)
    comments_info_dataframe['Comment_posted_date'] = pd.to_datetime(comments_info_dataframe['Comment_posted_date']).dt.date
    #comments_info_dataframe['Comment_posted_date'] = comments_info_dataframe['Comment_posted_date'][:10]
    comments_info_dataframe['Like_count']=comments_info_dataframe['Like_count'].fillna(0).astype(int)
    comments_info_dataframe['Reply_count']=comments_info_dataframe['Reply_count'].fillna(0).astype(int)
    #comments_info_dataframe.to_sql('Comment',con=engine,if_exists='append',index=False)

    
    channel_dataframe.to_sql('Channel', con=engine, if_exists='append', index=False)
    Playlist_dataframe.to_sql('playlist', con=engine, if_exists='append',index=False)
    videos_dataframe.to_sql('video',con=engine,if_exists='append', index=False)
    comments_info_dataframe.to_sql('Comment',con=engine,if_exists='append',index=False)



def on_button_click():
    st.write("You clicked the button")
    selected_question=st.session_state.selected_question
    if selected_question == '1. What are the names of all the videos and their corresponding channels?':
        st.write("You selected question 1")
        sql_query="""
            SELECT video.Title, Channel.Channel_name
            FROM video
            JOIN playlist ON video.Playlist_id = playlist.Playlist_id
            JOIN channel ON channel.Channel_id=playlist.Channel_id 
            """
        df=pd.read_sql_query(sql_query,con=engine)
        print(df)
        st.write(df)
    elif selected_question == '2. Which channels have the most number of videos, and how many videos do they have?':
        st.write("You selected question 2")
        sql_query="""
            SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
            FROM channel
            ORDER BY total_videos DESC
            """
        df=pd.read_sql_query(sql_query,con=engine)
        print(df)
        st.write(df)

    elif selected_question=='3. What are the top 10 most viewed videos and their respective channels?':
        sql_query="""
        SELECT video.Views as Views,channel.Channel_name as Channel_name
        FROM video
        JOIN playlist ON video.Playlist_id=playlist.Playlist_id
        JOIN channel ON channel.Channel_id=playlist.Channel_id
        ORDER BY  Views DESC
        LIMIT 10
        """
        df=pd.read_sql_query(sql_query,con=engine)
        print(df)
        st.write(df)
    elif selected_question == '4. How many comments were made on each video, and what are their corresponding video names?':
        sql_query="""
        SELECT video.video_id AS Video_id, video.title AS Video_Title, video.Comments AS Total_Comments
        FROM video
        LIMIT 10
        """
        df=pd.read_sql_query(sql_query,con=engine)
        print(df)
        st.write(df)
    elif selected_question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        sql_query="""
        SELECT video.Likes as Likes,channel.Channel_name as Channel_name
        FROM video
        JOIN playlist ON playlist.Playlist_id=video.Playlist_id
        JOIN channel ON channel.Channel_id=playlist.Channel_id
        ORDER BY Likes DESC
        LIMIT 10
        """
        df=pd.read_sql_query(sql_query,con=engine)
        print(df)
        st.write(df)
    elif selected_question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        sql_query="""
        SELECT video.Title as Title ,video.Likes as Likes
        FROM video
        ORDER BY Likes DESC
        """
        df=pd.read_sql_query(sql_query,con=engine)
        print(df)
        st.write(df)
    elif selected_question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        sql_query="""
        SELECT Channel_name,Views as Total_views
        From channel
        ORDER BY Total_views DESC
        """
        df=pd.read_sql_query(sql_query,con=engine)
        print(df)
        st.write(df)
    elif selected_question == '8. What are the names of all the channels that have published videos in the year 2022?':
        sql_query="""
        SELECT distinct(channel.Channel_name) as Channel_name 
        FROM channel
        JOIN playlist ON channel.Channel_id = playlist.Channel_id
        JOIN video ON video.Playlist_id=playlist.Playlist_id
        WHERE video.Published_date >= '2022-01-01' and video.Published_date<='2022-12-31';
        """
        df=pd.read_sql_query(sql_query,con=engine)
        print(df)
        st.write(df)
    elif selected_question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        sql_query="""
        SELECT channel.Channel_name as Channel_name,AVG(video.Duration) as Average
        FROM video
        JOIN playlist ON  playlist.Playlist_id = video.Playlist_id
        JOIN channel ON channel.Channel_id = playlist.Channel_id
        GROUP BY Channel_name
        ORDER BY Channel_name
        """
        df=pd.read_sql_query(sql_query,con=engine)
        print(df)
        st.write(df)
    
    elif selected_question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        sql_query="""
        SELECT channel.Channel_name as Channel_name,video.Video_id as Video_id,video.Comments as Comments
        FROM video
        JOIN playlist ON playlist.Playlist_id = video.Playlist_id
        JOIN channel ON channel.Channel_id=playlist.Channel_id
        ORDER BY Comments DESC
        LIMIT 10                         
        """
        df=pd.read_sql_query(sql_query,con=engine)
        print(df)
        st.write(df)

st.write("## :orange[Select any question to get Insights]")
options = [
        '1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
        ]

# if st.button("View"):
#         st.write("## :orange[Select any question to get Insights]")
#         options = [
#         '1. What are the names of all the videos and their corresponding channels?',
#         '2. Which channels have the most number of videos, and how many videos do they have?',
#         '3. What are the top 10 most viewed videos and their respective channels?',
#         '4. How many comments were made on each video, and what are their corresponding video names?',
#         '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
#         '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
#         '7. What is the total number of views for each channel, and what are their corresponding channel names?',
#         '8. What are the names of all the channels that have published videos in the year 2022?',
#         '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
#         '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
#         ]
if 'selected_question' not in st.session_state:
    print("Default value")
    st.session_state.selected_question = options[0]
        
selected_question = st.selectbox("Select a question", options)

if st.button("View"):
    if selected_question != st.session_state.selected_question:
        st.session_state.selected_question = selected_question

        # if selected_question != st.session_state.selected_question:
        #     print("The given query")
        #     st.session_state.selected_question = selected_question

if st.button("Get Insights"):
    on_button_click()

    
engine.dispose()
client.close()

 # default_value = questions[0]
        # default_value = options[0]
        # selected_question = st.selectbox("Select a option", options, key="selectbox", on_change=on_selectbox_change)
        # print(selected_question)