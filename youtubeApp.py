
# IMPORT NECESSARY PACKAGES
from googleapiclient.discovery import build
import pandas as pd
import isodate
import mysql.connector
from sqlalchemy import create_engine
import streamlit as st 
from PIL import Image
from streamlit_option_menu import option_menu
import requests
from streamlit_lottie import st_lottie 


# -------------------------------------------------------------------------------------------
# SETTING PAGE CONFIGURATIONS 
icon = Image.open("Youtube_logo.png")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items= None)


# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(menu_title = None,
                           options = ["Overview","Extraction and Storage","Q&A"], 
                           icons=["house","basket-fill","patch-question-fill"],
                           orientation="vertical")
    
# ---------------------------------------------------------------------------------------------
    
# MYSQL CREDENTIALS  
u_name = 'root'
u_pass = '1000Shaik#1'
host_name = 'localhost'
port = 3306
database_name = 'youtube'
    
# ESTABLISH MYSQL CONNECTION
connection  = mysql.connector.connect(host = host_name,      
                               user = u_name,
                               passwd = u_pass,
                               db = database_name)

mycursor = connection .cursor()

# CREATE A SQLALCHEMY ENGINE TO TRANSFER EXTRACTED DATA INTO SQL
engine = create_engine(f"mysql+mysqlconnector://{u_name}:{u_pass}@{host_name}/{database_name}")

# -----------------------------------------------------------------------------------------------

# BUILD CONNECTION WITH YOUTUBE API
def api_connect():    
    api_service_name = "youtube"
    api_version = "v3"
    my_api = "AIzaSyByR4OuPGCccdKKahj0xltv6HdfVco8O4g"
    youtube = build(api_service_name, api_version, developerKey = my_api)
    
    return youtube

youtube = api_connect()


# FUNCTION TO GET CHANNEL DETAILS
def channel_data(channel_id):
    chn_data = list()
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id = channel_id)
    response = request.execute() 
    
    for i in range(len(response["items"])): 
        data = dict(
            channel_id =  response["items"][i]["id"],
            channel_name =  response["items"][i]["snippet"]["title"],
            channel_desc =  response["items"][i]["snippet"]["description"],            
            playlist_id =  response["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
            channel_views =  response["items"][i]["statistics"]["viewCount"],
            subscription_count =  response["items"][i]["statistics"]["subscriberCount"],
            video_count =  response["items"][i]["statistics"]["videoCount"]
        )
        
        chn_data.append(data)
        
    return (pd.DataFrame(chn_data))

# CREATE channel_details TABLE AND INSERT EXTRACTED DATA INTO MYSQL 
def channel_to_sql(channel_details):
    
    mycursor.execute('''CREATE TABLE IF NOT EXISTS channel_details(
    channel_id varchar(255) unique not null,
    channel_name varchar(255),
    channel_desc text,
    playlist_id varchar(255),
    channel_views INT,
    subscription_count INT,
    video_count INT,
    primary key(channel_id))
    '''
    )

    channel_details['channel_views'] = pd.to_numeric(channel_details['channel_views'])
    channel_details['subscription_count'] = pd.to_numeric(channel_details['subscription_count'])
    channel_details['video_count'] = pd.to_numeric(channel_details['video_count'])

    channel_details.to_sql(name = 'channel_details', con=engine, if_exists='append', index=False)


#----------------------------------------------------------------------------------------------


# FUNCTION TO GET VIDEO IDs
def get_video_id(channel_id):
    
    video_ids = list()
    response = youtube.channels().list(id = channel_id,part = 'contentDetails').execute()

    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
                                                    part = 'snippet',
                                                    playlistId = playlist_id,
                                                    maxResults = 50,
                                                    pageToken = next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            
        next_page_token = response1.get('nextPageToken')
        
        if next_page_token is None:
            break

    return video_ids
 
 
# GET VIDEO INFORMATION
def get_video_info(video_IDs):
    video_data = list()
    for video_id in video_IDs:
        response = youtube.videos().list(part = "snippet,statistics,contentDetails",id = video_id).execute()
        
        for item in response['items']:   
            data = dict(
                video_id = item ['id'],
                channel_id = item ['snippet']['channelId'],
                channel_name =  item ['snippet']['channelTitle'],
                video_title = item['snippet']['title'],
                video_tags = ','.join(item['snippet'].get('tags',["NA"])),
                video_thumbnail = item['snippet']['thumbnails']["default"]["url"],
                video_desc = item['snippet'].get('description'),
                video_published_at = item['snippet']['publishedAt'],
                video_duration = item['contentDetails']['duration'],
                video_view_count = item['statistics'].get('viewCount'),
                video_comment_count = item['statistics'].get('commentCount'),
                video_like_count = item['statistics'].get('likeCount'),
                video_fav_count = item['statistics']['favoriteCount'],
                video_def = item['contentDetails']['definition'],
                video_cap_status = item['contentDetails']['caption']   
            )
            video_data.append(data)

    return (pd.DataFrame(video_data))


# CREATE video_details TABLE AND INSERT EXTRACTED DATA INTO MYSQL 
def video_to_sql(video_details):
    mycursor.execute(''' CREATE TABLE IF NOT EXISTS video_details(
    video_id varchar(255) unique not null,
    channel_id varchar(255),
    channel_name varchar(255),
    video_title varchar(255),
    video_tags text,
    video_thumbnail varchar(255),
    video_desc text,
    video_published_at DATETIME,
    video_duration INT,
    video_view_count INT,
    video_comment_count INT,
    video_like_count INT,
    video_fav_count INT,
    video_def CHAR(20),
    video_cap_status char(10),
    primary key(video_id)
    )''')


    video_details['video_view_count'] = pd.to_numeric(video_details['video_view_count'])
    video_details['video_comment_count'] = pd.to_numeric(video_details['video_comment_count'])
    video_details['video_like_count'] = pd.to_numeric(video_details['video_like_count'])
    video_details['video_fav_count'] = pd.to_numeric(video_details['video_fav_count'])
    video_details['video_published_at'] = pd.to_datetime(video_details['video_published_at']).dt.date
    
    # Video.duration --> Seconds
    for i in range(len(video_details["video_duration"])):
        duration = isodate.parse_duration(video_details["video_duration"].loc[i])
        seconds = duration.total_seconds()
        video_details.loc[i, 'video_duration'] = int(seconds)

    video_details['video_duration'] = pd.to_numeric(video_details['video_duration'])

    video_details.to_sql(name = 'video_details', con=engine, if_exists='append', index=False)


# -------------------------------------------------------------------------------------

# FUNCTION TO GET COMMENT DETAILS
def get_comment_info(videoIDs):
    
    comment_data = list()

    try: 
         
        for video_id in videoIDs:
            response = youtube.commentThreads().list(
                                                part = "snippet",
                                                videoId = video_id,
                                                maxResults = 100).execute()
            
            for item in response['items']:
                data = dict(
                    comment_id = item ['snippet']['topLevelComment']['id'],
                    video_id = item ['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_text = item ['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author = item ['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_pat = item ['snippet']['topLevelComment']['snippet']['publishedAt']    
                )
                comment_data.append(data)
            
        
    except:
        pass    
    
    return (pd.DataFrame(comment_data))

# CREATE comment_details TABLE AND INSERT EXTRACTED DATA INTO MYSQL
def comment_to_sql(comment_details):
    mycursor.execute(''' CREATE TABLE IF NOT EXISTS comment_details(
    comment_id varchar(255) unique not null,
    video_id varchar(255),
    comment_text text,
    comment_author varchar(255),
    comment_pat DATETIME,
    primary key(comment_id)
    )''')

    comment_details['comment_pat'] = pd.to_datetime(comment_details['comment_pat']).dt.date
    
    comment_details.to_sql(name = 'comment_details', con=engine, if_exists='append', index=False)
    
#-----------------------------------------------------------------------------------------------    

# FUNCTION GET PLAYLIST DETAILS
def get_playlist_details(channel_id):
    all_data = list() 
    next_page_token = None

    while True:
        response = youtube.playlists().list(
                                            part = "snippet,contentDetails",
                                            channelId = channel_id,
                                            maxResults = 50,
                                            pageToken = next_page_token).execute()


        for item in response['items']:
            data = dict(
                playlist_id = item['id'],
                playlist_title = item['snippet']['title'],
                channel_id = item['snippet']['channelId'],
                channel_name = item['snippet']['channelTitle'],
                published_at = item['snippet']['publishedAt'],
                video_count = item['contentDetails']['itemCount']
            )
            all_data.append(data)
            
        next_page_token = response.get("nextPageToken")
        
        if next_page_token is None:
            break
        
    return (pd.DataFrame(all_data))

# CREATE playlist_details TABLE AND INSERT EXTRACTED DATA INTO MYSQL
def playlist_to_sql(playlist_details):
    mycursor.execute(''' CREATE TABLE IF NOT EXISTS playlist_details(
    playlist_id varchar(255) unique not null,
    playlist_title varchar(255),
    channel_id varchar(255),
    channel_name varchar(255),
    published_at DATETIME,
    video_count int,
    primary key(playlist_id)
    )''')

    playlist_details['published_at'] = pd.to_datetime(playlist_details['published_at']).dt.date
    
    playlist_details.to_sql(name = 'playlist_details', con=engine, if_exists='append', index=False)    
    

#------------------------------------------------------------------------------------------------

# ANIMATION:
def lottie_url(url):
                r = requests.get(url)
                if r.status_code != 200:
                    return None
                return r.json()
            
# json URL
lottie_coding = "https://lottie.host/5c19e3cf-9a2d-45ac-bbc5-9cb8eae1f160/GSznaPhBj7.json"

if selected == "Overview":
    with st.container():
        st.title(":red[Youtube] Data Harvesting and Warehousing ")
        st.write('##')
        text_column, animae_column = st.columns((2,1))
        with text_column:
            st.subheader("Application Summary: YouTube Data Warehousing")
            st.write('''
                        This application extracts data from the YouTube API and stores it in a MySQL database.
                        The stored data can then be used to find answers to questions provided in a dropdown
                        menu. It serves as a convenient way to analyze YouTube-related information and
                        retrieve insights from the collected data. 🚀                  
                    ''')
            
        with animae_column: 
            
            st_lottie(lottie_coding, height = 400, key="coding")
            
if selected == "Extraction and Storage":
    st.title(f"EXTRACT 🛒 & STORE 👜")
    st_chn_id = st.text_input(" Enter the Channel id: ")
    
    if st.button("Extract Current Channel Information"):
        current_channel_info = channel_data(st_chn_id)
        st.write(current_channel_info.iloc[0,[0,1,4,5,6]])
        
        
    if st.button("Data to SQL"):
        try:
            with st.spinner('Please Wait for it...'):
                channel_details = channel_data(st_chn_id)
                video_ids = get_video_id(st_chn_id)
                video_details = get_video_info(video_ids)
                comment_details = get_comment_info(video_ids)
                playlist_details = get_playlist_details(st_chn_id)
                
                # convert DataFrame to SQL
                channel_to_sql(channel_details)
                video_to_sql(video_details)
                comment_to_sql(comment_details)
                playlist_to_sql(playlist_details)
                
                st.success("All the Details are Successfully transferred to MySql")
                
        except:
            st.error("Please try again!!..Error Occurred!!")
    
    if st.button("Channels in Database"):
        mycursor.execute("""SELECT channel_name, channel_id, channel_views, subscription_count, video_count
                            FROM channel_details
                        """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
      
            
                
if selected == "Q&A":
    st.title("Find your answers here 🧐")
    questions = st.selectbox("Choose your questions from below 👇",
                 ["1. What are the Names of all the videos and their corresponding channels?",
                  "2. Which Top 5 channels have the most number of videos, and how many videos do they have?",
                  "3. What are the top 10 most viewed videos and their respective channel?",
                  "4. How many comments were made on each video, and what are their corresponding video names?",
                  "5. Which Top 10 videos have the highest number of likes, and what are their corresponding channel names?",
                  "6. What is the total number of likes for each video, and what are  their corresponding video names?",
                  "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                  "8. What are the names of all the channels that have published videos in the year 2022?",
                  "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                  "10. Which Top 100 videos have the highest number of comments, and what are their corresponding channel names?"]
                      )
    
    if questions == '1. What are the Names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT video_title, channel_name
                            FROM video_details 
                            ORDER BY channel_name
                        """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '2. Which Top 5 channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name, video_count
                            FROM channel_details
                            ORDER BY video_count DESC
                            LIMIT 5
                        """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == "3. What are the top 10 most viewed videos and their respective channel?":
        mycursor.execute("""SELECT channel_name, video_title, video_view_count
                            FROM video_details 
                            ORDER BY video_view_count DESC
                            LIMIT 10
                        """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == "4. How many comments were made on each video, and what are their corresponding video names?":
        mycursor.execute("""SELECT video_title, video_comment_count
                            FROM video_details
                            ORDER BY video_comment_count DESC
                        """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == "5. Which Top 10 videos have the highest number of likes, and what are their corresponding channel names?":
        mycursor.execute("""SELECT channel_name, video_title, video_like_count
                            FROM video_details
                            ORDER BY video_like_count DESC
                            LIMIT 10
                        """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
            
    elif questions == "6. What is the total number of likes for each video, and what are  their corresponding video names?":
        mycursor.execute("""SELECT video_title, video_like_count
                            FROM video_details
                            ORDER BY video_like_count DESC;
                        """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
        mycursor.execute("""SELECT channel_name, channel_views
                            FROM channel_details
                            ORDER BY channel_views DESC;
                        """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == "8. What are the names of all the channels that have published videos in the year 2022?":
        mycursor.execute("""SELECT distinct(channel_name)
                            FROM video_details
                            WHERE video_published_at LIKE "2022%"
                        """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
        
    elif questions == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        mycursor.execute("""SELECT channel_name, avg(video_duration)
                            FROM video_details
                            GROUP BY channel_name
                            ORDER BY AVG(video_duration) DESC
                        """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        


    elif questions == "10. Which Top 100 videos have the highest number of comments, and what are their corresponding channel names?":
        mycursor.execute("""SELECT channel_name, video_title, video_comment_count
                            FROM video_details
                            ORDER BY video_comment_count DESC
                            LIMIT 100
                        """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        