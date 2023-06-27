import streamlit as st
from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import mysql.connector
from bson.objectid import ObjectId

# Connect to MongoDB Atlas
atlas_username = 'karthikeyan'
atlas_password = '123456789kar'
atlas_cluster = 'Cluster0'
client = MongoClient(f"mongodb+srv://karthikeyan:123456789kar@cluster0.aueynio.mongodb.net/")

db = client['youtube_data']
collection = db['channel_data']

# Connect to MySQL
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'Karthikeyan'
mysql_database = 'youtube_data'

mysql_conn = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    database=mysql_database
)
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("CREATE DATABASE IF NOT EXISTS youtube_data")

# Set Streamlit app title
st.title("YouTube Data Harvesting and Warehousing")

# Display input field for YouTube channel ID
channel_id = st.text_input("Enter YouTube Channel ID")

# Create MySQL tables
def create_mysql_tables():
    # Create 'channel' table
    create_channel_table = """
    CREATE TABLE channel_table(
        channel_id VARCHAR(255) ,
        channel_name VARCHAR(255),
        channel_type VARCHAR(255),
        channel_view INT,
        channel_description TEXT,
        channel_status VARCHAR(255)
    )
    """
    mysql_cursor.execute(create_channel_table)

    # Create 'playlist' table
    create_playlist_table = """
    CREATE TABLE playlist_table (
        playlist_id VARCHAR(255),
        channel_id VARCHAR(255),
        playlist_name VARCHAR(255),

    )
    """
    mysql_cursor.execute(create_playlist_table)

    # Create 'video' table
    create_video_table = """
        CREATE TABLE video_table (
            video_id VARCHAR(255) ,
            playlist_id VARCHAR(255),
            video_name VARCHAR(255),
            video_description TEXT,
            published_date DATETIME,
            view_count INT,
            like_count INT,
            dislike_count INT,
            favourite_count INT,
            comment_count INT,
            duration INT,
            thumbnail VARCHAR(255),
            caption_status VARCHAR(255),

        )
        """
    mysql_cursor.execute(create_video_table)

    # Create 'comment' table
    create_comment_table = """
    CREATE TABLE comment_table (
        comment_id VARCHAR(255) ,
        video_id VARCHAR(255),
        comment_text TEXT,
        comment_author VARCHAR(255),
        comment_published_date DATETIME,

    )
    """
    mysql_cursor.execute(create_comment_table)
    mysql_conn.commit()

# Retrieve videos for a given YouTube channel ID
def get_channel_videos(youtube, channel_id, api_key):
    videos = []
    request = youtube.search().list(
        part='id',
        channelId=channel_id,
        maxResults=10
    )
    response = request.execute()

    video_ids = [item['id']['videoId'] for item in response['items']]
    video_request = youtube.videos().list(
        part='snippet,statistics,contentDetails',
        id=','.join(video_ids)
    )
    video_response = video_request.execute()

    videos.extend(video_response['items'])
    return videos


# Retrieve videos for a given YouTube playlist ID
def get_playlist_videos(youtube, playlist_id, api_key):
    videos = []
    request = youtube.playlistItems().list(
        part='snippet',
        playlistId=playlist_id,
        maxResults=10
    )
    response = request.execute()

    video_ids = [item['snippet']['resourceId']['videoId'] for item in response['items']]
    video_request = youtube.videos().list(
        part='snippet,statistics,contentDetails',
        id=','.join(video_ids)
    )
    video_response = video_request.execute()

    videos.extend(video_response['items'])
    return videos


# Parse video duration
def parse_duration(duration):
    duration_str = ""
    hours = 0
    minutes = 0
    seconds = 0

    # Remove 'PT' prefix from duration
    duration = duration[2:]

    # Check if hours, minutes, and/or seconds are present in the duration string
    if "H" in duration:
        hours_index = duration.index("H")
        hours = int(duration[:hours_index])
        duration = duration[hours_index + 1:]
    if "M" in duration:
        minutes_index = duration.index("M")
        minutes = int(duration[:minutes_index])
        duration = duration[minutes_index + 1:]
    if "S" in duration:
        seconds_index = duration.index("S")
        seconds = int(duration[:seconds_index])

    # Format the duration string
    if hours > 0:
        duration_str += f"{hours}h "
    if minutes > 0:
        duration_str += f"{minutes}m "
    if seconds > 0:
        duration_str += f"{seconds}s"

    return duration_str.strip()


# Initialize YouTube Data API client
youtube = build('youtube', 'v3', developerKey='AIzaSyDZRngZir7VWON8dPuIb_mee0BvzHmMvlQ')

# Make API request to get channel data
request = youtube.channels().list(
    part='snippet,statistics,contentDetails',
    id=channel_id
)
response = request.execute()


if 'items' in response:
    channel_data = response['items'][0]
    snippet = channel_data['snippet']
    statistics = channel_data['statistics']
    content_details = channel_data.get('contentDetails', {})
    related_playlists = content_details.get('relatedPlaylists', {})
    # Extract relevant data
    data = {
        'Channel_Name': {
            'Channel_Name': snippet.get('title', ''),
            'Channel_Id': channel_id,
            'Subscription_Count': int(statistics.get('subscriberCount', 0)),
            'Channel_Views': int(statistics.get('viewCount', 0)),
            'Channel_Description': snippet.get('description', ''),
            'Playlist_Id': related_playlists.get('uploads', '')
        }
    }

    # Retrieve video data
    videos = get_channel_videos(youtube, channel_id, 'AIzaSyDZRngZir7VWON8dPuIb_mee0BvzHmMvlQ')
    for video in videos:
        video_id = video['id']
        video_data = {
            'Video_Id': video_id,
            'Video_Name': video['snippet'].get('title', ''),
            'Video_Description': video['snippet'].get('description', ''),
            'Tags': video['snippet'].get('tags', []),
            'PublishedAt': video['snippet'].get('publishedAt', ''),
            'View_Count': int(video['statistics'].get('viewCount', 0)),
            'Like_Count': int(video['statistics'].get('likeCount', 0)),
            'Dislike_Count': int(video['statistics'].get('dislikeCount', 0)),
            'Favorite_Count': int(video['statistics'].get('favoriteCount', 0)),
            'Comment_Count': int(video['statistics'].get('commentCount', 0)),
            'Duration': parse_duration(video['contentDetails'].get('duration', '')),
            'Thumbnail': video['snippet'].get('thumbnails', {}).get('default', {}).get('url', ''),
            'Caption_Status': video['snippet'].get('localized', {}).get('localized', 'Not Available'),
            'Comments': []
        }

        # Retrieve comments for the video
        comments_request = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=2
        )
        comments_response = comments_request.execute()
        for item in comments_response['items']:
            comment_data = item['snippet']['topLevelComment']['snippet']
            comment = {
                'Comment_Text': comment_data['textDisplay'],
                'Author': comment_data['authorDisplayName'],
                'Published_At': comment_data['publishedAt']

            }
            video_data['Comments'].append(comment)

        data[video_id] = video_data


# Store data in MongoDB Atlas
if st.button("Store Data in MongoDB Atlas"):
    collection.insert_one(data)
    st.success("Data stored successfully in MongoDB Atlas!")



# Retrieve data from MongoDB Atlas
if st.button("Retrieve Data from MongoDB Atlas"):
    retrieved_data = collection.find_one({'Channel_Name.Channel_Id': channel_id})
    if retrieved_data:
        st.subheader("Retrieved Data:")
        st.write("Channel Name:", retrieved_data['Channel_Name']['Channel_Name'])
        st.write("Subscribers:", retrieved_data['Channel_Name']['Subscription_Count'])
        st.write("Total Videos:", len(videos))
        for video_id, video_data in retrieved_data.items():
            if video_id != 'Channel_Name' and not isinstance(video_data, ObjectId):
                st.write("Video Name:", video_data['Video_Name'])
                st.write("Video Description:", video_data['Video_Description'])
                st.write("Published At:", video_data['PublishedAt'])
                st.write("View Count:", video_data['View_Count'])
                st.write("Like Count:", video_data['Like_Count'])
                st.write("Dislike Count:", video_data['Dislike_Count'])
                st.write("Comment Count:", video_data['Comment_Count'])
                st.write("Duration:", video_data['Duration'])
                st.write("Thumbnail:", video_data['Thumbnail'])
                st.subheader("Comments:")
                for comment in video_data['Comments']:
                    st.write("Author:", comment['Author'])
                    st.write("Comment:", comment['Comment_Text'])
                    st.write("Published At:", comment['Published_At'])
                    st.write("---")
    else:
        st.warning("Data not found in MongoDB Atlas!")

# Connect to MySQL and create tables


# Store data in MySQL
if st.button("Store Data in MySQL"):
    # Store channel data in 'channel' table
    channel_data = data['Channel_Name']
    channel_query = """
    INSERT INTO channel_table(channel_id, channel_name, channel_type, channel_view, channel_description, channel_status)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    channel_values = (
        channel_data['Channel_Id'],
        channel_data['Channel_Name'],
        '',  # Replace with the actual channel type
        channel_data['Channel_Views'],
        channel_data['Channel_Description'],
        ''  # Replace with the actual channel status
    )
    mysql_cursor.execute(channel_query, channel_values)

    # Store playlist data in 'playlist' table
    playlist_data = data[channel_data['Playlist_Id']]
    playlist_query = """
    INSERT INTO playlist_table(playlist_id, channel_id, playlist_name)
    VALUES (%s, %s, %s)
    """
    playlist_values = (
        playlist_data['Playlist_Id'],
        channel_data['Channel_Id'],
        playlist_data['Video_Name']
    )
    mysql_cursor.execute(playlist_query, playlist_values)

    # Store video data in 'video' table
    for video_id, video_data in data.items():
        if video_id != 'Channel_Name' and not isinstance(video_data, ObjectId):
            video_query = """
            INSERT INTO video_table(
                video_id, playlist_id, video_name, video_description, published_date,
                view_count, like_count, dislike_count, favourite_count, comment_count,
                duration, thumbnail, caption_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            video_values = (
                video_data['Video_Id'],
                playlist_data['Playlist_Id'],
                video_data['Video_Name'],
                video_data['Video_Description'],
                video_data['PublishedAt'],
                video_data['View_Count'],
                video_data['Like_Count'],
                video_data['Dislike_Count'],
                video_data['Favorite_Count'],
                video_data['Comment_Count'],
                video_data['Duration'],
                video_data['Thumbnail'],
                video_data['Caption_Status']
            )
            mysql_cursor.execute(video_query, video_values)

            # Store comment data in 'comment' table
            for comment in video_data['Comments']:
                comment_query = """
                INSERT INTO comment_table(comment_id, video_id, comment_text, comment_author, comment_published_date)
                VALUES (%s, %s, %s, %s, %s)
                """
                comment_values = (
                    '',  # Replace with the actual comment ID
                    video_data['Video_Id'],
                    comment['Comment_Text'],
                    comment['Author'],
                    comment['Published_At']
                )
                mysql_cursor.execute(comment_query, comment_values)

    mysql_conn.commit()
    st.success("Data stored successfully in MySQL!")

# Retrieve data from MySQL
if st.button("Retrieve Data from MySQL"):
    # Retrieve channel data from 'channel' table
    channel_query = "SELECT * FROM channel WHERE channel_id = %s"
    channel_values = (channel_id,)
    mysql_cursor.execute(channel_query, channel_values)
    channel_result = mysql_cursor.fetchone()

    if channel_result:
        st.subheader("Retrieved Data:")
        st.write("Channel Name:", channel_result[1])
        st.write("Subscribers:", channel_result[3])
        st.write("Total Videos:", len(videos))

        # Retrieve video data from 'video' table
        video_query = "SELECT * FROM video WHERE playlist_id = %s"
        video_values = (channel_result[5],)
        mysql_cursor.execute(video_query, video_values)
        video_results = mysql_cursor.fetchall()

        for video_result in video_results:
            st.write("Video Name:", video_result[2])
            st.write("Video Description:", video_result[3])
            st.write("Published At:", video_result[4])
            st.write("View Count:", video_result[5])
            st.write("Like Count:", video_result[6])
            st.write("Dislike Count:", video_result[7])
            st.write("Comment Count:", video_result[9])
            st.write("Duration:", video_result[10])
            st.write("Thumbnail:", video_result[11])
            st.subheader("Comments:")

            # Retrieve comment data from 'comment' table
            comment_query = "SELECT * FROM comment WHERE video_id = %s"
            comment_values = (video_result[1],)
            mysql_cursor.execute(comment_query, comment_values)
            comment_results = mysql_cursor.fetchall()

            for comment_result in comment_results:
                st.write("Author:", comment_result[3])
                st.write("Comment:", comment_result[2])
                st.write("Published At:", comment_result[4])
                st.write("---")
    else:
        st.warning("Data not found in MySQL!")

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()
