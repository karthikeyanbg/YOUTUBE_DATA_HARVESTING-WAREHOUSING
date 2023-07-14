# Importing the necessary libraries
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
import pymongo
import mysql.connector
import datetime
import re
import pandas as pd

# streamlit app background img setup
def add_bg_from_url():
    st.markdown(
        f"""
         <style>
         .stApp {{
             background-image: url("https://c4.wallpaperflare.com/wallpaper/275/735/625/youtube-wallpaper-preview.jpg");
             background-attachment: fixed;
             background-size: cover
         }}
         </style>
         """,
        unsafe_allow_html=True
    )


add_bg_from_url()


# Set up authentication
api_key = "AIzaSyBMAMSyYyw29vJ1F_9S0GxNbYXs0fDtURw"
api_service_name = "youtube"
api_version = "v3"


# Function to build and execute a YouTube API request
def execute_request(request):
    response = request.execute()
    return response


# Function to execute an SQL query and return the result as a pandas DataFrame
def execute_query(query):
    cursor.execute(query)
    columns = cursor.column_names
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=columns)
    return df


# Build api authenticate
def get_authenticated_service():
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube


# Get the channel_info
def get_channel_details(youtube, channel_id):
    # Call the YouTube API to get the channel details
    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id)
    response = execute_request(request)
    return response


# Get the channel playlists_info
def get_channel_playlists(youtube, channel_id):
    # Call the YouTube API to get the channel playlists
    request = youtube.playlists().list(part="snippet,contentDetails", channelId=channel_id)
    response = execute_request(request)
    playlists = response["items"]
    while "nextPageToken" in response:
        request = youtube.playlists().list(part="snippet,contentDetails", channelId=channel_id,
                                           pageToken=response["nextPageToken"])
        response = execute_request(request)
        playlists += response["items"]
    return playlists


# Get the playlist videos_info
def get_playlist_videos(youtube, playlist_id):
    videos = []
    request = youtube.playlistItems().list(part="snippet,contentDetails", playlistId=playlist_id, maxResults=20)
    while request:
        response = execute_request(request)
        videos += response["items"]
        request = youtube.playlistItems().list_next(request, response)
    return videos


# Get the video _info
def get_video_details(youtube, video_id):
    # Call the YouTube API to get the video details
    request = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id)
    response = execute_request(request)
    return response


# Get the video comments_info
def get_video_comments(youtube, video_id):
    try:
        comments = []
        request = youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=10, textFormat="plainText")
        # while request:
        response = execute_request(request)
        comments += response["items"]
        request = youtube.commentThreads().list_next(request, response)

            # Check if there are more comments available or comments are disabled
        if "nextPageToken" not in response or len(comments) >= 10:

            # Set the nextPageToken to fetch the next page of comments
          request.pageToken = response["nextPageToken"]

        return comments[:10]

    except HttpError as e:
        if e.resp.status == 403 and "commentsDisabled" in str(e):
            print(f"Comments are disabled for video ID: {video_id}")
        else:
            print(f"An error occurred for video ID: {video_id}", e)

        return []


# Connect to the MongoDB database
client = pymongo.MongoClient("mongodb+srv://karthikeyan:123456789kar@cluster0.aueynio.mongodb.net/")
db = client['youtube_info']
collection = db['channels_info']


def store_channel_data_mongo(channel_data):
    """
    Stores YouTube channel data in a MongoDB collection.

    Args:
        channel_data (dict): Dictionary containing YouTube channel data.

    Returns:
        None
    """

    # Insert the channel data into the collection
    collection.insert_one(channel_data)


# Create a connection to the MySQL database
conn = mysql.connector.connect(
    user='root',
    password='Karthikeyan',
    host='localhost',
    database='youtube_info'
)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Create the Channel table
channel_table_query = """
CREATE TABLE IF NOT EXISTS Channel (
  channel_id VARCHAR(255) NOT NULL PRIMARY KEY,
  channel_name VARCHAR(255),
  channel_type VARCHAR(255),
  channel_views INT,
  channel_description TEXT,
  channel_status VARCHAR(255)
);
"""
cursor.execute(channel_table_query)

# Create the Playlist table
playlist_table_query = """
CREATE TABLE IF NOT EXISTS Playlist (
  playlist_id VARCHAR(255) NOT NULL PRIMARY KEY,
  channel_id VARCHAR(255),
  playlist_name VARCHAR(255),
  FOREIGN KEY (channel_id) REFERENCES Channel (channel_id)
);
"""
cursor.execute(playlist_table_query)

# Create the Video table
video_table_query = """
CREATE TABLE IF NOT EXISTS Video (
  video_id VARCHAR(255) NOT NULL PRIMARY KEY,
  playlist_id VARCHAR(255),
  video_name VARCHAR(255),
  video_description TEXT,
  published_date DATETIME,
  view_count INT,
  like_count INT,
  dislike_count INT,
  favorite_count INT,
  comment_count INT,
  duration INT,
  thumbnail VARCHAR(255),
  caption_status VARCHAR(255),
  FOREIGN KEY (playlist_id) REFERENCES Playlist (playlist_id)
);
"""
cursor.execute(video_table_query)

# Create the Comment table
comment_table_query = """
CREATE TABLE IF NOT EXISTS Comment (
  comment_id VARCHAR(255) NOT NULL PRIMARY KEY,
  video_id VARCHAR(255),
  comment_text TEXT,
  comment_author VARCHAR(255),
  comment_published_date DATETIME,
  FOREIGN KEY (video_id) REFERENCES Video (video_id)
);
"""
cursor.execute(comment_table_query)

# Commit the changes to the database
conn.commit()


def migrate_data(channel_name):
    # Retrieve channel data
    channel_data = collection.find_one(
        {"Channel_Name.Channel_Name": channel_name},
        {"_id": 0}  # Exclude the _id field from the result
    )

    # Insert channel data into MySQL
    channel_query = "INSERT INTO Channel (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status) VALUES (%s, %s, %s, %s, %s, %s)"
    channel_values = (
        channel_data['Channel_Name']['Channel_Id'],
        channel_data['Channel_Name']['Channel_Name'],
        channel_data['Channel_Name']['Channel_Type'],
        channel_data['Channel_Name']['Channel_Views'],
        channel_data['Channel_Name']['Channel_Description'],
        channel_data['Channel_Name']['Channel_Status']
    )
    cursor.execute(channel_query, channel_values)

    # Iterate over the playlists in the channel data
    for playlist_id, playlist_data in channel_data.items():
        if playlist_id == 'Channel_Name':
            continue

        # Insert playlist data into the Playlist table
        insert_playlist_query = "INSERT INTO Playlist (playlist_id, channel_id, playlist_name) VALUES (%s, %s, %s)"
        playlist_values = (
            playlist_data['Playlist_Id'],
            channel_data['Channel_Name']['Channel_Id'],
            playlist_data['Playlist_Title']
        )
        cursor.execute(insert_playlist_query, playlist_values)

        # Check if the 'Videos' key exists within the current playlist data
        if 'Videos' in playlist_data:
            # Iterate over the videos in the playlist
            for video_id, video_data in playlist_data['Videos'].items():
                # Check if the video already exists in the table
                select_video_query = "SELECT * FROM video WHERE Video_Id = %s"
                cursor.execute(select_video_query, (video_id,))
                result = cursor.fetchone()

                if result:
                    # Video already exists, update or skip as desired
                    continue

                # Convert the YouTube datetime string to the MySQL datetime format
                pbat = video_data['PublishedAt']
                if pbat is None:
                    published_date = datetime.datetime.min
                else:
                    published_date = datetime.datetime.strptime(pbat, "%Y-%m-%dT%H:%M:%SZ").strftime(
                        "%Y-%m-%d %H:%M:%S")

                # Extract the duration and convert it to an integer value
                duration = video_data.get('Duration', '0')
                if duration is not None:
                    match = re.match(r'PT((\d+)H)?((\d+)M)?((\d+)S)?', duration)
                    if match:
                        hours = int(match.group(2) or 0)
                        minutes = int(match.group(4) or 0)
                        seconds = int(match.group(6) or 0)
                        duration_in_seconds = hours * 3600 + minutes * 60 + seconds
                    else:
                        duration_in_seconds = 0
                else:
                    duration_in_seconds = 0

                # Insert video data into the Video table
                insert_video_query = "INSERT INTO Video (video_id, playlist_id, video_name, video_description, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                video_values = (
                    video_id,
                    playlist_data['Playlist_Id'],
                    video_data['Video_Name'],
                    video_data['Video_Description'],
                    published_date,
                    video_data['View_Count'],
                    video_data['Like_Count'],
                    video_data['Dislike_Count'],
                    video_data['Favorite_Count'],
                    video_data['Comment_Count'],
                    duration_in_seconds,
                    video_data['Thumbnail'],
                    video_data['Caption_Status']
                )
                cursor.execute(insert_video_query, video_values)

                # Iterate over the comments on the video
                for comment_id, comment_data in video_data['Comments'].items():
                    # Convert comment published date to a valid datetime format
                    comment_published_date_str = comment_data['Comment_PublishedAt']
                    comment_published_date = datetime.datetime.strptime(comment_published_date_str,
                                                                        '%Y-%m-%dT%H:%M:%SZ')
                    # Insert comment data into the Comment table
                    insert_comment_query = "INSERT INTO Comment (comment_id, video_id, comment_text, comment_author, comment_published_date) VALUES (%s, %s, %s, %s, %s)"
                    comment_values = (
                        comment_id,
                        video_id,
                        comment_data['Comment_Text'],
                        comment_data['Comment_Author'],
                        comment_published_date
                    )
                    cursor.execute(insert_comment_query, comment_values)

    # Commit changes to MySQL
    conn.commit()

    st.success("Channel data migrated successfully!")


def main():
    try:
        # Authenticate and build the API client
        youtube = get_authenticated_service()

        st.title(':red[YOUTUBE DATA HARVESTING & WAREHOUSING]')
        st.write('Enter the channel ID and click the button to get the information')
        # Get the channel ID from the user
        channel_id = st.text_input('Enter the channel ID')


        # Get the channel information when the user clicks the button
        if channel_id:
            # Get the channel details
            channel_details = get_channel_details(youtube, channel_id)

            # Get the channel playlists
            channel_playlists = get_channel_playlists(youtube, channel_id)

            # Create dictionary to store channel details
            channel_dict = {
                "Channel_Name": {
                    "Channel_Name": channel_details["items"][0]["snippet"]["title"],
                    "Channel_Id": channel_id,
                    "Subscription_Count": channel_details["items"][0]["statistics"]["subscriberCount"],
                    "Channel_Views": channel_details["items"][0]["statistics"]["viewCount"],
                    "Channel_Description": channel_details["items"][0]["snippet"]["description"],
                    "Channel_Type": channel_details["items"][0]["snippet"].get("channelType", "Unknown"),
                    "Channel_Status": channel_details["items"][0].get("status", {}).get("privacyStatus", "Unknown"),
                    "Playlist_Id": ""
                }
            }

            # Loop through the playlists and add them to the channel dictionary
            for playlist in channel_playlists:
                playlist_id = playlist["id"]
                playlist_title = playlist["snippet"]["title"]

                # Add the playlist to the channel dictionary
                channel_dict[playlist_id] = {
                    "Playlist_Id": playlist_id,
                    "Playlist_Title": playlist_title,
                    "Videos": {}
                }

                # Get the playlist videos
                videos = get_playlist_videos(youtube, playlist_id)

                # Loop through the videos and add them to the playlist dictionary
                for video in videos:
                    video_id = video["contentDetails"]["videoId"]
                    video_details = get_video_details(youtube, video_id)

                    if "items" in video_details and len(video_details["items"]) > 0:
                        item = video_details["items"][0]


                        dislike_count = item["statistics"]["dislikeCount"] if "statistics" in item and "dislikeCount" in \
                                                                              item["statistics"] else 0
                        tags = item["snippet"]["tags"] if "snippet" in item and "tags" in item["snippet"] else None
                        view_count = item["statistics"]["viewCount"] if "statistics" in item and "viewCount" in item[
                            "statistics"] else 0
                        video_title = item["snippet"]["title"] if "snippet" in item and "title" in item[
                            "snippet"] else "Unknown"
                        comment_count = item["statistics"]["commentCount"] if "statistics" in item and "commentCount" in \
                                                                              item["statistics"] else 0
                        video_desc = item["snippet"]["description"] if "snippet" in item and "description" in item[
                            "snippet"] else ""
                        like_count = item["statistics"]["likeCount"] if "statistics" in item and "likeCount" in item[
                            "statistics"] else 0
                        fav_count = item["statistics"]["favoriteCount"] if "statistics" in item and "favoriteCount" in \
                                                                           item["statistics"] else 0
                        published_At = item["snippet"]["publishedAt"] if "snippet" in item and "publishedAt" in item[
                            "snippet"] else None
                        thumb = item["snippet"]["thumbnails"]["default"]["url"] if "snippet" in item and "thumbnails" in \
                                                                                   item["snippet"] else "None"
                        due = item["contentDetails"]["duration"] if "contentDetails" in item and "duration" in item[
                            "contentDetails"] else None
                        cpts = item["contentDetails"]["caption"] if "contentDetails" in item and "caption" in item[
                            "contentDetails"] else "None"
                    else:
                        dislike_count = 0
                        tags = None
                        view_count = 0
                        video_title = "Unknown"
                        comment_count = 0
                        video_desc = ""
                        like_count = 0
                        fav_count = 0
                        published_At = None
                        thumb = "None"
                        due = None
                        cpts = "None"

                    # Add the video to the playlist dictionary
                    channel_dict[playlist_id]["Videos"][video_id] = {
                        "Video_Id": video_id,
                        "Video_Name": video_title,
                        "Video_Description": video_desc,
                        "Tags": tags,
                        "PublishedAt": published_At,
                        "View_Count": view_count,
                        "Like_Count": like_count,
                        "Dislike_Count": dislike_count,
                        "Favorite_Count": fav_count,
                        "Comment_Count": comment_count,
                        "Duration": due,
                        "Thumbnail": thumb,
                        "Caption_Status": cpts,
                        "Comments": {}
                    }

                    # Get the video comments
                    comments = get_video_comments(youtube, video_id)

                    # Loop through the comments and add them to the video dictionary
                    for comment in comments:
                        comment_id = comment["snippet"]["topLevelComment"]["id"]
                        comment_text = comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                        comment_author = comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
                        comment_publishedAt = comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"]

                        # Add the comment to the video dictionary
                        channel_dict[playlist_id]["Videos"][video_id]["Comments"][comment_id] = {
                            "Comment_Id": comment_id,
                            "Comment_Text": comment_text,
                            "Comment_Author": comment_author,
                            "Comment_PublishedAt": comment_publishedAt
                        }

        # Define the SQL queries for each question
        queries = {
            "1. What are the names of all the videos and their corresponding channels?":
                "SELECT Video.video_name, Channel.channel_name FROM Video JOIN Playlist ON Video.playlist_id = Playlist.playlist_id JOIN Channel ON Playlist.channel_id = Channel.channel_id",
            "2. Which channels have the most number of videos, and how many videos do they have?":
                "SELECT Channel.channel_name, COUNT(Video.video_id) AS video_count FROM Channel JOIN Playlist ON Channel.channel_id = Playlist.channel_id JOIN Video ON Playlist.playlist_id = Video.playlist_id GROUP BY Channel.channel_id ORDER BY video_count DESC LIMIT 10",
            "3. What are the top 10 most viewed videos and their respective channels?":
                "SELECT Video.video_name, Channel.channel_name, Video.view_count FROM Video JOIN Playlist ON Video.playlist_id = Playlist.playlist_id JOIN Channel ON Playlist.channel_id = Channel.channel_id ORDER BY Video.view_count DESC LIMIT 10",
            "4. How many comments were made on each video, and what are their corresponding video names?":
                "SELECT Video.video_name, COUNT(Comment.comment_id) AS comment_count FROM Video LEFT JOIN Comment ON Video.video_id = Comment.video_id GROUP BY Video.video_id",
            "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
                "SELECT Video.video_name, Channel.channel_name, Video.like_count FROM Video JOIN Playlist ON Video.playlist_id = Playlist.playlist_id JOIN Channel ON Playlist.channel_id = Channel.channel_id ORDER BY Video.like_count DESC LIMIT 10",
            "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
                "SELECT Video.video_name, SUM(Video.like_count) AS total_likes, SUM(Video.dislike_count) AS total_dislikes FROM Video GROUP BY Video.video_id",
            "7. What is the total number of views for each channel, and what are their corresponding channel names?":
                "SELECT Channel.channel_name, SUM(Video.view_count) AS total_views FROM Channel JOIN Playlist ON Channel.channel_id = Playlist.channel_id JOIN Video ON Playlist.playlist_id = Video.playlist_id GROUP BY Channel.channel_id",
            "8. What are the names of all the channels that have published videos in the year 2022?":
                "SELECT DISTINCT Channel.channel_name FROM Channel JOIN Playlist ON Channel.channel_id = Playlist.channel_id JOIN Video ON Playlist.playlist_id = Video.playlist_id WHERE YEAR(Video.published_date) = 2022",
            "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
                "SELECT Channel.channel_name, AVG(Video.duration) AS average_duration_in_s FROM Channel JOIN Playlist ON Channel.channel_id = Playlist.channel_id JOIN Video ON Playlist.playlist_id = Video.playlist_id GROUP BY Channel.channel_id",
            "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
                "SELECT Video.video_name, Channel.channel_name, COUNT(Comment.comment_id) AS comment_count FROM Video JOIN Playlist ON Video.playlist_id = Playlist.playlist_id JOIN Channel ON Playlist.channel_id = Channel.channel_id JOIN Comment ON Video.video_id = Comment.video_id GROUP BY Video.video_id ORDER BY comment_count DESC LIMIT 10"
        }

        if st.button(':blue[Get Channel Information]'):

            # Print the channel dictionary
            st.write(channel_dict)
        if st.button(':green[Store data in Mongo DB]'):
            store_channel_data_mongo(channel_dict)
            st.write('Data retrieved successfully and stored in MongoDB!')

            # Retrieve the channel data from the MongoDB database
        channel_data = collection.find()

        # Create an empty list to store the channel names
        channel_names = []

        # Loop through the channel data and append the channel names to the list
        for channel in channel_data:
            channel_names.append(channel["Channel_Name"]["Channel_Name"])

        # Create a dropdown list in the Streamlit app using the `selectbox` function
        selected_channel = st.selectbox("Select a channel", channel_names)

        # Create a migrate button
        if st.button("Migrate"):
            migrate_data(selected_channel)

        # Add a button to execute the selected query
        query_selected = st.selectbox("Select a query:", list(queries.keys()))
        if st.button(":red[DISPLAY QUERY]"):
            query = queries[query_selected]
            df = execute_query(query)
            st.write(df)


    except HttpError as error:
        st.write('An error occurred: %s' % error)

    finally:
        # Close the MongoDB connection
        client.close()

        # Close the MySQL connection
        cursor.close()
        conn.close()


# Run the Streamlit app
if __name__ == "__main__":
    main()
