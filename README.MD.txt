PROJECT TITLE:
YOUTUBE DATA HARVESTING & WAREHOUSING 
(using streamlit , mongodb , mysql)
OVERVIEW: 
This project is a Python script that retrieves channel, playlist, video, and comment data from the YouTube API and migrates it to a MongoDB collection and a MySQL database.
Prerequisites:
Before running the script, make sure you have:
•	The required Python libraries installed: google-api-python-client, streamlit, pymongo, mysql-connector-python, pandas.
Set up:
1.	Clone the repository to your local machine.
2.	Install the required Python libraries by running the following command:
pip install google-api-python-client streamlit pymongo mysql-connector-python pandas
3.	Set up authentication:
•	Obtain a YouTube Data API v3 API key from the Google Developers Console.
•	Replace the api_key variable in the script with your API key.
4.	Set up the MongoDB database:
•	Install MongoDB on your local machine or use a remote MongoDB service.
•	Replace the client connection URL in the script with your MongoDB connection URL.
•	Update the db and collection variables in the script to match your database and collection names.
5.	Set up the MySQL database:
•	Install MySQL on your local machine or use a remote MySQL service.
•	Replace the user, password, host, and database variables in the script with your MySQL connection details.
6.	Create the necessary database tables in MySQL:
•	Run the script to create the Channel, Playlist, Video, and Comment tables.

Usuage:
1.	Run the script by executing the main() function.
2.	Access the Streamlit web interface in your browser.
3.	Enter the YouTube channel ID in the input field and click the button to retrieve the channel information.
4.	The script will fetch the channel details, playlists, videos, and comments using the YouTube API.
5.	The retrieved data will be stored in both MongoDB and MySQL databases.
6.	You can view the migrated data in the respective databases




