# Importing Dependencies
import pandas as pd
import warnings
import spotipy
import logging
import yaml

# Filtering out potential warnings
warnings.filterwarnings(action = "ignore")

# Creating a logger file
logging.basicConfig(filename = "C:/Users/Kanan/Desktop/Taskilled/Capstone Project/Logs/spotify_music.log",
                    filemode = "w",
                    format = "%(asctime)s - %(levelname)s - %(message)s",
                    level = logging.INFO)

# Loading the configuration file
with open(file = "C:/Users/Kanan/Desktop/Taskilled/Capstone Project/Configuration/config.yml") as yaml_file:
    config = yaml.safe_load(stream = yaml_file)

# Extracting the client secret
sp_client_secret = config.get("client_secret")    
    
# Extracting the client ID
sp_client_id = config.get("client_id")

# Setting up the authentication with Spotify API
sp_credentials_manager = spotipy.oauth2.SpotifyClientCredentials(client_id = sp_client_id, client_secret = sp_client_secret)

# Instantiating the crawler
sp = spotipy.Spotify(client_credentials_manager = sp_credentials_manager)

# Extracting the list of tracks
sp_tracks = config.get("singers_tracks").values()

# Extracting the list of singers
sp_singers = config.get("singers_tracks").keys()

# Extracting the filepath
filepath = config.get("filepath")

# Defining a function to scrape data
class SpotifyAPICrawler:
    # Defining the instance attributes
    def __init__(self, singers, tracks):
        self.singers = singers
        self.tracks = tracks
        
    # Defining the instance method to crawl audio features
    def extract_audio_features(self):
        # Logging information to the log file
        logging.info(msg = "Web scraping process has started!")
        
        # Creating an empty list to store data frames
        audio_data = []
        
        # Looping through zipped list
        for singer, tracks in list(zip(self.singers, self.tracks)):
            # Looping through each track
            for track in tracks:
                # Defining the search query
                query = f"track:{track} artist:{singer}"
        
                # Running the query to extract data in JSON format
                search_results = sp.search(q = query, type = "track")

                try:
                    # Extracting the track ID
                    track_id = search_results.get("tracks").get("items")[0].get("id")
        
                    # Extracting the audio features
                    audio_features = sp.audio_features(tracks = track_id)[0]
            
                    # Creating a dictionary to store relevant features
                    audio_dict = {"singer": singer,
                                  "track": track,
                                  "danceability": audio_features.get("danceability"),
                                  "energy": audio_features.get("energy"),
                                  "loudness": audio_features.get("loudness"),
                                  "speechiness": audio_features.get("speechiness"),
                                  "acousticness": audio_features.get("acousticness"),
                                  "instrumentalness": audio_features.get("instrumentalness"),
                                  "liveness": audio_features.get("liveness"),
                                  "valence": audio_features.get("valence"),
                                  "tempo": audio_features.get("tempo")}

                    # Converting dictionary to a data frame
                    audio_df = pd.DataFrame(data = audio_dict, index = [0])
                    
                    # Appending to the list
                    audio_data.append(audio_df)
                    
                    # Logging information to the log file
                    logging.info(msg = f"Audio features of {track} by {singer} has been extracted successfully!")
                except:
                    # Passing to the next track in case an error occurs
                    pass
                
        # Concatenating the data frames
        spotify_data = pd.concat(objs = audio_data, ignore_index = True)

        # Removing potential dublicate observations
        spotify_data.drop_duplicates(inplace = True, ignore_index = True)
        
        # Asserting the number of duplicate observations to be equal to zero
        assert spotify_data.duplicated().sum() == 0

        # Shuffling the data
        spotify_data = spotify_data.sample(frac = 1.0, random_state = 42).reset_index(drop = True)
        
        # Defining the unique number of singers
        n_singers = spotify_data.singer.nunique()
        
        # Defining the unique number of tracks
        n_tracks = spotify_data.shape[0]
        
        # Logging information to the log file
        logging.info(msg = f"Audio features of {n_tracks} tracks by {n_singers} singers have been extracted successfully!")
        
        # Writing the data to a separate csv file
        spotify_data.to_csv(path_or_buf = filepath, index = False)
    
# Running the script
if __name__ == "__main__":
    # Creating an instance of a class
    crawler = SpotifyAPICrawler(singers = sp_singers, tracks = sp_tracks)
    
    # Calling the function
    crawler.extract_audio_features()