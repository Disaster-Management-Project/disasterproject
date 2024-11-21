import pandas as pd
import numpy as np
import json
import math
from difflib import SequenceMatcher
import re
from anytree import Node
import requests
import os

class LocationProcessor:
    """
    A class for handling geographical and location data processing.
    """
    def __init__(self, pincode_file, village_file):
        # Load data files
        self.df = pd.read_csv(pincode_file, low_memory=False, keep_default_na=False, na_values=np.nan)
        self.village_df = pd.read_csv(village_file, low_memory=False, keep_default_na=False, na_values=np.nan)
        self.tomtom_key = '2TSfZpz5CAX9JuBvV6rKgck4qRGUuZrm'
        self.here_key = 'HutZZzn35Qr_Ej-wsHo4sJhFqDlmlYUsYQsgv3fBBAI'

    @staticmethod
    def find_similarity(string1, string2):
        """
        Check if one string is a subsequence of another.
        """
        index = 0
        for char in string2:
            index = string1.find(char, index)
            if index == -1:
                return False
            index += 1
        return True

    @staticmethod
    def similarity_percentage(string1, string2):
        """
        Calculate the similarity percentage between two strings.
        """
        s = SequenceMatcher(None, string1, string2)
        return s.ratio()

    @staticmethod
    def remove_punctuation(text):
        """
        Remove punctuation from a given text.
        """
        import string
        translation_table = str.maketrans('', '', string.punctuation)
        return text.translate(translation_table)

    def search_dataset(self, column, search_string):
        """
        Search the dataset for rows matching a string using regex.
        """
        regex_pattern = re.compile(f".*{re.escape(search_string)}.*", flags=re.IGNORECASE)
        return self.df[self.df[column].apply(lambda x: bool(regex_pattern.match(str(x))))]

    def search_locations(self, loc_array):
        """
        Search for matching locations based on the provided data structure.
        """
        places_df = self.search_dataset('OfficeName', loc_array['village_name/town_name'])
        return places_df

    def create_tree(self, loc_array, places_df):
        """
        Create a tree structure from location data.
        """
        root_node = Node(loc_array['village_name/town_name'])
        for _, row in places_df.iterrows():
            state_name = Node(row['StateName'], parent=root_node)
            district_name = Node(row['District'], parent=state_name)
            office_name = Node(row['OfficeName'], parent=district_name)
        return root_node

    def find_nearby_location(self, loc_array, candidate_loc):
        """
        Find the most accurate nearby location using the TomTom API.
        """
        base_url = 'https://api.tomtom.com/search/2/poiSearch/'
        final_url = (
            f"{base_url}{loc_array['village_name/town_name']}.json?"
            f"limit=10&lat={candidate_loc[1]}&lon={candidate_loc[2]}"
            f"&view=IN&radius=20000&relatedPois=off&key={self.tomtom_key}"
        )
        final_url = final_url.replace(' ', '+')
        print("TomTom API URL:", final_url)

        rsp = requests.get(final_url)
        if rsp.status_code != 200:
            print("TomTom API Request Failed:", rsp.status_code)
            return [candidate_loc[1], candidate_loc[2]]

        place_data = rsp.json()
        latlon = [candidate_loc[1], candidate_loc[2]]

        # Enhance results with similarity scores
        for result in place_data.get('results', []):
            acc = self.find_similarity_percentage(
                loc_array['village_name/town_name'].lower(),
                result['poi']['name'].lower()
            )
            result['poi']['accuracy'] = acc

        # Find the best match based on accuracy
        best_result = max(
            place_data.get('results', []),
            key=lambda x: x['poi']['accuracy'],
            default=None
        )
        if best_result:
            latlon = best_result['position']

        return latlon

    def geocode_locations(self, loc_array):
        """
        Perform geocoding using the HERE API.
        """
        base_url = 'https://geocode.search.hereapi.com/v1/geocode?q='

        # Construct the URL based on available location data
        if loc_array.get('district_name', ''):
            final_url = (
                f"{base_url}{loc_array['village_name/town_name']}+"
                f"{loc_array['district_name']}+{loc_array['state_name']}"
                f"&apikey={self.here_key}"
            )
        elif 'pincode' in loc_array:
            final_url = (
                f"{base_url}{loc_array['village_name/town_name']}+"
                f"{loc_array['district_name']}+{loc_array['pincode']}+{loc_array['state_name']}"
                f"&apikey={self.here_key}"
            )
        else:
            final_url = (
                f"{base_url}{loc_array['village_name/town_name']}+"
                f"{loc_array['state_name']}&apikey={self.here_key}"
            )

        final_url = final_url.replace(' ', '+')
        print("HERE API URL:", final_url)

        rsp = requests.get(final_url)
        if rsp.status_code != 200:
            print("HERE API Request Failed:", rsp.status_code)
            return []

        place_data = rsp.json()
        latlon = []

        # Extract the best result based on queryScore
        best_result = max(
            place_data.get('items', []),
            key=lambda x: x['scoring']['queryScore'],
            default=None
        )
        if best_result:
            latlon = [best_result['position']['lat'], best_result['position']['lng']]

        return latlon
    
    def save_data(self, articles, json_file):
        base_name, ext = os.path.splitext(json_file)
        output_filename = f"{base_name}_geocoded{ext}"

        with open(output_filename, 'w') as f:
            json.dump(articles, f, indent=4)
        print(f"Articles saved to {output_filename}")


# Driver code
def process_json(json_file, processor):
    with open(json_file, 'r') as file:
        articles = json.load(file)

    for article in articles:
        print("New Article")
        if 'landslide_record' in article:
            if article['landslide_record']['landslide_report'] == "yes":
                records = article['landslide_record']['locations']
                for record in records:
                    if 'location' not in record:
                        loc_array = record
                        try:
                            # Search locations and build tree
                            places = processor.search_locations(loc_array)
                            root = processor.create_tree(loc_array, places)
                            
                            # Determine candidate location
                            if loc_array.get('village/town') == 'village':
                                # Add `search_tree_village` logic if defined
                                candidate_loc = None  # Placeholder
                            else:
                                # Add `search_tree` logic if defined
                                candidate_loc = None  # Placeholder

                            print("Processed Location Array:", loc_array)
                            print("Candidate Location:", candidate_loc)

                            if candidate_loc is None or (
                                math.isnan(candidate_loc[0]) and math.isnan(candidate_loc[1])
                            ):
                                loc = processor.geocode_locations(loc_array)
                            else:
                                loc = processor.find_nearby_location(loc_array, candidate_loc)

                            record['location'] = loc
                        except Exception as e:
                            print("Error processing record:", e)
    processor.save_data(articles, json_file)                   
    

# File paths
pincode_file = "new_pincode.csv"
village_file = "village_record.csv"
# json_file = "landslide_news_data_test_processed.json"

# Instantiate the class and process the JSON file
# processor = LocationProcessor(pincode_file, village_file)
# process_json(json_file, processor)
