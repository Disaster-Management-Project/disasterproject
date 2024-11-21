from news_fetch import NewsFetcher
from resolution import LocationResolution
from location_processor import LocationProcessor, process_json
import json
import pandas as pd
import re
import os


newsFetcher =  NewsFetcher()   
df = pd.read_csv('states.csv')
date_arr = newsFetcher.generate_monthly_dates(2024, 1, 2)
for month in date_arr:
    for index, row in df.iterrows():
        search_string = (str(row['State']) + ' landslide reports').lower()
        search_string= re.sub(r'\band\b', '', search_string)
        # Remove extra spaces resulting from the removal
        search_string= re.sub(r'\s+', ' ', search_string).strip()
        gnews_rss_url = newsFetcher.get_gnews_rss_url(search_string, country_code='IN', language_code='en', start_date=month[0], end_date=month[1])
        print(gnews_rss_url)
        print(newsFetcher.rss_to_json())
        newsFetcher.get_news_data()
        newsFetcher.trif_fetch()
        newsFetcher.add_full()

filename = 'landslide_news_data_test_new.json'
base_name, ext = os.path.splitext(filename)
# output_filename = f"{base_name}_processed{ext}"
resolution_data_file = f"{base_name}_processed{ext}"
geocoded_data_file = f"{base_name}_processed_geocoded{ext}"

with open(filename, 'w') as json_file:
    json.dump(newsFetcher.get_news_list(), json_file, indent=4)

pincode_file = "new_pincode.csv"
village_file = "village_record.csv"

processor = LocationResolution(filename, model_name = 'gemini-1.5-flash')
# Process and save the articles
processor.process_articles()

processor = LocationProcessor(pincode_file, village_file)
process_json(resolution_data_file, processor)





