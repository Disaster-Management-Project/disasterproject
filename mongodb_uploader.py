import pandas as pd
from pymongo import MongoClient
import json
import time
from datetime import datetime


class MongoDBUploader:
    def __init__(self, uri):
        self.client = MongoClient(uri)
        self.db = None
        self.coll = None

    def set_collection(self, database_name, collection_name):
        """Set the target MongoDB collection."""
        self.db = self.client[database_name]
        self.coll = self.db[collection_name]

    def load_json(self, filename):
        """Load JSON data from a file."""
        with open(filename, 'r') as file:
            return json.load(file)

    def bulk_insert(self, data, batch_size=100, delay=10):
        """Insert data into the collection in batches."""
        for i in range(0, len(data), batch_size):
            try:
                self.coll.insert_many(data[i:i + batch_size])
                print(f"Inserted batch {i // batch_size + 1}.")
            except Exception as e:
                print(f"Error during insertion: {e}")
            time.sleep(delay)

    def insert_location_data(self, articles, date_format):
        """Insert location-specific data into the collection."""
        for article in articles:
            if 'landslide_record' in article:
                record = article['landslide_record']
                if 'locations' in record:
                    for location in record['locations']:
                        loc_record = self._extract_location_record(article, location, date_format)
                        if loc_record:
                            try:
                                self.coll.insert_one(loc_record)
                                print(f"Inserted location record for {loc_record['url']}.")
                            except Exception as e:
                                print(f"Error inserting location record: {e}")

    def _extract_location_record(self, article, location, date_format):
        """Extract location record from the article."""
        if 'location' in location:
            loc = location['location']
            if isinstance(loc, list):
                if loc[0] == '' and loc[1] == '':
                    return None
                return {
                    'url': article['link'],
                    'date': datetime.strptime(article['published'], date_format),
                    'lat': loc[0],
                    'lon': loc[1],
                    'state': location.get('state_name')
                }
            else:
                if loc.get('lat') == '' and loc.get('lon') == '':
                    return None
                return {
                    'url': article['link'],
                    'date': datetime.strptime(article['published'], date_format),
                    'lat': loc['lat'],
                    'lon': loc['lon'],
                    'state': location.get('state_name')
                }
        return None

    def delete_by_url(self, url):
        """Delete documents by URL."""
        result = self.coll.delete_many({'url': url})
        print(f"Deleted {result.deleted_count} documents.")

    def close_connection(self):
        """Close the MongoDB client connection."""
        self.client.close()
        print("Connection closed.")


if __name__ == "__main__":
    # MongoDB URI and file configuration
    uri = '<your-mongodb-uri>'
    filename = 'news_data_2024_jul_aug_loc.json'
    date_format = '%a, %d %b %Y %H:%M:%S %Z'

    # Initialize MongoDBUploader
    uploader = MongoDBUploader(uri)

    # Set collection and load data
    uploader.set_collection("test_landslide", "landslide_news")
    articles = uploader.load_json(filename)

    # Perform bulk insert
    uploader.bulk_insert(articles)

    # Insert location data into another collection
    uploader.set_collection("landslide_inventory", "landslide_loc_news")
    uploader.insert_location_data(articles, date_format)

    # Example delete operation
    test_url = 'https://economictimes.indiatimes.com/news/india/...'
    uploader.delete_by_url(test_url)

    # Close connection
    uploader.close_connection()
