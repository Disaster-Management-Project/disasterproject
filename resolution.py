import pandas as pd
import google.generativeai as genai
import json
import time
import statistics
import random
import re
import os

class LocationResolution:
    def __init__(self, filename, model_name):
        ''' set all credentials required for gemini'''
        self.gemini_api_key = 'AIzaSyDlTLIqfuNaITcgWEn-mCNtN3YDEacpRas'
        self.filename = filename
        self.articles = []
        genai.configure(api_key = self.gemini_api_key)
        self.model = genai.GenerativeModel(model_name)
        self.max_requests = 1500
        self.request_count = 0
        
    @staticmethod
    def remove_keywords(text):
        """
        Remove specified keywords from a text string.
        :param text: The text to clean.
        :return: The cleaned text.
        """
        keywords = [
            "village", "hamlet", "settlement",
            "town", "city", "municipality",
            "locality", "neighborhood", "neighbourhood", "vicinity", "community",
            "area", "region", "zone", "district", "near"
        ]
        pattern = r'\b(' + '|'.join(keywords) + r')\b'
        cleaned_text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        return re.sub(r'\s+', ' ', cleaned_text).strip()

    @staticmethod
    def clean_location_records(records):
        """
        Clean the location data in each record.
        :param records: A list of location records.
        :return: The cleaned records.
        """
        for record in records:
            for key in record.keys():
                if record[key] is None:
                    record[key] = ""
            for key in ['nearby', 'village_name/town_name', 'area_name', 'district_name', 'state_name']:
                if key in record:
                    record[key] = LocationResolution.remove_keywords(record[key])
        return records

    @staticmethod
    def generate_prompt(article_content):
        """
        Generate the prompt for processing the article.
        :param article_content: The content of the article.
        :return: The complete prompt string.
        """
        prompt = (
            "In this prompt a news article is provided. The news article contains news reporting about landslides that have occurred in India. "
            "Go through the article thoroughly and populate the provided json format. In the 'landslide_report' field, put yes or no to indicate whether it is a landslide report. "
            "In the 'village/town' field, mention 'village' or 'town'. The rest put whatever information you get from the article. Leave the fields blank for which you do not get any information. "
            "Keep the fields as is. If there are multiple landslide locations, keep them appending in the 'locations' array in the suggested output format. "
            "DO NOT put null if there is no information, just leave it blank."
        )
        sample_op_format = """{
            "landslide_report":, 
            "source_name":"", 
            "locations":[
                {
                    "nearby":"", 
                    "road_name":"", 
                    "village/town": "", 
                    "village_name/town_name":, 
                    "area_name":"", 
                    "district_name":"", 
                    "state_name":"", 
                    "landslide_type":,
                    "casualty_description":"", 
                    "landslide_size":"", 
                    "triggering_factor":"", 
                    "infrastructural_damage":"", 
                    "date":"", 
                    "time":""
                }
            ]
        }"""
        return f"{prompt}\n{article_content}\n{sample_op_format}"

    def process_article(self, article):
        """
        Process a single article and populate the 'landslide_record' field.
        :param article: The article dictionary.
        """
        if 'landslide_record' in article or article['contents'] == None or article['contents'] == "":
            return  # Skip processing if already done or there is no content

        prompt = self.generate_prompt(article['contents'])

        if self.request_count < self.max_requests:
            try:
                response = self.model.generate_content(
                    prompt,
                    generation_config={
                        "response_mime_type": "application/json",
                        "temperature": 1.0
                    }
                )
                self.request_count += 1
                print(f"Request count: {self.request_count}")

                try:
                    response_data = json.loads(response.text)
                    article['landslide_record'] = response_data
                except json.JSONDecodeError:
                    print("Response is not a valid JSON!")
            except Exception as e:
                print(f"Error processing article: {e}")

            time.sleep(5)  # Pause to avoid hitting rate limits
        else:
            print("Request limit reached. Skipping further processing.")

    def clean_articles(self, articles):
        """
        Clean the processed articles by removing keywords and handling missing data.
        :param articles: A list of article dictionaries.
        """
        for article in articles:
            if 'landslide_record' in article and article['landslide_record'].get('landslide_report') == "yes":
                article['landslide_record']['locations'] = self.clean_location_records(article['landslide_record']['locations'])

    def save_articles(self, articles):
        """
        Save the processed articles to a dynamically generated output filename.
        :param articles: A list of article dictionaries.
        :param input_filename: The name of the input file.
        """
        base_name, ext = os.path.splitext(self.filename)
        output_filename = f"{base_name}_processed{ext}"

        with open(output_filename, 'w') as f:
            json.dump(articles, f, indent=4)
        print(f"Articles saved to {output_filename}")

    def process_articles(self):
        """
        Process a list of articles, clean them, and save the results.
        :param articles: A list of article dictionaries.
        """
        # Load articles from the input file
        with open(self.filename, 'r') as f:
            self.articles = json.load(f)

        for article in self.articles:
            self.process_article(article)
        self.clean_articles(self.articles)
        self.save_articles(self.articles)

# Example input filename
input_filename = 'landslide_news_data_test.json'



# Initialize the processor with your model
processor = LocationResolution(input_filename, model_name = 'gemini-1.5-flash')
# Process and save the articles
processor.process_articles()