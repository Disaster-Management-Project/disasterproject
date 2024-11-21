''' Functions and code to automatically fetch and download news articles from google news.'''
import json
import requests
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import *
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import re
import random
import pandas as pd
from datetime import datetime, timedelta
import calendar
from trafilatura import fetch_url, extract, html2txt
from trafilatura.meta import reset_caches
import urllib.parse
import feedparser
import html

class NewsFetcher:
    def __init__(self):
        '''initializes the news fetcher class to fetch data from the google news website'''
        self.news_list_full = []
        self.date_arr = []
        self.news_output = []
        self.rss_url = ''
        self.json_feed = {}

    def generate_weekly_dates(self, start_year, start_month, num_weeks):
        date_arr = []
        start_date = datetime(start_year, start_month, 1)
        
        # Adjust start date to the first Monday of the month
        if start_date.weekday() != 0:
            start_date += timedelta(days=(7 - start_date.weekday()))
        
        for i in range(num_weeks):
            end_date = start_date + timedelta(days=6)
            date_arr.append((start_date.date().strftime("%Y-%m-%d"), end_date.date().strftime("%Y-%m-%d")))
            # print(f"Week {i + 1}: Start Date = {start_date.date()}, End Date = {end_date.date()}")
            start_date += timedelta(days=7)
        self.date_arr = date_arr
        return None



    def generate_monthly_dates(self, start_year, start_month, num_months):
        date_arr = []
        start_date = datetime(start_year, start_month, 1)
        
        for i in range(num_months):
            year = start_date.year
            month = start_date.month
            
            # Calculate the last day of the month
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime(year, month, last_day)
            
            date_arr.append((start_date.date().strftime("%Y-%m-%d"), end_date.date().strftime("%Y-%m-%d")))
            
            # Move to the first day of the next month
            if month == 12:
                start_date = datetime(year + 1, 1, 1)
            else:
                start_date = datetime(year, month + 1, 1)
        
        self.date_arr = date_arr
        return date_arr
    
    def generate_yearly_dates(self, start_year, num_years):
        date_arr = []
        start_date = datetime(start_year, 1, 1)

        for i in range(num_years):
            year = start_date.year
            # Start date is always January 1 of the current year
            start_date = datetime(year, 1, 1)
            # End date is always December 31 of the current year
            end_date = datetime(year, 12, 31)
            
            date_arr.append((start_date.date().strftime("%Y-%m-%d"), end_date.date().strftime("%Y-%m-%d")))

            # Move to the first day of the next year
            start_date = datetime(year + 1, 1, 1)
        
        self.date_arr = date_arr
        return date_arr
    
    def get_news_data(self):
        chrome_opt = webdriver.firefox.options.Options()
        chrome_opt.add_argument('--headless')
        chrome_opt.add_argument('--disable-notifications')
        # chrome_opt.headless = True
        service = Service()
        browser = webdriver.Firefox(service=service, options=chrome_opt)
        browser.set_page_load_timeout(30)
        for news_element in self.news_output:
            if 'contents' in news_element:
                continue
            elif 'contents' not in news_element or news_element['contents'] == None:
                article_text = ''
                try:
                    browser.get(news_element['link'])
                    print('Website has been opened!')
                except:
                    assert 'A problem has occured'
                time.sleep(5)
                start_time = time.time()
                page_loaded = False
                while not page_loaded:
                    page_loaded = browser.execute_script("return document.readyState") == "complete"
                    if time.time() - start_time > 20:
                        print("Page load timed out after 20 seconds.")
                        break
                try:
                    html = browser.page_source
                    bs = BeautifulSoup(html, 'html.parser')
                    p_data = ''
                    for data in bs.find_all('p'):
                        p_data = p_data + ' ' + str(data.get_text()).replace('\n', ' ')
                        # print(p_data)
                except:
                    assert 'Error in extracting content!'
                try:
                    news_element['contents'] = p_data
                    news_element['title'] = news_element['title'].encode('latin-1').decode('utf-8')
                    # news_article_data.append(news_element)
                    news_element['link'] = browser.current_url
                except:
                    continue
        # time.sleep(3)
        browser.quit()
        return None
    
    def trif_fetch(self):
        for article in self.news_output:
            url = article['link']
            try: 
                downloaded = fetch_url(url)
                result = extract(downloaded)
            except:
                 print('Error in fetching with trif!') 
            # print(result
            if result != None:
                article['contents'] =  result
        reset_caches()
        return None
        
    def get_gnews_rss_url(self, search_string, country_code="IN", language_code="en", time_period="", start_date=None, end_date=None):
        base_url = "https://news.google.com/rss/search"
        query_params = {
            "q": search_string,
            "hl": language_code,
            "gl": country_code,
            "ceid": f"{country_code}:{language_code}"
        }
        
        if start_date:
            query_params["q"] += f" after:{start_date}"
        
        if end_date:
            query_params["q"] += f" before:{end_date}"
        
        query_string = urllib.parse.urlencode(query_params)
        self.rss_url = f"{base_url}?{query_string}"
        return self.rss_url


    def convert_unicode(self, text):
        if all(ord(char) < 128 for char in text):
            return text
        else:
            return bytes(text, 'utf-8').decode('unicode-escape')
        
    def rss_to_json(self, encoding='utf-8'):
        """
        Convert an RSS feed to JSON format.

        Args:
            rss_url (str): The URL of the RSS feed.
            encoding (str, optional): The character encoding of the RSS feed. Default is 'utf-8'.

        Returns:
            dict: The RSS feed data in JSON format.
        """
        feed = feedparser.parse(self.rss_url, response_headers={'Content-Type': f'application/rss+xml; charset={encoding}'})
        json_feed = {
            "feed": {
                "title": html.unescape(self.convert_unicode(feed.feed.title)),
                "description": html.unescape(self.convert_unicode(feed.feed.description)),
                "link": feed.feed.link
            },
            "entries": []
        }

        for entry in feed.entries:
            json_entry = {
                "title": html.unescape(self.convert_unicode(entry.title)),
                "link": entry.link,
                "published": entry.published,
                "summary": html.unescape(self.convert_unicode(entry.summary))
            }
            json_feed["entries"].append(json_entry)

        self.news_output += json_feed["entries"]
        return json_feed
    
    def get_news_list(self):
        return self.news_list_full
    
    def add_full(self):
        self.news_list_full += self.news_output
        try:
            self.news_output = []
        except:
            print("Error in emptying news output!")


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

filename = 'landslide_news_data_test_1.json'
with open(filename, 'w') as json_file:
    json.dump(newsFetcher.get_news_list(), json_file, indent=4)
