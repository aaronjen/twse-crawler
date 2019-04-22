# -*- coding: utf-8 -*-
import scrapy
import os
from pandas import date_range
from datetime import datetime, timedelta
import json
import re

DATA_DIR = './data'
# url ex: http://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=20190419&type=ALL&_=155584230

class TwseSpider(scrapy.Spider):
    name = 'twse'
    allowed_domains = ['twse.com']
    
    def start_requests(self):
        self.logger.info('Check data folder exist...')
        if not os.path.exists(DATA_DIR):
            self.logger.info('Data folder not exist...')
            os.mkdir(DATA_DIR)
            self.logger.info('Create Data folder...')
        
        end = datetime.now()
        start = end - timedelta(days=5 * 365)

        # TODO
        self.logger.info('Check latest data file...')
        data_list = sorted(os.listdir(DATA_DIR))
        if len(data_list) > 0:
            latest_file = data_list[-1]
            search = re.search(r'\d+', latest_file)
            start = datetime.strptime(search.group(0), '%Y%m%d')

        start_str = start.strftime('%Y-%m-%d')
        end_str = end.strftime('%Y-%m-%d')
        self.logger.info(f'Create requests from {start_str} to {end_str}')
        
        for date in date_range(start=start, end=end):
            date_str = date.strftime('%Y%m%d')
            url = f'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALL&_=155584230'

            yield scrapy.Request(url=url, callback=self.parse_json)
            
    def parse_json(self, response):
        self.logger.info(f'fetch {response.url} done...')
        text = response.text
        data = json.loads(text)
        if data['stat'] == 'OK':
            file_name = f'{data["date"]}.json'
            self.logger.info(f'{file_name} saved...')
            with open(os.path.join(DATA_DIR, file_name), 'w') as f:
                f.write(text)
