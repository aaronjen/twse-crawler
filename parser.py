import json
import os
from pymongo import MongoClient
import pymongo

# CONSTANT
DATA_DIR = './data'

client = MongoClient()

# print('reset database...')
# client.drop_database('trading')

db = client.trading

collection = db.stock
collection.create_index('date')


data_list = os.listdir(DATA_DIR)

def parse_float(s):
  try:
    return float(s.replace(',', ''))
  except:
    return s
  

def parse_int(s):
  try:
    return int(s.replace(',', ''))
  except:
    return s
  

# ['證券代號', '證券名稱', '成交股數', '成交筆數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示買量', '最後揭示賣價', '最後揭示賣量', '本益比']
# find last date data in database
last_record = collection.find_one(sort=[('date', -1)])
for d in sorted(data_list):
  if last_record and last_record['date'] + '.json' >= d:
    continue

  with open(os.path.join(DATA_DIR, d), 'rb') as f:
    raw_data = json.load(f)
    date = raw_data['date']
    field = raw_data['fields5']
    data = raw_data['data5']

    items = []
    for row in data:
      try:
        item = {
          "date": date,
          "code": row[0],
          "volume": parse_int(row[2]),
          "transaction": parse_int(row[3]),
          "value": parse_int(row[4]),
          "open": parse_float(row[5]),
          "high": parse_float(row[6]),
          "low": parse_float(row[7]),
          "close": parse_float(row[8]),
          "change": -1 * parse_float(row[10]) if 'green' in row[9] else parse_float(row[10]),
          "priceEarningRatio": parse_float(row[15]),
        }
        items.append(item)
      except:
        print(row)
        exit()

    print(f'insert {d} into database...')
    insert_result = collection.insert_many(items)

    if len(insert_result.inserted_ids) != len(items):
      print(f'{d} insert error!!')
      exit()

    
    
