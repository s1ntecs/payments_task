import pymongo
import bson

# Установить соединение с MongoDB
client = pymongo.MongoClient('localhost', 27017)
db = client["payments"]
payments = db["payments"]

# Открыть файл BSON и получить коллекцию данных
with open('sample_collection.bson', 'rb') as f:
    data = bson.decode_all(f.read())
    collection = db['payments']

# Вставить данные в MongoDB
result = collection.insert_many(data)
