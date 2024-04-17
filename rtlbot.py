import asyncio
import logging
import pymongo
import json
import os
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters.command import Command
from datetime import datetime
from aiogram.types import Message
from aiogram.types.force_reply import ForceReply
from aiogram.filters import Command
from dotenv import load_dotenv
from dataclasses import dataclass


load_dotenv()

@dataclass  
class Secrets:  
    token: str = os.environ.get("BOT_TOKEN") 

logging.basicConfig(level=logging.INFO)

bot = Bot(token=Secrets.token)
# bot = Bot(token='6726224615:AAEwI9h1VMDz9g01cgv540FlN8XgcZQzpog')

dp = Dispatcher()



@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("hello", reply_markup=ForceReply())

# Запуск процесса поллинга новых апдейтов
async def main():
    await dp.start_polling(bot)


@dp.message(F.text.contains('dt_from'))
async def aggregate(message: Message):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    database = client["test"]
    
    python_dict = json.loads(message.text)
    dt_from, dt_upto, group_type =  python_dict['dt_from'], python_dict['dt_upto'], python_dict['group_type']
    print(dt_from, dt_upto, group_type)

    if group_type == "hour":
        group_field = {"$hour": "$dt"}
    elif group_type == "day":
        group_field = {"$dayOfMonth": "$dt"}
    elif group_type == "month":
        group_field = {"$month": "$dt"}
    elif group_type == "year":
        group_field = {"$year": "$dt"}
    else:
        raise ValueError(f"Invalid group_type value: {group_type}")
    print(group_type)

    collection = database["sample_collection"]

    cursor = collection.aggregate([
            {"$match": {
                "dt":{"$gte": datetime.fromisoformat(dt_from), "$lte": datetime.fromisoformat(dt_upto)}
            }},
            {"$group":
                {
                "_id":group_field,
                "labels":{"$first":"$dt"},
                "dataset":{"$sum":"$value"}
                }
        },
        {
        '$sort': {
            '_id': 1
        }
    }])

    result = {
        "dataset":[],
        "labels": []
    }
    
    for document in cursor:
        document['labels'] = document['labels'].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        print(datetime.isoformat(document['labels']))
        result["dataset"].append(document['dataset'])
        result['labels'].append(datetime.isoformat(document['labels']))

    print(result)
    
    await message.answer(json.dumps(result))

if __name__ == "__main__":
    asyncio.run(main())