import asyncio
import logging
import pymongo
import json
import os
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters.command import Command
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
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

dp = Dispatcher()



@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("hello", reply_markup=ForceReply())

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
        group_field = {"$dayOfYear": "$dt"}
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

    fromquery = {
        "dataset":[],
        "labels": []
    }
    

    if group_type == "hour":
            n = relativedelta(hours=1)
    elif group_type == "day":
            n = relativedelta(days=1)
    elif group_type == "month":
            n = relativedelta(months=1)
    elif group_type == "year":
            n = relativedelta(years=1)
    date = datetime.fromisoformat(dt_from)
    while date <= datetime.fromisoformat(dt_upto):
        result["labels"].append(datetime.isoformat(date))
        result['dataset'].append(0)
        date += n
    for document in cursor:
        if group_type == "hour":
            document['labels'] = document['labels'].replace(minute=0, second=0, microsecond=0)
        elif group_type == "day":
            document['labels'] = document['labels'].replace(hour=0, minute=0, second=0, microsecond=0)
        elif group_type == "month":
            document['labels'] = document['labels'].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        elif group_type == "year":
            document['labels'] = document['labels'].replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        print(type(document['labels']))
        for i in range(len(result["labels"])):
            print(str(result["labels"][i]))
            print('from doc', document['labels'])
            if datetime.fromisoformat(result["labels"][i]) == document['labels']:
                print(result["dataset"][i], "and", document['dataset'])
                result["dataset"][i] = document['dataset']
                break
        
    print(result)
    
    await message.answer(json.dumps(result))

if __name__ == "__main__":
    asyncio.run(main())