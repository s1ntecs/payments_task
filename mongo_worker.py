import os
from datetime import datetime, timedelta
from typing import Dict
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


async def aggregate_payments(dt_from: str,
                             dt_upto: str,
                             group_type: str) -> Dict:
    """
        Функция считает сумму всех выплат за период
        c dt_from по dt_upto и группирует по group_type
    """
    start_date = datetime.fromisoformat(dt_from)
    end_date = datetime.fromisoformat(dt_upto)
    client = AsyncIOMotorClient(DATABASE_URL)
    db = client["payments"]
    payments = db["payments"]
    pipeline = [
        {
            "$match": {
                "dt": {
                    "$gte": start_date,
                    "$lte": end_date,
                }
            }
        }
    ]
    # Тип группирования
    match group_type:
        case "hour":
            group_id = {
                "$dateToString": {
                    "format": "%Y-%m-%dT%H:%M:%S",
                    "date": {
                        "$dateFromParts": {
                            "year": {"$year": "$dt"},
                            "month": {"$month": "$dt"},
                            "day": {"$dayOfMonth": "$dt"},
                            "hour": {"$hour": "$dt"}
                        }},
                    "timezone": "UTC",
                }
            }

        case "day":
            group_id = {
                "$dateToString": {
                    "format": "%Y-%m-%dT%H:%M:%S",
                    "date": {
                        "$dateFromParts": {
                            "year": {"$year": "$dt"},
                            "month": {"$month": "$dt"},
                            "day": {"$dayOfMonth": "$dt"},
                        }
                    },
                    "timezone": "UTC",
                }
            }

        case "month":
            group_id = {
                "$dateToString": {
                    "format": "%Y-%m-%dT%H:%M:%S",
                    "date": {
                        "$dateFromParts": {
                            "year": {"$year": "$dt"},
                            "month": {"$month": "$dt"},
                        }
                    },
                    "timezone": "UTC",
                }
            }

        case _:
            raise ValueError(f"Invalid group_type: {group_type}")
    # Добавляем группирование
    pipeline.append(
        {"$group": {"_id": group_id, "total_amount": {"$sum": "$value"}}})
    # Добавляем сортировку
    pipeline.append({"$sort": {"_id": 1}})
    # Посылаем запрос в БД на аггрегирование
    cursor = payments.aggregate(pipeline)
    result = await cursor.next()
    dataset = []
    labels = []
    current_date = start_date
    while current_date <= end_date:
        dt = current_date.isoformat()
        try:
            if result is not None and result['_id'] == dt:
                dataset.append(result['total_amount'])
                labels.append(result['_id'])
                result = await cursor.next()
            else:
                dataset.append(0)
                labels.append(dt)
        except StopAsyncIteration:
            pass
        if group_type == 'day':
            current_date += timedelta(days=1)
        elif group_type == 'hour':
            current_date += timedelta(hours=1)
        if group_type == 'month':
            if current_date.month == 12:
                current_date = current_date.replace(
                    day=1, month=1, year=current_date.year+1)
            else:
                current_date = current_date.replace(
                    day=1, month=current_date.month+1)

    return {"dataset": dataset, "labels": labels}
