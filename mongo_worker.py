import os
from datetime import datetime
from typing import Dict, List
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
    client = AsyncIOMotorClient(DATABASE_URL)
    db = client["payments"]
    payments = db["payments"]
    pipeline = [
        {
            "$match": {
                "dt": {
                    "$gte": datetime.fromisoformat(dt_from),
                    "$lte": datetime.fromisoformat(dt_upto),
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
                    "date": "$dt",
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
    result = await payments.aggregate(pipeline).to_list(None)
    dataset = [r["total_amount"] for r in result]
    labels = [r["_id"] for r in result]
    return {"dataset": dataset, "labels": labels}
