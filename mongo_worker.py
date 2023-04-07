import asyncio
from datetime import datetime
from typing import Dict, List
from motor.motor_asyncio import AsyncIOMotorClient


DATABASE_URL = "mongodb://localhost:27017/payments"


async def aggregate_payments(dt_from: str,
                             dt_upto: str,
                             group_type: str) -> Dict[str, List[int]]:
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

    pipeline.append(
        {"$group": {"_id": group_id, "total_amount": {"$sum": "$value"}}})
    pipeline.append({"$sort": {"_id": 1}})

    result = await payments.aggregate(pipeline).to_list(None)
    dataset = [r["total_amount"] for r in result]
    labels = [r["_id"] for r in result]
    return {"dataset": dataset, "labels": labels}


# asyncio.run(
#     aggregate_payments(
#         dt_from="2022-10-01T00:00:00",
#         dt_upto="2022-11-30T23:59:00",
#         group_type="day",
#     )
# )
