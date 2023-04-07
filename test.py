import asyncio
from datetime import datetime, timedelta
from typing import Dict, List
import time
from motor.motor_asyncio import AsyncIOMotorClient


DATABASE_URL = "mongodb://localhost:27017/payments"


async def aggregate_payments(dt_from: str,
                             dt_upto: str,
                             group_type: str) -> Dict[str, List[int]]:

    client = AsyncIOMotorClient(DATABASE_URL)
    db = client["payments"]
    payments = db["payments"]
    pipeline = []

    pipeline.append(
        {
         "$match": {
            "dt": {
                "$gte": datetime.fromisoformat(dt_from),
                "$lte": datetime.fromisoformat(dt_upto)
            }
         }
        }
    )

    # if group_type == "hour":
    #     group_id = {"$dateToString":
    #                 {"format": "%Y-%m-%dT%H:%M:%S",
    #                  "date": {"$dateFromParts":
    #                           {"year": {"$year": "$dt"},
    #                            "month": {"$month": "$dt"},
    #                            "day": {"$dayOfMonth": "$dt"},
    #                            "$hour": "$dt"
    #                            }
    #                           },
    #                  "timezone": "UTC"}}
    #     group = {"_id": group_id, "total_amount": {"$sum": "$value"}}

    # if group_type == "day":
    #     group_id = {"$dateToString":
    #                 {"format": "%Y-%m-%dT%H:%M:%S",
    #                  "date": {"$dateFromParts":
    #                           {"year": {"$year": "$dt"},
    #                            "month": {"$month": "$dt"},
    #                            "day": {"$dayOfMonth": "$dt"}
    #                            }
    #                           },
    #                  "timezone": "UTC"}},
    #     group = {"_id": group_id, "total_amount": {"$sum": "$value"}}

    # elif group_type == "month":
    #     group_id = {"$dateToString":
    #                 {"format": "%Y-%m-%dT%H:%M:%S",
    #                  "date": {"$dateFromParts":
    #                           {"year": {"$year": "$dt"},
    #                            "month": {"$month": "$dt"}
    #                            }
    #                           },
    #                  "timezone": "UTC"}},
    #     group = {"_id": group_id, "total_amount": {"$sum": "$value"}}
    match group_type:
        case "hour":
            group_id = {"$dateToString":
                        {"format": "%Y-%m-%dT%H:%M:%S",
                        "date": {"$dateFromParts":
                                {"year": {"$year": "$dt"},
                                "month": {"$month": "$dt"},
                                "day": {"$dayOfMonth": "$dt"},
                                "$hour": "$dt"
                                }
                                },
                        "timezone": "UTC"}}
            group = {"_id": group_id, "total_amount": {"$sum": "$value"}}

        case "day":
            group_id = {"$dateToString":
                        {"format": "%Y-%m-%dT%H:%M:%S",
                        "date": {"$dateFromParts":
                                {"year": {"$year": "$dt"},
                                "month": {"$month": "$dt"},
                                "day": {"$dayOfMonth": "$dt"}
                                }
                                },
                        "timezone": "UTC"}},
            group = {"_id": group_id, "total_amount": {"$sum": "$value"}}

        case "month":
            group_id = {"$dateToString":
                        {"format": "%Y-%m-%dT%H:%M:%S",
                        "date": {"$dateFromParts":
                                {"year": {"$year": "$dt"},
                                "month": {"$month": "$dt"}
                                }
                                },
                        "timezone": "UTC"}},
            group = {"_id": group_id, "total_amount": {"$sum": "$value"}}

        case _:
            raise ValueError(f"Invalid group_type: {group_type}")

    pipeline.append({"$group": group})
    pipeline.append({"$sort": {"_id": 1}})

    result = await payments.aggregate(pipeline).to_list(None)
    dataset = [r["total_amount"] for r in result]
    labels = [r["_id"][0] for r in result]
    print({"dataset": dataset, "labels": labels})
    return {"dataset": dataset, "labels": labels}

asyncio.run(aggregate_payments(dt_from="2022-09-01T00:00:00", dt_upto="2022-12-31T23:59:00", group_type="month"))
