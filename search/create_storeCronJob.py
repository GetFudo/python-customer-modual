from pymongo import MongoClient
from bson import ObjectId
import os
from datetime import datetime, timedelta

MONGO_URL = "mongodb+srv://root_db:fgtyRhpGeir5RbsH@getfudo.qvcx9.mongodb.net/getfudoDB?retryWrites=True&w=majority&readPreference=secondary"
MONGO_DB_NAME = "getfudoDB"


print('for cronJob Url : ' , MONGO_URL)

client = MongoClient(MONGO_URL)
db = client[MONGO_DB_NAME]

stream_action = {
    "update":"patch",
    "insert":"patch",
    "delete":"delete"
}

zone_details = db.zones.find({"status":1,"title":"Zone 1"})
''' get a zone details '''

for zone in zone_details:

    ''' recomdedStoresByZone - get those store which have avgratting more then 0'''

    recomded_stores_details = db.stores.find({'serviceZones.zoneId': str(zone['_id']), "avgRating": {'$gt': 0}})
    for stores in recomded_stores_details:
        if str(stores['status']) == "1":
            specialities_data_list = []
            if 'specialities' in stores and len(stores['specialities']) > 0:
                for spe in stores['specialities']:
                    if spe != "":
                        specialities_data = db.specialities.find_one({'_id': ObjectId(spe)})
                        if specialities_data is not None:
                            specialities_data_list.append(
                                {
                                    'id': str(specialities_data['_id']),
                                    'name': specialities_data['specialityName']['en'],
                                    'image': specialities_data['image'],
                                }
                            )
            a_check_stores = db.recomdedStoresByZone.find_one({ 'storeId' : str(stores['_id'])})
            if a_check_stores is not None:
                db.recomdedStoresByZone.delete_one(
                    {'storeId': str(stores['_id'])}
                )
            db.recomdedStoresByZone.insert_one(
                {
                    'storeId': str(stores['_id']),
                    'storeName': stores['storeName']['en'],
                    'speciality': specialities_data_list,
                    'zoneId': str(zone['_id']),
                    'storeLocation': stores['location'] if 'location' in stores else {},
                    'businessLocationAddress': stores['businessLocationAddress'] if 'businessLocationAddress' in stores else {},
                    'avgRatting': stores['avgRatting'] if 'avgRatting' in stores else 0.0,
                    "logoImages": stores['logoImages']['logoImageMobile'] if 'logoImageMobile' in stores['logoImages'] else "",
                    "bannerImages": stores['bannerImages']['bannerImageMobile'] if 'bannerImageMobile' in stores['bannerImages'] else "",
                    'hygieneRating': stores['hygieneRating'] if 'hygieneRating' in stores else 0,
                    "address": stores['businessLocationAddress']['address'] if 'address' in stores['businessLocationAddress'] else "",
                    "city": stores['businessLocationAddress']['city'] if 'city' in stores['businessLocationAddress'] else "",
                    "bannerImages": stores['bannerImages']['bannerImageMobile'] if 'bannerImageMobile' in stores['bannerImages'] else "",
                }
            )
        else:
            check_stores = db.recomdedStoresByZone.find_one({ 'storeId' : str(stores['_id'])})
            if check_stores is not None:
                db.recomdedStoresByZone.delete_one(
                    {'storeId': str(stores['_id'])}
                )

    ''' newStoresByZone - add those stores which are created under 60 days'''

    current_time = datetime.now()
    sixty_days_ago = current_time - timedelta(days=60)
    sixty_days_ago_timestamp = int(sixty_days_ago.timestamp())
    query_store = [
            { "$unwind": "$statusLogs" },
            { 
                "$match": { 
                    "statusLogs.status": 1, 
                    "statusLogs.timestamp": { "$lte": sixty_days_ago_timestamp },
                    'serviceZones.zoneId': str(zone['_id'])
                } 
            }
        ]
    new_store_details = db.stores.aggregate(query_store)
    print("sixty_days_ago_timestamp",sixty_days_ago_timestamp)
    print("query_store",query_store)
    new_store_details = list(new_store_details)
    for n_stores in new_store_details:
        if str(n_stores['status']) == "1":
            n_specialities_data = []
            if 'specialities' in n_stores and len(n_stores['specialities']) > 0:
                for spe in n_stores['specialities']:
                    if spe != "":
                        specialities_data = db.specialities.find_one({'_id': ObjectId(spe)})
                        if specialities_data is not None:
                            n_specialities_data.append(
                                {
                                    'id': str(specialities_data['_id']),
                                    'name': specialities_data['specialityName']['en'],
                                    'image': specialities_data['image'],
                                }
                            )
            a_check_stores = db.newStoresByZone.find_one({ 'storeId' : str(n_stores['_id'])})
            if a_check_stores is not None:
                db.newStoresByZone.delete_one(
                    {'storeId': str(n_stores['_id'])}
                )
            db.newStoresByZone.insert_one(
                {
                    'storeId': str(n_stores['_id']),
                    'storeName': n_stores['storeName']['en'],
                    'speciality': n_specialities_data,
                    'zoneId': str(zone['_id']),
                    'storeLocation': n_stores['location'] if 'location' in n_stores else {},
                    'businessLocationAddress': n_stores['businessLocationAddress'] if 'businessLocationAddress' in n_stores else {},
                    'avgRatting': n_stores['avgRatting'] if 'avgRatting' in n_stores else 0.0,
                    "logoImages": n_stores['logoImages']['logoImageMobile'] if 'logoImageMobile' in n_stores['logoImages'] else "",
                    "bannerImages": n_stores['bannerImages']['bannerImageMobile'] if 'bannerImageMobile' in n_stores['bannerImages'] else "",
                    'hygieneRating': n_stores['hygieneRating'] if 'hygieneRating' in n_stores else 0,
                    "address": n_stores['businessLocationAddress']['address'] if 'address' in n_stores['businessLocationAddress'] else "",
                    "city": n_stores['businessLocationAddress']['city'] if 'city' in n_stores['businessLocationAddress'] else "",
                    "bannerImages": n_stores['bannerImages']['bannerImageMobile'] if 'bannerImageMobile' in n_stores['bannerImages'] else "",
                }
            )
        else:
            check_stores = db.newStoresByZone.find_one({ 'storeId' : str(n_stores['_id'])})
            if check_stores is not None:
                db.newStoresByZone.delete_one(
                    {'storeId': str(n_stores['_id'])}
                )

    ''' popularStoresByZone '''

    populer_store_details = db.stores.aggregate([
            {"$sort": {"_id":-1}},
            {"$unwind": "$serviceZones"},
        
            {
                "$lookup": {
                    "from": 'storeOrder',
                    "let": { "storeId": "$storeId" },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        { "$eq": ["$storeId", "$$storeId"] },
                                    
                                    ]
                                }
                            }
                        }
                    ],
                    "as": 'recentOrders'
                }
            },
            {
                "$addFields": {
                    "recentOrderCount": { "$size": "$recentOrders" }
                }
            },
            {
                "$match": {
                    "recentOrderCount": { "$gt": 0 }
                }
            },
            {
                "$sort": {
                    "recentOrderCount": -1
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "storeId": 1,
                    "storeName": "$storeName.en",
                    "recentOrderCount": 1,
                    "logoImage": "$logoImages",
                    "speciality": "$specialities",
                    "avgRatting": "$avgRatting",
                    "businessLocationAddress": "$businessLocationAddress"
                }
            }
        ])
    populer_store_details = list(populer_store_details)
    for p_stores in populer_store_details:
        if str(p_stores['status']) == "1":
            p_specialities_data = []
            if 'specialities' in p_stores and len(p_stores['specialities']) > 0:
                for spe in p_stores['specialities']:
                    if spe != "":
                        specialities_data = db.specialities.find_one({'_id': ObjectId(spe)})
                        if specialities_data is not None:
                            p_specialities_data.append(
                                {
                                    'id': str(specialities_data['_id']),
                                    'name': specialities_data['specialityName']['en'],
                                    'image': specialities_data['image'],
                                }
                            )
            db.popularStoresByZone.insert_one(
                {
                    'storeId': str(p_stores['_id']),
                    'storeName': p_stores['storeName']['en'],
                    'speciality': p_specialities_data,
                    'zoneId': str(zone['_id']),
                    'storeLocation': p_stores['location'] if 'location' in p_stores else {},
                     'businessLocationAddress': p_stores['businessLocationAddress'] if 'businessLocationAddress' in p_stores else {},
                    'avgRatting': p_stores['avgRatting'] if 'avgRatting' in p_stores else 0.0,
                    "logoImages": p_stores['logoImages']['logoImageMobile'] if 'logoImageMobile' in p_stores['logoImages'] else "",
                     "bannerImages": p_stores['bannerImages']['bannerImageMobile'] if 'bannerImageMobile' in n_stores['bannerImages'] else "",
                    'hygieneRating': p_stores['hygieneRating'] if 'hygieneRating' in p_stores else 0,
                    "address": p_stores['businessLocationAddress']['address'] if 'address' in p_stores['businessLocationAddress'] else "",
                    "city": p_stores['businessLocationAddress']['city'] if 'city' in p_stores['businessLocationAddress'] else "",
                    "bannerImages": p_stores['bannerImages']['bannerImageMobile'] if 'bannerImageMobile' in p_stores['bannerImages'] else "",
                }
            )
        else:
            check_stores = db.popularStoresByZone.find_one({ 'storeId' : str(p_stores['_id'])})
            if check_stores is not None:
                db.popularStoresByZone.delete_one(
                    {'storeId': str(p_stores['_id'])}
                )