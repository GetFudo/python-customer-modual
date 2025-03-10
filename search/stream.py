from dotenv import load_dotenv
import redis
import os
from pymongo import MongoClient
import threading
from bson import ObjectId
import html2text
import sys
import pandas as pd
from elasticsearch import Elasticsearch
import time

env_path = "/usr/etc/env"
# env_path = "/home/monil/projects/EcomQA/env"
load_dotenv(dotenv_path=env_path)

ELASTIC_SEARCH_URL = os.getenv("ELASTIC_SEARCH_URL")
DINE_STORE_CATEGORY_ID=os.getenv("DINE_STORE_CATEGORY_ID")
MONGO_URL = os.getenv("MONGO_URL")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
CHILD_PRODUCT_INDEX = os.getenv("CHILD_PRODUCT_INDEX")
index_products = CHILD_PRODUCT_INDEX
print(DINE_STORE_CATEGORY_ID)



es=Elasticsearch([{'host':ELASTIC_SEARCH_URL,'port':9200}])

print('for streaming mongo : ' , MONGO_URL)
client = MongoClient(MONGO_URL)
db = client[MONGO_DB_NAME]


stream_action = {
    "update":"patch",
    "insert":"patch",
    "delete":"delete"
}

stream = db.childProducts.watch([{
    '$match': {
        'operationType': { '$in': list(stream_action.keys()) }
        }
}])



for product in stream:
    operationType = product["operationType"]
    product_id = str(product["documentKey"]["_id"])
    print("food product  ", product_id)
    find_store = db.childProducts.find_one({'_id': ObjectId(product_id)}, {"storeId": 1, "storeCategoryId":1})
    if 'storeCategoryId' in find_store and  str(find_store['storeCategoryId']) == DINE_STORE_CATEGORY_ID:
        if find_store is not None and 'storeId' in find_store and find_store['storeId'] != "0":
            store_id = str(find_store['storeId'])
            try:
                print("1111111111111111111111111111111111111111")
                # start_time =time.time()
                productData = []
                recommFilter = []
                must_query = []
                must_query.append({"terms": {"status": [1, 2]}})
                must_query.append({"match": {"storeId": str(store_id)}})
                must_query.append({"match": {"units.isPrimary": True}})
                bool_query = {"must": must_query}
                query = {
                    "query": {
                        "bool": bool_query
                    },
                    "aggs": {
                        "group_by_catName": {
                            "terms": {
                                "field": "categoryList.parentCategory.categoryName.en.keyword",
                                "size": 100
                            },
                            "aggs": {
                                "top_sales_hits": {
                                    "top_hits": {
                                        "_source": {
                                            "includes": [
                                                "_id",
                                                "parentProductId",
                                                # "firstCategoryId",
                                                # "catName",
                                                # "secondCategoryId",
                                                # "subCatName",
                                                "childproductid",
                                                "storeId",
                                                # "thirdCategoryId",
                                                "detailDescription",
                                                # "subSubCatName",
                                                "images.small",
                                                # "suppliers",
                                                "containsMeat",
                                                "consumptionTime",
                                                "currencySymbol",
                                                "currency",
                                                "units.b2cPricing",
                                                "units.productTag",
                                                "units.categorySeqId",
                                                "units.productSeqId",
                                                "pName",
                                                "storeCategoryId"
                                            ]
                                        },
                                        "size": 100,
                                        # "sort": [
                                        #         {"units.productSeqId": {"order": "asc"}} 
                                        #     ]
                                        
                                    }
                                }
                            }
                        }
                    }
                }
                
                res = es.search(index=index_products, body=query)
                # print("res",res)
                for bucket in res["aggregations"]["group_by_catName"]["buckets"]:
                    product_data = []
                    categorySeqId = 0
                    for hit in bucket["top_sales_hits"]["hits"]["hits"]:
                        source = hit.get("_source", {})
                        # print("source",source)
                        if source.get("units") and source["units"]:
                            categorySeqId = source["units"][0].get("categorySeqId", -1)
                        else:
                            categorySeqId = -1
                        
                        if source.get("units") and source["units"]:
                            productSeqId = source["units"][0].get("productSeqId", -1)
                        else:
                            productSeqId = -1

                        flattened_data = {
                            # "_id": hit.get("_id"),
                            "parentProductId": source.get("parentProductId"),
                            # "firstCategoryId": source.get("firstCategoryId"),
                            # "catName": source.get("catName"),
                            # "secondCategoryId": source.get("secondCategoryId"),
                            # "subCatName": source.get("subCatName"),
                            # "childProductId": source.get("_id"),
                            "childProductId": hit.get("_id"),
                            # "storeId": source.get("storeId"),
                            # "thirdCategoryId": source.get("thirdCategoryId"),
                            "detailDescription": source.get("detailDescription", {}).get("en"),
                            # "subSubCatName": source.get("subSubCatName"),
                            "images": source["images"][0]["small"] if source.get("images") else "",
                            # "suppliers": source.get("suppliers"),
                            "containsMeat": source.get("containsMeat"),
                            "consumptionTime": source.get("consumptionTime"),
                            "currencySymbol": source.get("currencySymbol"),
                            "currency": source.get("currency"),
                            "allPrice": source["units"][0]["b2cPricing"] if source.get("units") else [],
                            "productName": source.get("pName", "").get("en"),
                            "storeCategoryId": source.get("storeCategoryId"),
                            "isFavourite":False,
                            "categorySeqId":categorySeqId,
                            "productSeqId":productSeqId,
                        }

                        # Add 'productTag' if present
                        if 'units' in source and 'productTag' in source['units']:
                            flattened_data['productTag'] = source['units']['productTag']
                        else:
                            flattened_data['productTag'] = {
                                        "isDairyFree" : False,
                                        "isPopular" : False,
                                        "isSpicy" : False,
                                        "isVegan" : False,
                                        "isVegetarian" :False,
                                    }
                        product_data.append(flattened_data)

                    productData.append({
                        "catName": bucket["key"],
                        "categorySeqId":categorySeqId,
                        "products": product_data,
                        "isSubCategories": False,
                        "subCategories": [],
                    })
                    
                    recommFilter.append({
                        "catName": bucket["key"],
                        "penCount": bucket["doc_count"],
                        "categorySeqId":categorySeqId,
                    })
                        
                total_products = res.get("hits", {}).get("total", {}).get("value", 0)
                if total_products != 0:
                    print("adddddddddddddddddddddddddddd")
                    response = {
                        "storeId": str(store_id),
                        "storeData":{},
                        "parentCategoryProduct": productData,
                        "subCategoryProduct":[],
                        "recommFilter": recommFilter,
                        "total_products": total_products,
                        "recommdedProducts":[],
                        "offerData": []
                    }
                    filter_data = {"storeId": str(store_id)}
                    update = {"$set": response}
                    db.storeBestProductsNew.update_one(filter_data, update, upsert=True)
                else:
                    db.storeBestFailed.insert_one({"s_id": store_id,"reason":str(res)})
                    traceback.print_exc()
            except Exception as e:
                print("error in store best products ")
                db.storeBestFailed.insert_one({"s_id": store_id,"reason":str(e)})
                traceback.print_exc()
                

# for product in stream:
#     operationType = product["operationType"]
#     product_id = str(product["documentKey"]["_id"])
#     print("food product  ", product_id)
#     find_store = db.childProducts.find_one({'_id': ObjectId(product_id)}, {"storeId": 1, "storeCategoryId":1})
#     if 'storeCategoryId' in find_store and  str(find_store['storeCategoryId']) == DINE_STORE_CATEGORY_ID:
#         print('start data query')
#         recommanded_details = []
#         language  = "en"
#         final_data = {}
#         favourite_data = []
#         recomended_data = []
#         category_details = []
#         parent_category_details= []
#         count = 2
#         sub_category_details = []
#         user_id = ""
#         offer_json = []
#         print('start store data fatching')
#         if find_store is not None and 'storeId' in find_store and find_store['storeId'] != "0":
#             product_store_id = str(find_store['storeId'])
#             try:
#                 ''' add data in stores '''
#                 consumption_lists = {
#                     'breakfast': [],
#                     'brunch': [],
#                     'lunch': [],
#                     "latenightdinner": [],
#                     "tea": [],
#                     "dinner": []

#                 }
#                 final_data['storeId'] = str(find_store['storeId'])
#                 store_data = db.stores.find_one({'_id': ObjectId(str(find_store['storeId']))},
#                                                 {
#                                     "averageCostForMealForTwo": 1,
#                                     "favouriteUsers": 1,
#                                     "safetyStandards": 1,
#                                     "businessLocationAddress": 1,
#                                     "specialities": 1,
#                                     "storeName": 1,
#                                     "storeFrontTypeId": 1,
#                                     "uniqStoreId": 1,
#                                     "storeIsOpen": 1,
#                                     "nextCloseTime": 1,
#                                     "nextOpenTime": 1,
#                                     "serviceZones": 1,
#                                     "logoImages": 1,
#                                     "bannerImages": 1,
#                                     "listingImage": 1,
#                                     "cityId": 1,
#                                     "minimumOrder": 1,
#                                     "citiesOfOperation": 1,
#                                     "freeDeliveryAbove": 1,
#                                     "safetyStandardsSortDiscription": 1,
#                                     "safetyStandardsDynamicContent": 1,
#                                     "averageDeliveryTimeInMins": 1,
#                                     "supportedOrderTypes": 1,
#                                     "driverTypeId": 1,
#                                     "driverType": 1,
#                                     "storeType": 1,
#                                     "currencySymbol": 1,
#                                     "currencyCode": 1,
#                                     "tableReservations": 1,
#                                     "priceForBookingTable":1,
                                    
#                                 })
                
#                 offer_details = db.offers.find(
#                                 {
#                                     "storeId": {"$in": [str(store_data['_id'])]}, "status": 1
#                                 },
#                                 {
#                                     "termscond": 1,
#                                     "name": 1,
#                                     "discountValue": 1,
#                                     "offerType": 1,
#                                     "webimages": 1,
#                                     "images": 1
#                                 }
#                             )
                
#                 cusine_name = ""
#                 for offer in offer_details:
#                     terms_condition = html2text.html2text(offer["termscond"])
#                     offer_json.append(
#                         {
#                             "offerName": offer["name"][language]
#                             if language in offer["name"]
#                             else offer["name"]["en"],
#                             "termscond": terms_condition,
#                             "offerId": str(offer["_id"]),
#                             "discountValue": int(offer["discountValue"]),
#                             "offerType": offer["offerType"],
#                             "webimages": offer["webimages"],
#                             "images": offer["images"],
#                         }
#                     )
#                 if len(offer_json) > 0:
#                     best_offer_store = max(offer_json, key=lambda x: x["discountValue"])
#                     if best_offer_store["offerType"] == 0:
#                         percentage_text = (
#                                 str(best_offer_store["discountValue"]) + "%" + " " + "off"
#                         )
#                     else:
#                         percentage_text = "₹" + str(best_offer_store["discountValue"]) + " off"
#                     offer_name = best_offer_store["offerName"]
#                 else:
#                     best_offer_store = {}
#                     offer_name = ""
#                     percentage_text = ""

#                 if store_data != None:
#                     if "averageCostForMealForTwo" in store_data:
#                         cost_for_two = store_data["averageCostForMealForTwo"]
#                     else:
#                         cost_for_two = 0

#                     if "favouriteUsers" in store_data:
#                         if user_id in store_data["favouriteUsers"]:
#                             favourite_store = True
#                         else:
#                             favourite_store = False
#                     else:
#                         favourite_store = False
#                     if "safetyStandards" in store_data:
#                         if int(store_data["safetyStandards"]) == 0:
#                             safety_standard = False
#                             safety_standards_sort_discription = ""
#                             safety_standards_dynamic_content = ""
#                         else:
#                             safety_standard = True
#                             safety_standards_sort_discription = store_data[
#                                 "safetyStandardsSortDiscription"
#                             ] if "safetyStandardsSortDiscription" in store_data else ""
#                             safety_standards_dynamic_content = store_data[
#                                 "safetyStandardsDynamicContent"
#                             ] if "safetyStandardsDynamicContent" in store_data else ""
#                     else:
#                         safety_standard = False
#                         safety_standards_sort_discription = ""
#                         safety_standards_dynamic_content = ""
#                     store_lat = (
#                         float(store_data["businessLocationAddress"]["lat"])
#                         if "lat" in store_data["businessLocationAddress"]
#                         else 0
#                     )
#                     store_long = (
#                         float(store_data["businessLocationAddress"]["long"])
#                         if "long" in store_data["businessLocationAddress"]
#                         else 0
#                     )
#                     address = (
#                         store_data["businessLocationAddress"]["address"]
#                         if "address" in store_data["businessLocationAddress"]
#                         else ""
#                     )
#                     locality = (
#                         store_data["businessLocationAddress"]["locality"]
#                         if "locality" in store_data["businessLocationAddress"]
#                         else ""
#                     )
#                     post_code = (
#                         store_data["businessLocationAddress"]["postCode"]
#                         if "postCode" in store_data["businessLocationAddress"]
#                         else ""
#                     )
#                     state = (
#                         store_data["businessLocationAddress"]["state"]
#                         if "state" in store_data["businessLocationAddress"]
#                         else ""
#                     )
#                     country = (
#                         store_data["businessLocationAddress"]["country"]
#                         if "country" in store_data["businessLocationAddress"]
#                         else ""
#                     )
#                     addressArea = (
#                         store_data["businessLocationAddress"]["addressArea"]
#                         if "addressArea" in store_data["businessLocationAddress"]
#                         else ""
#                     )
#                     city = (
#                         store_data["businessLocationAddress"]["city"]
#                         if "city" in store_data["businessLocationAddress"]
#                         else ""
#                     )
#                     # ===================================for the cusines=============================================
#                     if "specialities" in store_data:
#                         if len(store_data["specialities"]):
#                             for spec in store_data["specialities"]:
#                                 spec_data = db.specialities.find_one(
#                                     {"_id": ObjectId(spec)}, {"specialityName": 1, "image": 1}
#                                 )
#                                 if spec_data != None:
#                                     if cusine_name == "":
#                                         cusine_name = (
#                                             spec_data["specialityName"][language]
#                                             if language in spec_data["specialityName"]
#                                             else spec_data["specialityName"]["en"]
#                                         )
#                                     else:
#                                         cusine_name = (
#                                             cusine_name
#                                             + ", "
#                                             + spec_data["specialityName"][language]
#                                             if language in spec_data["specialityName"]
#                                             else spec_data["specialityName"]["en"]
#                                         )
#                                 else:
#                                     pass
#                         else:
#                             pass
#                     else:
#                         pass

#                     store_name = (
#                         store_data["storeName"][language]
#                         if language in store_data["storeName"]
#                         else store_data["storeName"]["en"]
#                     )

#                     # ================================for more stores==================================
#                     store_id = str(store_data["_id"])
#                     # =====================================about store tags=================================
#                     if "storeIsOpen" in store_data:
#                         store_is_open = store_data["storeIsOpen"]
#                     else:
#                         store_is_open = False

#                     if "nextCloseTime" in store_data:
#                         next_close_time = store_data["nextCloseTime"]
#                     else:
#                         next_close_time = ""

#                     if "nextOpenTime" in store_data:
#                         next_open_time = store_data["nextOpenTime"]
#                     else:
#                         next_open_time = ""

#                     try:
#                         if "timeZoneWorkingHour" in store_data:
#                             timeZoneWorkingHour = store_data['timeZoneWorkingHour']
#                         else:
#                             timeZoneWorkingHour = ""
#                     except:
#                         timeZoneWorkingHour = ""


#                     avg_rating_value = 0
#                     seller_rating = db.sellerReviewRatings.aggregate(
#                         [
#                             {
#                                 "$match": {
#                                     "sellerId": str(store_data["_id"]),
#                                     "status": 1,
#                                     "rating": {"$ne": 0},
#                                 }
#                             },
#                             {"$group": {"_id": "$sellerId", "avgRating": {"$avg": "$rating"}}},
#                         ]
#                     )
#                     for avg_rating in seller_rating:
#                         avg_rating_value = avg_rating["avgRating"]
#                     is_temp_close = False
#                     final_data['storeData'] = {
#                         "storeId": store_id,
#                         "lat": store_lat,
#                         "long": store_long,
#                         "isFavourite": favourite_store,
#                         "offer": best_offer_store,
#                         "address": address,
#                         "locality": locality,
#                         "addressArea": addressArea,
#                         "isTempClose": is_temp_close,
#                         "offerName": offer_name,
#                         "safetyStandards": safety_standard,
#                         "safetyStandardsSortDiscription": safety_standards_sort_discription,
#                         "safetyStandardsDynamicContent": safety_standards_dynamic_content,
#                         "cuisines": cusine_name,
#                         "averageCostForMealForTwo": cost_for_two,
#                         "logoImages": store_data["logoImages"],
#                         "bannerImages": store_data["bannerImages"],
#                         "listingImage": store_data["listingImage"]
#                         if "listingImage" in store_data
#                         else {},
#                         "priceForBookingTable": store_data['priceForBookingTable'] if 'priceForBookingTable'in store_data else 0,
#                         "tableReservations": store_data['tableReservations'] if 'tableReservations'in store_data else False,
#                         "cityId": store_data["cityId"],
#                         "minimumOrder": store_data["minimumOrder"],
#                         "citiesOfOperation": store_data["citiesOfOperation"],
#                         "freeDeliveryAbove": store_data["freeDeliveryAbove"]
#                         if "freeDeliveryAbove" in store_data
#                         else 0,
#                         "averageDeliveryTime": str(
#                             store_data["averageDeliveryTimeInMins"]
#                         )
#                                                 + " "
#                                                 + "Mins"
#                         if "averageDeliveryTimeInMins" in store_data
#                         else "",
#                         "nextCloseTime": next_close_time,
#                         "nextOpenTime": next_open_time,
#                         "supportedOrderTypes": store_data["supportedOrderTypes"]
#                         if "supportedOrderTypes" in store_data
#                         else 3,
#                         "avgRating": round(avg_rating_value, 2),
#                         "storeIsOpen": store_is_open,
#                         "storeTag": "",
#                         "driverTypeId": store_data["driverTypeId"]
#                         if "driverTypeId" in store_data
#                         else 0,
#                         "driverType": store_data["driverType"]
#                         if "driverType" in store_data
#                         else "",
#                         "storeType": store_data["storeType"]
#                         if "storeType" in store_data
#                         else "Food",
#                         "postCode": post_code,
#                         "percentageText": percentage_text,
#                         "state": state,
#                         "moreSellerCount": 0,
#                         "country": country,
#                         "city": city,
#                         "distanceKm": 0,
#                         "distanceMiles": 0,
#                         "currencySymbol": store_data["currencySymbol"]
#                         if "currencySymbol" in store_data
#                         else "₹",
#                         "currency": store_data["currencyCode"]
#                         if "currencyCode" in store_data
#                         else "INR",
#                         "distanceMiles": 0,
#                         "storeName": store_name,
                    
#                     }
                    
#                 ''' product find query '''
#                 store_id = str(find_store['storeId'])
#                 must_query = []
#                 must_query.append({"terms": {"status": [1, 2]}})
#                 must_query.append({"match": {"storeId": str(store_id)}})
#                 must_query.append({"match": {"units.isPrimary": True}})
#                 bool_query = {"must": must_query}
#                 query = {
#                 "query": {"bool": bool_query},
#                 "aggs": {
#                     "group_by_catName": {
#                         "terms": {
#                             "field": "categoryList.parentCategory.categoryName.en.keyword",
#                             "size": 100,
#                         },
#                         "aggs": {
#                             "top_sub_cat_name": {
#                                 "terms": {
#                                     # "field": "categoryList.parentCategory.childCategory.categoryName.en.keyword",
#                                     "field": "categoryList.parentCategory.childCategory.categoryId.keyword",
#                                     "size": 100,
#                                 },
#                                 "aggs": {
#                                     "top_sales_hits": {
#                                         "top_hits": {
#                                             "_source": {
#                                                 "includes": [
#                                                     "_id",
#                                                     "parentProductId",
#                                                     "firstCategoryId",
#                                                     "catName",
#                                                     "secondCategoryId",
#                                                     "subCatName",
#                                                     "childproductid",
#                                                     "storeId",
#                                                     "thirdCategoryId",
#                                                     "detailDescription",
#                                                     "subSubCatName",
#                                                     "offer",
#                                                     "images",
#                                                     "suppliers",
#                                                     "containsMeat",
#                                                     "consumptionTime",
#                                                     "currencySymbol",
#                                                     "currency",
#                                                     "tax",
#                                                     "units",
#                                                     "pName",
#                                                     "storeCategoryId",
#                                                 ]
#                                             },
#                                             "size": 100,
#                                         }
#                                     }
#                                 },
#                             }
#                         },
#                     }
#                 },
#                 }
#                 print(query)
#                 res = es.search(index=index_products, body=query)

#                 for bucket in res["aggregations"]["group_by_catName"]["buckets"]:
#                     category_name = bucket["key"]
#                     main_category_details_data = db.category.find_one(
#                         {
#                             "categoryName.en": category_name,
#                             "status": 1,
#                             "storeid": {"$in": [store_id]},
#                         }
#                     )
#                     if main_category_details_data is None:
#                         main_category_details_data = db.category.find_one(
#                             {
#                                 "categoryName.en": category_name,
#                                 "status": 1,
#                                 "storeId": store_id,
#                             }
#                         )
#                     if main_category_details_data is None:
#                         main_category_details_data = db.category.find_one(
#                             {
#                                 "categoryName.en": category_name
#                             }
#                         )
#                     category_data = []
#                     pen_count_product = 0
#                     if main_category_details_data is not None:
#                         count = count + 1
#                         for hits_bucket in bucket["top_sub_cat_name"]["buckets"]:
#                             sub_cat_id = hits_bucket["key"]
#                             category_details_data = db.category.find_one(
#                                 {
#                                     "_id": ObjectId(sub_cat_id),
#                                     "status": 1,
#                                     "storeid": {"$in": [store_id]},
#                                 }
#                             )
#                             if category_details_data is None:
#                                 category_details_data = db.category.find_one(
#                                     {"_id": ObjectId(sub_cat_id), "status": 1}
#                                 )
#                             if category_details_data is not None:
#                                 # sub_cat_name = hits_bucket['key']
#                                 sub_cat_name = category_details_data["categoryName"]["en"]
#                                 doc_count = hits_bucket["doc_count"]
#                                 product_data = []
#                                 for hits in hits_bucket["top_sales_hits"]["hits"]["hits"]:
#                                     try:
#                                         tax_value = []
#                                         # =========================for addons=======================================
#                                         try:
#                                             if "addOns" in hits["_source"]["units"][0]:
#                                                 if (
#                                                         hits["_source"]["units"][0]["addOns"]
#                                                         != None
#                                                 ):
#                                                     if (
#                                                             len(
#                                                                 hits["_source"]["units"][0][
#                                                                     "addOns"
#                                                                 ]
#                                                             )
#                                                             > 0
#                                                     ):
#                                                         addons_count = True
#                                                     else:
#                                                         addons_count = False
#                                                 else:
#                                                     addons_count = False
#                                             else:
#                                                 addons_count = False
#                                         except:
#                                             addons_count = False

#                                         if addons_count == False:
#                                             product_count = db.childProducts.find(
#                                                 {
#                                                     "storeId": ObjectId(
#                                                         hits["_source"]["storeId"]
#                                                     ),
#                                                     "parentProductId": hits["_source"][
#                                                         "parentProductId"
#                                                     ],
#                                                     "status": 1,
#                                                 }
#                                             ).count()
#                                             if product_count > 1:
#                                                 addons_count = True

#                                         best_supplier = {}
#                                         best_supplier["id"] = hits["_source"]["storeId"]
#                                         best_supplier["productId"] = hits["_id"]
#                                         try:
#                                             best_supplier["retailerQty"] = (
#                                                 hits["_source"]["units"][0][
#                                                     "availableQuantity"
#                                                 ]
#                                                 if hits["_source"]["units"][0][
#                                                         "availableQuantity"
#                                                     ]
#                                                     != ""
#                                                 else 0
#                                             )
#                                         except:
#                                             best_supplier["retailerQty"] = 0

#                                         try:
#                                             best_supplier["distributorQty"] = (
#                                                 hits["_source"]["units"][0]["distributor"][
#                                                     "availableQuantity"
#                                                 ]
#                                                 if hits["_source"]["units"][0][
#                                                         "distributor"
#                                                     ]["availableQuantity"]
#                                                     != ""
#                                                 else 0
#                                             )
#                                         except:
#                                             best_supplier["distributorQty"] = 0

#                                         try:
#                                             best_supplier["retailerPrice"] = (
#                                                 hits["_source"]["units"][0]["b2cPricing"][
#                                                     0
#                                                 ]["b2cproductSellingPrice"]
#                                                 if "b2cproductSellingPrice"
#                                                     in hits["_source"]["units"][0][
#                                                         "b2cPricing"
#                                                     ][0]
#                                                 else hits["_source"]["units"][0][
#                                                     "floatValue"
#                                                 ]
#                                             )
#                                         except:
#                                             best_supplier["retailerPrice"] = hits[
#                                                 "_source"
#                                             ]["units"][0]["floatValue"]

#                                         try:
#                                             best_supplier["distributorPrice"] = (
#                                                 hits["_source"]["units"][0]["b2bPricing"][
#                                                     0
#                                                 ]["b2bproductSellingPrice"]
#                                                 if "b2bproductSellingPrice"
#                                                     in hits["_source"]["units"][0][
#                                                         "b2bPricing"
#                                                     ][0]
#                                                 else hits["_source"]["units"][0][
#                                                     "floatValue"
#                                                 ]
#                                             )
#                                         except:
#                                             best_supplier["distributorPrice"] = hits[
#                                                 "_source"
#                                             ]["units"][0]["floatValue"]

#                                         if len(best_supplier) > 0:
#                                             # ===========================tax for the product=========================================================
#                                             tax_details = db.childProducts.find_one(
#                                                 {
#                                                     "_id": ObjectId(
#                                                         best_supplier["productId"]
#                                                     )
#                                                 }
#                                             )
#                                             detail_description = (
#                                                 tax_details["detailDescription"][language]
#                                                 if language
#                                                     in tax_details["detailDescription"]
#                                                 else tax_details["detailDescription"]["en"]
#                                             )
#                                             if detail_description == None:
#                                                 detail_description = ""
#                                             # =================================price calculation===================================================================
#                                             price = best_supplier["retailerPrice"]
#                                             if type(price) == str:
#                                                 price = float(price)

#                                             tax_price = 0
#                                             offer_data = []
#                                             if tax_details != None:
#                                                 if "offer" in tax_details:
#                                                     for offer in tax_details["offer"]:
#                                                         if offer["status"] == 1:
#                                                             offer_terms = (
#                                                                 db.offers.find_one(
#                                                                     {
#                                                                         "_id": ObjectId(
#                                                                             offer["offerId"]
#                                                                         )
#                                                                     }
#                                                                 )
#                                                             )
#                                                             if offer_terms != None:
#                                                                 terms_condition = (
#                                                                     html2text.html2text(
#                                                                         offer_terms[
#                                                                             "termscond"
#                                                                         ]
#                                                                     )
#                                                                 )
#                                                                 offer[
#                                                                     "termscond"
#                                                                 ] = terms_condition
#                                                                 if offer_terms[
#                                                                     "startDateTime"
#                                                                 ] <= int(time.time()):
#                                                                     offer_data.append(offer)
#                                                             else:
#                                                                 pass
#                                                         else:
#                                                             pass
#                                             else:
#                                                 pass

#                                             if len(offer_data) > 0:
#                                                 best_offer = max(
#                                                     offer_data,
#                                                     key=lambda x: x["discountValue"],
#                                                 )
#                                                 best_offer = {
#                                                 'offerId': best_offer['offerId'],
#                                                 'offerName': best_offer['offerName']['en'],
#                                                 'images': best_offer['images']['mobile'],
#                                                 'discountType': best_offer['discountType'],
#                                                 'discountValue':  best_offer['discountValue']
#                                                 }
#                                             else:
#                                                 best_offer = {}

#                                             if len(best_offer) == 0:
#                                                 percentage = 0
#                                                 discount_type = 0
#                                             else:
#                                                 if "discountType" in best_offer:
#                                                     percentage = int(
#                                                         best_offer["discountValue"]
#                                                     )
#                                                     discount_type = best_offer[
#                                                         "discountType"
#                                                     ]
#                                                 else:
#                                                     percentage = 0
#                                                     discount_type = 0

#                                             if tax_details != None:
#                                                 if type(tax_details["tax"]) == list:
#                                                     for tax in tax_details["tax"]:
#                                                         tax_value.append(
#                                                             {"value": tax["taxValue"]}
#                                                         )
#                                                 else:
#                                                     if tax_details["tax"] != None:
#                                                         if "taxValue" in tax_details["tax"]:
#                                                             tax_value.append(
#                                                                 {
#                                                                     "value": tax_details[
#                                                                         "tax"
#                                                                     ]["taxValue"]
#                                                                 }
#                                                             )
#                                                         else:
#                                                             tax_value.append(
#                                                                 {
#                                                                     "value": tax_details[
#                                                                         "tax"
#                                                                     ]
#                                                                 }
#                                                             )
#                                                     else:
#                                                         pass
#                                             else:
#                                                 pass
#                                             tax_price = 0

#                                             # ==================================get currecny rate============================
#                                             # try:
#                                             #     currency_rate = currency_exchange_rate[
#                                             #         str(tax_details["currency"])
#                                             #         + "_"
#                                             #         + str(currency_code)
#                                             #         ]
#                                             # except:
#                                             #     currency_rate = 0
#                                             # currency_details = db.currencies.find_one(
#                                             #     {"currencyCode": currency_code}
#                                             # )
#                                             # if currency_details is not None:
#                                             #     currency_symbol = currency_details[
#                                             #         "currencySymbol"
#                                             #     ]
#                                             #     currency = currency_details["currencyCode"]
#                                             # else:
#                                             #     currency_symbol = tax_details[
#                                             #         "currencySymbol"
#                                             #     ]
#                                             #     currency = tax_details["currency"]

#                                             # if float(currency_rate) > 0:
#                                             #     price = price * float(currency_rate)

#                                             tax_price_data = price + (
#                                                     (price * tax_price) / 100
#                                             )
#                                             if discount_type == 0:
#                                                 discount_price = float(percentage)
#                                             elif discount_type == 1:
#                                                 discount_price = (
#                                                                             float(tax_price_data)
#                                                                             * float(percentage)
#                                                                     ) / 100
#                                             else:
#                                                 discount_price = 0
#                                             base_price = tax_price_data - discount_price

#                                             try:
#                                                 ch_pro = db.childProducts.find_one(
#                                                     {"_id": ObjectId(str(best_supplier["productId"]))},
#                                                     {"isMembersOnly": 1, "units": 1})
#                                                 isMembersOnly = ch_pro.get("isMembersOnly", False)
#                                                 nonMemberPrice = ch_pro["units"][0][
#                                                     "discountPriceForNonMembers"] if "discountPriceForNonMembers" in \
#                                                                                         ch_pro["units"][
#                                                                                             0] else round(
#                                                     discount_price, 2)
#                                                 memberPrice = ch_pro["units"][0][
#                                                     "memberPrice"] if "memberPrice" in ch_pro["units"][
#                                                     0] else round(discount_price, 2)
#                                             except:
#                                                 isMembersOnly = False
#                                                 nonMemberPrice = round(discount_price, 2)
#                                                 memberPrice = round(discount_price, 2)

#                                             final_price_list = {
#                                                 "basePrice": round(tax_price_data, 2),
#                                                 "finalPrice": round(base_price, 2),
#                                                 "discountType": discount_type,
#                                                 "discountPrice": discount_price,
#                                                 # "discountPriceForNonMembers": nonMemberPrice,
#                                                 # "memberPrice": memberPrice
#                                             }
#                                             # try:
#                                             #     response_casandra = session.execute(
#                                             #         """SELECT * FROM favouriteproductsuserwise where userid=%(userid)s AND childproductid=%(productid)s ALLOW FILTERING""",
#                                             #         {
#                                             #             "userid": user_id,
#                                             #             "productid": str(
#                                             #                 best_supplier["productId"]
#                                             #             ),
#                                             #         },
#                                             #     )

#                                             #     if not response_casandra:
#                                             #         response_casandra = session.execute(
#                                             #             """SELECT * FROM favouriteproductsuserwise where userid=%(userid)s AND childproductid=%(productid)s ALLOW FILTERING""",
#                                             #             {
#                                             #                 "userid": user_id,
#                                             #                 "productid": str(
#                                             #                     hits["_source"][
#                                             #                         "childproductid"
#                                             #                     ]
#                                             #                 ),
#                                             #             },
#                                             #         )
#                                             #         if not response_casandra:
#                                             #             isFavourite = False
#                                             #         else:
#                                             #             isFavourite = True
#                                             #     else:
#                                             #         for fav in response_casandra:
#                                             #             isFavourite = True
#                                             # except Exception as e:
#                                             #     print(e)
#                                             isFavourite = False
#                                             response_casandra = None
#                                         # =================================recomanded products===========================

#                                             try:
#                                                 product_name = (
#                                                     tax_details["pName"][language]
#                                                     if language in tax_details["pName"]
#                                                     else tax_details["pName"]["en"]
#                                                 )
#                                             except:
#                                                 product_name = (
#                                                     tax_details["units"][0]["unitName"][
#                                                         language
#                                                     ]
#                                                     if language
#                                                         in tax_details["units"][0]["unitName"]
#                                                     else tax_details["units"][0][
#                                                         "unitName"
#                                                     ]["en"]
#                                                 )
#                                             variant_count = db.childProducts.find({'units.isPrimary': False,'parentProductId': tax_details["parentProductId"], 'status':1, 'storeId': ObjectId(str(product_store_id))}).count()
#                                             try:
#                                                 addons_product_count = db.childProducts.find_one({'_id': best_supplier[
#                                                         "productId"
#                                                     ]}, {'units':1})
#                                                 addons_count_data = len(addons_product_count['units'][0]['addOns'])
#                                             except:
#                                                 addons_count_data = 0
#                                             json_data = {
#                                                 "parentProductId": str(
#                                                     tax_details["parentProductId"]
#                                                 ),
#                                                 "childProductId": best_supplier[
#                                                     "productId"
#                                                 ],
#                                                 # "unitId": best_supplier["productId"],
#                                                 # "catName": category_name,
#                                                 "offers": best_offer,
#                                                 "availableForSale": tax_details['status'],
#                                                 "productAvailabilityMessage": "",
#                                                 "finalPriceList": final_price_list,
#                                                 "addOnsCount": addons_count,
#                                                 "detailDescription": detail_description,
#                                                 "allPrice" :tax_details["units"][0]["b2cPricing"],
#                                                 # "isFavourite": isFavourite,
#                                                 "productName": product_name,
#                                                 "images": tax_details["images"][0]['small'] if len(tax_details["images"]) > 0 else [],
#                                                 "containsMeat": tax_details["containsMeat"]
#                                                 if "containsMeat" in tax_details
#                                                 else False,
#                                                 "createdTime" :tax_details["createdTimestamp"] if "createdTimestamp" in tax_details else 0,
#                                                 # "currencySymbol": tax_details['currencySymbol'],
#                                                 # "currency": tax_details['currency'],
#                                                 'Customizable': True if variant_count > 0 or addons_count_data > 0 else False,
#                                                 # "availableQuantity": best_supplier[
#                                                 #     "retailerQty"
#                                                 # ],
#                                                 # "isMembersOnly": isMembersOnly,
#                                                 "nextAvailableTime": tax_details['nextAvailableTime'] if 'nextAvailableTime' in tax_details else 0,
#                                                 # "mealConsumptionTime": tax_details['mealConsumptionTime'] if 'mealConsumptionTime' in tax_details else {},
#                                                 # "consumptionTime": tax_details['consumptionTime'],
#                                                 # "status": tax_details['status']
#                                             }
#                                             if isFavourite == True:
#                                                 favourite_data.append(json_data)
#                                             else:
#                                                 pass
#                                             if "consumptionTime" in tax_details and len(tax_details['consumptionTime']) > 0:
#                                                 for category in tax_details['consumptionTime']:
#                                                     if category in consumption_lists:
#                                                         consumption_lists[category].append(json_data)
#                                             product_data.append(json_data)
#                                     except Exception as ex:
#                                         template = "An exception of type {0} occurred. Arguments:\n{1!r}"
#                                         message = template.format(
#                                             type(ex).__name__, ex.args
#                                         )
#                                         print(
#                                             "Error on line {}".format(
#                                                 sys.exc_info()[-1].tb_lineno
#                                             ),
#                                             type(ex).__name__,
#                                             ex,
#                                         )

#                                 if len(product_data) > 0:
#                                     dataframe_fav = pd.DataFrame(product_data)
#                                     dataframe_fav = dataframe_fav.drop_duplicates(
#                                         subset="childProductId", keep="last"
#                                     )
#                                     new_fav_list = dataframe_fav.to_dict(orient="records")
#                                     pen_count_product = pen_count_product + len(
#                                         new_fav_list
#                                     )
#                                     category_data.append(
#                                         {
#                                             "name": sub_cat_name,
#                                             "penCount": len(new_fav_list),
#                                             "products": new_fav_list,
#                                         }
#                                     )
#                                     recommanded_details.append(
#                                     {
#                                         "catName": category_name,
#                                         "penCount": pen_count_product,
#                                         "seqId": count,
#                                     }
#                                 )
#                                 else:
#                                     pass


#                     if len(category_data) > 0:
#                         sub_category_details.append(
#                             {
#                                 "catName": category_name,
#                                 "isSubCategories": True,
#                                 "products": [],
#                                 "subCategories": category_data,
#                                 "seqId": count,
#                             }
#                         )

#                 final_data['subCategoryProduct'] = sub_category_details
#                 ''' parent Category Product '''
#                 query = {
#                     "query": {"bool": bool_query},
#                     "aggs": {
#                         "group_by_catName": {
#                             "terms": {
#                                 "field": "categoryList.parentCategory.categoryId.keyword",
#                                 "size": 100,
#                             },
#                             "aggs": {
#                                 "top_sales_hits": {
#                                     "top_hits": {
#                                         "_source": {
#                                             "includes": [
#                                                 "_id",
#                                                 "parentProductId",
#                                                 "firstCategoryId",
#                                                 "catName",
#                                                 "secondCategoryId",
#                                                 "subCatName",
#                                                 "childproductid",
#                                                 "storeId",
#                                                 "thirdCategoryId",
#                                                 "detailDescription",
#                                                 "subSubCatName",
#                                                 "offer",
#                                                 "images",
#                                                 "suppliers",
#                                                 "containsMeat",
#                                                 "consumptionTime",
#                                                 "currencySymbol",
#                                                 "currency",
#                                                 "tax",
#                                                 "units",
#                                                 "pName",
#                                                 "storeCategoryId",
#                                             ]
#                                         },
#                                         "size": 100,
#                                     }
#                                 }
#                             },
#                         }
#                     },
#                 }
#                 res = es.search(index=index_products, body=query)
#                 for bucket in res["aggregations"]["group_by_catName"]["buckets"]:
#                     count = count + 1
#                     product_data = []
#                     # category_name = bucket['key']
#                     category_id = bucket["key"]
#                     parent_cat_details = db.category.find_one(
#                         {
#                             "_id": ObjectId(category_id),
#                             "status": 1
#                         }
#                     )
#                     if parent_cat_details is None:
#                         parent_cat_details = db.category.find_one(
#                             {
#                                 "_id": ObjectId(category_id)
#                             }
#                         )
#                     if parent_cat_details is not None:
#                         category_name = parent_cat_details["categoryName"]["en"]
#                         if any(category_name in d["catName"] for d in category_details):
#                             pass
#                         else:
#                             for hits in bucket["top_sales_hits"]["hits"]["hits"]:
#                                 tax_value = []
#                                 # =========================for addons=======================================
#                                 if "addOns" in hits["_source"]["units"][0]:
#                                     if hits["_source"]["units"][0]["addOns"] != None:
#                                         if len(hits["_source"]["units"][0]["addOns"]) > 0:
#                                             addons_count = True
#                                         else:
#                                             addons_count = False
#                                     else:
#                                         addons_count = False
#                                 else:
#                                     addons_count = False

#                                 if addons_count == False:
#                                     product_count = db.childProducts.find(
#                                         {
#                                             "storeId": ObjectId(hits["_source"]["storeId"]),
#                                             "parentProductId": hits["_source"][
#                                                 "parentProductId"
#                                             ],
#                                             "status": 1,
#                                         }
#                                     ).count()
#                                     if product_count > 1:
#                                         addons_count = True
#                                 best_supplier = {}
#                                 best_supplier["id"] = hits["_source"]["storeId"]
#                                 best_supplier["productId"] = hits["_id"]
#                                 try:
#                                     best_supplier["retailerQty"] = (
#                                         hits["_source"]["units"][0]["availableQuantity"]
#                                         if hits["_source"]["units"][0]["availableQuantity"]
#                                             != ""
#                                         else 0
#                                     )
#                                 except:
#                                     best_supplier["retailerQty"] = 0

#                                 try:
#                                     best_supplier["distributorQty"] = (
#                                         hits["_source"]["units"][0]["distributor"][
#                                             "availableQuantity"
#                                         ]
#                                         if hits["_source"]["units"][0]["distributor"][
#                                                 "availableQuantity"
#                                             ]
#                                             != ""
#                                         else 0
#                                     )
#                                 except:
#                                     best_supplier["distributorQty"] = 0

#                                 try:
#                                     best_supplier["retailerPrice"] = (
#                                         hits["_source"]["units"][0]["b2cPricing"][0][
#                                             "b2cproductSellingPrice"
#                                         ]
#                                         if "b2cproductSellingPrice"
#                                             in hits["_source"]["units"][0]["b2cPricing"][0]
#                                         else hits["_source"]["units"][0]["floatValue"]
#                                     )
#                                 except:
#                                     best_supplier["retailerPrice"] = hits["_source"][
#                                         "units"
#                                     ][0]["floatValue"]

#                                 try:
#                                     best_supplier["distributorPrice"] = (
#                                         hits["_source"]["units"][0]["b2bPricing"][0][
#                                             "b2bproductSellingPrice"
#                                         ]
#                                         if "b2bproductSellingPrice"
#                                             in hits["_source"]["units"][0]["b2bPricing"][0]
#                                         else hits["_source"]["units"][0]["floatValue"]
#                                     )
#                                 except:
#                                     best_supplier["distributorPrice"] = hits["_source"][
#                                         "units"
#                                     ][0]["floatValue"]

#                                 if len(best_supplier) > 0:
#                                     detail_description = (
#                                         hits["_source"]["detailDescription"][language]
#                                         if language in hits["_source"]["detailDescription"]
#                                         else hits["_source"]["detailDescription"]["en"]
#                                     )
#                                     if detail_description == None:
#                                         detail_description = ""
#                                     # ===========================tax for the product=========================================================
#                                     tax_details = db.childProducts.find_one(
#                                         {"_id": ObjectId(best_supplier["productId"])}
#                                     )
#                                     if tax_details != None:
#                                         if type(tax_details["tax"]) == list:
#                                             for tax in tax_details["tax"]:
#                                                 tax_value.append({"value": tax["taxValue"]})
#                                         else:
#                                             if tax_details["tax"] != None:
#                                                 if "taxValue" in tax_details["tax"]:
#                                                     tax_value.append(
#                                                         {
#                                                             "value": tax_details["tax"][
#                                                                 "taxValue"
#                                                             ]
#                                                         }
#                                                     )
#                                                 else:
#                                                     tax_value.append(
#                                                         {"value": tax_details["tax"]}
#                                                     )
#                                             else:
#                                                 pass
#                                         # =================================price calculation===================================================================
#                                         price = best_supplier["retailerPrice"]
#                                         if type(price) == str:
#                                             price = float(price)

#                                         tax_price = 0
#                                         offer_data = []
#                                         if "offer" in tax_details:
#                                             for offer in tax_details["offer"]:
#                                                 if offer["status"] == 1:
#                                                     offer_terms = db.offers.find_one(
#                                                         {
#                                                             "_id": ObjectId(
#                                                                 offer["offerId"]
#                                                             ),
#                                                             "storeId": store_id,
#                                                             "status": 1,
#                                                         }
#                                                     )
#                                                     if offer_terms != None:
#                                                         terms_condition = (
#                                                             html2text.html2text(
#                                                                 offer_terms["termscond"]
#                                                             )
#                                                         )
#                                                         if offer_terms[
#                                                             "startDateTime"
#                                                         ] <= int(time.time()):
#                                                             offer[
#                                                                 "termscond"
#                                                             ] = terms_condition
#                                                             offer_data.append(offer)
#                                                     else:
#                                                         pass
#                                                 else:
#                                                     pass
#                                         else:
#                                             pass

#                                         if len(offer_data) > 0:
#                                             best_offer = max(
#                                                 offer_data, key=lambda x: x["discountValue"]
#                                             )
#                                             best_offer = {
#                                                 'offerId': best_offer['offerId'],
#                                                 'offerName': best_offer['offerName']['en'],
#                                                 'images': best_offer['images']['mobile'],
#                                                 'discountType': best_offer['discountType'],
#                                                 'discountValue':  best_offer['discountValue']
#                                             }
#                                         else:
#                                             best_offer = {}

#                                         if len(best_offer) == 0:
#                                             percentage = 0
#                                             discount_type = 0
#                                         else:
#                                             if "discountType" in best_offer:
#                                                 percentage = int(
#                                                     best_offer["discountValue"]
#                                                 )
#                                                 discount_type = best_offer["discountType"]
#                                             else:
#                                                 percentage = 0
#                                                 discount_type = 0

#                                         # ==================================get currecny rate============================
#                                         # try:
#                                         #     currency_rate = currency_exchange_rate[
#                                         #         str(tax_details["currency"])
#                                         #         + "_"
#                                         #         + str(currency_code)
#                                         #         ]
#                                         # except:
#                                         #     currency_rate = 0
#                                         # currency_details = db.currencies.find_one(
#                                         #     {"currencyCode": currency_code}
#                                         # )
#                                         # if currency_details is not None:
#                                         #     currency_symbol = currency_details[
#                                         #         "currencySymbol"
#                                         #     ]
#                                         #     currency = currency_details["currencyCode"]
#                                         # else:
#                                         #     currency_symbol = tax_details["currencySymbol"]
#                                         #     currency = tax_details["currency"]

#                                         # if float(currency_rate) > 0:
#                                         #     price = price * float(currency_rate)

#                                         tax_price_data = price + ((price * tax_price) / 100)
#                                         if discount_type == 0:
#                                             discount_price = float(percentage)
#                                         elif discount_type == 1:
#                                             discount_price = (
#                                                                         float(tax_price_data) * float(percentage)
#                                                                 ) / 100
#                                         else:
#                                             discount_price = 0
#                                         base_price = tax_price_data - discount_price

#                                         try:
#                                             ch_pro = db.childProducts.find_one(
#                                                 {"_id": ObjectId(str(best_supplier["productId"]))},
#                                                 {"isMembersOnly": 1, "units": 1})
#                                             isMembersOnly = ch_pro.get("isMembersOnly", False)
#                                             nonMemberPrice = ch_pro["units"][0][
#                                                 "discountPriceForNonMembers"] if "discountPriceForNonMembers" in \
#                                                                                     ch_pro["units"][0] else round(
#                                                 discount_price, 2)
#                                             memberPrice = ch_pro["units"][0]["memberPrice"] if "memberPrice" in \
#                                                                                                 ch_pro["units"][
#                                                                                                     0] else round(
#                                                 discount_price, 2)
#                                         except:
#                                             isMembersOnly = False
#                                             nonMemberPrice = round(discount_price, 2)
#                                             memberPrice = round(discount_price, 2)

#                                         final_price_list = {
#                                             "basePrice": round(tax_price_data, 2),
#                                             "finalPrice": round(base_price, 2),
#                                             "discountType": discount_type,
#                                             "discountPrice": discount_price,
#                                             # "discountPriceForNonMembers": nonMemberPrice,
#                                             # "memberPrice": memberPrice
#                                         }
#                                         # try:
#                                         # # ========================================favourite products=========================
#                                         #     response_casandra = session.execute(
#                                         #         """SELECT * FROM favouriteproductsuserwise where userid=%(userid)s AND childproductid=%(productid)s ALLOW FILTERING""",
#                                         #         {
#                                         #             "userid": user_id,
#                                         #             "productid": str(
#                                         #                 best_supplier["productId"]
#                                         #             ),
#                                         #         },
#                                         #     )

#                                         #     if not response_casandra:
#                                         #         response_casandra = session.execute(
#                                         #             """SELECT * FROM favouriteproductsuserwise where userid=%(userid)s AND childproductid=%(productid)s ALLOW FILTERING""",
#                                         #             {
#                                         #                 "userid": user_id,
#                                         #                 "productid": str(
#                                         #                     hits["_source"]["childproductid"]
#                                         #                 ),
#                                         #             },
#                                         #         )
#                                         #         if not response_casandra:
#                                         #             isFavourite = False
#                                         #         else:
#                                         #             isFavourite = True
#                                         #     else:
#                                         #         for fav in response_casandra:
#                                         #             isFavourite = True
#                                         # except Exception as e:
#                                         #     print(e)
#                                         isFavourite = False
#                                         response_casandra = None

#                                         # =================================recomanded products===========================
#                                         try:
#                                             product_name = (
#                                                 hits["_source"]["pName"][language]
#                                                 if language in hits["_source"]["pName"]
#                                                 else hits["_source"]["pName"]["en"]
#                                             )
#                                         except:
#                                             product_name = (
#                                                 hits["_source"]["units"][0]["unitName"][
#                                                     language
#                                                 ]
#                                                 if language
#                                                     in hits["_source"]["units"][0]["unitName"]
#                                                 else hits["_source"]["units"][0][
#                                                     "unitName"
#                                                 ]["en"]
#                                             )

#                                         # (
#                                         #     currency_symbol,
#                                         #     currency,
#                                         #     product_status,
#                                         #     product_tag,
#                                         # ) = next_availbale_time(best_supplier["productId"])

#                                         # if tax_details["status"] == 1:
#                                         #     if "mealConsumptionTime" in tax_details:
#                                         #         if (
#                                         #                 current_text
#                                         #                 in tax_details["mealConsumptionTime"]
#                                         #         ):
#                                         #             pass
#                                         #         elif (
#                                         #                 len(tax_details["mealConsumptionTime"])
#                                         #                 > 0
#                                         #         ):
#                                         #             if (
#                                         #                     int(current_hour) >= 0
#                                         #                     and int(current_hour) < 5
#                                         #             ):
#                                         #                 current_text = "latenightdinner"
#                                         #                 currenct_text_value = [
#                                         #                     "breakfast",
#                                         #                     "brunch",
#                                         #                     "lunch",
#                                         #                     "tea",
#                                         #                     "dinner",
#                                         #                 ]
#                                         #             elif (
#                                         #                     int(current_hour) >= 5
#                                         #                     and int(current_hour) < 10
#                                         #             ):
#                                         #                 current_text = "breakfast"
#                                         #                 currenct_text_value = [
#                                         #                     "brunch",
#                                         #                     "lunch",
#                                         #                     "tea",
#                                         #                     "dinner",
#                                         #                 ]
#                                         #             elif (
#                                         #                     int(current_hour) >= 10
#                                         #                     and int(current_hour) < 11
#                                         #             ):
#                                         #                 current_text = "brunch"
#                                         #                 currenct_text_value = [
#                                         #                     "lunch",
#                                         #                     "tea",
#                                         #                     "dinner",
#                                         #                 ]
#                                         #             elif (
#                                         #                     int(current_hour) >= 11
#                                         #                     and int(current_hour) < 15
#                                         #             ):
#                                         #                 current_text = "lunch"
#                                         #                 currenct_text_value = [
#                                         #                     "tea",
#                                         #                     "dinner",
#                                         #                 ]
#                                         #             elif (
#                                         #                     int(current_hour) >= 15
#                                         #                     and int(current_hour) < 19
#                                         #             ):
#                                         #                 current_text = "tea"
#                                         #                 currenct_text_value = ["dinner"]
#                                         #             else:
#                                         #                 current_text = "dinner"
#                                         #                 currenct_text_value = []
#                                         #             for k, v in tax_details[
#                                         #                 "mealConsumptionTime"
#                                         #             ].items():
#                                         #                 if k in currenct_text_value:
#                                         #                     product_status = True
#                                         #                     next_day = (
#                                         #                         datetime.datetime.now()
#                                         #                     )
#                                         #                     next_day_midnight = (
#                                         #                         next_day.replace(
#                                         #                             hour=int(
#                                         #                                 meal_timing[k]
#                                         #                             ),
#                                         #                             minute=00,
#                                         #                             second=00,
#                                         #                         )
#                                         #                     )
#                                         #                     # open_time = next_day_midnight.strftime("%b %d %Y, %I:%M %p")
#                                         #                     open_time = (
#                                         #                         next_day_midnight.strftime(
#                                         #                             "%I:%M %p"
#                                         #                         )
#                                         #                     )
#                                         #                     product_tag = (
#                                         #                             "Available On Today At "
#                                         #                             + open_time
#                                         #                     )
#                                         #                 else:
#                                         #                     product_status = True
#                                         #                     next_day = (
#                                         #                             datetime.datetime.now()
#                                         #                             + datetime.timedelta(days=1)
#                                         #                     )
#                                         #                     next_day_midnight = (
#                                         #                         next_day.replace(
#                                         #                             hour=meal_timing[k],
#                                         #                             minute=00,
#                                         #                             second=00,
#                                         #                         )
#                                         #                     )
#                                         #                     open_time = (
#                                         #                         next_day_midnight.strftime(
#                                         #                             "%I:%M %p"
#                                         #                         )
#                                         #                     )
#                                         #                     product_tag = (
#                                         #                             "Next available at "
#                                         #                             + open_time
#                                         #                             + " tomorrow"
#                                         #                     )
#                                         #         else:
#                                         #             product_tag = ""
#                                         #             product_status = False
#                                         #     else:
#                                         #         pass
#                                         # else:
#                                         #     pass

#                                         # try:
#                                         #     isMembersOnly = db.childProducts.find_one(
#                                         #         {"_id": ObjectId(str(best_supplier["productId"]))},
#                                         #         {"isMembersOnly": 1}).get("isMembersOnly", False)
#                                         # except:
#                                         #     isMembersOnly = False
#                                         variant_count = db.childProducts.find({'units.isPrimary': False,'parentProductId': tax_details["parentProductId"], 'status':1, 'storeId':  ObjectId(str(product_store_id))}).count()
#                                         try:
#                                             addons_product_count = db.childProducts.find_one({'_id': best_supplier[
#                                                     "productId"
#                                                 ]}, {'units':1})
#                                             addons_count_data = len(addons_product_count['units'][0]['addOns'])
#                                         except:
#                                             addons_count_data = 0
#                                         json_data = {
#                                             "parentProductId": str(
#                                                 hits["_source"]["parentProductId"]
#                                             ),
#                                             "childProductId": best_supplier["productId"],
#                                             # "productStatus": True,
#                                             # "productTag": "",
#                                             "offers": best_offer,
#                                             # "unitId": best_supplier["productId"],
#                                             "finalPriceList": final_price_list,
#                                             "availableForSale": True ,
#                                             "productAvailabilityMessage": "",
#                                             # "catName": category_name,
#                                             "detailDescription": detail_description,
#                                             "containsMeat": hits["_source"]["containsMeat"]
#                                             if "containsMeat" in hits["_source"]
#                                             else False,
#                                             "allPrice" :tax_details["units"][0]["b2cPricing"],
#                                             # "addOnsCount": addons_count,
#                                             # "isFavourite": isFavourite,
#                                             "productName": product_name,
#                                             "createdTime" :tax_details["createdTimestamp"] if "createdTimestamp" in tax_details else 0,
#                                             "Customizable": True if variant_count > 0 or addons_count_data > 0 else False,
#                                             "images": hits["_source"]["images"][0]['small'] if len(hits["_source"]["images"]) > 0 else "",
#                                             # "currencySymbol": tax_details['currencySymbol'],
#                                             # "currency": tax_details['currency'],
#                                             # "availableQuantity": best_supplier[
#                                             #     "retailerQty"
#                                             # ],
#                                             # "isMembersOnly": isMembersOnly,
#                                             "nextAvailableOn": tax_details['nextAvailableTime'] if 'nextAvailableTime' in tax_details else 0,
#                                             # "mealConsumptionTime": tax_details['mealConsumptionTime'] if 'mealConsumptionTime' in tax_details else {},
#                                             # "consumptionTime": tax_details['consumptionTime'],
#                                             # "status": tax_details['status']
#                                         }
#                                         if isFavourite == True:
#                                             favourite_data.append(json_data)
#                                         else:
#                                             pass
#                                         if "consumptionTime" in tax_details and len(tax_details['consumptionTime']) > 0:
#                                             for category in tax_details['consumptionTime']:
#                                                 if category in consumption_lists:
#                                                     consumption_lists[category].append(json_data)
#                                         product_data.append(json_data)
#                             if len(product_data) > 0:
                                
#                                 parent_category_details.append(
#                                     {
#                                         "catName": category_name,
#                                         "isSubCategories": False,
#                                         "subCategories": [],
#                                         "products": product_data,
#                                         "seqId": count,
#                                     }
#                                 )
#                                 recommanded_details.append(
#                                     {
#                                         "catName": category_name,
#                                         "penCount": len(product_data),
#                                         "seqId": count,
#                                     }
#                                 )
#                 final_data['parentCategoryProduct'] = parent_category_details
#                 newlist_recomnded = sorted(
#                             recommanded_details, key=lambda k: k["seqId"], reverse=False
#                         )
#                 final_data["recommFilter"] =  newlist_recomnded
#                 final_data['recommdedProducts'] = consumption_lists
#                 final_data['offerData'] = offer_json
#                 ''' add data in db '''
#                 print('storeId--', str(store_data['_id']))
#                 find_store_data = db.storeBestProductsNew.find_one({"storeData.storeId": str(store_data['_id'])})
#                 if find_store_data is not None:
#                     print('delete data')
#                     db.storeBestProductsNew.delete_one({"storeId": str(store_data['_id'])})
#                     print('add data')
#                     db.storeBestProductsNew.insert_one(final_data)
#                 else:
#                     db.storeBestProductsNew.insert_one(final_data)
#                 print('data added')
#             except Exception as ex:
#                 template = "An exception of type {0} occurred. Arguments:\n{1!r}"
#                 message = template.format(
#                     type(ex).__name__, ex.args
#                 )
#                 print(
#                     "Error on line {}".format(
#                         sys.exc_info()[-1].tb_lineno
#                     ),
#                     type(ex).__name__,
#                     ex,
#                 )