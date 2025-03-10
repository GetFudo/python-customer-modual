import os
import sys
import traceback
from pytz import timezone
from bson.objectid import ObjectId
import datetime
import time
from validations.time_zone_validation import time_zone_converter

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from search_api.settings import db, es, STORE_PRODUCT_DOC_TYPE, STORE_PRODUCT_INDEX, TIME_ZONE, EARTH_REDIS, conv_fac

R = float(EARTH_REDIS)
conv_fac = float(conv_fac)

timezonename = TIME_ZONE
index_store = STORE_PRODUCT_INDEX
doc_type_store = STORE_PRODUCT_DOC_TYPE


def store_function(language, store_id, user_id, user_latitude, user_longtitude, timezone):
    try:
        print("timezone", timezone)
        must_query = [{"match": {"_id": store_id}}]
        # must_not_query.append({"terms": {"storeFrontTypeId": [2]}})
        geo_distance_sort = {
            "_geo_distance": {
                "distance_type": "plane",
                "location": {
                    "lat": float(user_latitude),
                    "lon": float(user_longtitude)
                },
                "order": "asc",
                "unit": "km"
            }
        }

        sort_query = [geo_distance_sort]
        query = {
            "query":
                {
                    "bool":
                        {
                            "must": must_query
                        }
                },
            "size": int(10),
            "from": int(0),
            "sort": sort_query
        }

        res = es.search(
            index=index_store,
            body=query,
            filter_path=[
                "hits.total",
                "hits.hits._id",
                "hits.hits.sort",
                "hits.hits._source"
            ],
        )
        store_data_json = []
        close_data_json = []
        specialities_data = []
        store_data_count = res['hits']['total']['value']
        is_temp_close = True
        if store_data_count > 0:
            if "hits" in res['hits']:
                for seller in res['hits']['hits']:
                    store_details = db.stores.find_one({"_id": ObjectId(seller['_id'])})
                    cusine_name = ""
                    avg_rating_value = 0
                    seller_rating = db.sellerReviewRatings.aggregate(
                        [
                            {"$match": {
                                "sellerId": str(seller['_id']),
                                "rating": {"$ne": 0},
                                "status": 1}},
                            {
                                "$group":
                                    {
                                        "_id": "$sellerId",
                                        "avgRating": {"$avg": "$rating"}
                                    }
                            }
                        ]
                    )
                    for avg_rating in seller_rating:
                        avg_rating_value = avg_rating['avgRating']

                    distance_km = round(seller['sort'][0], 2)
                    distance_miles = round(distance_km * conv_fac, 2)

                    if "averageCostForMealForTwo" in store_details:
                        cost_for_two = store_details['averageCostForMealForTwo']
                    else:
                        cost_for_two = 0

                    offer_details = db.offers.find({"storeId": {"$in": [str(seller['_id'])]}, "status": 1})
                    offer_json = []
                    for offer in offer_details:
                        offer_json.append({
                            "offerName": offer['name'][language] if language in offer['name'] else offer['name']['en'],
                            "offerId": str(offer['_id']),
                            "offerType": offer['offerType'],
                            "discountValue": offer['discountValue']
                        })

                    if len(offer_json) > 0:
                        best_offer_store = max(offer_json, key=lambda x: x['discountValue'])
                        if best_offer_store['offerType'] == 0:
                            percentage_text = str(best_offer_store['discountValue']) + "%" + " " + "off"
                        else:
                            percentage_text = "₹" + str(best_offer_store['discountValue']) + " off"
                        offer_name = best_offer_store['offerName']
                    else:
                        offer_name = ""
                        percentage_text = ""

                    address = store_details['businessLocationAddress']['address'] if "address" in store_details[
                        'businessLocationAddress'] else ""
                    addressArea = store_details['businessLocationAddress']['addressArea'] if "addressArea" in \
                                                                                             store_details[
                                                                                                 'businessLocationAddress'] else ""
                    locality = store_details['businessLocationAddress']['locality'] if "locality" in store_details[
                        'businessLocationAddress'] else ""
                    post_code = store_details['businessLocationAddress']['postCode'] if "postCode" in store_details[
                        'businessLocationAddress'] else ""
                    state = store_details['businessLocationAddress']['state'] if "state" in store_details[
                        'businessLocationAddress'] else ""
                    country = store_details['businessLocationAddress']['country'] if "country" in store_details[
                        'businessLocationAddress'] else ""
                    city = store_details['businessLocationAddress']['city'] if "city" in store_details[
                        'businessLocationAddress'] else ""

                    # ===================================for the cusines=============================================
                    if "specialities" in store_details:
                        if len(store_details['specialities']):
                            for spec in store_details['specialities']:
                                spec_data = db.specialities.find_one({"_id": ObjectId(spec)},
                                                                     {"specialityName": 1, "image": 1})
                                if spec_data != None:
                                    specialities_data.append({
                                        "id": str(spec),
                                        "image": spec_data['image'] if "image" in spec_data else "",
                                        "name": spec_data['specialityName'][language] if language in spec_data[
                                            'specialityName'] else spec_data['specialityName']["en"],
                                    })
                                    if cusine_name == "":
                                        cusine_name = spec_data['specialityName'][language] if language in spec_data[
                                            'specialityName'] else spec_data['specialityName']["en"]
                                    else:
                                        cusine_name = cusine_name + ", " + spec_data['specialityName'][
                                            language] if language in spec_data['specialityName'] else \
                                            spec_data['specialityName']["en"]
                                else:
                                    pass
                        else:
                            pass
                    else:
                        pass

                    if "storeIsOpen" in store_details:
                        store_is_open = store_details['storeIsOpen']
                    else:
                        store_is_open = False

                    if "nextCloseTime" in store_details:
                        next_close_time = store_details['nextCloseTime']
                    else:
                        next_close_time = ""

                    if "nextOpenTime" in store_details:
                        next_open_time = store_details['nextOpenTime']
                    else:
                        next_open_time = ""

                    try:
                        if "timeZoneWorkingHour" in seller["_source"]:
                            timeZoneWorkingHour = seller["_source"]['timeZoneWorkingHour']
                        else:
                            timeZoneWorkingHour = ""
                    except:
                        timeZoneWorkingHour = ""

                    if next_close_time == "" and next_open_time == "":
                        is_temp_close = True
                        store_tag = "Temporarily Closed"
                    elif next_open_time != "" and store_is_open == False:
                        try:
                            next_open_time = time_zone_converter(timezone, next_open_time, timeZoneWorkingHour)
                        except Exception as ex:
                            print(
                                "Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex
                            )
                            next_open_time = int(next_open_time)
                        is_temp_close = False
                        local_time = datetime.datetime.fromtimestamp(next_open_time)
                        next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                        next_day_midnight = next_day.replace(hour=0, minute=0, second=0)
                        next_day_midnight_timestamp = int(next_day_midnight.timestamp())
                        if next_day_midnight_timestamp > next_open_time:
                            open_time = local_time.strftime("%I:%M %p")
                            store_tag = "Opens Next At " + open_time
                        else:
                            open_time = local_time.strftime("%I:%M %p")
                            store_tag = "Opens Tomorrow At " + open_time
                    else:
                        is_temp_close = False
                        store_tag = ""

                    # =============================================for safety=======================================
                    if "safetyStandards" in store_details:
                        if int(store_details['safetyStandards']) == 0:
                            safety_standard = False
                            safety_standards_sort_discription = ""
                        else:
                            safety_standard = True
                            safety_standards_sort_discription = store_details['safetyStandardsSortDiscription']
                    else:
                        safety_standard = False
                        safety_standards_sort_discription = ""

                    # ====================================favourite stores==========================================
                    if "favouriteUsers" in store_details:
                        if user_id in store_details['favouriteUsers']:
                            favourite_store = True
                        else:
                            favourite_store = False
                    else:
                        favourite_store = False

                    print("favourite_store", favourite_store)
                    # =======================================safetyStandards description================================
                    if "safetyStandardsDynamicContent" in store_details:
                        if store_details['safetyStandardsDynamicContent'] != "":
                            safety_standards_dynamic_content = store_details['safetyStandardsDynamicContent']
                        else:
                            safety_standards_dynamic_content = ""
                    else:
                        safety_standards_dynamic_content = ""

                    if "shopifyStoreDetails" in seller['_source']:
                        if "enable" in seller["_source"]["shopifyStoreDetails"]:
                            shopify_enable = seller["_source"]["shopifyStoreDetails"]['enable']
                        else:
                            shopify_enable = False
                    else:
                        shopify_enable = False

                    # ======================================open stores=============================================
                    store_data_json.append({
                        "_id": str(seller['_id']),
                        "shopifyEnable": shopify_enable,
                        "avgRating": round(avg_rating_value, 2),
                        "averageCostForMealForTwo": cost_for_two,
                        "currencyCode": store_details['currencyCode'] if "currencyCode" in store_details else "₹",
                        "businessLocationAddress": store_details['businessLocationAddress'],
                        "billingAddress": store_details['billingAddress'] if "billingAddress" in store_details else {},
                        "headOffice": store_details['headOffice'] if "headOffice" in store_details else {},
                        "logoImages": store_details['logoImages'],
                        "nextCloseTime": store_details['nextCloseTime'] if "nextCloseTime" in store_details else "",
                        "nextOpenTime": store_details['nextOpenTime'] if "nextOpenTime" in store_details else "",
                        "distanceKm": round(distance_km, 2),
                        "freeDeliveryAbove": store_details[
                            'freeDeliveryAbove'] if "freeDeliveryAbove" in store_details else 0,
                        "currencySymbol": store_details['currencySymbol'] if "currencySymbol" in store_details else "₹",
                        "currency": store_details['currencyCode'] if "currencyCode" in store_details else "INR",
                        "safetyStandards": safety_standard,
                        "safetyStandardsSortDiscription": safety_standards_sort_discription,
                        "safetyStandardsDynamicContent": safety_standards_dynamic_content,
                        "storeTag": store_tag,
                        "isTempClose": is_temp_close,
                        "offerName": offer_name,
                        "cuisines": cusine_name,
                        "address": address,
                        "locality": locality,
                        "postCode": post_code,
                        "addressArea": addressArea,
                        "state": state,
                        "country": country,
                        "city": city,
                        "isFavourite": favourite_store,
                        "distanceMiles": distance_miles,
                        "bannerImages": store_details['bannerImages'],
                        "storeSeo": store_details['storeSeo'] if "storeSeo" in store_details else {},
                        "minimumOrder": store_details['minimumOrder'],
                        "minimumOrderValue": str(store_details['minimumOrder']) if store_details[
                                                                                       'minimumOrder'] != 0 else "No "
                                                                                                                 "Minimum",
                        "galleryImages": store_details['galleryImages'],
                        "cityId": store_details['cityId'],
                        "citiesOfOperation": store_details['citiesOfOperation'],
                        "isExpressDelivery": int(
                            store_details['isExpressDelivery']) if "isExpressDelivery" in store_details else 0,
                        "parentSellerIdOrSupplierId": store_details['parentSellerIdOrSupplierId'],
                        "storeName": store_details['storeName'][language] if language in store_details['storeName'] else
                        store_details['storeName']['en'],
                        "address": store_details['headOffice']['headOfficeAddress'] if "headOfficeAddress" in
                                                                                       store_details[
                                                                                           'headOffice'] else "",
                        "sellerTypeId": store_details['sellerTypeId'],
                        "sellerType": store_details['sellerType'],
                        "averageDeliveryTimeInMins": store_details[
                            'averageDeliveryTimeInMins'] if "averageDeliveryTimeInMins" in seller['_source'] else 0,
                        "storeFrontTypeId": store_details['storeFrontTypeId'],
                        "storeFrontType": store_details['storeFrontType'],
                        "driverTypeId": store_details['driverTypeId'] if "driverTypeId" in store_details else 0,
                        "driverType": store_details['driverType'] if "driverType" in store_details else 0,
                        "nextCloseTime": next_close_time,
                        "nextOpenTime": next_open_time,
                        "storeIsOpen": store_is_open,
                        "status": store_details['status'],
                        "percentageText": percentage_text,
                    })
        json_response = {
            "data": store_data_json
        }
        return json_response
    except Exception as ex:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex
        )
        json_response = {
            "data": []
        }
        return json_response


def store_function_zonewise(language, zone_id, user_id, user_latitude, user_longtitude, timezone, from_data, to_data,
                            es_res=None):
    try:
        print("timezone", timezone)
        if not es_res:
            must_query = [{"match": {"serviceZones.zoneId": zone_id}}]
            # must_not_query.append({"terms": {"storeFrontTypeId": [2]}})
            geo_distance_sort = {
                "_geo_distance": {
                    "distance_type": "plane",
                    "location": {
                        "lat": float(user_latitude),
                        "lon": float(user_longtitude)
                    },
                    "order": "asc",
                    "unit": "km"
                }
            }

            sort_query = [geo_distance_sort]
            query = {
                "query":
                    {
                        "bool":
                            {
                                "must": must_query
                            }
                    },
                "size": int(to_data),
                "from": int(from_data),
                "sort": sort_query
            }

            res = es.search(
                index=STORE_PRODUCT_INDEX,
                body=query,
                filter_path=[
                    "hits.total",
                    "hits.hits._id",
                    "hits.hits.sort",
                    "hits.hits._source"
                ],
            )
        else:
            res = es_res
        store_data_json = []
        close_data_json = []
        specialities_data = []
        store_data_count = res['hits']['total']['value']
        is_temp_close = True
        categoriesed = {}
        store_count = 0
        if store_data_count > 0:
            if "hits" in res['hits']:
                for seller in res['hits']['hits']:
                    try:
                        store_details = db.stores.find_one({"_id": ObjectId(seller['_id'])})
                        cusine_name = ""
                        avg_rating_value = 0
                        seller_rating = db.sellerReviewRatings.aggregate(
                            [
                                {"$match": {
                                    "sellerId": str(seller['_id']),
                                    "rating": {"$ne": 0},
                                    "status": 1}},
                                {
                                    "$group":
                                        {
                                            "_id": "$sellerId",
                                            "avgRating": {"$avg": "$rating"}
                                        }
                                }
                            ]
                        )
                        for avg_rating in seller_rating:
                            avg_rating_value = avg_rating['avgRating']

                        distance_km = round(seller['sort'][0], 2)
                        distance_miles = round(distance_km * conv_fac, 2)

                        if "averageCostForMealForTwo" in store_details:
                            cost_for_two = store_details['averageCostForMealForTwo']
                        else:
                            cost_for_two = 0

                        offer_details = db.offers.find({"storeId": {"$in": [str(seller['_id'])]}, "status": 1})
                        offer_json = []
                        for offer in offer_details:
                            offer_json.append({
                                "offerName": offer['name'][language] if language in offer['name'] else offer['name'][
                                    'en'],
                                "offerId": str(offer['_id']),
                                "offerType": offer['offerType'],
                                "discountValue": offer['discountValue']
                            })

                        if len(offer_json) > 0:
                            best_offer_store = max(offer_json, key=lambda x: x['discountValue'])
                            if best_offer_store['offerType'] == 0:
                                percentage_text = str(best_offer_store['discountValue']) + "%" + " " + "off"
                            else:
                                percentage_text = "₹" + str(best_offer_store['discountValue']) + " off"
                            offer_name = best_offer_store['offerName']
                        else:
                            offer_name = ""
                            percentage_text = ""

                        address = store_details['businessLocationAddress']['address'] if "address" in store_details[
                            'businessLocationAddress'] else ""
                        addressArea = store_details['businessLocationAddress']['addressArea'] if "addressArea" in \
                                                                                                 store_details[
                                                                                                     'businessLocationAddress'] else ""
                        locality = store_details['businessLocationAddress']['locality'] if "locality" in store_details[
                            'businessLocationAddress'] else ""
                        post_code = store_details['businessLocationAddress']['postCode'] if "postCode" in store_details[
                            'businessLocationAddress'] else ""
                        state = store_details['businessLocationAddress']['state'] if "state" in store_details[
                            'businessLocationAddress'] else ""
                        country = store_details['businessLocationAddress']['country'] if "country" in store_details[
                            'businessLocationAddress'] else ""
                        city = store_details['businessLocationAddress']['city'] if "city" in store_details[
                            'businessLocationAddress'] else ""

                        # ===================================for the cusines=============================================
                        if "specialities" in store_details:
                            if len(store_details['specialities']):
                                for spec in store_details['specialities']:
                                    spec_data = db.specialities.find_one({"_id": ObjectId(spec)},
                                                                         {"specialityName": 1, "image": 1})
                                    if spec_data != None:
                                        if str(spec) not in str(specialities_data):
                                            specialities_data.append({
                                                "id": str(spec),
                                                "image": spec_data['image'] if "image" in spec_data else "",
                                                "name": spec_data['specialityName'][language] if language in spec_data[
                                                    'specialityName'] else spec_data['specialityName']["en"],
                                            })
                                            if cusine_name == "":
                                                cusine_name = spec_data['specialityName'][language] if language in \
                                                                                                       spec_data[
                                                                                                           'specialityName'] else \
                                                spec_data['specialityName']["en"]
                                            else:
                                                cusine_name = cusine_name + ", " + spec_data['specialityName'][
                                                    language] if language in spec_data['specialityName'] else \
                                                    spec_data['specialityName']["en"]
                                    else:
                                        pass
                            else:
                                pass
                        else:
                            pass

                        if "storeIsOpen" in store_details:
                            store_is_open = store_details['storeIsOpen']
                        else:
                            store_is_open = False

                        if "nextCloseTime" in store_details:
                            next_close_time = store_details['nextCloseTime']
                        else:
                            next_close_time = ""

                        if "nextOpenTime" in store_details:
                            next_open_time = store_details['nextOpenTime']
                        else:
                            next_open_time = ""

                        try:
                            if "timeZoneWorkingHour" in seller["_source"]:
                                timeZoneWorkingHour = seller["_source"]['timeZoneWorkingHour']
                            else:
                                timeZoneWorkingHour = ""
                        except:
                            timeZoneWorkingHour = ""

                        if next_close_time == "" and next_open_time == "":
                            is_temp_close = True
                            store_tag = "Temporarily Closed"
                        elif next_open_time != "" and store_is_open == False:
                            try:
                                next_open_time = time_zone_converter(timezone, next_open_time, timeZoneWorkingHour)
                            except Exception as ex:
                                print(
                                    "Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex
                                )
                                next_open_time = int(next_open_time)
                            is_temp_close = False
                            local_time = datetime.datetime.fromtimestamp(next_open_time)
                            next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                            next_day_midnight = next_day.replace(hour=0, minute=0, second=0)
                            next_day_midnight_timestamp = int(next_day_midnight.timestamp())
                            if next_day_midnight_timestamp > next_open_time:
                                open_time = local_time.strftime("%I:%M %p")
                                store_tag = "Opens Next At " + open_time
                            else:
                                open_time = local_time.strftime("%I:%M %p")
                                store_tag = "Opens Tomorrow At " + open_time
                        else:
                            is_temp_close = False
                            store_tag = ""

                        # =============================================for safety=======================================
                        if "safetyStandards" in store_details:
                            if int(store_details['safetyStandards']) == 0:
                                safety_standard = False
                                safety_standards_sort_discription = ""
                            else:
                                safety_standard = True
                                safety_standards_sort_discription = store_details['safetyStandardsSortDiscription']
                        else:
                            safety_standard = False
                            safety_standards_sort_discription = ""

                        # ====================================favourite stores==========================================
                        if "favouriteUsers" in store_details:
                            if user_id in store_details['favouriteUsers']:
                                favourite_store = True
                            else:
                                favourite_store = False
                        else:
                            favourite_store = False

                        print("favourite_store", favourite_store)
                        # =======================================safetyStandards description================================
                        if "safetyStandardsDynamicContent" in store_details:
                            if store_details['safetyStandardsDynamicContent'] != "":
                                safety_standards_dynamic_content = store_details['safetyStandardsDynamicContent']
                            else:
                                safety_standards_dynamic_content = ""
                        else:
                            safety_standards_dynamic_content = ""

                        if "shopifyStoreDetails" in seller['_source']:
                            if "enable" in seller["_source"]["shopifyStoreDetails"]:
                                shopify_enable = seller["_source"]["shopifyStoreDetails"]['enable']
                            else:
                                shopify_enable = False
                        else:
                            shopify_enable = False

                        store_category = "N/A"
                        if "categoryName" in store_details:
                            if "en" in store_details["categoryName"]:
                                store_category = store_details["categoryName"]["en"] if store_details["categoryName"][
                                                                                            "en"] != "" else "N/A"

                        # ======================================open stores=============================================
                        store_data_dict = {
                            "_id": str(seller['_id']),
                            "shopifyEnable": shopify_enable,
                            "avgRating": round(avg_rating_value, 2),
                            "averageCostForMealForTwo": cost_for_two,
                            "currencyCode": store_details['currencyCode'] if "currencyCode" in store_details else "₹",
                            "businessLocationAddress": store_details['businessLocationAddress'],
                            "billingAddress": store_details[
                                'billingAddress'] if "billingAddress" in store_details else {},
                            "headOffice": store_details['headOffice'] if "headOffice" in store_details else {},
                            "logoImages": store_details['logoImages'],
                            "nextCloseTime": store_details['nextCloseTime'] if "nextCloseTime" in store_details else "",
                            "nextOpenTime": store_details['nextOpenTime'] if "nextOpenTime" in store_details else "",
                            "distanceKm": round(distance_km, 2),
                            "freeDeliveryAbove": store_details[
                                'freeDeliveryAbove'] if "freeDeliveryAbove" in store_details else 0,
                            "currencySymbol": store_details['currencyCode'] if "currencyCode" in store_details else "₹",
                            "currency": store_details['currencyCode'] if "currencyCode" in store_details else "₹",
                            "safetyStandards": safety_standard,
                            "safetyStandardsSortDiscription": safety_standards_sort_discription,
                            "safetyStandardsDynamicContent": safety_standards_dynamic_content,
                            "storeTag": store_tag,
                            "isTempClose": is_temp_close,
                            "offerName": offer_name,
                            "cuisines": cusine_name,
                            "address": address,
                            "locality": locality,
                            "postCode": post_code,
                            "addressArea": addressArea,
                            "state": state,
                            "country": country,
                            "city": city,
                            "isFavourite": favourite_store,
                            "distanceMiles": distance_miles,
                            "bannerImages": store_details['bannerImages'],
                            "storeSeo": store_details['storeSeo'] if "storeSeo" in store_details else {},
                            "minimumOrder": store_details['minimumOrder'],
                            "minimumOrderValue": str(store_details['minimumOrder']) if store_details[
                                                                                           'minimumOrder'] != 0 else "No "
                                                                                                                     "Minimum",
                            "galleryImages": store_details['galleryImages'],
                            "cityId": store_details['cityId'],
                            "citiesOfOperation": store_details['citiesOfOperation'],
                            "isExpressDelivery": int(
                                store_details['isExpressDelivery']) if "isExpressDelivery" in store_details else 0,
                            "parentSellerIdOrSupplierId": store_details['parentSellerIdOrSupplierId'],
                            "storeName": store_details['storeName'][language] if language in store_details[
                                'storeName'] else
                            store_details['storeName']['en'],
                            "address": store_details['headOffice']['headOfficeAddress'] if "headOfficeAddress" in
                                                                                           store_details[
                                                                                               'headOffice'] else "",
                            "sellerTypeId": store_details['sellerTypeId'],
                            "sellerType": store_details['sellerType'],
                            "averageDeliveryTimeInMins": store_details[
                                'averageDeliveryTimeInMins'] if "averageDeliveryTimeInMins" in seller['_source'] else 0,
                            "storeFrontTypeId": store_details['storeFrontTypeId'],
                            "storeFrontType": store_details['storeFrontType'],
                            "driverTypeId": store_details['driverTypeId'] if "driverTypeId" in store_details else 0,
                            "driverType": store_details['driverType'] if "driverType" in store_details else 0,
                            "nextCloseTime": next_close_time,
                            "nextOpenTime": next_open_time,
                            "storeIsOpen": store_is_open,
                            "status": store_details['status'],
                            "percentageText": percentage_text,
                            "storeCategory": store_category
                        }
                        if store_category in categoriesed:
                            categoriesed[store_category].append(store_data_dict)
                        else:
                            categoriesed[store_category] = [store_data_dict]
                        store_count += 1
                    except Exception as e:
                        traceback.print_exc()
                        print(f"Error while getting store details zonewise: {str(e)}")
                        continue
        json_response = {
            "data": categoriesed,
            "specialities": specialities_data,
            "scount": store_count
        }
        return json_response
    except Exception as ex:
        print(
            "Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex
        )
        json_response = {
            "data": []
        }
        return json_response
