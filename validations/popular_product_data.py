import os
import sys
from pytz import timezone
from bson.objectid import ObjectId
import datetime
import pandas as pd
from geopy.geocoders import Nominatim
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from search_api.settings import db, es, POPULAR_API_URL, CASSANDRA_KEYSPACE, CASSANDRA_SERVER, CENTRAL_PRODUCT_INDEX, \
    CENTRAL_PRODUCT_DOC_TYPE, CENTRAL_PRODUCT_VARIENT_INDEX, CHILD_PRODUCT_INDEX, CHILD_PRODUCT_DOC_TYPE, \
    STORE_PRODUCT_INDEX, STORE_PRODUCT_DOC_TYPE, CENRAL_STORE_NAME, PYTHON_PRODUCT_URL, \
    OFFER_DOC_TYPE, OFFER_INDEX, KAFKA_URL, PYTHON_BASE_URL, REDIS_IP, STORE_CREATE_TIME, \
    TIME_ZONE, PHARMACY_STORE_CATEGORY_ID, ECOMMERCE_STORE_CATEGORY_ID, WEBSITE_URL, \
    conv_fac, EARTH_REDIS, MEAT_STORE_CATEGORY_ID, DINE_STORE_CATEGORY_ID, \
    GROCERY_STORE_CATEGORY_ID, CANNABIS_STORE_CATEGORY_ID, \
    LIQUOR_STORE_CATEGORY_ID, REBBITMQ_USERNAME, REBBITMQ_PASSWORD, REBBITMQ_IP, GRPC_URL
from search.views import home_units_data
from validations.driver_roaster import next_availbale_driver_shift_out_stock, next_availbale_driver_roaster, next_availbale_driver_out_stock_shift, next_availbale_driver_shift_in_stock
from validations.product_dc_validation import validate_dc_product_data
import time


def city_state_country(coord):
    """
    Provide the city, state and, country name with respect to co-ordinate
    """
    try:
        geo_locator = Nominatim(user_agent=GEO_LOC_USER_AGENT)
        location = geo_locator.reverse(coord, exactly_one=True)
        address = location.raw['address']
        city = address.get('city', '')
        state = address.get('state', '')
        country = address.get('country', '')
        return {"city": city, "state": state, "country": country}
    except:
        return {}


def popular_product_data_validate(store_category_id, user_id, store_id, zone_id, latitude, longitude):
    co_ord = str(latitude) + ", " + str(longitude)
    location = city_state_country(co_ord)
    city_name = location.get("city")
    state_name = location.get("state")
    country_name = location.get("country")
    remove_central = False
    category_details = db.cities.find_one({"storeCategory.storeCategoryId": store_category_id},
                                          {"storeCategory": 1})
    if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
        is_ecommerce = True
        remove_central = False
        hide_recent_view = False
    else:
        if "storeCategory" in category_details:
            for cat in category_details['storeCategory']:
                if cat['storeCategoryId'] == store_category_id:
                    if cat['hyperlocal'] == True and cat['storeListing'] == 1:
                        remove_central = True
                    elif cat['hyperlocal'] == True and cat['storeListing'] == 0:
                        remove_central = True
                    else:
                        remove_central = False
                else:
                    pass
        else:
            remove_central = False
    skip = 0
    limit = 6

    query = {}
    if store_category_id: query["store_category_id"] = store_category_id
    store_details = []
    if store_id != "" and zone_id != "" and store_category_id != PHARMACY_STORE_CATEGORY_ID:
        store_details.append(str(store_id))
    elif store_category_id == MEAT_STORE_CATEGORY_ID:
        pass
    elif store_id != "" and zone_id != "":
        store_details.append(str(store_id))
        store_data = db.stores.find(
            {"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id, "status": 1})
        for store in store_data:
            store_details.append(str(store['_id']))
    elif store_id != "" and zone_id == "":
        store_details.append(str(store_id))
    elif store_id == "" and zone_id != "":
        store_data = db.stores.find(
            {"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id, "status": 1})
        for store in store_data:
            store_details.append(str(store['_id']))
    else:
        pass
    if len(store_details) > 0:
        query["store_id"] = {"$in": store_details}

    if city_name: query["city_name"] = {"$regex": "^" + city_name + "$", "$options": "i"}
    if state_name: query["state_name"] = {"$regex": "^" + state_name + "$", "$options": "i"}
    if country_name: query["country_name"] = {"$regex": "^" + country_name + "$", "$options": "i"}
    data = {"central_product_id": 1, "_id": 0}
    product_data = list(db.popularProducts.find(query, data).sort([("popular_score", -1)]))
    if not product_data:
        final_json = {"data": [], "message": "No Data Found"}
        return final_json

    product_data = [product["central_product_id"] for product in product_data]
    product_ids = list(set(product_data))
    filter_responseJson = []
    elatic_search_query = {"query": {"bool": {"must": [{"terms": {"_id": product_ids}},
                                                       {"match": {"status": 1}}, {"match": {"storeCategoryId": store_category_id}}
                                                       ]}},
                           "size": limit,
                           "from": skip * limit
                           }
    res = es.search(
        index=CENTRAL_PRODUCT_INDEX,
        body=elatic_search_query,
        filter_path=[
            "hits.total",
            "hits.hits._score",
            "hits.hits._id",
            "hits.hits._source",
        ],
    )
    try:
        if "value" in res['hits']['total']:
            if res['hits']['total'] == 0 or "hits" not in res['hits']:
                final_json = {"products": [], "message": "No Data Found"}
                return final_json
        else:
            if res['hits']['total'] == 0 or "hits" not in res['hits']:
                final_json = {"products": [], "message": "No Data Found"}
                return final_json
    except:
        if res['hits']['total'] == 0 or "hits" not in res['hits']:
            final_json = {"products": [], "message": "No Data Found"}
            return final_json

    resData = []
    base_price = 0
    final_price = 0
    login_type = 0
    language = "en"
    sort_type = 0
    best_supplier = {}
    store_list_json = []
    zone_id = zone_id
    discount_price = 0
    isShoppingList = False
    allow_order_out_of_stock = False
    offer_details = []
    mou_data = ""
    popularstatus = 0
    store_category_id = store_category_id
    try:
        user_id = userId
    except:
        user_id = "5ec91f85eac3521dd112c5b9"

    if zone_id != "" and store_category_id == PHARMACY_STORE_CATEGORY_ID or store_category_id == MEAT_STORE_CATEGORY_ID:
        stores_list = db.stores.find({"status": 1, "serviceZones.zoneId": zone_id})
        for s in stores_list:
            store_list_json.append(str(s['_id']))
    else:
        pass

    if zone_id != "":
        driver_roaster = next_availbale_driver_roaster(zone_id)
        next_delivery_slot = driver_roaster['text']
        slot_id = driver_roaster['slotId']
        next_availbale_driver_time = driver_roaster['productText']
    else:
        next_delivery_slot = ""
        slot_id = ""
        next_availbale_driver_time = ""

    store_details = db.stores.find({"serviceZones.zoneId": zone_id, "storeFrontTypeId": 5, "status": 1})
    dc_seller_list = []
    for dc_seller in store_details:
        dc_seller_list.append(str(dc_seller['_id']))
    resData = validate_dc_product_data(res["hits"]["hits"][skip:limit],store_id,zone_id,language,next_availbale_driver_time, user_id)
    if len(resData) > 0:
        if store_category_id != MEAT_STORE_CATEGORY_ID:
            newlist = sorted(resData, key=lambda k: k['availableQuantity'], reverse=True)
        else:
            newlist = sorted(resData, key=lambda k: k['isDcAvailable'], reverse=False)
        try:
            if "value" in res["hits"]["total"]:
                pen_count = res["hits"]["total"]['value']
            else:
                pen_count = res["hits"]["total"]
        except:
            pen_count = res["hits"]["total"]
        serarchResults_products = {
            "products": newlist,
            "penCount": pen_count,
            "offerBanner": [],
        }
        return serarchResults_products
    else:
        serarchResults_products = {
            "products": [],
            "penCount": 0,
            "offerBanner": [],
        }
        return serarchResults_products