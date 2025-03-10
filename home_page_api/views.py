# Create your views here.
from re import T
from cassandra import auth
import redis
from collections import OrderedDict

from bson import json_util
from home_page_api.utils import (
    UtilsObj,
    CLUBMART_MANUAL_PARAM_REQ,
    CLUBMART_REQ_BODY,
    CLUBMART_STORES_REQ_BODY,
    COMMON_RESPONSE,
)
from threading import Thread
from search.views import HyperLocalStores, search_read_stores, search_read_new
from validations.calculate_avg_rating import product_avg_rating
from validations.combo_special_validation import combo_special_type_validation
from validations.product_validation import product_modification
from validations.get_stores_zone_wise import store_validation_function
from search_api.settings import (
    RJ_DEALS,
    db,
    es,
    CASSANDRA_KEYSPACE,
    CENTRAL_PRODUCT_INDEX,
    CENTRAL_PRODUCT_DOC_TYPE,
    CENTRAL_PRODUCT_VARIENT_INDEX,
    CHILD_PRODUCT_INDEX,
    CHILD_PRODUCT_DOC_TYPE,
    STORE_PRODUCT_INDEX,
    STORE_PRODUCT_DOC_TYPE,
    CENRAL_STORE_NAME,
    PYTHON_PRODUCT_URL,
    OFFER_DOC_TYPE,
    OFFER_INDEX,
    KAFKA_URL,
    STORE_CREATE_TIME,
    TIME_ZONE,
    PHARMACY_STORE_CATEGORY_ID,
    ECOMMERCE_STORE_CATEGORY_ID,
    conv_fac,
    EARTH_REDIS,
    MEAT_STORE_CATEGORY_ID,
    session,
    session2,
    APP_NAME,
    currency_exchange_rate,
    SHOP_LOCAL_STORE_CATEGORY_ID,
    GROCERY_STORE_CATEGORY_ID,
    REDIS_IP,
    REDIS_PASSWORD,
    RJ_HOMEPAGE_DATA
)
from json import dumps
from pytz import timezone
import json
from rest_framework.views import APIView
from rest_framework.decorators import action
from kafka import KafkaProducer
from googletrans import Translator
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from django.http import JsonResponse
import time
import pandas as pd
import queue
import datetime
from search.views import home_units_data
from elastic_search_query_module.es_query_module import (
    product_es_search_data,
    product_es_aggrigate_data,
    child_product_es_aggrigate_data,
    child_product_es_search_data,
)
from mongo_query_module.query_module import (
    store_find,
    category_find_one,
    category_find,
    category_find_count,
    symptom_find_count,
    symptom_find,
    banner_find,
    product_find_count,
    offer_find_one,
    home_page_find_one,
    zone_find,
)
from validations.product_best_offer_redis import (
    product_best_offer_data,
    product_get_best_offer_data,
)
from validations.supplier_validation import best_supplier_function
from validations.store_validate_data import store_function, store_function_zonewise
from validations.product_unit_validation import validate_units_data
from validations.product_data_validation import get_linked_unit_attribute
from validations.store_category_validation import validate_store_category
from validations.meat_availbility_validation import meat_availability_check
from validations.product_city_pricing import cal_product_city_pricing
from bson.objectid import ObjectId
from validations.driver_roaster import (
    next_availbale_driver_roaster,
    next_availbale_driver_shift_in_stock,
    next_availbale_driver_shift_out_stock,
    next_availbale_driver_out_stock_shift,
)
from validations.time_zone_validation import time_zone_converter
from validations.product_validation import (
    get_avaialable_quantity,
    best_offer_function_validate,
    best_supplier_function_cust,
    product_type_validation,
    cal_star_rating_product,
    linked_unit_attribute,
    linked_variant_data,
)
from search.db_helper import DbHelper
import os
import sys
import asyncio

from rejson import Client, Path
DbHelper = DbHelper()

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
# =====================import all the files which need to call from api============================

r = redis.StrictRedis(host=REDIS_IP, password=REDIS_PASSWORD, port=6379, db=13)
redis_mega_menu = redis.StrictRedis(host=REDIS_IP, password=REDIS_PASSWORD, port=6379, db=12)
# approximate radius of earth in km
R = float(EARTH_REDIS)
conv_fac = float(conv_fac)

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_URL], value_serializer=lambda x: dumps(x).encode("utf-8")
    )
except:
    pass

translator = Translator()
res1 = queue.Queue()
res2 = queue.Queue()
res3 = queue.Queue()
res4 = queue.Queue()
res5 = queue.Queue()
res6 = queue.Queue()
res7 = queue.Queue()
res8 = queue.Queue()
res9 = queue.Queue()
# ===================================for the new home page===============================================================
res10 = queue.Queue()
res11 = queue.Queue()
res12 = queue.Queue()
res13 = queue.Queue()
res14 = queue.Queue()
res15 = queue.Queue()
res16 = queue.Queue()
res17 = queue.Queue()
res18 = queue.Queue()
res22 = queue.Queue()  # sysmptoms
res29 = queue.Queue()  # home page seo
res30 = queue.Queue()  # home page store details
# ===========================================new pdp page================================================================
res19 = queue.Queue()
res20 = queue.Queue()
res21 = queue.Queue()

# =======================================for food stores================================================
res23 = queue.Queue()
res24 = queue.Queue()

# =================================================disptcher======================================================
res25 = queue.Queue()
res26 = queue.Queue()
res27 = queue.Queue()
res28 = queue.Queue()

central_store = CENRAL_STORE_NAME
central_zero_store_creation_ts = int(STORE_CREATE_TIME)

currentDate = datetime.datetime.now()

index_central_product = CENTRAL_PRODUCT_INDEX
doc_central_product = CENTRAL_PRODUCT_DOC_TYPE

index_central_varient_product = CENTRAL_PRODUCT_VARIENT_INDEX

index_products = CHILD_PRODUCT_INDEX
doc_type_products = CHILD_PRODUCT_DOC_TYPE

index_store = STORE_PRODUCT_INDEX
doc_type_store = STORE_PRODUCT_DOC_TYPE

index_offers = OFFER_INDEX
doc_type_offers = OFFER_DOC_TYPE

SERVER = PYTHON_PRODUCT_URL
try:
    session.set_keyspace(CASSANDRA_KEYSPACE)
except:
    pass
try:
    session2.set_keyspace("notification")
except:
    pass
timezonename = TIME_ZONE


"""
    Function for the get the next available time for the products
"""


def next_availbale_time(product_id):
    child_product = db.childProducts.find_one(
        {"_id": ObjectId(product_id)}, {"currencySymbol": 1, "currency": 1, "nextAvailableTime": 1}
    )
    if child_product is not None:
        currency_symbol = child_product["currencySymbol"]
        currency = child_product["currency"]
        # ==========================for the available product===========================
        if "nextAvailableTime" in child_product:
            if child_product["nextAvailableTime"] != "":
                product_status = True
                next_available_time = child_product["nextAvailableTime"]
                next_open_time = int(next_available_time)
                local_time = datetime.datetime.fromtimestamp(next_open_time)
                next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                next_day_midnight = next_day.replace(hour=23, minute=59, second=59)
                next_day_midnight_timestamp = int(next_day_midnight.timestamp())
                if next_day_midnight_timestamp < next_open_time:
                    open_time = local_time.strftime("%b %d %Y, %I:%M %p")
                    product_tag = "Available On " + open_time
                else:
                    open_time = local_time.strftime("%I:%M %p")
                    product_tag = "Available On Tomorrow At " + open_time
            else:
                product_status = False
                product_tag = ""
        else:
            product_status = False
            product_tag = ""
    else:
        currency_symbol = (
            hits["_source"]["currencySymbol"] if "currencySymbol" in hits["_source"] else "₹"
        )
        currency = hits["_source"]["currency"] if "currency" in hits["_source"] else "INR"
        product_status = False
        product_tag = ""
    return currency_symbol, currency, product_status, product_tag


"""
    type 0  for website
    type 1 for app
    deviceType:
                0 for website
                1 for IOS
                2 for Android
    LoginType:
                0 for retailer
                1 for distributor
    Type in Response:
                1 for banner
                2 for category list
                3 for recently bought
                4 for hot deals
                5 for popular item
                6 for symptoms
                7 for products
                8 for recent view
                9 for brands
                10 for store-list

"""


class HomePage(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the data on home page in app and website",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="loginType",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="getting the data for retailer or distributor. "
                "1 for retailer \n"
                "2 for distributor",
                default="1",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default=MEAT_STORE_CATEGORY_ID,
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5df8b6ea8798dc19d926bd28",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular zone",
            ),
            openapi.Parameter(
                name="cityId",
                default="5df7b7218798dc2c1114e6bf",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular city",
            ),
            openapi.Parameter(
                name="storeId",
                default="5f06d68bb18ddc49df328de5",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular store",
            ),
            openapi.Parameter(
                name="lat",
                required=True,
                default="13.05176",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="latitude of the user's location",
            ),
            openapi.Parameter(
                name="timezone",
                default="Asia/Calcutta",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="offset of the timezone",
            ),
            openapi.Parameter(
                name="long",
                default="77.580448",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="longtitue of the user's location",
            ),
            openapi.Parameter(
                name="integrationType",
                default="0",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for filter out the products base on product setup configuration, value should be"
                "0 for All products, "
                "1 for Only Magento Products, "
                "2 for Only Shopify Products, "
                "3 for Only Roadyo or shopar products",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        global pre_order
        try:
            start_time_date = time.time()
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            last_json_response = []
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            zone_id = str(request.META["HTTP_ZONEID"]) if "HTTP_ZONEID" in request.META else ""
            store_id = str(request.META["HTTP_STOREID"]) if "HTTP_STOREID" in request.META else ""
            city_id = (
                str(request.META["HTTP_CITYID"])
                if "HTTP_CITYID" in request.META
                else "5df7b7218798dc2c1114e6bf"
            )
            token = request.META["HTTP_AUTHORIZATION"]
            user_latitude = float(request.GET.get("lat", 12.9692))
            user_longtitude = float(request.GET.get("long", 77.7499))
            timezone_data = request.GET.get("timezone", "")
            timezone_data = timezone_data.replace("%2F", "/")
            # timezone_data = float(request.GET.get("timezone", "330"))
            integration_type = float(request.GET.get("integrationType", 0))
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            try:
                user_id = json.loads(token)["userId"]
            except:
                user_id = "5df87244cb12e160bf694309"

            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)

            if zone_id != "":
                city_details = db.zones.find_one({"_id": ObjectId(zone_id)}, {"city_ID": 1})
                categoty_details = db.cities.find_one(
                    {
                        "storeCategory.storeCategoryId": store_category_id,
                        "_id": ObjectId(city_details["city_ID"]),
                    },
                    {"storeCategory": 1},
                )
            elif store_id != "" and store_id != "0":
                city_details = db.stores.find_one({"_id": ObjectId(store_id)}, {"cityId": 1})
                categoty_details = db.cities.find_one(
                    {
                        "storeCategory.storeCategoryId": store_category_id,
                        "_id": ObjectId(city_details["cityId"]),
                    },
                    {"storeCategory": 1},
                )
            else:
                categoty_details = db.cities.find_one(
                    {"storeCategory.storeCategoryId": store_category_id}, {"storeCategory": 1}
                )

            if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                is_ecommerce = True
                hyperlocal = False
                storelisting = False
                remove_central = False
            else:
                if categoty_details is not None:
                    if "storeCategory" in categoty_details:
                        for cat in categoty_details["storeCategory"]:
                            if cat["storeCategoryId"] == store_category_id:
                                if cat["hyperlocal"] == True and cat["storeListing"] == 1:
                                    remove_central = True
                                    hyperlocal = True
                                    storelisting = True
                                    is_ecommerce = False
                                elif cat["hyperlocal"] == True and cat["storeListing"] == 0:
                                    hyperlocal = True
                                    storelisting = False
                                    remove_central = True
                                    is_ecommerce = False
                                else:
                                    remove_central = False
                                    hyperlocal = False
                                    storelisting = False
                                    is_ecommerce = True
                            else:
                                pass
                    else:
                        remove_central = False
                        hyperlocal = False
                        storelisting = False
                        is_ecommerce = True
                else:
                    remove_central = False
                    hyperlocal = False
                    storelisting = False
                    is_ecommerce = True

            if hyperlocal == True and storelisting == True:
                store_id = store_id
                zone_id = ""
            elif hyperlocal == True and storelisting == False:
                zone_id = zone_id
                store_id = ""
            elif hyperlocal == False and storelisting == False:
                zone_id = ""
                store_id = ""

            # get the data from redis
            print("store_category_id", store_category_id)
            home_page_data = []
            try:
                if hyperlocal == True and storelisting == True:
                    home_page_data = RJ_HOMEPAGE_DATA.jsonget("homepage_" + str(store_category_id) + "_" + str(store_id))
                elif hyperlocal == True and storelisting == False:
                    home_page_data = RJ_HOMEPAGE_DATA.jsonget("homepage_" + str(store_category_id) + "_" + str(zone_id))
                elif hyperlocal == False and storelisting == False:
                    home_page_data = RJ_HOMEPAGE_DATA.jsonget("homepage_" + str(store_category_id) + "_ecommerce")
                else:
                    pass
            except: pass
            # ==================================store details page======================================
            store_data_json = []
            store_details = []
            if hyperlocal == True and storelisting == True:
                store_details.append(str(store_id))
                store_data_json.append(ObjectId(store_id))
            elif hyperlocal == True and storelisting == False:
                store_data = db.stores.find(
                    {
                        "categoryId": str(store_category_id),
                        "serviceZones.zoneId": zone_id,
                        "storeFrontTypeId": {"$ne": 5},
                        "status": 1,
                    }
                )
                for store in store_data:
                    store_details.append(str(store["_id"]))
                    store_data_json.append(ObjectId(store["_id"]))
            else:
                pass
            if hyperlocal == True and storelisting == False and len(store_details) == 0:
                store_details.append("0")
            home_page_data = []
            # if home_page_data is None:
            #     home_page_data = []
            # home_page_data = []
            if len(home_page_data) > 0:
                try:
                    if zone_id != "":
                        driver_roaster = next_availbale_driver_roaster(zone_id)
                        next_delivery_slot = driver_roaster["text"]
                        next_availbale_driver_time = driver_roaster["productText"]
                    else:
                        next_delivery_slot = ""
                        next_availbale_driver_time = ""
                except:
                    next_delivery_slot = ""
                    next_availbale_driver_time = ""
                is_temp_close = False
                store_tag = ""
                store_is_open = False
                if hyperlocal == True and storelisting == False:
                    if zone_id != "":
                        dc_details = db.zones.find_one({"_id": ObjectId(zone_id)}, {"DCStoreId": 1})
                    else:
                        dc_details = None
                    if dc_details is not None:
                        if "DCStoreId" in dc_details:
                            if dc_details["DCStoreId"] == "":
                                dc_store_details = None
                            else:
                                dc_store_details = db.stores.find_one(
                                    {"_id": ObjectId(dc_details["DCStoreId"])}
                                )
                            if dc_store_details is not None:
                                # =====================================about store tags=================================
                                if "storeIsOpen" in dc_store_details:
                                    store_is_open = dc_store_details["storeIsOpen"]
                                else:
                                    store_is_open = False

                                if "nextCloseTime" in dc_store_details:
                                    next_close_time = dc_store_details["nextCloseTime"]
                                else:
                                    next_close_time = ""

                                if "nextOpenTime" in dc_store_details:
                                    next_open_time = dc_store_details["nextOpenTime"]
                                else:
                                    next_open_time = ""

                                try:
                                    if "timeZoneWorkingHour" in seller["_source"]:
                                        timeZoneWorkingHour = seller["_source"][
                                            "timeZoneWorkingHour"
                                        ]
                                    else:
                                        timeZoneWorkingHour = ""
                                except:
                                    timeZoneWorkingHour = ""

                                is_delivery = True
                                if next_close_time == "" and next_open_time == "":
                                    is_temp_close = True
                                    store_tag = "Store is temporarily closed"
                                elif next_open_time != "" and store_is_open == False:
                                    is_temp_close = False
                                    # next_open_time = int(next_open_time + timezone_data * 60)
                                    next_open_time = time_zone_converter(
                                        timezone_data, next_open_time, timeZoneWorkingHour
                                    )
                                    local_time = datetime.datetime.fromtimestamp(next_open_time)
                                    next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                                    next_day_midnight = next_day.replace(hour=0, minute=0, second=0)
                                    next_day_midnight_timestamp = int(next_day_midnight.timestamp())
                                    if next_day_midnight_timestamp > next_open_time:
                                        open_time = local_time.strftime("%I:%M %p")
                                        store_tag = "Store is closed, opens next at " + open_time
                                    else:
                                        open_time = local_time.strftime("%I:%M %p")
                                        store_tag = (
                                            "Store is closed, next opens tomorrow At " + open_time
                                        )
                                else:
                                    is_temp_close = False
                                    store_tag = ""
                else:
                    pass
                # ============================find all dc in zone===========================================
                dc_seller_list = []
                if zone_id != "":
                    dc_list_details = db.stores.find(
                        {"serviceZones.zoneId": zone_id, "storeFrontTypeId": 5, "status": 1}
                    )
                    for dc_seller in dc_list_details:
                        dc_seller_list.append(str(dc_seller["_id"]))
                else:
                    pass

                notification_count_query = {
                    "app_name": APP_NAME,
                    "userid": user_id,
                    "isSeen": False,
                }
                if store_category_id != "":
                    notification_count_query["store_category_id"] = store_category_id
                notification_count = db.notificationLogs.find(notification_count_query).count()

                # ===============================for home page seo=================================================
                response = {}
                home_page_seo = db.homepage.find_one({})
                if home_page_seo is not None:
                    if "homepage" in home_page_seo:
                        response = {
                            "facebookIcon": home_page_seo["homepage"]["facebookIcon"]
                            if "facebookIcon" in home_page_seo["homepage"]
                            else "",
                            "facebookLink": home_page_seo["homepage"]["facebookLink"]
                            if "facebookLink" in home_page_seo["homepage"]
                            else "",
                            "twitterIcon": home_page_seo["homepage"]["twitterIcon"]
                            if "twitterIcon" in home_page_seo["homepage"]
                            else "",
                            "twitterLink": home_page_seo["homepage"]["twitterLink"]
                            if "twitterLink" in home_page_seo["homepage"]
                            else "",
                            "instagramIcon": home_page_seo["homepage"]["instagramIcon"]
                            if "instagramIcon" in home_page_seo["homepage"]
                            else "",
                            "instagramLink": home_page_seo["homepage"]["instagramLink"]
                            if "instagramLink" in home_page_seo["homepage"]
                            else "",
                            "youtubeIcon": home_page_seo["homepage"]["youtubeIcon"]
                            if "youtubeIcon" in home_page_seo["homepage"]
                            else "",
                            "youtubeLink": home_page_seo["homepage"]["youtubeLink"]
                            if "youtubeLink" in home_page_seo["homepage"]
                            else "",
                            "linkedInIcon": home_page_seo["homepage"]["linkedInIcon"]
                            if "linkedInIcon" in home_page_seo["homepage"]
                            else "",
                            "linkedInLink": home_page_seo["homepage"]["linkedInLink"]
                            if "linkedInLink" in home_page_seo["homepage"]
                            else "",
                            "copyRight": home_page_seo["homepage"]["copyRight"][language]
                            if language in home_page_seo["homepage"]["copyRight"]
                            else home_page_seo["homepage"]["copyRight"]["en"],
                        }
                home_page_seo_response = {"data": response}

                if hyperlocal == True and storelisting == True:
                    try:
                        if store_id != "":
                            json_response = store_function(
                                language,
                                store_id,
                                user_id,
                                user_latitude,
                                user_longtitude,
                                timezone_data,
                            )
                            store_response_data = json_response["data"]
                        else:
                            store_response_data = []
                    except Exception as ex:
                        print(
                            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                            type(ex).__name__,
                            ex,
                        )
                        store_response_data = []
                    home_page_data["storeData"] = store_response_data
                else:
                    pass

                last_json_response = home_page_data["list"]
                try:
                    store_agg_query = []
                    if len(store_details) > 0:
                        store_agg_query.append({"$match": {"storeId": {"$in": store_details}}})
                    store_agg_query.append({"$match": {"products.status.status": 7}})
                    store_agg_query.append({"$match": {"createdBy.userId": user_id}})
                    store_agg_query.append({"$match": {"storeCategoryId": store_category_id}})
                    store_agg_query.append({"$project": {"products": 1}})
                    store_agg_query.append({"$unwind": "$products"})
                    store_agg_query.append({"$sort": {"createdTimeStamp": -1}})
                    store_agg_query.append({"$skip": 0})
                    store_agg_query.append({"$limit": 5})
                    store_agg_query.append(
                        {
                            "$group": {
                                "products": {"$push": "$products.centralProductId"},
                                "_id": None,
                            }
                        }
                    )
                    store_order_data = db.storeOrder.aggregate(store_agg_query)
                    store_agg_count_query = []
                    if len(store_details) > 0:
                        store_agg_count_query.append(
                            {"$match": {"storeId": {"$in": store_details}}}
                        )
                    store_agg_count_query.append({"$match": {"products.status.status": 7}})
                    store_agg_count_query.append({"$match": {"createdBy.userId": user_id}})
                    store_agg_count_query.append({"$match": {"storeCategoryId": store_category_id}})
                    store_agg_count_query.append({"$project": {"products": 1}})
                    store_agg_count_query.append({"$unwind": "$products"})
                    store_agg_count_query.append({"$count": "total_products"})
                    total_product_data = db.storeOrder.aggregate(store_agg_count_query)
                    total_product_count = 0
                    for count in total_product_data:
                        total_product_count = count["total_products"]
                    recently_bought = []
                    must_not = []
                    should_query = []
                    for product in store_order_data:
                        must_query = [
                            {"match": {"status": 1}},
                            {"match": {"storeCategoryId": str(store_category_id)}},
                            {"terms": {"parentProductId": product["products"]}},
                        ]
                        if len(store_details) > 0:
                            must_query.append({"terms": {"storeId": store_details}})
                        sort_query = [
                            {"isInStock": {"order": "desc"}},
                            {"units.discountPrice": {"order": "asc"}},
                        ]

                        must_query.append({"match": {"status": 1}})

                        if int(integration_type) == 0:
                            pass
                        elif int(integration_type) == 1:
                            must_not.append({"match": {"magentoId": -1}})
                            must_query.append({"exists": {"field": "magentoId"}})
                        elif int(integration_type) == 2:
                            must_not.append({"term": {"shopify_variant_id.keyword": {"value": ""}}})
                            must_query.append({"exists": {"field": "shopify_variant_id"}})
                        elif int(integration_type) == 3:
                            should_query.append({"term": {"magentoId.keyword": ""}})
                            should_query.append(
                                {"bool": {"must_not": [{"exists": {"field": "magentoId"}}]}}
                            )
                        must_not.append({"match": {"storeId": "0"}})
                        aggr_json = {
                            "group_by_sub_category": {
                                "terms": {
                                    "field": "parentProductId.keyword",
                                    "order": {"avg_score": "desc"},
                                    "size": 20,
                                },
                                "aggs": {
                                    "avg_score": {"max": {"script": "doc.isInStock"}},
                                    "top_sales_hits": {
                                        "top_hits": {
                                            "sort": sort_query,
                                            "_source": {
                                                "includes": [
                                                    "_id",
                                                    "_score",
                                                    "pName",
                                                    "secondCategoryName",
                                                    "prescriptionRequired",
                                                    "needsIdProof",
                                                    "saleOnline",
                                                    "uploadProductDetails",
                                                    "storeId",
                                                    "parentProductId",
                                                    "currencySymbol",
                                                    "currency",
                                                    "pPName",
                                                    "tax",
                                                    "offer",
                                                    "brandTitle",
                                                    "productType",
                                                    "categoryList",
                                                    "images",
                                                    "avgRating",
                                                    "units",
                                                    "storeCategoryId",
                                                    "manufactureName",
                                                    "maxQuantity",
                                                ]
                                            },
                                            "size": 1,
                                        }
                                    },
                                },
                            }
                        }
                        if int(integration_type) != 3:
                            search_item_query = {
                                "query": {"bool": {"must": must_query, "must_not": must_not}},
                                "track_total_hits": True,
                                "sort": sort_query,
                                "aggs": aggr_json,
                            }
                        else:
                            search_item_query = {
                                "query": {"bool": {"must": must_query, "must_not": must_not}},
                                "track_total_hits": True,
                                "sort": sort_query,
                                "aggs": aggr_json,
                            }
                        res = es.search(index=index_products, body=search_item_query)
                        try:
                            if "value" in res["hits"]["total"]:
                                if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                                    response = {
                                        "id": "",
                                        "catName": "Recently Bought",
                                        "imageUrl": "",
                                        "bannerImageUrl": "",
                                        "websiteImageUrl": "",
                                        "websiteBannerImageUrl": "",
                                        "offers": [],
                                        "penCount": 0,
                                        "categoryData": [],
                                        "type": 3,
                                        "seqId": 3,
                                    }
                                    if "Recently Bought" not in str(last_json_response):
                                        last_json_response.append(response)
                            else:
                                if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                                    response = {
                                        "id": "",
                                        "catName": "Recently Bought",
                                        "imageUrl": "",
                                        "bannerImageUrl": "",
                                        "websiteImageUrl": "",
                                        "websiteBannerImageUrl": "",
                                        "offers": [],
                                        "penCount": 0,
                                        "categoryData": [],
                                        "type": 3,
                                        "seqId": 3,
                                    }
                                    if "Recently Bought" not in str(last_json_response):
                                        last_json_response.append(response)
                        except Exception as ex:
                            print(
                                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                                type(ex).__name__,
                                ex,
                            )
                            if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                                response = {
                                    "id": "",
                                    "catName": "Recently Bought",
                                    "imageUrl": "",
                                    "bannerImageUrl": "",
                                    "websiteImageUrl": "",
                                    "websiteBannerImageUrl": "",
                                    "offers": [],
                                    "penCount": 0,
                                    "categoryData": [],
                                    "type": 3,
                                    "seqId": 3,
                                }
                                if "Recently Bought" not in str(last_json_response):
                                    last_json_response.append(response)

                        main_sellers = []
                        if zone_id != "":
                            store_details_data = db.stores.find(
                                {
                                    "serviceZones.zoneId": zone_id,
                                    "storeFrontTypeId": {"$ne": 5},
                                    "status": 1,
                                }
                            )
                            for seller in store_details_data:
                                main_sellers.append(str(seller["_id"]))

                        if zone_id != "":
                            driver_roaster = next_availbale_driver_roaster(zone_id)
                        else:
                            driver_roaster = {}

                        recently_bought = product_modification(
                            res["aggregations"]["group_by_sub_category"]["buckets"],
                            language,
                            "",
                            zone_id,
                            "",
                            store_category_id,
                            0,
                            True,
                            main_sellers,
                            driver_roaster,
                            "",
                            user_id
                        )
                        if len(recently_bought) == 0:
                            response = {
                                "id": "",
                                "catName": "Recently Bought",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": [],
                                "type": 3,
                                "seqId": 3,
                            }
                            if "Recently Bought" not in str(last_json_response):
                                last_json_response.append(response)
                        else:
                            if len(recently_bought) > 0:
                                dataframe = pd.DataFrame(recently_bought)
                                # dataframe = dataframe.drop_duplicates(subset='parentProductId', keep="last")
                                details = dataframe.to_json(orient="records")
                                data = json.loads(details)
                                recent_data = validate_units_data(data, False)
                                newlist = sorted(
                                    recent_data, key=lambda k: k["createdTimestamp"], reverse=True
                                )
                            else:
                                newlist = []
                            response = {
                                "id": "",
                                "catName": "Recently Bought",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": total_product_count,
                                "categoryData": newlist,
                                "type": 3,
                                "seqId": 3,
                            }
                            last_json_response.append(response)
                except Exception as ex:
                    print(
                        "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                        type(ex).__name__,
                        ex,
                    )
                    response = {
                        "id": "",
                        "catName": "Recently Bought",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": [],
                        "type": 3,
                        "seqId": 3,
                    }
                    if "Recently Bought" not in str(last_json_response):
                        last_json_response.append(response)
                newlist = sorted(last_json_response, key=lambda k: k["seqId"])
                last_response = {
                    "data": {
                        "notificationCount": notification_count,
                        "list": newlist,
                        "storeIsOpen": store_is_open,
                        "storeTag": store_tag,
                        "isTempClose": is_temp_close,
                        "totalCatCount": home_page_data["totalCatCount"],
                        "seoData": home_page_seo_response["data"],
                        "storeData": home_page_data["storeData"],
                        "nextDeliverySlot": next_delivery_slot,
                    }
                }
                print("from redis time", time.time() - start_time_date)
                return JsonResponse(last_response, safe=False, status=200)
            else:
                try:
                    if zone_id != "":
                        driver_roaster = next_availbale_driver_roaster(zone_id)
                        next_delivery_slot = driver_roaster["text"]
                        next_availbale_driver_time = driver_roaster["productText"]
                    else:
                        next_delivery_slot = ""
                        next_availbale_driver_time = ""
                except:
                    next_delivery_slot = ""
                    next_availbale_driver_time = ""

                is_temp_close = False
                store_tag = ""
                store_is_open = False
                if hyperlocal == True and storelisting == False:
                    if zone_id != "":
                        dc_details = db.zones.find_one({"_id": ObjectId(zone_id)}, {"DCStoreId": 1})
                    else:
                        dc_details = None
                    if dc_details is not None:
                        if "DCStoreId" in dc_details:
                            if dc_details["DCStoreId"] == "":
                                dc_store_details = None
                            else:
                                dc_store_details = db.stores.find_one(
                                    {"_id": ObjectId(dc_details["DCStoreId"])}
                                )
                            if dc_store_details is not None:
                                # =====================================about store tags=================================
                                if "storeIsOpen" in dc_store_details:
                                    store_is_open = dc_store_details["storeIsOpen"]
                                else:
                                    store_is_open = False

                                if "nextCloseTime" in dc_store_details:
                                    next_close_time = dc_store_details["nextCloseTime"]
                                else:
                                    next_close_time = ""

                                if "nextOpenTime" in dc_store_details:
                                    next_open_time = dc_store_details["nextOpenTime"]
                                else:
                                    next_open_time = ""

                                try:
                                    if "timeZoneWorkingHour" in seller["_source"]:
                                        timeZoneWorkingHour = seller["_source"][
                                            "timeZoneWorkingHour"
                                        ]
                                    else:
                                        timeZoneWorkingHour = ""
                                except:
                                    timeZoneWorkingHour = ""

                                is_delivery = True
                                if next_close_time == "" and next_open_time == "":
                                    is_temp_close = True
                                    store_tag = "Store is temporarily closed"
                                elif next_open_time != "" and store_is_open == False:
                                    is_temp_close = False
                                    # next_open_time = int(next_open_time + timezone_data * 60)
                                    next_open_time = time_zone_converter(
                                        timezone_data, next_open_time, timeZoneWorkingHour
                                    )
                                    local_time = datetime.datetime.fromtimestamp(next_open_time)
                                    next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                                    next_day_midnight = next_day.replace(hour=0, minute=0, second=0)
                                    next_day_midnight_timestamp = int(next_day_midnight.timestamp())
                                    if next_day_midnight_timestamp > next_open_time:
                                        open_time = local_time.strftime("%I:%M %p")
                                        store_tag = "Store is closed, opens next at " + open_time
                                    else:
                                        open_time = local_time.strftime("%I:%M %p")
                                        store_tag = (
                                            "Store is closed, next opens tomorrow At " + open_time
                                        )
                                else:
                                    is_temp_close = False
                                    store_tag = ""
                else:
                    pass
                # ============================find all dc in zone===========================================
                dc_seller_list = []
                if zone_id != "":
                    dc_list_details = db.stores.find(
                        {"serviceZones.zoneId": zone_id, "storeFrontTypeId": 5, "status": 1}
                    )
                    for dc_seller in dc_list_details:
                        dc_seller_list.append(str(dc_seller["_id"]))
                else:
                    pass

                try:
                    store_agg_query = []
                    if len(store_details) > 0:
                        store_agg_query.append({"$match": {"storeId": {"$in": store_details}}})
                    store_agg_query.append({"$match": {"products.status.status": 7}})
                    store_agg_query.append({"$match": {"createdBy.userId": user_id}})
                    store_agg_query.append({"$match": {"storeCategoryId": store_category_id}})
                    store_agg_query.append({"$project": {"products": 1}})
                    store_agg_query.append({"$unwind": "$products"})
                    store_agg_query.append({"$sort": {"createdTimeStamp": -1}})
                    store_agg_query.append({"$skip": 0})
                    store_agg_query.append({"$limit": 5})
                    store_agg_query.append(
                        {
                            "$group": {
                                "products": {"$push": "$products.centralProductId"},
                                "_id": None,
                            }
                        }
                    )
                    store_order_data = db.storeOrder.aggregate(store_agg_query)
                    store_agg_count_query = []
                    if len(store_details) > 0:
                        store_agg_count_query.append(
                            {"$match": {"storeId": {"$in": store_details}}}
                        )
                    store_agg_count_query.append({"$match": {"products.status.status": 7}})
                    store_agg_count_query.append({"$match": {"createdBy.userId": user_id}})
                    store_agg_count_query.append({"$match": {"storeCategoryId": store_category_id}})
                    store_agg_count_query.append({"$project": {"products": 1}})
                    store_agg_count_query.append({"$unwind": "$products"})
                    store_agg_count_query.append({"$count": "total_products"})
                    total_product_data = db.storeOrder.aggregate(store_agg_count_query)
                    total_product_count = 0
                    for count in total_product_data:
                        total_product_count = count["total_products"]
                    recently_bought = []
                    must_not = []
                    should_query = []
                    for product in store_order_data:
                        must_query = [
                            {"match": {"status": 1}},
                            {"match": {"storeCategoryId": str(store_category_id)}},
                            {"terms": {"parentProductId": product["products"]}},
                        ]
                        if len(store_details) > 0:
                            must_query.append({"terms": {"storeId": store_details}})
                        sort_query = [
                            {"isInStock": {"order": "desc"}},
                            {"units.discountPrice": {"order": "asc"}},
                        ]

                        must_query.append({"match": {"status": 1}})

                        if int(integration_type) == 0:
                            pass
                        elif int(integration_type) == 1:
                            must_not.append({"match": {"magentoId": -1}})
                            must_query.append({"exists": {"field": "magentoId"}})
                        elif int(integration_type) == 2:
                            must_not.append({"term": {"shopify_variant_id.keyword": {"value": ""}}})
                            must_query.append({"exists": {"field": "shopify_variant_id"}})
                        elif int(integration_type) == 3:
                            should_query.append({"term": {"magentoId.keyword": ""}})
                            should_query.append(
                                {"bool": {"must_not": [{"exists": {"field": "magentoId"}}]}}
                            )
                        must_not.append({"match": {"storeId": "0"}})
                        aggr_json = {
                            "group_by_sub_category": {
                                "terms": {
                                    "field": "parentProductId.keyword",
                                    "order": {"avg_score": "desc"},
                                    "size": 20,
                                },
                                "aggs": {
                                    "avg_score": {"max": {"script": "doc.isInStock"}},
                                    "top_sales_hits": {
                                        "top_hits": {
                                            "sort": sort_query,
                                            "_source": {
                                                "includes": [
                                                    "_id",
                                                    "_score",
                                                    "pName",
                                                    "prescriptionRequired",
                                                    "needsIdProof",
                                                    "saleOnline",
                                                    "uploadProductDetails",
                                                    "storeId",
                                                    "parentProductId",
                                                    "currencySymbol",
                                                    "currency",
                                                    "pPName",
                                                    "tax",
                                                    "offer",
                                                    "brandTitle",
                                                    "categoryList",
                                                    "images",
                                                    "avgRating",
                                                    "units",
                                                    "storeCategoryId",
                                                    "manufactureName",
                                                    "maxQuantity",
                                                ]
                                            },
                                            "size": 1,
                                        }
                                    },
                                },
                            }
                        }
                        if int(integration_type) != 3:
                            search_item_query = {
                                "query": {"bool": {"must": must_query, "must_not": must_not}},
                                "track_total_hits": True,
                                "sort": sort_query,
                                "aggs": aggr_json,
                            }
                        else:
                            search_item_query = {
                                "query": {"bool": {"must": must_query, "must_not": must_not}},
                                "track_total_hits": True,
                                "sort": sort_query,
                                "aggs": aggr_json,
                            }
                        res = es.search(index=index_products, body=search_item_query)
                        try:
                            if "value" in res["hits"]["total"]:
                                if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                                    response = {
                                        "id": "",
                                        "catName": "Recently Bought",
                                        "imageUrl": "",
                                        "bannerImageUrl": "",
                                        "websiteImageUrl": "",
                                        "websiteBannerImageUrl": "",
                                        "offers": [],
                                        "penCount": 0,
                                        "categoryData": [],
                                        "type": 3,
                                        "seqId": 3,
                                    }
                                    if "Recently Bought" not in str(last_json_response):
                                        last_json_response.append(response)
                            else:
                                if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                                    response = {
                                        "id": "",
                                        "catName": "Recently Bought",
                                        "imageUrl": "",
                                        "bannerImageUrl": "",
                                        "websiteImageUrl": "",
                                        "websiteBannerImageUrl": "",
                                        "offers": [],
                                        "penCount": 0,
                                        "categoryData": [],
                                        "type": 3,
                                        "seqId": 3,
                                    }
                                    if "Recently Bought" not in str(last_json_response):
                                        last_json_response.append(response)
                        except Exception as ex:
                            print(
                                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                                type(ex).__name__,
                                ex,
                            )
                            if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                                response = {
                                    "id": "",
                                    "catName": "Recently Bought",
                                    "imageUrl": "",
                                    "bannerImageUrl": "",
                                    "websiteImageUrl": "",
                                    "websiteBannerImageUrl": "",
                                    "offers": [],
                                    "penCount": 0,
                                    "categoryData": [],
                                    "type": 3,
                                    "seqId": 3,
                                }
                                if "Recently Bought" not in str(last_json_response):
                                    last_json_response.append(response)
                        main_sellers = []
                        if zone_id != "":
                            store_details_data = db.stores.find(
                                {
                                    "serviceZones.zoneId": zone_id,
                                    "storeFrontTypeId": {"$ne": 5},
                                    "status": 1,
                                }
                            )
                            for seller in store_details_data:
                                main_sellers.append(str(seller["_id"]))

                        if zone_id != "":
                            driver_roaster = next_availbale_driver_roaster(zone_id)
                        else:
                            driver_roaster = {}

                        recently_bought = product_modification(
                            res["aggregations"]["group_by_sub_category"]["buckets"],
                            language,
                            "",
                            zone_id,
                            "",
                            store_category_id,
                            0,
                            True,
                            main_sellers,
                            driver_roaster,
                            "",
                            user_id
                        )
                        if len(recently_bought) == 0:
                            response = {
                                "id": "",
                                "catName": "Recently Bought",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": [],
                                "type": 3,
                                "seqId": 3,
                            }
                            if "Recently Bought" not in str(last_json_response):
                                last_json_response.append(response)
                        else:
                            if len(recently_bought) > 0:
                                dataframe = pd.DataFrame(recently_bought)
                                # dataframe = dataframe.drop_duplicates(subset='parentProductId', keep="last")
                                details = dataframe.to_json(orient="records")
                                data = json.loads(details)
                                recent_data = validate_units_data(data, False)
                                newlist = sorted(
                                    recent_data, key=lambda k: k["createdTimestamp"], reverse=True
                                )
                            else:
                                newlist = []
                            response = {
                                "id": "",
                                "catName": "Recently Bought",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": total_product_count,
                                "categoryData": newlist,
                                "type": 3,
                                "seqId": 3,
                            }
                            last_json_response.append(response)
                except Exception as ex:
                    print(
                        "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                        type(ex).__name__,
                        ex,
                    )
                    response = {
                        "id": "",
                        "catName": "Recently Bought",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": [],
                        "type": 3,
                        "seqId": 3,
                    }
                    if "Recently Bought" not in str(last_json_response):
                        last_json_response.append(response)
                # ==============================================get product details===================================
                # or store_category_id == SHOP_LOCAL_STORE_CATEGORY_ID:
                allow_out_of_stock = False
                if store_category_id == MEAT_STORE_CATEGORY_ID:
                    language = language
                    store_data_details = store_details
                    store_data_details.append("0")

                    response_json = []
                    must_query = [
                        {"terms": {"status": [1, 2]}},
                        {"match": {"storeCategoryId": str(store_category_id)}},
                    ]
                    if len(store_data_details) > 0:
                        must_query.append({"terms": {"units.suppliers.id": store_data_details}})
                    else:
                        pass

                    if store_id != "":
                        must_query.append({"match": {"units.suppliers.id": str(store_id)}})
                    else:
                        pass

                    query = {
                        "query": {"bool": {"must": must_query}},
                        "aggs": {
                            "group_by_catName": {
                                "terms": {
                                    "field": "categoryList.parentCategory.categoryName.en.keyword"
                                },
                                "aggs": {
                                    "top_sub_category_hits": {
                                        "top_hits": {
                                            "_source": {
                                                "includes": [
                                                    "pPName",
                                                    "units",
                                                    "avgRating",
                                                    "allowOrderOutOfStock",
                                                    "offer",
                                                    "brandTitle",
                                                    "manufactureName",
                                                    "productSeo",
                                                    "images",
                                                    "avgRating",
                                                    "variantCount",
                                                ]
                                            },
                                            "size": 20,
                                        }
                                    }
                                },
                            }
                        },
                    }
                    categoty_data = []
                    res = es.search(index=CENTRAL_PRODUCT_INDEX, body=query)
                    if len(res["aggregations"]["group_by_catName"]["buckets"]) == 0:
                        response = {
                            "list": [],
                        }
                        for product in response["list"]:
                            last_json_response.append(product)
                    else:
                        pass
                    # =================get all the zone dc list=====================================================
                    store_details_data = db.stores.find(
                        {"serviceZones.zoneId": zone_id, "storeFrontTypeId": 5, "status": 1}
                    )
                    dc_seller_list = []
                    for dc_seller in store_details_data:
                        dc_seller_list.append(str(dc_seller["_id"]))

                    if "group_by_catName" in res["aggregations"]:
                        for i in res["aggregations"]["group_by_catName"]["buckets"]:
                            category_name = i["key"]
                            doc_count = i["doc_count"]
                            resData = []
                            for product in i["top_sub_category_hits"]["hits"]["hits"]:
                                try:
                                    is_dc_linked = False
                                    variant_data = []
                                    supplier_data = []
                                    dc_data = []
                                    hard_limit = 0
                                    if "units" in product["_source"]:
                                        for supplier in product["_source"]["units"][0]["suppliers"]:
                                            if supplier["id"] != "0":
                                                store_count = db.stores.find(
                                                    {"_id": ObjectId(supplier["id"]), "status": 1}
                                                ).count()
                                                if store_count > 0:
                                                    if supplier["id"] in dc_seller_list:
                                                        child_product_count = (
                                                            db.childProducts.find_one(
                                                                {
                                                                    "_id": ObjectId(
                                                                        supplier["productId"]
                                                                    ),
                                                                    "status": 1,
                                                                },
                                                                {"seller": 1},
                                                            )
                                                        )
                                                        if child_product_count is not None:
                                                            if "seller" in child_product_count:
                                                                if (
                                                                    len(
                                                                        child_product_count[
                                                                            "seller"
                                                                        ]
                                                                    )
                                                                    > 0
                                                                ):
                                                                    is_dc_linked = True
                                                                    dc_data.append(supplier)
                                                                else:
                                                                    pass
                                                            else:
                                                                pass
                                                        else:
                                                            pass
                                                    else:
                                                        store_count = db.stores.find(
                                                            {
                                                                "_id": ObjectId(supplier["id"]),
                                                                "storeFrontTypeId": {"$ne": 5},
                                                            }
                                                        ).count()
                                                        if store_count > 0:
                                                            child_product_count = (
                                                                db.childProducts.find(
                                                                    {
                                                                        "_id": ObjectId(
                                                                            supplier["productId"]
                                                                        )
                                                                    }
                                                                ).count()
                                                            )
                                                            if child_product_count > 0:
                                                                supplier_data.append(supplier)
                                                        else:
                                                            pass
                                                else:
                                                    pass
                                            else:
                                                pass
                                    else:
                                        pass
                                    # ==================================for meat need to give central product===============
                                    if (
                                        len(supplier_data) == 0
                                        and store_category_id != SHOP_LOCAL_STORE_CATEGORY_ID
                                    ):
                                        if "units" in product["_source"]:
                                            for supplier in product["_source"]["units"][0][
                                                "suppliers"
                                            ]:
                                                if supplier["id"] != "0":
                                                    pass
                                                else:
                                                    supplier_data.append(supplier)
                                    else:
                                        pass

                                    if is_dc_linked:
                                        if len(dc_data) > 0:
                                            best_dc = min(dc_data, key=lambda x: x["retailerPrice"])
                                        else:
                                            best_dc = {}
                                    else:
                                        best_dc = {}

                                    if len(supplier_data) > 0:
                                        best_supplier = min(
                                            supplier_data, key=lambda x: x["retailerPrice"]
                                        )
                                        if best_supplier["retailerQty"] == 0:
                                            best_supplier = max(
                                                supplier_data, key=lambda x: x["retailerQty"]
                                            )
                                        else:
                                            best_supplier = best_supplier
                                    else:
                                        best_supplier = {}

                                    if len(best_supplier) > 0:
                                        dc_product_id = best_supplier["productId"]
                                        child_product_details = db.childProducts.find_one(
                                            {"_id": ObjectId(dc_product_id)}
                                        )
                                        if child_product_details is not None:
                                            if len(best_dc) > 0:
                                                dc_inner_product_id = best_dc["productId"]
                                            else:
                                                dc_inner_product_id = best_supplier["productId"]
                                            child_inner_product_details = db.childProducts.find_one(
                                                {"_id": ObjectId(dc_inner_product_id)}
                                            )
                                            try:
                                                available_qty = child_inner_product_details[
                                                    "units"
                                                ][0]["availableQuantity"]
                                            except:
                                                try:
                                                    available_qty = child_product_details["units"][
                                                        0
                                                    ]["availableQuantity"]
                                                except:
                                                    available_qty = 0
                                            if "seller" in child_inner_product_details:
                                                for seller in child_inner_product_details["seller"]:
                                                    if seller["storeId"] == best_supplier["id"]:
                                                        hard_limit = seller["hardLimit"]
                                                        pre_order = seller["preOrder"]
                                                    else:
                                                        pass
                                            else:
                                                pass
                                            # ===============================offer data======================================
                                            offer_details_data = []
                                            # ===============================offer data======================================
                                            best_offer = product_get_best_offer_data(
                                                str(child_inner_product_details["_id"]), zone_id
                                            )
                                            if len(best_offer) == 0:
                                                best_offer = product_best_offer_data(
                                                    str(child_inner_product_details["_id"])
                                                )
                                            else:
                                                pass

                                            # ======================================product seo======================================================
                                            try:
                                                if "productSeo" in child_product_details:
                                                    if (
                                                        len(
                                                            child_product_details["productSeo"][
                                                                "title"
                                                            ]
                                                        )
                                                        > 0
                                                    ):
                                                        title = (
                                                            child_product_details["productSeo"][
                                                                "title"
                                                            ][language]
                                                            if language
                                                            in child_product_details["productSeo"][
                                                                "title"
                                                            ]
                                                            else child_product_details[
                                                                "productSeo"
                                                            ]["title"]["en"]
                                                        )
                                                    else:
                                                        title = ""

                                                    if (
                                                        len(
                                                            child_product_details["productSeo"][
                                                                "description"
                                                            ]
                                                        )
                                                        > 0
                                                    ):
                                                        description = (
                                                            child_product_details["productSeo"][
                                                                "description"
                                                            ][language]
                                                            if language
                                                            in child_product_details["productSeo"][
                                                                "description"
                                                            ]
                                                            else child_product_details[
                                                                "productSeo"
                                                            ]["description"]["en"]
                                                        )
                                                    else:
                                                        description = ""

                                                    if (
                                                        len(
                                                            child_product_details["productSeo"][
                                                                "metatags"
                                                            ]
                                                        )
                                                        > 0
                                                    ):
                                                        metatags = (
                                                            child_product_details["productSeo"][
                                                                "metatags"
                                                            ][language]
                                                            if language
                                                            in child_product_details["productSeo"][
                                                                "metatags"
                                                            ]
                                                            else child_product_details[
                                                                "productSeo"
                                                            ]["metatags"]["en"]
                                                        )
                                                    else:
                                                        metatags = ""

                                                    if (
                                                        len(
                                                            child_product_details["productSeo"][
                                                                "slug"
                                                            ]
                                                        )
                                                        > 0
                                                    ):
                                                        slug = (
                                                            child_product_details["productSeo"][
                                                                "slug"
                                                            ][language]
                                                            if language
                                                            in child_product_details["productSeo"][
                                                                "slug"
                                                            ]
                                                            else child_product_details[
                                                                "productSeo"
                                                            ]["slug"]["en"]
                                                        )
                                                    else:
                                                        slug = ""

                                                    product_seo = {
                                                        "title": title,
                                                        "description": description,
                                                        "metatags": metatags,
                                                        "slug": slug,
                                                    }
                                                else:
                                                    product_seo = {
                                                        "title": "",
                                                        "description": "",
                                                        "metatags": "",
                                                        "slug": "",
                                                    }
                                            except:
                                                product_seo = {
                                                    "title": "",
                                                    "description": "",
                                                    "metatags": "",
                                                    "slug": "",
                                                }
                                            tax_value = []

                                            # =========================================pharmacy details=========================================
                                            if "prescriptionRequired" in child_product_details:
                                                if (
                                                    child_product_details["prescriptionRequired"]
                                                    == 0
                                                ):
                                                    prescription_required = False
                                                else:
                                                    prescription_required = True
                                            else:
                                                prescription_required = False

                                            if "saleOnline" in child_product_details:
                                                if child_product_details["saleOnline"] == 0:
                                                    sales_online = False
                                                else:
                                                    sales_online = True
                                            else:
                                                sales_online = False

                                            if "uploadProductDetails" in child_product_details:
                                                upload_details = child_product_details[
                                                    "uploadProductDetails"
                                                ]
                                            else:
                                                upload_details = ""

                                            # ========================= for the get the linked the unit data====================================
                                            try:
                                                for link_unit in child_product_details["units"][0][
                                                    "attributes"
                                                ]:
                                                    try:
                                                        for attrlist in link_unit["attrlist"]:
                                                            try:
                                                                if attrlist is None:
                                                                    pass
                                                                else:
                                                                    if (
                                                                        attrlist["linkedtounit"]
                                                                        == 1
                                                                    ):
                                                                        if (
                                                                            attrlist[
                                                                                "measurementUnit"
                                                                            ]
                                                                            == ""
                                                                        ):
                                                                            attr_name = (
                                                                                str(
                                                                                    attrlist[
                                                                                        "value"
                                                                                    ][language]
                                                                                )
                                                                                if language
                                                                                in attrlist["value"]
                                                                                else str(
                                                                                    attrlist[
                                                                                        "value"
                                                                                    ]["en"]
                                                                                )
                                                                            )
                                                                        else:
                                                                            attr_name = (
                                                                                str(
                                                                                    attrlist[
                                                                                        "value"
                                                                                    ][language]
                                                                                )
                                                                                + " "
                                                                                + attrlist[
                                                                                    "measurementUnit"
                                                                                ]
                                                                                if language
                                                                                in attrlist["value"]
                                                                                else str(
                                                                                    attrlist[
                                                                                        "value"
                                                                                    ]["en"]
                                                                                )
                                                                                + " "
                                                                                + attrlist[
                                                                                    "measurementUnit"
                                                                                ]
                                                                            )
                                                                        variant_data.append(
                                                                            {
                                                                                "attrname": attrlist[
                                                                                    "attrname"
                                                                                ][
                                                                                    "en"
                                                                                ],
                                                                                "value": str(
                                                                                    attr_name
                                                                                ),
                                                                                "name": attrlist[
                                                                                    "attrname"
                                                                                ]["en"],
                                                                            }
                                                                        )
                                                                    else:
                                                                        pass
                                                            except:
                                                                pass
                                                    except:
                                                        pass
                                            except:
                                                pass
                                            # =========================for max quantity=================================================
                                            if "maxQuantity" in child_product_details:
                                                if child_product_details["maxQuantity"] != "":
                                                    max_quantity = int(
                                                        child_product_details["maxQuantity"]
                                                    )
                                                else:
                                                    max_quantity = 30
                                            else:
                                                max_quantity = 30
                                            # ==========================================================================================
                                            if "allowOrderOutOfStock" in child_product_details:
                                                allow_out_of_order = child_product_details[
                                                    "allowOrderOutOfStock"
                                                ]
                                            else:
                                                allow_out_of_order = False

                                            mobile_images = []

                                            if "productType" in child_product_details:
                                                if child_product_details["productType"] == 2:
                                                    combo_product = True
                                                else:
                                                    combo_product = False
                                            else:
                                                combo_product = False

                                            try:
                                                base_price = child_product_details["units"][0][
                                                    "b2cPricing"
                                                ][0]["b2cproductSellingPrice"]
                                            except:
                                                base_price = child_product_details["units"][0][
                                                    "floatValue"
                                                ]

                                            tax_price = 0
                                            if "tax" in child_product_details:
                                                if len(child_product_details["tax"]) == 0:
                                                    tax_price = 0
                                                else:
                                                    for amount in child_product_details["tax"]:
                                                        if "taxValue" in amount:
                                                            tax_price = tax_price + (
                                                                int(amount["taxValue"])
                                                            )
                                                        if "value" in amount:
                                                            tax_price = tax_price + (
                                                                int(amount["value"])
                                                            )
                                                        else:
                                                            tax_price = tax_price + 0
                                            else:
                                                tax_price = 0

                                            if len(best_offer) > 0:
                                                discount_type = (
                                                    int(float(best_offer["discountType"]))
                                                    if "discountType" in best_offer
                                                    else 1
                                                )
                                                discount_value = (
                                                    int(float(best_offer["discountValue"]))
                                                    if "discountValue" in best_offer
                                                    else 0
                                                )
                                            else:
                                                discount_type = 2
                                                discount_value = 0

                                            if discount_type == 0:
                                                percentage = 0
                                            else:
                                                percentage = int(discount_value)

                                            base_price = base_price + (
                                                (float(base_price) * tax_price) / 100
                                            )

                                            # ==============calculate discount price =============================
                                            if discount_type == 0:
                                                discount_price = float(discount_value)
                                            elif discount_type == 1:
                                                discount_price = (
                                                    float(base_price) * float(discount_value)
                                                ) / 100
                                            else:
                                                discount_price = 0
                                            final_price = base_price - discount_price

                                            try:
                                                product_name = (
                                                    child_product_details["pName"]["en"]
                                                    if "pName" in child_product_details
                                                    else child_product_details["pPName"]["en"]
                                                )
                                            except:
                                                product_name = child_product_details["units"][0][
                                                    "unitName"
                                                ]["en"]

                                            execution_time = time.time()

                                            if best_supplier["id"] == "0":
                                                outOfStock = True
                                                next_available_time = ""
                                            elif store_category_id == SHOP_LOCAL_STORE_CATEGORY_ID:
                                                if available_qty > 0:
                                                    outOfStock = False
                                                    next_available_time = ""
                                                else:
                                                    outOfStock = True
                                                    next_available_time = ""
                                            else:
                                                if available_qty > 0 and is_dc_linked == True:
                                                    if zone_id != "":
                                                        next_delivery_slot = driver_roaster["text"]
                                                        next_availbale_driver_time = driver_roaster[
                                                            "productText"
                                                        ]
                                                    else:
                                                        next_delivery_slot = ""
                                                        next_availbale_driver_time = ""
                                                    if (
                                                        next_delivery_slot != ""
                                                        and next_availbale_driver_time != ""
                                                    ):
                                                        out_of_stock = False
                                                    else:
                                                        out_of_stock = True

                                                elif (
                                                    available_qty < 0
                                                    and hard_limit != 0
                                                    and is_dc_linked == True
                                                    and driver_roaster["productText"] != ""
                                                ):
                                                    delivery_slot = (
                                                        next_availbale_driver_shift_out_stock(
                                                            zone_id,
                                                            0,
                                                            hard_limit,
                                                            str(best_supplier["productId"]),
                                                        )
                                                    )
                                                    try:
                                                        next_availbale_driver_time = delivery_slot[
                                                            "productText"
                                                        ]
                                                    except:
                                                        next_availbale_driver_time = ""
                                                    if (
                                                        allow_out_of_stock == True
                                                        and next_availbale_driver_time != ""
                                                    ):
                                                        out_of_stock = False
                                                    else:
                                                        out_of_stock = True

                                                elif (
                                                    available_qty <= 0
                                                    and is_dc_linked == True
                                                    and pre_order == True
                                                ):
                                                    dc_product_details = db.childProducts.find_one(
                                                        {"_id": ObjectId(best_dc["productId"])},
                                                        {"seller": 1},
                                                    )
                                                    if dc_product_details is not None:
                                                        if "seller" in dc_product_details:
                                                            if (
                                                                len(dc_product_details["seller"])
                                                                > 0
                                                            ):
                                                                best_buffer = min(
                                                                    dc_product_details["seller"],
                                                                    key=lambda x: x[
                                                                        "procurementTime"
                                                                    ],
                                                                )
                                                                delivery_slot = next_availbale_driver_shift_out_stock(
                                                                    zone_id,
                                                                    best_buffer["procurementTime"],
                                                                    hard_limit,
                                                                    str(best_supplier["productId"]),
                                                                )
                                                                try:
                                                                    next_availbale_driver_time = (
                                                                        delivery_slot["productText"]
                                                                    )
                                                                except:
                                                                    next_availbale_driver_time = ""
                                                                if (
                                                                    allow_out_of_stock == True
                                                                    and next_availbale_driver_time
                                                                    != ""
                                                                ):
                                                                    out_of_stock = False
                                                                else:
                                                                    out_of_stock = True
                                                            else:
                                                                next_availbale_driver_time = ""
                                                                out_of_stock = True
                                                        else:
                                                            next_availbale_driver_time = ""
                                                            out_of_stock = True
                                                    else:
                                                        next_availbale_driver_time = ""
                                                        out_of_stock = True
                                                elif available_qty == 0 and is_dc_linked == False:
                                                    next_availbale_driver_time = ""
                                                    out_of_stock = True
                                                else:
                                                    next_availbale_driver_time = ""
                                                    out_of_stock = True

                                            # ===================================variant count======================================
                                            variant_query = {
                                                "parentProductId": product["_id"],
                                                "status": 1,
                                            }
                                            if best_supplier["id"] == "0":
                                                variant_query["storeId"] = best_supplier["id"]
                                            else:
                                                variant_query["storeId"] = ObjectId(
                                                    best_supplier["id"]
                                                )
                                            # product['_source']['variantCount'] if "variantCount" in product['_source'] else 1
                                            variant_count_data = db.childProducts.find(
                                                variant_query
                                            ).count()
                                            if variant_count_data > 1:
                                                variant_count = True
                                            else:
                                                variant_count = False
                                            isShoppingList = False

                                            if "containsMeat" in child_product_details:
                                                contains_Meat = child_product_details[
                                                    "containsMeat"
                                                ]
                                            else:
                                                contains_Meat = False
                                            avg_rating = (
                                                product["avgRating"]
                                                if "avgRating" in product
                                                else 0
                                            )
                                            # =====================from here need to send dc supplier id for the product===============
                                            resData.append(
                                                {
                                                    "maxQuantity": max_quantity,
                                                    "isComboProduct": combo_product,
                                                    "childProductId": str(
                                                        best_supplier["productId"]
                                                    ),
                                                    "availableQuantity": available_qty,
                                                    "offerDetailsData": offer_details_data,
                                                    "productName": product_name,
                                                    "parentProductId": product["_id"],
                                                    "suppliers": best_supplier,
                                                    "supplier": best_supplier,
                                                    "containsMeat": contains_Meat,
                                                    "isShoppingList": isShoppingList,
                                                    "tax": tax_value,
                                                    "linkedAttribute": variant_data,
                                                    "allowOrderOutOfStock": allow_out_of_order,
                                                    "moUnit": "Pcs",
                                                    "outOfStock": outOfStock,
                                                    "variantData": variant_data,
                                                    "addOnsCount": 0,
                                                    "variantCount": variant_count,
                                                    "prescriptionRequired": prescription_required,
                                                    "saleOnline": sales_online,
                                                    "uploadProductDetails": upload_details,
                                                    "productSeo": product_seo,
                                                    "brandName": child_product_details[
                                                        "brandTitle"
                                                    ][language]
                                                    if language
                                                    in child_product_details["brandTitle"]
                                                    else child_product_details["brandTitle"]["en"],
                                                    "manufactureName": child_product_details[
                                                        "manufactureName"
                                                    ][language]
                                                    if language
                                                    in child_product_details["manufactureName"]
                                                    else "",
                                                    "TotalStarRating": avg_rating,
                                                    "currencySymbol": child_product_details[
                                                        "currencySymbol"
                                                    ]
                                                    if child_product_details["currencySymbol"]
                                                    is not None
                                                    else "₹",
                                                    "mobileImage": [],
                                                    "currency": child_product_details["currency"],
                                                    "storeCategoryId": child_product_details[
                                                        "storeCategoryId"
                                                    ]
                                                    if "storeCategoryId" in child_product_details
                                                    else "",
                                                    "images": child_product_details["images"],
                                                    "mobimages": mobile_images,
                                                    "units": child_product_details["units"],
                                                    "finalPriceList": {
                                                        "basePrice": round(base_price, 2),
                                                        "finalPrice": round(final_price, 2),
                                                        "discountPrice": round(discount_price, 2),
                                                        "discountType": discount_type,
                                                        "discountPercentage": percentage,
                                                    },
                                                    "price": int(final_price),
                                                    "isDcAvailable": is_dc_linked,
                                                    "discountType": discount_type,
                                                    "unitId": str(
                                                        child_product_details["units"][0]["unitId"]
                                                    ),
                                                    "offer": best_offer,
                                                    # "offers": best_offer,
                                                    "nextSlotTime": next_available_time,
                                                }
                                            )
                                        else:
                                            pass
                                except Exception as ex:
                                    template = (
                                        "An exception of type {0} occurred. Arguments:\n{1!r}"
                                    )
                                    print(
                                        "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                                        type(ex).__name__,
                                        ex,
                                    )
                            if len(resData) > 0:
                                newlist = sorted(
                                    resData, key=lambda k: k["isDcAvailable"], reverse=True
                                )
                                newlist = sorted(
                                    newlist, key=lambda k: k["outOfStock"], reverse=False
                                )
                                res_data_dataframe = pd.DataFrame(newlist)
                                res_data_dataframe = res_data_dataframe.drop_duplicates(
                                    "productName", keep="first"
                                )
                                newlist = res_data_dataframe.to_dict(orient="records")
                                offers_data = []
                                category_count = db.category.find_one(
                                    {
                                        "categoryName.en": category_name,
                                        "status": 1,
                                        "storeCategory.storeCategoryId": store_category_id,
                                    }
                                )
                                if category_count is not None:
                                    if len(newlist) > 0:
                                        response_json.append(
                                            {
                                                "id": "",
                                                "catName": category_name,
                                                "imageUrl": "",
                                                "bannerImageUrl": "",
                                                "websiteImageUrl": "",
                                                "websiteBannerImageUrl": "",
                                                "offers": offers_data,
                                                "penCount": doc_count,
                                                "catSeq": category_count["seqId"],
                                                "categoryData": newlist,
                                                "type": 4,
                                                "seqId": 7,
                                            }
                                        )
                        if len(response_json) > 0:
                            newlist = sorted(response_json, key=lambda k: k["catSeq"], reverse=True)
                            response = {
                                "list": newlist,
                            }
                            for product in response["list"]:
                                last_json_response.append(product)
                        else:
                            response = {
                                "list": [],
                            }
                            for product in response["list"]:
                                last_json_response.append(product)
                    else:
                        response = {
                            "list": [],
                        }
                        for product in response["list"]:
                            last_json_response.append(product)
                else:
                    language = language
                    match_query = {
                        "status": 1,
                        "productCount": {"$gt": 0},
                        "parentId": {"$exists": False},
                        "_id": {"$type": "objectId"},
                        "storeCategory.storeCategoryId": store_category_id,
                    }
                    expr_query = []
                    offer_query = []
                    or_query = []
                    expr_query.append({"$in": ["$$root_category_id", "$rootCategoryId"]})
                    expr_query.append({"$eq": ["$status", 1]})
                    expr_query.append({"$eq": ["$showImages", 1]})
                    expr_query.append({"$eq": ["$storeCategoryId", store_category_id]})

                    offer_query.append({"$in": ["$$root_category_id", "$rootCategoryId"]})
                    offer_query.append({"$eq": ["$status", 1]})
                    offer_query.append({"$eq": ["$showImages", 1]})
                    offer_query.append({"$eq": ["$storeCategoryId", store_category_id]})
                    and_query = []

                    if len(store_details) == 0:
                        if store_id != "":
                            # match_query["storeid"] = store_id
                            if s_id =="0":
                                match_query["$or"] = [
                                    {"storeid": s_id}
                                    ]
                            else:
                                match_query["$or"] = [
                                    {"storeId": ObjectId(s_id)},
                                    {"storeid": s_id}
                                    ]
                            or_query.append({"$in": [store_id, "$storeId"]})
                            # expr_query.append({"$in": [store_id, "$storeId"]})
                    else:
                        # match_query["storeid"] = {"$in": store_details}
                        for s_id in store_details:
                            or_query.append({"$in": [s_id, "$storeId"]})
                            # expr_query.append({"$in": [s_id, "$storeId"]})
                            if s_id =="0":
                                match_query["$or"] = [
                                    {"storeid": s_id}
                                    ]
                            else:
                                match_query["$or"] = [
                                    {"storeId": ObjectId(s_id)},
                                    {"storeid": s_id}
                                    ]

                    if len(or_query) > 0:
                        expr_query.append({"$or": or_query})
                        offer_query.append({"$or": or_query})

                    and_query.append({"$eq": ["$parentId", "$$parent_id"]})

                    and_query.append({"$eq": ["$status", 1]})
                    and_query.append({"$gt": ["$productCount", 0]})
                    aggregate_result = db.category.aggregate(
                        [
                            {"$match": match_query},
                            {"$sort": {"seqId": -1}},
                            {"$limit": 20},
                            {
                                "$project": {
                                    "seqId": 1,
                                    "categoryName": 1,
                                    "mobileImage": 1,
                                    "mobileIcon": 1,
                                    "websiteImage": 1,
                                    "websiteIcon": 1,
                                }
                            },
                            {
                                "$lookup": {
                                    "from": "category",
                                    "let": {"parent_id": "$_id"},
                                    "pipeline": [
                                        {"$match": {"$expr": {"$and": and_query}}},
                                        {
                                            "$project": {
                                                "seqId": 1,
                                                "categoryName": 1,
                                                "mobileImage": 1,
                                                "mobileIcon": 1,
                                                "websiteImage": 1,
                                                "websiteIcon": 1,
                                            }
                                        },
                                        {"$limit": 10},
                                    ],
                                    "as": "subCategories",
                                }
                            },
                            {
                                "$lookup": {
                                    "from": "offers",
                                    "let": {"root_category_id": "$_id"},
                                    "pipeline": [
                                        {"$match": {"$expr": {"$and": offer_query}}},
                                        {
                                            "$project": {
                                                "_id": 1,
                                                "name": 1,
                                                "images": 1,
                                                "webimages": 1,
                                            }
                                        },
                                        {"$limit": 10},
                                    ],
                                    "as": "offers",
                                }
                            },
                        ]
                    )
                    if store_category_id == MEAT_STORE_CATEGORY_ID:
                        seq_id = 1
                    else:
                        seq_id = 2
                    category_data_new = []
                    for parent_category in aggregate_result:
                        categoty_json = []
                        if len(parent_category["subCategories"]) > 0:
                            sub_category_data = []
                            category_id = str(parent_category["_id"])
                            category_name = parent_category["categoryName"]["en"]
                            offer_details = []
                            for j in parent_category["subCategories"]:
                                sub_cat_id = str(j["_id"])
                                child_cat_query = {
                                    "_id": ObjectId(sub_cat_id),
                                    "parentId": ObjectId(category_id),
                                }
                                # if len(store_details) > 0:
                                #     # child_cat_query["storeId"] = {"$in": store_details}
                                #     child_cat_query["$or"] = [
                                #         {"storeid": {"$in": store_details}},
                                #         {"storeId": {"$in": [ObjectId(x) for x in store_details]}},
                                #     ]
                                # else:
                                #     pass
                                # print(category_name, j["categoryName"]["en"], "----------> ", child_cat_query)
                                child_category_count = db.category.find(child_cat_query).count()
                                product_query = {
                                    "categoryList.parentCategory.childCategory.categoryId": str(
                                        j["_id"]
                                    ),
                                    "status": 1,
                                }
                                if store_id != "" and store_id != "0":
                                    product_query["storeId"] = ObjectId(store_id)
                                if len(store_details) > 0:
                                    product_query["$or"] = [
                                        {"storeId": {"$in": [ObjectId(x) for x in store_details]}},
                                    ]
                                product_count = db.childProducts.find(product_query).count()
                                if child_category_count > 0 and product_count > 0:
                                    mobileIcon = j["mobileIcon"] if "mobileIcon" in j else ""
                                    sub_cat_query = {"parentId": ObjectId(sub_cat_id), "status": 1}
                                    # if len(store_details) > 0:
                                    #     sub_cat_query["$or"] = [
                                    #         {"storeid": {"$in": store_details}},
                                    #         {"storeId": {"$in": [ObjectId(x) for x in store_details]}},
                                    #     ]
                                    #     # sub_cat_query["storeid"] = {"$in": store_details}
                                    # elif store_id != "":
                                    #     # sub_cat_query["storeid"] = store_id
                                    #     sub_cat_query["$or"] = [
                                    #         {"storeId": ObjectId(store_id)},
                                    #         {"storeid": store_id},
                                    #     ]
                                    # else:
                                    #     pass
                                    sub_cat_count = db.category.find(sub_cat_query).count()
                                    sub_category_data.append(
                                        {
                                            "id": sub_cat_id,
                                            "subCategoryName": j["categoryName"]["en"],
                                            "seqId": j["seqId"],
                                            "imageUrl": mobileIcon,
                                            "childCount": sub_cat_count,
                                            "penCount": sub_cat_count,
                                        }
                                    )
                            if len(parent_category["offers"]) > 0:
                                for hits in parent_category["offers"]:
                                    offer_details.append(
                                        {
                                            "offerId": str(hits["_id"]),
                                            "offerName": hits["name"][language]
                                            if language in hits["name"]
                                            else hits["name"]["en"],
                                            "webimages": hits["webimages"]["image"],
                                            "mobimage": hits["images"]["image"],
                                            "discountValue": 0,
                                        }
                                    )

                            if len(offer_details) > 0:
                                offer_dataframe = pd.DataFrame(offer_details)
                                offer_dataframe = offer_dataframe.drop_duplicates(
                                    "offerName", keep="first"
                                )
                                offers_data = offer_dataframe.to_dict(orient="records")
                            else:
                                offers_data = []
                            if len(sub_category_data) > 0:
                                dataframe_sub_cat = pd.DataFrame(sub_category_data)
                                dataframe_sub_cat = dataframe_sub_cat.drop_duplicates(
                                    subset="subCategoryName", keep="first"
                                )
                                sub_cat_details = dataframe_sub_cat.to_json(orient="records")
                                sub_cat_json_data = json.loads(sub_cat_details)
                                sub_cat_json_data = sorted(
                                    sub_cat_json_data, key=lambda k: k["seqId"], reverse=True
                                )
                            else:
                                sub_cat_json_data = []
                            category_data_new.append(
                                {
                                    "id": str(parent_category["_id"]),
                                    "catName": parent_category["categoryName"][language]
                                    if language in parent_category["categoryName"]
                                    else parent_category["categoryName"]["en"],
                                    "imageUrl": parent_category["mobileImage"]
                                    if "mobileImage" in parent_category
                                    else "",
                                    "bannerImageUrl": parent_category["mobileImage"]
                                    if "mobileImage" in parent_category
                                    else "",
                                    "websiteImageUrl": parent_category["websiteImage"]
                                    if "websiteImage" in parent_category
                                    else "",
                                    "websiteBannerImageUrl": parent_category["websiteImage"]
                                    if "websiteImage" in parent_category
                                    else "",
                                }
                            )
                            categoty_json.append(
                                {
                                    "id": str(parent_category["_id"]),
                                    "catSeqId": parent_category["seqId"],
                                    "catName": category_name,
                                    "imageUrl": parent_category["mobileImage"]
                                    if "mobileImage" in parent_category
                                    else "",
                                    "bannerImageUrl": parent_category["mobileImage"]
                                    if "mobileImage" in parent_category
                                    else "",
                                    "websiteImageUrl": parent_category["websiteImage"]
                                    if "websiteImage" in parent_category
                                    else "",
                                    "websiteBannerImageUrl": parent_category["websiteImage"]
                                    if "websiteImage" in parent_category
                                    else "",
                                    "categoryData": sub_cat_json_data,
                                    "offers": offers_data,
                                    "type": 4,
                                    "seqId": 7,
                                }
                            )
                        dataframe = pd.DataFrame(categoty_json)
                        dataframe = dataframe.drop_duplicates(subset="catName", keep="first")
                        details = dataframe.to_json(orient="records")
                        cat_json_data = json.loads(details)
                        cat_json_data = sorted(
                            cat_json_data, key=lambda k: k["catSeqId"], reverse=True
                        )
                        response = {
                            "list": cat_json_data,
                        }
                        for product in response["list"]:
                            last_json_response.append(product)
                    else:
                        response = {
                            "list": [],
                        }
                        for product in response["list"]:
                            last_json_response.append(product)
                    print("category_data_new",category_data_new)
                    dataframe = pd.DataFrame(category_data_new)
                    dataframe = dataframe.drop_duplicates(subset="catName", keep="last")
                    product_list = dataframe.to_dict(orient="records")
                    newlist = product_list  # sorted(
                    # product_list, key=lambda k: k['catName'], reverse=True)
                    if store_category_id == MEAT_STORE_CATEGORY_ID:
                        seq_id = 1
                    else:
                        seq_id = 2
                    response = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": newlist,
                        "seqId": seq_id,
                        "type": 2,
                    }
                    last_json_response.append(response)
                # ===============================================category data======================================
                category_time = time.time()
                # if store_category_id == MEAT_STORE_CATEGORY_ID:
                #     seq_id = 1
                # else:
                #     seq_id = 2
                # try:
                #     category_json = []
                #     if store_category_id == MEAT_STORE_CATEGORY_ID:
                #         to_data = 20
                #     else:
                #         to_data = 4
                #     if store_id == "":
                #         if zone_id == "":
                #             category_query = {
                #                 "parentId": {"$exists": False},
                #                 "status": 1,
                #                 "storeCategory.storeCategoryId": str(store_category_id),
                #                 "storeId": "0",
                #             }
                #             if int(integration_type) == 0:
                #                 pass
                #             elif int(integration_type) == 1:
                #                 category_query["magentoId"] = {"$ne": 0}
                #             elif int(integration_type) == 2:
                #                 category_query["shopify_id"] = {"$ne": ""}
                #             elif int(integration_type) == 3:
                #                 or_query = []
                #                 or_query.append({"magentoId": 0})
                #                 or_query.append({"shopify_id": ""})
                #                 category_query["$and"] = or_query
                #             category_data_db = category_find(category_query, 0, to_data)
                #         else:
                #             store_data_details = []
                #             if zone_id != "" and store_category_id == PHARMACY_STORE_CATEGORY_ID:
                #                 store_query = {
                #                     "categoryId": str(store_category_id),
                #                     "cityId": city_id,
                #                 }
                #             else:
                #                 store_query = {
                #                     "categoryId": str(store_category_id),
                #                     "serviceZones.zoneId": zone_id,
                #                     "storeFrontTypeId": {"$ne": 5},
                #                     "status": 1,
                #                 }
                #             store_data = store_find(store_query)
                #             if store_data.count() > 0:
                #                 for store in store_data:
                #                     store_data_details.append(str(store["_id"]))
                #                 category_query = {
                #                     "status": 1,
                #                     "storeId" : "0",
                #                     "parentId": {"$exists": False},
                #                     "storeCategory.storeCategoryId": str(store_category_id),
                #                     "$or": [
                #                         {"storeid": {"$in": store_data_details}},
                #                         {"storeId": {"$in": store_data_details}},
                #                     ],
                #                 }
                #                 if int(integration_type) == 0:
                #                     pass
                #                 elif int(integration_type) == 1:
                #                     category_query["magentoId"] = {"$ne": 0}
                #                 elif int(integration_type) == 2:
                #                     category_query["shopify_id"] = {"$ne": ""}
                #                 elif int(integration_type) == 3:
                #                     or_query = []
                #                     or_query.append({"magentoId": 0})
                #                     or_query.append({"shopify_id": ""})
                #                     category_query["$and"] = or_query
                #                 category_data_db = category_find(category_query, 0, to_data)
                #                 if category_data_db.count() == 0:
                #                     response = {
                #                         "id": "",
                #                         "catName": "",
                #                         "imageUrl": "",
                #                         "bannerImageUrl": "",
                #                         "websiteImageUrl": "",
                #                         "websiteBannerImageUrl": "",
                #                         "offers": [],
                #                         "penCount": 0,
                #                         "categoryData": [],
                #                         "type": 2,
                #                         "seqId": seq_id,
                #                     }
                #                     last_json_response.append(response)
                #             else:
                #                 response = {
                #                     "id": "",
                #                     "catName": "",
                #                     "imageUrl": "",
                #                     "bannerImageUrl": "",
                #                     "websiteImageUrl": "",
                #                     "websiteBannerImageUrl": "",
                #                     "offers": [],
                #                     "penCount": 0,
                #                     "categoryData": [],
                #                     "type": 2,
                #                     "seqId": seq_id,
                #                 }
                #                 last_json_response.append(response)
                #     else:
                #         category_query = {
                #             "status": 1,
                #             "parentId": {"$exists": False},
                #             "storeCategory.storeCategoryId": str(store_category_id),
                #             "storeid": {"$in": [store_id]},
                #             "storeId": "0"
                #         }
                #         if int(integration_type) == 0:
                #             pass
                #         elif int(integration_type) == 1:
                #             category_query["magentoId"] = {"$ne": 0}
                #         elif int(integration_type) == 2:
                #             category_query["shopify_id"] = {"$ne": ""}
                #         elif int(integration_type) == 3:
                #             or_query = []
                #             or_query.append({"magentoId": 0})
                #             or_query.append({"shopify_id": ""})
                #             category_query["$and"] = or_query
                #         category_data_db = category_find(category_query, 0, to_data)
                #         if category_data_db.count() == 0:
                #             category_query = {
                #                 "status": 1,
                #                 "parentId": {"$exists": False},
                #                 "storeCategory.storeCategoryId": str(store_category_id),
                #                 "storeId": store_id,
                #             }
                #             if int(integration_type) == 0:
                #                 pass
                #             elif int(integration_type) == 1:
                #                 category_query["magentoId"] = {"$ne": 0}
                #             elif int(integration_type) == 2:
                #                 category_query["shopify_id"] = {"$ne": ""}
                #             elif int(integration_type) == 3:
                #                 or_query = []
                #                 or_query.append({"magentoId": 0})
                #                 or_query.append({"shopify_id": ""})
                #                 category_query["$and"] = or_query
                #             category_data_db = category_find(category_query, 0, to_data)
                #
                #     try:
                #         if category_data_db.count() != 0:
                #             for cate_gory in category_data_db:
                #                 category_json.append(
                #                     {
                #                         "id": str(cate_gory["_id"]),
                #                         "catName": cate_gory["categoryName"][language]
                #                         if language in cate_gory["categoryName"]
                #                         else cate_gory["categoryName"]["en"],
                #                         "imageUrl": cate_gory["mobileImage"]
                #                         if "mobileImage" in cate_gory
                #                         else "",
                #                         "bannerImageUrl": cate_gory["mobileImage"]
                #                         if "mobileImage" in cate_gory
                #                         else "",
                #                         "websiteImageUrl": cate_gory["websiteImage"]
                #                         if "websiteImage" in cate_gory
                #                         else "",
                #                         "websiteBannerImageUrl": cate_gory["websiteImage"]
                #                         if "websiteImage" in cate_gory
                #                         else "",
                #                     }
                #                 )
                #
                #             dataframe = pd.DataFrame(category_json)
                #             dataframe = dataframe.drop_duplicates(subset="catName", keep="last")
                #             product_list = dataframe.to_dict(orient="records")
                #             newlist = product_list  # sorted(
                #             # product_list, key=lambda k: k['catName'], reverse=True)
                #             print(newlist)
                #             response = {
                #                 "id": "",
                #                 "catName": "",
                #                 "imageUrl": "",
                #                 "bannerImageUrl": "",
                #                 "websiteImageUrl": "",
                #                 "websiteBannerImageUrl": "",
                #                 "offers": [],
                #                 "penCount": 0,
                #                 "categoryData": newlist,
                #                 "seqId": seq_id,
                #                 "type": 2,
                #             }
                #             last_json_response.append(response)
                #         else:
                #             response = {
                #                 "id": "",
                #                 "catName": "",
                #                 "imageUrl": "",
                #                 "bannerImageUrl": "",
                #                 "websiteImageUrl": "",
                #                 "websiteBannerImageUrl": "",
                #                 "offers": [],
                #                 "penCount": 0,
                #                 "categoryData": category_json,
                #                 "type": 2,
                #                 "seqId": seq_id,
                #             }
                #             last_json_response.append(response)
                #     except:
                #         response = {
                #             "id": "",
                #             "catName": "",
                #             "imageUrl": "",
                #             "bannerImageUrl": "",
                #             "websiteImageUrl": "",
                #             "websiteBannerImageUrl": "",
                #             "offers": [],
                #             "penCount": 0,
                #             "categoryData": category_json,
                #             "type": 2,
                #             "seqId": seq_id,
                #         }
                #         last_json_response.append(response)
                # except Exception as ex:
                #     print(
                #         "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                #         type(ex).__name__,
                #         ex,
                #     )
                #     error = {
                #         "id": "",
                #         "catName": "",
                #         "imageUrl": "",
                #         "bannerImageUrl": "",
                #         "websiteImageUrl": "",
                #         "websiteBannerImageUrl": "",
                #         "offers": [],
                #         "penCount": 0,
                #         "categoryData": [],
                #         "type": 2,
                #         "seqId": seq_id,
                #     }
                #     last_json_response.append(error)
                # ===========================================symptom data====================================
                if store_category_id == PHARMACY_STORE_CATEGORY_ID:
                    try:
                        sysmptom_data = []
                        symptom_query = {"status": 1}
                        sysmptom_count = db.symptom.find(symptom_query).count()
                        sysmptom_details = symptom_find(symptom_query, 0, 6)

                        if sysmptom_details.count() > 0:
                            for i in sysmptom_details:
                                sysmptom_data.append(
                                    {
                                        "id": str(i["_id"]),
                                        "name": i["name"][language]
                                        if language in i["name"]
                                        else i["name"]["en"],
                                        "imageWeb": i["webImage"],
                                        "logo": i["mobileImage"],
                                        "bannerImage": i["webImage"],
                                    }
                                )

                            df = pd.DataFrame(sysmptom_data)
                            df = df.drop_duplicates(subset="name", keep="last")
                            response = {
                                "id": "",
                                "catName": "Health Concerns",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "type": 8,
                                "seqId": 6,
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": sysmptom_count,
                                "categoryData": df.to_dict(orient="records"),
                            }
                            last_json_response.append(response)
                        else:
                            response = {
                                "id": "",
                                "catName": "",
                                "type": 8,
                                "seqId": 6,
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": [],
                            }
                            last_json_response.append(response)
                    except Exception as ex:
                        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                        message = template.format(type(ex).__name__, ex.args)
                        print(message)
                        error = {
                            "id": "",
                            "catName": "",
                            "type": 8,
                            "seqId": 6,
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "categoryData": [],
                        }
                        last_json_response.append(error)
                # ===============================for home page seo=================================================
                home_page_seo_time = time.time()
                language = language
                response = {}
                home_page_data = home_page_find_one()
                if home_page_data is not None:
                    if "homepage" in home_page_data:
                        try:
                            response = {
                                "facebookIcon": home_page_data["homepage"]["facebookIcon"]
                                if "facebookIcon" in home_page_data["homepage"]
                                else "",
                                "facebookLink": home_page_data["homepage"]["facebookLink"]
                                if "facebookLink" in home_page_data["homepage"]
                                else "",
                                "twitterIcon": home_page_data["homepage"]["twitterIcon"]
                                if "twitterIcon" in home_page_data["homepage"]
                                else "",
                                "twitterLink": home_page_data["homepage"]["twitterLink"]
                                if "twitterLink" in home_page_data["homepage"]
                                else "",
                                "instagramIcon": home_page_data["homepage"]["instagramIcon"]
                                if "instagramIcon" in home_page_data["homepage"]
                                else "",
                                "instagramLink": home_page_data["homepage"]["instagramLink"]
                                if "instagramLink" in home_page_data["homepage"]
                                else "",
                                "youtubeIcon": home_page_data["homepage"]["youtubeIcon"]
                                if "youtubeIcon" in home_page_data["homepage"]
                                else "",
                                "youtubeLink": home_page_data["homepage"]["youtubeLink"]
                                if "youtubeLink" in home_page_data["homepage"]
                                else "",
                                "linkedInIcon": home_page_data["homepage"]["linkedInIcon"]
                                if "linkedInIcon" in home_page_data["homepage"]
                                else "",
                                "linkedInLink": home_page_data["homepage"]["linkedInLink"]
                                if "linkedInLink" in home_page_data["homepage"]
                                else "",
                                "copyRight": home_page_data["homepage"]["copyRight"][language]
                                if language in home_page_data["homepage"]["copyRight"]
                                else home_page_data["homepage"]["copyRight"]["en"],
                            }
                        except:
                            pass
                home_page_seo_response = {"data": response}
                # =========================================store details for home===========================================
                store_details_time = time.time()
                try:
                    if store_id != "":
                        json_response = store_function(
                            language,
                            store_id,
                            user_id,
                            user_latitude,
                            user_longtitude,
                            timezone_data,
                        )
                        store_response_data = json_response["data"]
                    else:
                        store_response_data = []
                except Exception as ex:
                    print(
                        "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                        type(ex).__name__,
                        ex,
                    )
                    store_response_data = []
                # ===========================total count data========================================================
                # must_query = [
                #     {"match": {"status": 1}},
                #     {"match": {"storeCategoryId": str(store_category_id)}},
                # ]
                # if zone_id == "":
                #     pass
                # else:
                #     store_data_details = []
                #     store_data = store_find(
                #         {"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id}
                #     )
                #     if store_data.count() > 0:
                #         for store in store_data:
                #             store_data_details.append(str(store["_id"]))
                #         must_query.append({"terms": {"storeId": store_data_details}})
                #     else:
                #         must_query.append({"match": {"zoneId": zone_id}})
                # query = {
                #     "sort": [
                #         {"isCentral": {"order": "desc"}},
                #         {"isInStock": {"order": "desc"}},
                #         {"units.discountPrice": {"order": "asc"}},
                #     ],
                #     "query": {
                #         "bool": {
                #             "must": must_query,
                #             "must_not": [
                #                 {"match": {"firstCategoryName": ""}},
                #                 {"match": {"secondCategoryName": ""}},
                #                 {"match": {"units.b2cPricing.b2cproductSellingPrice": 0}},
                #             ],
                #         }
                #     },
                #     "aggs": {
                #         "group_by_sub_category": {
                #             "terms": {
                #                 "field": "secondCategoryName.keyword",
                #                 "size": 500,
                #                 "order": {"avg_score": "desc"},
                #             },
                #             "aggs": {
                #                 "avg_score": {"sum": {"field": "isInStock"}},
                #                 "top_hits": {
                #                     "terms": {"field": "parentProductId.keyword", "size": 20},
                #                     "aggs": {
                #                         "top_sales_hits": {
                #                             "top_hits": {
                #                                 "sort": [
                #                                     {"isCentral": {"order": "desc"}},
                #                                     {"isInStock": {"order": "desc"}},
                #                                     {"units.discountPrice": {"order": "asc"}},
                #                                 ],
                #                                 "_source": {
                #                                     "includes": [
                #                                         "_id",
                #                                         "pName",
                #                                         "storeId",
                #                                         "parentProductId",
                #                                         "currencySymbol",
                #                                         "currency",
                #                                         "pPName",
                #                                         "needsIdProof",
                #                                         "tax",
                #                                         "brandTitle",
                #                                         "categoryList",
                #                                         "images",
                #                                         "avgRating",
                #                                         "units",
                #                                         "storeCategoryId",
                #                                         "manufactureName",
                #                                         "maxQuantity",
                #                                     ]
                #                                 },
                #                                 "size": 1,
                #                             }
                #                         }
                #                     },
                #                 },
                #             },
                #         }
                #     },
                # }
                # res = child_product_es_aggrigate_data(query)
                # try:
                #     total_data_count = len(res["aggregations"]["group_by_sub_category"]["buckets"])
                # except:
                total_data_count = 20

                # ===========================================banner details=========================================if store_category_id == MEAT_STORE_CATEGORY_ID:
                if store_category_id == MEAT_STORE_CATEGORY_ID:
                    seq_id = 2
                else:
                    seq_id = 1
                try:
                    banner_deatils_data = []
                    if store_id == "":
                        if zone_id == "":
                            banner_query = {"status": 1, "storeCategoryId": store_category_id}
                        else:
                            store_data_details = []
                            store_data = store_find(
                                {
                                    "categoryId": str(store_category_id),
                                    "serviceZones.zoneId": zone_id,
                                }
                            )
                            if store_data.count() > 0:
                                for store in store_data:
                                    store_data_details.append(str(store["_id"]))
                            banner_query = {
                                "status": 1,
                                "storeCategoryId": store_category_id,
                                "zones.zoneId": zone_id,
                            }
                            # banner_query = {"status": 1, "storeCategoryId": store_category_id, "storeId": {"$in": store_data_details}}
                    else:
                        banner_query = {
                            "status": 1,
                            "storeCategoryId": store_category_id,
                            "storeId": store_id,
                        }
                    banner_type = ""
                    print("banner_query", banner_query)
                    banner_details = banner_find(banner_query)
                    if banner_details.count() > 0:
                        for i in banner_details:
                            base_category_id = ""
                            brand_id = ""
                            sub_category_name = ""
                            sub_category_id = ""
                            sub_sub_category_name = ""
                            sub_sub_category_id = ""
                            # try:
                            category_name = ""
                            sub_category_name = ""
                            sub_sub_category_name = ""
                            if int(i["type"]) == 1:
                                banner_type = "offer"
                            elif int(i["type"]) == 2:
                                banner_type = "brands"
                            elif int(i["type"]) == 3:
                                banner_type = "category"
                            elif int(i["type"]) == 4:
                                banner_type = "stores"
                            elif int(i["type"]) == 5:
                                banner_type = "subcategory"
                            elif int(i["type"]) == 6:
                                banner_type = "subsubcategory"
                            elif int(i["type"]) == 7:
                                banner_type = "supplier"
                            elif int(i["type"]) == 8:
                                banner_type = "products"
                            elif int(i["type"]) == 9:
                                banner_type = "url"
                            if int(i["type"]) == 3:

                                base_category = category_find_one(
                                    {"_id": ObjectId(str(i["data"][0]["id"]).replace(" ", ""))}
                                )
                                if base_category is not None:
                                    if "parentId" in base_category:
                                        second_category = category_find_one(
                                            {"_id": ObjectId(base_category["parentId"])}
                                        )
                                        if second_category is not None:
                                            if "parentId" in second_category:
                                                sub_sub_category_name = base_category["categoryName"]["en"]
                                                sub_sub_category_id = str(base_category["_id"])
                                                sub_category_name = second_category["categoryName"]["en"]
                                                sub_category_id = str(second_category["_id"])
                                                sub_sub_category_name = base_category[
                                                    "categoryName"
                                                ]["en"]
                                                sub_category_name = second_category["categoryName"][
                                                    "en"
                                                ]
                                                first_category = category_find_one(
                                                    {"_id": ObjectId(second_category["parentId"])}
                                                )
                                                if first_category is not None:
                                                    category_name = first_category["categoryName"][
                                                        "en"
                                                    ]
                                                    base_category_id = str(first_category["_id"])
                                                else:
                                                    category_name = ""
                                            else:
                                                category_name = second_category["categoryName"][
                                                    "en"
                                                ]
                                                base_category_id = str(second_category["_id"])
                                                sub_category_name = base_category["categoryName"][
                                                    "en"
                                                ]
                                                sub_category_id = str(base_category["_id"])
                                                sub_sub_category_name = ""
                                        else:
                                            first_category = category_find_one(
                                                {
                                                    "_id": ObjectId(
                                                        str(i["data"][0]["id"]).replace(" ", "")
                                                    )
                                                }
                                            )
                                            if first_category is not None:
                                                base_category_id = str(first_category["_id"])
                                                category_name = first_category["categoryName"]["en"]
                                            else:
                                                category_name = ""
                                                sub_category_name = ""
                                                sub_sub_category_name = ""
                                    else:
                                        first_category = category_find_one(
                                            {
                                                "_id": ObjectId(
                                                    str(i["data"][0]["id"]).replace(" ", "")
                                                )
                                            }
                                        )
                                        if first_category is not None:
                                            category_name = first_category["categoryName"]["en"]
                                            base_category_id = str(first_category["_id"])
                                        else:
                                            category_name = ""
                                            sub_category_name = ""
                                            sub_sub_category_name = ""
                                else:
                                    category_name = ""
                                    sub_category_name = ""
                                    sub_sub_category_name = ""

                            if sub_category_name != "" and sub_sub_category_name == "":
                                banner_deatils_data.append(
                                    {
                                        "type": 5,
                                        "bannerTypeMsg": "subcategory",
                                        "firstCategoryId": base_category_id,
                                        "secondCategoryId": sub_category_id,
                                        "thirdCategoryId": sub_sub_category_id,
                                        "catName": category_name,
                                        "offerName": "",
                                        "name": i["data"][0]["name"]["en"],
                                        "imageWeb": i["image_web"],
                                        "imageMobile": i["image_mobile"],
                                    }
                                )
                            elif sub_category_name != "" and sub_sub_category_name != "":
                                banner_deatils_data.append(
                                    {
                                        "type": 6,
                                        "bannerTypeMsg": "subsubcategory",
                                        "offerName": "",
                                        "catName": category_name,
                                        "subCatName": sub_category_name,
                                        "firstCategoryId": base_category_id,
                                        "secondCategoryId": sub_category_id,
                                        "thirdCategoryId": sub_sub_category_id,
                                        "name": i["data"][0]["name"]["en"],
                                        "imageWeb": i["image_web"],
                                        "imageMobile": i["image_mobile"],
                                    }
                                )
                            elif int(i["type"]) == 1:
                                offer_id = ""
                                for o_id in i["data"]:
                                    offer_inner = offer_find_one(
                                        {"_id": ObjectId(o_id["id"]), "status": 1}
                                    )
                                    if offer_inner is not None:
                                        if offer_id != "":
                                            offer_id = offer_id + "," + o_id["id"]
                                        else:
                                            offer_id = o_id["id"]
                                    else:
                                        pass
                                offer_query = {"_id": ObjectId(i["data"][0]["id"]), "status": 1}
                                offer_details = offer_find_one(offer_query)
                                if offer_details is not None:
                                    product_query = {
                                        "offer.status": 1,
                                        "offer.offerId": str(offer_details["_id"]),
                                        "status": 1,
                                    }
                                    child_product_count = product_find_count(product_query)
                                    if child_product_count > 0:
                                        banner_deatils_data.append(
                                            {
                                                "type": i["type"],
                                                "bannerTypeMsg": banner_type,
                                                "catName": "",
                                                "subCatName": "",
                                                "firstCategoryId": "",
                                                "secondCategoryId": "",
                                                "thirdCategoryId": "",
                                                "offerName": offer_details["name"]["en"],
                                                "name": str(offer_id),
                                                "imageWeb": i["image_web"],
                                                "imageMobile": i["image_mobile"],
                                            }
                                        )
                            elif int(i["type"]) == 8:
                                product_details = db.products.find_one(
                                    {"_id": ObjectId(i["data"][0]["id"]), "status": 1}, {"units": 1}
                                )
                                if product_details is not None:
                                    supplier_list = []
                                    if "suppliers" in product_details["units"][0]:
                                        for s in product_details["units"][0]["suppliers"]:
                                            if s["id"] != "0":
                                                child_product_count = db.childProducts.find(
                                                    {"_id": ObjectId(s["productId"]), "status": 1}
                                                ).count()
                                                if child_product_count > 0:
                                                    supplier_list.append(s)
                                            else:
                                                pass
                                    if len(supplier_list) > 0:
                                        best_supplier = min(
                                            supplier_list, key=lambda x: x["retailerPrice"]
                                        )
                                        if best_supplier["retailerQty"] == 0:
                                            best_supplier = max(
                                                supplier_list, key=lambda x: x["retailerQty"]
                                            )
                                        else:
                                            best_supplier = best_supplier
                                        if len(best_supplier) > 0:
                                            central_product_id = str(product_details["_id"])
                                            child_product_id = str(best_supplier["productId"])
                                            banner_deatils_data.append(
                                                {
                                                    "type": i["type"],
                                                    "bannerTypeMsg": banner_type,
                                                    "firstCategoryId": "",
                                                    "secondCategoryId": "",
                                                    "thirdCategoryId": "",
                                                    "catName": "",
                                                    "parentProductId": central_product_id,
                                                    "childProductId": child_product_id,
                                                    "subCatName": "",
                                                    "offerName": "/python/product/details?&parentProductId="
                                                    + central_product_id
                                                    + "&productId="
                                                    + child_product_id,
                                                    "name": "/python/product/details?&parentProductId="
                                                    + central_product_id
                                                    + "&productId="
                                                    + child_product_id,
                                                    "imageWeb": i["image_web"],
                                                    "imageMobile": i["image_mobile"],
                                                }
                                            )
                            elif int(i["type"]) == 9:
                                banner_deatils_data.append(
                                    {
                                        "type": i["type"],
                                        "bannerTypeMsg": banner_type,
                                        "firstCategoryId": base_category_id,
                                        "secondCategoryId": sub_category_id,
                                        "thirdCategoryId": sub_sub_category_id,
                                        "catName": "",
                                        "subCatName": "",
                                        "offerName": i["data"][0]["name"]["en"],
                                        "name": i["data"][0]["name"]["en"],
                                        "imageWeb": i["image_web"],
                                        "imageMobile": i["image_mobile"],
                                    }
                                )
                            else:
                                try:
                                    store_details_name = json.loads(i["data"][0]["name"])
                                    id = json.loads(i["data"][0]["id"])
                                except:
                                    try:
                                        store_details_name = i["data"][0]["name"]
                                        id = i["data"][0]["id"]
                                    except:
                                        store_details_name = {"en": ""}
                                        id = ""
                                try:
                                    if store_details_name["en"] != "":
                                        banner_deatils_data.append(
                                            {
                                                "type": i["type"],
                                                "bannerTypeMsg": banner_type,
                                                "offerName": "",
                                                "id": id,
                                                "firstCategoryId": base_category_id,
                                                "secondCategoryId": sub_category_id,
                                                "thirdCategoryId": sub_sub_category_id,
                                                "name": store_details_name["en"],
                                                "imageWeb": i["image_web"],
                                                "imageMobile": i["image_mobile"],
                                            }
                                        )
                                except:
                                    pass
                        # except:
                        #     pass
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "penCount": 0,
                            "offers": [],
                            "type": 1,
                            "seqId": seq_id,
                            "categoryData": banner_deatils_data,
                        }
                        last_json_response.append(response)
                    else:
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "type": 1,
                            "seqId": seq_id,
                            "categoryData": [],
                        }
                        last_json_response.append(response)
                except Exception as ex:
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    print(
                        "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                        type(ex).__name__,
                        ex,
                    )
                    error = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "seqId": 1,
                        "type": 1,
                        "categoryData": [],
                    }
                    last_json_response.append(response)
                # ====================================last response==============================================
                newlist = sorted(last_json_response, key=lambda k: k["seqId"])
                # =================================get notification count for user===============================
                notification_count_query = {
                    "app_name": APP_NAME,
                    "userid": user_id,
                    "isSeen": False,
                }
                if store_category_id != "":
                    notification_count_query["store_category_id"] = store_category_id
                notification_count = db.notificationLogs.find(notification_count_query).count()
                last_response = {
                    "data": {
                        "notificationCount": notification_count,
                        "list": newlist,
                        "storeIsOpen": store_is_open,
                        "storeTag": store_tag,
                        "isTempClose": is_temp_close,
                        "totalCatCount": total_data_count,
                        "seoData": home_page_seo_response["data"],
                        "storeData": store_response_data,
                        "nextDeliverySlot": next_delivery_slot,
                    }
                }
                json_response = {
                    "list": newlist,
                    "storeIsOpen": store_is_open,
                    "storeTag": store_tag,
                    "isTempClose": is_temp_close,
                    "totalCatCount": total_data_count,
                    "seoData": home_page_seo_response["data"],
                    "storeData": store_response_data,
                    "nextDeliverySlot": next_delivery_slot,
                }
                # json_images = json.dumps(json_response)
                try:
                    if hyperlocal == True and storelisting == False:
                        rjId = "homepage_" + str(store_category_id) + "_" + str(zone_id)
                        RJ_HOMEPAGE_DATA.jsonset(rjId, Path.rootPath(), json_response)
                        RJ_HOMEPAGE_DATA.expire(rjId, 3600)
                    elif hyperlocal == True and storelisting == True:
                        rjId = "homepage_" + str(store_category_id) + "_" + str(store_id)
                        RJ_HOMEPAGE_DATA.jsonset(rjId, Path.rootPath(), json_response)
                    elif hyperlocal == False and storelisting == False:
                        rjId = "homepage_" + str(store_category_id) + "_ecommerce"
                        RJ_HOMEPAGE_DATA.jsonset(rjId, Path.rootPath(), json_response)
                    else:
                        pass
                except: pass
                print("****************data added sucessfully******************")
                return JsonResponse(last_response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class ClubMartHomePage(APIView):
    @swagger_auto_schema(
        method="post",
        tags=["Clubmart HomePage"],
        operation_description="API for getting the data on home page in app and website for clubmart",
        required=["AUTHORIZATION"],
        manual_parameters=CLUBMART_MANUAL_PARAM_REQ,
        request_body=CLUBMART_REQ_BODY,
        responses=COMMON_RESPONSE,
    )
    @action(detail=False, methods=["post"])
    def post(self, request):
        try:
            auth_err = UtilsObj.check_authentication(request)
            if auth_err:
                return auth_err
            language = UtilsObj.get_lang(request)

            try:
                user_id = UtilsObj.get_userId(request)
            except:
                user_id = "5df87244cb12e160bf694309"

            err = UtilsObj.check_req_params(request.data, ["zoneId"])
            if err:
                return err

            rdata = request.data
            store_response_data = []
            store_category_id_list = []
            category_response = []
            banners = []

            zone_id = rdata.get("zoneId", "")
            store_category_id = rdata.get("storeCategoryId", False)
            city_id = rdata.get("cityId", False)
            user_latitude = float(rdata.get("lat", 12.9692))
            user_longtitude = float(rdata.get("long", 77.7499))
            timezone_data = rdata.get("timezone", "")
            timezone_data = timezone_data.replace("%2F", "/")
            from_data = rdata.get("fromData", 0)
            to_data = rdata.get("toData", 50)

            # get store specialized category details
            if city_id and store_category_id:
                categoty_details = db.cities.find_one(
                    {
                        "storeCategory.storeCategoryId": store_category_id,
                        "_id": ObjectId(city_id),
                    },
                    {"storeCategory.$.storeCategoryId": 1},
                )
            elif zone_id and not store_category_id and not city_id:
                city_details = db.zones.find_one({"_id": ObjectId(zone_id)}, {"city_ID": 1})
                if city_details:
                    categoty_details = db.cities.find_one(
                        {
                            "_id": ObjectId(city_details["city_ID"]),
                        },
                        {"storeCategory": 1},
                    )
            elif city_id:
                categoty_details = db.cities.find_one(
                    {"_id": ObjectId(city_id)}, {"storeCategory": 1}
                )
            if categoty_details:
                for cat in categoty_details["storeCategory"]:
                    try:
                        store_category_id_list.append(cat["storeCategoryId"])
                    except:
                        pass
            else:
                return JsonResponse({"No data found."}, status=404)

            category_json = []

            total_count = 0
            seqId = 1
            category_list = []
            if store_category_id_list:
                category_query = {
                    "status": 1,
                    "parentId": {"$exists": False},
                    "storeCategory.storeCategoryId": {"$in": store_category_id_list},
                }
                category_data_db = category_find(category_query, 0, to_data)

                try:
                    if category_data_db.count() != 0:
                        for cate_gory in category_data_db:
                            category_json.append(
                                {
                                    "id": str(cate_gory["_id"]),
                                    "catName": cate_gory["categoryName"][language]
                                    if language in cate_gory["categoryName"]
                                    else cate_gory["categoryName"]["en"],
                                    "imageUrl": cate_gory["mobileImage"]
                                    if "mobileImage" in cate_gory
                                    else "",
                                    "bannerImageUrl": cate_gory["mobileImage"]
                                    if "mobileImage" in cate_gory
                                    else "",
                                    "websiteImageUrl": cate_gory["websiteImage"]
                                    if "websiteImage" in cate_gory
                                    else "",
                                    "websiteBannerImageUrl": cate_gory["websiteImage"]
                                    if "websiteImage" in cate_gory
                                    else "",
                                }
                            )

                        dataframe = pd.DataFrame(category_json)
                        dataframe = dataframe.drop_duplicates(subset="catName", keep="last")
                        product_list = dataframe.to_dict(orient="records")
                        category_list = product_list

                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "categoryData": category_list,
                            "seqId": seqId,
                            "type": 2,
                        }
                        category_response.append(response)
                        total_count += len(category_list)
                    else:
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "categoryData": category_list,
                            "type": 2,
                            "seqId": seqId,
                        }
                        category_response.append(response)
                        total_count += len(category_list)
                    seqId += 1
                except:
                    response = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": category_list,
                        "type": 2,
                        "seqId": seqId,
                    }
                    category_response.append(response)
                    total_count += len(category_list)
                    seqId += 1
            else:
                response = {
                    "id": "",
                    "catName": "",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "offers": [],
                    "penCount": 0,
                    "categoryData": category_list,
                    "type": 2,
                    "seqId": seqId,
                }
                category_response.append(response)
                total_count += len(category_list)

            # get the store details
            if zone_id != "":
                json_response = store_function_zonewise(
                    language,
                    zone_id,
                    user_id,
                    user_latitude,
                    user_longtitude,
                    timezone_data,
                    from_data,
                    to_data,
                )
                store_response_data = json_response["data"]
                specialities = json_response["specialities"]
                scount = json_response["scount"]
            else:
                store_response_data = {}
                specialities = []
                scount = 0

            store_data = OrderedDict(sorted(store_response_data.items()))

            category_data = sorted(category_response, key=lambda k: k["seqId"])

            # banners data of store
            bannerq = {}
            if zone_id and city_id:
                bannerq["city.cityId"] = str(city_id)
                bannerq["zones.zoneId"] = str(zone_id)
            elif zone_id:
                bannerq["zones.zoneId"] = str(zone_id)
            elif city_id:
                bannerq["city.cityId"] = str(city_id)
            banners_count = db.banner.find(bannerq).count()
            if banners_count:
                banners = json.loads(json_util.dumps(db.banner.find(bannerq)))

            last_response = {
                "data": {
                    "specialities": specialities,
                    "categoryData": category_data,
                    "totalCatCount": total_count,
                    "storeData": store_data,
                    "storeDataCount": scount,
                    "banners": banners,
                }
            }
            return JsonResponse(last_response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class ClubMartStores(APIView):
    @swagger_auto_schema(
        method="post",
        tags=["Clubmart Stores"],
        operation_description="API to get the stores for clubmart",
        required=["AUTHORIZATION"],
        manual_parameters=CLUBMART_MANUAL_PARAM_REQ,
        request_body=CLUBMART_STORES_REQ_BODY,
        responses=COMMON_RESPONSE,
    )
    @action(detail=False, methods=["post"])
    def post(self, request):
        try:
            auth_err = UtilsObj.check_authentication(request)
            if auth_err:
                return auth_err
            language = UtilsObj.get_lang(request)

            try:
                user_id = UtilsObj.get_userId(request)
            except:
                user_id = "5df87244cb12e160bf694309"

            err = UtilsObj.check_req_params(request.data, ["zoneId"])
            if err:
                return err

            rdata = request.data
            store_response_data = []

            zone_id = rdata.get("zoneId", "")
            store_category_id = rdata.get("storeCategoryId", False)
            speiciality_id = rdata.get("specialityId", False)
            user_latitude = float(rdata.get("lat", 12.9692))
            user_longtitude = float(rdata.get("long", 77.7499))
            timezone_data = rdata.get("timezone", "")
            timezone_data = timezone_data.replace("%2F", "/")
            from_data = rdata.get("fromData", 0)
            to_data = rdata.get("toData", 50)

            query = {}
            must_match = []
            # get store specialized category details
            if zone_id and store_category_id and speiciality_id:
                must_match.extend(
                    [
                        {"match": {"serviceZones.zoneId": str(zone_id)}},
                        {"match": {"specialities": str(speiciality_id)}},
                        {"match": {"categoryId": str(store_category_id)}},
                    ]
                )
            elif zone_id and not speiciality_id and store_category_id:
                must_match.extend(
                    [
                        {"match": {"serviceZones.zoneId": str(zone_id)}},
                        {"match": {"categoryId": str(store_category_id)}},
                    ]
                )
            elif zone_id and speiciality_id and not store_category_id:
                must_match.extend(
                    [
                        {"match": {"serviceZones.zoneId": str(zone_id)}},
                        {"match": {"specialities": str(speiciality_id)}},
                    ]
                )
            elif zone_id:
                must_match.extend([{"match": {"serviceZones.zoneId": str(zone_id)}}])
            else:
                return JsonResponse({"No data provided to find store."}, safe=False, status=404)

            geo_distance_sort = {
                "_geo_distance": {
                    "distance_type": "plane",
                    "location": {"lat": float(user_latitude), "lon": float(user_longtitude)},
                    "order": "asc",
                    "unit": "km",
                }
            }

            sort_query = [geo_distance_sort]
            query = {
                "query": {"bool": {"must": must_match}},
                "size": int(to_data),
                "from": int(from_data),
                "sort": sort_query,
            }

            res = es.search(
                index=index_store,
                body=query,
                filter_path=["hits.total", "hits.hits._id", "hits.hits.sort", "hits.hits._source"],
            )

            json_response = store_function_zonewise(
                language,
                zone_id,
                user_id,
                user_latitude,
                user_longtitude,
                timezone_data,
                from_data,
                to_data,
                res,
            )
            store_response_data = json_response["data"]
            specialities = json_response["specialities"]
            scount = json_response["scount"]

            store_data = OrderedDict(sorted(store_response_data.items()))

            last_response = {
                "data": {
                    "storeData": store_data,
                    "storeDataCount": scount,
                }
            }
            return JsonResponse(last_response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


"""
    get the delivery slot
    :parameter
    zoneId
"""


class DeliverySlot(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the data for delivery slot",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="loginType",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="getting the data for retailer or distributor. "
                "1 for retailer \n"
                "2 for distributor",
                default="1",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5df8b6ea8798dc19d926bd28",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular zone",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            start_time = time.time()
            zone_id = request.GET.get("zoneId", "")
            token = request.META["HTTP_AUTHORIZATION"]
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif zone_id == "":
                response_data = {
                    "message": "zone id is empty",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=404)
            elif zone_id == "undefined":
                response_data = {
                    "message": "zone id is empty",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=404)
            else:
                if zone_id != "":
                    driver_roaster = next_availbale_driver_roaster(zone_id)
                    next_delivery_slot = driver_roaster["text"]
                    slot_id = driver_roaster["slotId"]
                    next_availbale_driver_time = driver_roaster["productText"]
                else:
                    next_delivery_slot = ""
                    slot_id = ""
                    next_availbale_driver_time = ""
                last_response = {
                    "data": {
                        "nextDeliverySlot": next_delivery_slot,
                        "nextDriverAvailableTime": next_availbale_driver_time,
                        "slotId": slot_id,
                    }
                }
                return JsonResponse(last_response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class UserRecentView(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the data for recent view",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="storeId",
                default="5e20914ac348027af2f9028e",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store id from which store we need to get the data",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5df8b6ea8798dc19d926bd28",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular zone",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default=MEAT_STORE_CATEGORY_ID,
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="integrationType",
                default="0",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for filter out the products base on product setup configuration, value should be"
                "0 for All products, "
                "1 for Only Magento Products, "
                "2 for Only Shopify Products, "
                "3 for Only Roadyo or shopar products",
            ),
            openapi.Parameter(
                name="cityId",
                default="5df7b7218798dc2c1114e6bf",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="city id from which city we need to show price, for city pricing, mainly we are using for meat flow",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            store_id = request.GET.get("storeId", "")
            zone_id = request.GET.get("zoneId", "")
            if zone_id == "":
                zone_id = request.GET.get("z_id", "")
            integration_type = int(request.GET.get("integrationType", 0))
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            city_id = request.GET.get("cityId", "5df7b7218798dc2c1114e6bf")
            language = (
                str(request.META["HTTP_LANGUAGE"]) if "HTTP_LANGUAGE" in request.META else "en"
            )
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            token = request.META["HTTP_AUTHORIZATION"]
            last_json_response = []
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            else:
                user_id = json.loads(token)["userId"]
                if "loginType" in json.loads(token):
                    login_type = json.loads(token)["loginType"]
                else:
                    login_type = 1

                try:
                    institution_type = json.loads(token)["metaData"]["institutionType"]
                except:
                    institution_type = 1

                product_for = [0]
                if institution_type == 1:
                    product_for.append(1)
                else:
                    product_for.append(2)

                try:
                    resData = []
                    mongo_query = {"userid": user_id, "store_category_id": store_category_id}
                    if store_id == "":
                        pass
                    else:
                        mongo_query["storeid"] = store_id
                    product_details = (
                        db.userRecentView.find(mongo_query)
                        .sort([("createdtimestamp", -1)])
                        .skip(0)
                        .limit(10)
                    )
                    total_count = db.userRecentView.find(mongo_query).count()
                    if product_details.count() == 0:
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "categoryData": [],
                            "type": 5,
                            "seqId": 8,
                        }
                        last_json_response.append(response)
                        last_response = {"data": {"list": last_json_response}}
                        return JsonResponse(last_response, safe=False, status=404)
                    product_ids = []
                    product_timestamp = {}
                    for pro_data in product_details:
                        product_query = {"_id": ObjectId(pro_data["productId"])}
                        product = db.childProducts.find_one(product_query)
                        try:
                            if product is not None:
                                product_ids.append(str(product["parentProductId"]))
                                product_timestamp[product["parentProductId"]] = pro_data[
                                    "createdtimestamp"
                                ]
                            else:
                                pass
                        except:
                            print('exception :',product)
                    if len(product_ids) > 0:
                        category_query = {"storeCategory.storeCategoryId": store_category_id}
                        if zone_id != "":
                            zone_details = zone_find({"_id": ObjectId(zone_id)})
                            category_query["_id"] = ObjectId(zone_details["city_ID"])
                        elif store_id != "":
                            store_details = db.stores.find_one(
                                {"_id": ObjectId(store_id)}, {"cityId": 1}
                            )
                            category_query["_id"] = str(store_details["cityId"])
                        else:
                            pass
                        categoty_details = db.cities.find_one(category_query, {"storeCategory": 1})
                        hyperlocal = False
                        remove_central = False
                        storelisting = False
                        if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                            hyperlocal = False
                            remove_central = False
                            storelisting = False
                        else:
                            if categoty_details is not None:
                                if "storeCategory" in categoty_details:
                                    for cat in categoty_details["storeCategory"]:
                                        if cat["storeCategoryId"] == store_category_id:
                                            if (
                                                cat["hyperlocal"] == True
                                                and cat["storeListing"] == 1
                                            ):
                                                hyperlocal = True
                                                remove_central = True
                                                storelisting = True
                                                store_id = store_id
                                            elif (
                                                cat["hyperlocal"] == True
                                                and cat["storeListing"] == 0
                                            ):
                                                hyperlocal = True
                                                remove_central = True
                                                storelisting = False
                                            else:
                                                hyperlocal = False
                                                remove_central = False
                                                storelisting = False
                                        else:
                                            pass
                                else:
                                    remove_central = False
                                    hyperlocal = False
                                    storelisting = False
                            else:
                                remove_central = False
                                hyperlocal = False
                                storelisting = False

                        query = [{"terms": {"productFor": [0, str(institution_type)]}}]
                        must_not = []
                        should_query = []
                        try:
                            if hyperlocal == True and storelisting == False:
                                if zone_id != "":
                                    # store_data_details = ["0"]
                                    store_data_details = []
                                    if store_category_id == MEAT_STORE_CATEGORY_ID:
                                        zone_details = zone_find({"_id": ObjectId(zone_id)})
                                        store_query = {"_id": ObjectId(zone_details["DCStoreId"])}
                                        store_data = db.stores.find(store_query)
                                    else:
                                        zone_details = zone_find({"_id": ObjectId(zone_id)})
                                        store_query = {
                                            "categoryId": str(store_category_id),
                                            "serviceZones.zoneId": zone_id,
                                            "storeFrontTypeId": {"$ne": 5},
                                            "status": 1,
                                        }
                                        store_data = db.stores.find(store_query)

                                    if store_data.count() > 0:
                                        for store in store_data:
                                            store_data_details.append(str(store["_id"]))
                                        query.append({"terms": {"storeId": store_data_details}})
                                else:
                                    pass
                            elif hyperlocal == True and storelisting == True:
                                if store_id != "":
                                    query.append({"match": {"storeId": store_id}})
                            else:
                                if store_id != "":
                                    query.append({"match": {"storeId": store_id}})
                        except:
                            print("except")
                            pass

                        # ===================check price is available or not=======================
                        if int(institution_type) != 2:
                            must_not.append(
                                {"match": {"units.b2cPricing.b2cproductSellingPrice": 0}}
                            )
                        else:
                            must_not.append(
                                {"match": {"units.b2bPricing.b2bproductSellingPrice": 0}}
                            )

                        # ==========check for integration type================
                        if int(integration_type) == 0:
                            pass
                        elif int(integration_type) == 1:
                            must_not.append({"match": {"magentoId": -1}})
                            must_not.append(
                                {"match": {"units.b2cPricing.b2cproductSellingPrice": 0}}
                            )
                            query.append({"exists": {"field": "magentoId"}})
                        elif int(integration_type) == 2:
                            must_not.append({"term": {"shopify_variant_id.keyword": {"value": ""}}})
                            query.append({"exists": {"field": "shopify_variant_id"}})
                        elif int(integration_type) == 3:
                            query.append({"match": {"magentoId": -1}})
                            query.append({"term": {"shopify_variant_id.keyword": ""}})

                        query.append({"match": {"storeCategoryId": store_category_id}})

                        query.append({"match": {"status": 1}})

                        query.append({"terms": {"parentProductId": list(set(product_ids))}})
                        sort_type = 2
                        sort_query = [
                            {"isCentral": {"order": "desc"}},
                            {"isInStock": {"order": "desc"}},
                            {"units.discountPrice": {"order": "asc"}},
                        ]
                        must_not.append({"match": {"storeId": "0"}})
                        aggs_query = {
                            "group_by_sub_category": {
                                "terms": {
                                    "field": "parentProductId.keyword",
                                    "order": {"avg_score": "desc"},
                                    "size": 20,
                                },
                                "aggs": {
                                    "avg_score": {"max": {"script": "doc.isInStock"}},
                                    "top_sales_hits": {
                                        "top_hits": {
                                            "sort": sort_query,
                                            "_source": {
                                                "includes": [
                                                    "_id",
                                                    "_score",
                                                    "pName",
                                                    "storeId",
                                                    "detailDescription",
                                                    "secondCategoryName",
                                                    "offer",
                                                    "parentProductId",
                                                    "currencySymbol",
                                                    "prescriptionRequired",
                                                    "needsIdProof",
                                                    "saleOnline",
                                                    "uploadProductDetails",
                                                    "currency",
                                                    "pPName",
                                                    "tax",
                                                    "brandTitle",
                                                    "categoryList",
                                                    "images",
                                                    "avgRating",
                                                    "units",
                                                    "productType",
                                                    "storeCategoryId",
                                                    "manufactureName",
                                                    "maxQuantity",
                                                ]
                                            },
                                            "size": 1,
                                        }
                                    },
                                },
                            }
                        }
                        if store_id == "":
                            search_item_query = {
                                "size": 20,
                                "query": {"bool": {"must": query, "must_not": must_not}},
                                "track_total_hits": True,
                                "sort": sort_query,
                                "aggs": aggs_query,
                            }
                        else:
                            search_item_query = {
                                "size": 20,
                                "query": {"bool": {"must": query}},
                                "track_total_hits": True,
                                "sort": sort_query,
                                "aggs": aggs_query,
                            }
                        categoty_data_json = []
                        res = es.search(index=index_products, body=search_item_query)

                        if zone_id != "":
                            driver_roaster = next_availbale_driver_roaster(zone_id)
                            next_delivery_slot = driver_roaster["text"]
                            slot_id = driver_roaster["slotId"]
                            next_availbale_driver_time = driver_roaster["productText"]
                        else:
                            next_delivery_slot = ""
                            slot_id = ""
                            next_availbale_driver_time = ""

                        if store_category_id == MEAT_STORE_CATEGORY_ID:
                            data = search_read_new(
                                res,
                                int(time.time()),
                                language,
                                [],
                                [],
                                1,
                                2,
                                institution_type,
                                store_id,
                                sort_type,
                                store_category_id,
                                0,
                                20,
                                user_id,
                                remove_central,
                                zone_id,
                                "",
                                next_availbale_driver_time,
                                "",
                                city_id,
                                currency_code=currency_code,
                            )
                        else:
                            data = search_read_stores(
                                res,
                                int(time.time()),
                                language,
                                [],
                                [],
                                1,
                                2,
                                1,
                                store_id,
                                sort_type,
                                store_category_id,
                                0,
                                20,
                                user_id,
                                remove_central,
                                zone_id,
                                0,
                                0,
                                True,
                                currency_code,
                                "",
                                token,
                                hyperlocal,
                                storelisting,
                                "",
                            )

                        if len(data["data"]) > 0:
                            for product in data["data"]["products"]:
                                if product["parentProductId"] in product_timestamp:
                                    product["timestamp"] = product_timestamp[
                                        product["parentProductId"]
                                    ]
                                else:
                                    product["timestamp"] = int(datetime.datetime.now().timestamp())
                            additional_info = sorted(
                                data["data"]["products"], key=lambda k: k["timestamp"], reverse=True
                            )
                            data["data"]["products"] = additional_info
                            response = {
                                "id": "",
                                "catName": "Recently Viewed",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": total_count,
                                "categoryData": data["data"],
                                "type": 5,
                                "seqId": 8,
                            }
                            last_json_response.append(response)
                            last_response = {"data": {"list": last_json_response}}
                            return JsonResponse(last_response, safe=False, status=200)
                        else:
                            response = {
                                "id": "",
                                "catName": "Recently Viewed",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": [],
                                "type": 5,
                                "seqId": 8,
                            }
                            last_json_response.append(response)
                            last_response = {"data": {"list": last_json_response}}
                            return JsonResponse(last_response, safe=False, status=404)
                    else:
                        response = {
                            "id": "",
                            "catName": "Recently Viewed",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "categoryData": [],
                            "type": 5,
                            "seqId": 8,
                        }
                        last_json_response.append(response)
                        last_response = {"data": {"list": last_json_response}}
                        return JsonResponse(last_response, safe=False, status=404)
                except Exception as ex:
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    print(
                        "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                        type(ex).__name__,
                        ex,
                    )
                    error = {
                        "id": "",
                        "catName": "Recently Viewed",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": [],
                        "type": 5,
                        "seqId": 8,
                    }
                    last_json_response.append(error)
                    last_response = {"data": {"list": last_json_response}}
                    return JsonResponse(last_response, safe=False, status=500)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class UserRecentBought(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the data for recent bought",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="storeId",
                default="5e20914ac348027af2f9028e",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store id from which store we need to get the data",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5fcf541871d7bd51e6008c94",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="zone id from which zone we need to get the data",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default=GROCERY_STORE_CATEGORY_ID,
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="page",
                default=1,
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="page number from which page need to get the data",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            422: "requeired fieild is missing",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            language = request.META["HTTP_LAUNGUAGE"] if "HTTP_LAUNGUAGE" in request.META else "en"
            store_agg_query = []
            last_json_response = []
            store_id = request.GET.get("storeId", "")
            zone_id = request.GET.get("zoneId", "")
            store_category_id = request.GET.get("storeCategoryId", "")
            page = int(request.GET.get("page", "1"))
            limit = page * 10
            skip = limit - 10
            if store_category_id == "":
                response = {
                    "id": "",
                    "catName": "Recently Bought",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "offers": [],
                    "penCount": 0,
                    "categoryData": [],
                    "type": 3,
                    "seqId": 3,
                }
                last_json_response.append(response)
                last_response = {
                    "data": {"list": last_json_response, "message": "store category id is missing"}
                }
                return JsonResponse(last_response, safe=False, status=422)
            elif token == "":
                response = {
                    "id": "",
                    "catName": "Recently Bought",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "offers": [],
                    "penCount": 0,
                    "categoryData": [],
                    "type": 3,
                    "seqId": 3,
                }
                last_json_response.append(response)
                last_response = {
                    "data": {"list": last_json_response, "message": "token is missing"}
                }
                return JsonResponse(last_response, safe=False, status=400)
            else:
                user_id = json.loads(token)["userId"]
                store_details = []
                if store_id != "":
                    store_details.append(store_id)
                elif zone_id != "":
                    dc_list_details = db.stores.find(
                        {
                            "serviceZones.zoneId": zone_id,
                            "status": 1,
                            "categoryId": store_category_id,
                        }
                    )
                    for dc_seller in dc_list_details:
                        store_details.append(str(dc_seller["_id"]))
                else:
                    pass

                if len(store_details) > 0:
                    store_agg_query.append({"$match": {"storeId": {"$in": store_details}}})
                store_agg_query.append({"$match": {"products.status.status": 7}})
                store_agg_query.append({"$match": {"createdBy.userId": user_id}})
                store_agg_query.append({"$match": {"storeCategoryId": store_category_id}})
                store_agg_query.append({"$project": {"products": 1}})
                store_agg_query.append({"$unwind": "$products"})
                store_agg_query.append({"$sort": {"createdTimeStamp": -1}})
                store_agg_query.append({"$skip": skip})
                store_agg_query.append({"$limit": limit})
                store_agg_query.append(
                    {"$group": {"products": {"$push": "$products.centralProductId"}, "_id": None}}
                )
                store_order_data = db.storeOrder.aggregate(store_agg_query)
                store_agg_count_query = []
                if len(store_details) > 0:
                    store_agg_count_query.append({"$match": {"storeId": {"$in": store_details}}})
                store_agg_count_query.append({"$match": {"products.status.status": 7}})
                store_agg_count_query.append({"$match": {"createdBy.userId": user_id}})
                store_agg_count_query.append({"$match": {"storeCategoryId": store_category_id}})
                store_agg_count_query.append({"$project": {"products": 1}})
                store_agg_count_query.append({"$unwind": "$products"})
                store_agg_count_query.append({"$count": "total_products"})
                total_product_data = db.storeOrder.aggregate(store_agg_count_query)
                total_product_count = 0
                for count in total_product_data:
                    total_product_count = count["total_products"]
                recently_bought = []
                must_not = []
                should_query = []
                for product in store_order_data:
                    must_query = [
                        {"match": {"status": 1}},
                        {"match": {"storeCategoryId": str(store_category_id)}},
                        {"terms": {"parentProductId": product["products"]}},
                    ]
                    if len(store_details) > 0:
                        must_query.append({"terms": {"storeId": store_details}})
                    sort_query = [
                        {"isInStock": {"order": "desc"}},
                        {"units.discountPrice": {"order": "asc"}},
                    ]

                    must_query.append({"match": {"status": 1}})

                    must_not.append({"match": {"storeId": "0"}})
                    aggr_json = {
                        "group_by_sub_category": {
                            "terms": {
                                "field": "parentProductId.keyword",
                                "order": {"avg_score": "desc"},
                                "size": 20,
                            },
                            "aggs": {
                                "avg_score": {"max": {"script": "doc.isInStock"}},
                                "top_sales_hits": {
                                    "top_hits": {
                                        "sort": sort_query,
                                        "_source": {
                                            "includes": [
                                                "_id",
                                                "_score",
                                                "pName",
                                                "prescriptionRequired",
                                                "needsIdProof",
                                                "saleOnline",
                                                "uploadProductDetails",
                                                "storeId",
                                                "parentProductId",
                                                "currencySymbol",
                                                "currency",
                                                "pPName",
                                                "tax",
                                                "offer",
                                                "brandTitle",
                                                "categoryList",
                                                "images",
                                                "avgRating",
                                                "units",
                                                "storeCategoryId",
                                                "manufactureName",
                                                "maxQuantity",
                                            ]
                                        },
                                        "size": 1,
                                    }
                                },
                            },
                        }
                    }
                    search_item_query = {
                        "query": {"bool": {"must": must_query, "must_not": must_not}},
                        "track_total_hits": True,
                        "sort": sort_query,
                        "aggs": aggr_json,
                    }
                    res = es.search(index=index_products, body=search_item_query)
                    try:
                        if "value" in res["hits"]["total"]:
                            if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                                response = {
                                    "id": "",
                                    "catName": "Recently Bought",
                                    "imageUrl": "",
                                    "bannerImageUrl": "",
                                    "websiteImageUrl": "",
                                    "websiteBannerImageUrl": "",
                                    "offers": [],
                                    "penCount": 0,
                                    "categoryData": [],
                                    "type": 3,
                                    "seqId": 3,
                                }
                                last_json_response.append(response)
                                last_response = {"data": {"list": last_json_response}}
                                return JsonResponse(last_response, safe=False, status=404)
                        else:
                            if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                                response = {
                                    "id": "",
                                    "catName": "Recently Bought",
                                    "imageUrl": "",
                                    "bannerImageUrl": "",
                                    "websiteImageUrl": "",
                                    "websiteBannerImageUrl": "",
                                    "offers": [],
                                    "penCount": 0,
                                    "categoryData": [],
                                    "type": 3,
                                    "seqId": 3,
                                }
                                last_json_response.append(response)
                                last_response = {"data": {"list": last_json_response}}
                                return JsonResponse(last_response, safe=False, status=404)
                    except Exception as ex:
                        print(
                            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                            type(ex).__name__,
                            ex,
                        )
                        if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                            response = {
                                "id": "",
                                "catName": "Recently Bought",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": [],
                                "type": 3,
                                "seqId": 3,
                            }
                            last_json_response.append(response)
                            last_response = {"data": {"list": last_json_response}}
                            return JsonResponse(last_response, safe=False, status=404)

                    main_sellers = []
                    if zone_id != "":
                        store_details = db.stores.find(
                            {
                                "serviceZones.zoneId": zone_id,
                                "storeFrontTypeId": {"$ne": 5},
                                "status": 1,
                            }
                        )
                        for seller in store_details:
                            main_sellers.append(str(seller["_id"]))

                    if zone_id != "":
                        driver_roaster = next_availbale_driver_roaster(zone_id)
                    else:
                        driver_roaster = {}

                    recently_bought = product_modification(
                        res["aggregations"]["group_by_sub_category"]["buckets"],
                        language,
                        "",
                        zone_id,
                        "",
                        store_category_id,
                        0,
                        True,
                        main_sellers,
                        driver_roaster,
                        city_id="",
                    )
                    if len(recently_bought) == 0:
                        response = {
                            "id": "",
                            "catName": "Recently Bought",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "categoryData": [],
                            "type": 3,
                            "seqId": 3,
                        }
                        last_json_response.append(response)
                        last_response = {"data": {"list": last_json_response}}
                        return JsonResponse(last_response, safe=False, status=404)
                    else:
                        if len(recently_bought) > 0:
                            dataframe = pd.DataFrame(recently_bought)
                            # dataframe = dataframe.drop_duplicates(subset='parentProductId', keep="last")
                            details = dataframe.to_json(orient="records")
                            data = json.loads(details)
                            recent_data = validate_units_data(data, False)
                            newlist = sorted(
                                recent_data, key=lambda k: k["createdTimestamp"], reverse=True
                            )
                        else:
                            newlist = []
                        response = {
                            "id": "",
                            "catName": "Recently Bought",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": total_product_count,
                            "categoryData": newlist,
                            "type": 3,
                            "seqId": 3,
                        }
                        last_json_response.append(response)
                        last_response = {"data": {"list": last_json_response}}
                        return JsonResponse(last_response, safe=False, status=200)
                response = {
                    "id": "",
                    "catName": "Recently Bought",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "offers": [],
                    "penCount": 0,
                    "categoryData": [],
                    "type": 3,
                    "seqId": 3,
                }
                last_json_response.append(response)
                last_response = {"data": {"list": last_json_response}}
                return JsonResponse(last_response, safe=False, status=404)
        except Exception as ex:
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            response = {
                "id": "",
                "catName": "Recently Bought",
                "imageUrl": "",
                "bannerImageUrl": "",
                "websiteImageUrl": "",
                "websiteBannerImageUrl": "",
                "offers": [],
                "penCount": 0,
                "categoryData": [],
                "type": 3,
                "seqId": 3,
            }
            last_json_response.append(response)
            last_response = {"data": {"list": last_json_response}}
            return JsonResponse(last_response, safe=False, status=500)


class PopularItems(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the data for popular item",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="storeId",
                default="5e20914ac348027af2f9028e",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store id from which store we need to get the data",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5df8b6ea8798dc19d926bd28",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular zone",
            ),
            openapi.Parameter(
                name="page",
                default="1",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="pagination from which page we need to get the data",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default=MEAT_STORE_CATEGORY_ID,
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="integrationType",
                default="0",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for filter out the products base on product setup configuration, value should be"
                "0 for All products, "
                "1 for Only Magento Products, "
                "2 for Only Shopify Products, "
                "3 for Only Roadyo or shopar products",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        global remove_central
        try:
            store_id = request.GET.get("storeId", "")
            zone_id = request.GET.get("zoneId", "")
            page = int(request.GET.get("page", 1))
            integration_type = int(request.GET.get("integrationType", 0))
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            language = (
                str(request.META["HTTP_LANGUAGE"]) if "HTTP_LANGUAGE" in request.META else "en"
            )
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            token = request.META["HTTP_AUTHORIZATION"]
            last_json_response = []
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            else:
                try:
                    login_type = json.loads(token)["metaData"]["institutionType"]
                except:
                    login_type = 1
                product_for = [0]
                if login_type == 1:
                    product_for.append("1")
                else:
                    product_for.append("2")
                if zone_id != "":
                    city_details = db.zones.find_one({"_id": ObjectId(zone_id)}, {"city_ID": 1})
                    categoty_details = db.cities.find_one(
                        {
                            "storeCategory.storeCategoryId": store_category_id,
                            "_id": ObjectId(city_details["city_ID"]),
                        },
                        {"storeCategory": 1},
                    )
                else:
                    categoty_details = db.cities.find_one(
                        {"storeCategory.storeCategoryId": store_category_id}, {"storeCategory": 1}
                    )

                hyperlocal = False
                remove_central = False
                storelisting = False
                if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                    hyperlocal = False
                    remove_central = False
                    storelisting = False
                else:
                    if categoty_details is not None:
                        if "storeCategory" in categoty_details:
                            for cat in categoty_details["storeCategory"]:
                                if cat["storeCategoryId"] == store_category_id:
                                    if cat["hyperlocal"] == True and cat["storeListing"] == 1:
                                        hyperlocal = True
                                        remove_central = True
                                        storelisting = True
                                        store_id = store_id
                                    elif cat["hyperlocal"] == True and cat["storeListing"] == 0:
                                        hyperlocal = True
                                        remove_central = True
                                        storelisting = False
                                    else:
                                        hyperlocal = False
                                        remove_central = False
                                        storelisting = False
                                else:
                                    pass
                        else:
                            remove_central = False
                            hyperlocal = False
                            storelisting = False
                    else:
                        remove_central = False
                        hyperlocal = False
                        storelisting = False

                if hyperlocal == True and storelisting == True:
                    zone_id == ""
                elif hyperlocal == True and storelisting == False:
                    store_id == ""
                else:
                    pass

                user_id = json.loads(token)["userId"]
                # user_id = "5df87244cb12e160bf694309"
                # ============================find all dc in zone===========================================
                dc_seller_list = []
                if zone_id != "":
                    dc_list_details = db.stores.find(
                        {"serviceZones.zoneId": zone_id, "storeFrontTypeId": 5, "status": 1}
                    )
                    for dc_seller in dc_list_details:
                        dc_seller_list.append(str(dc_seller["_id"]))
                else:
                    pass
                # ==================================store details page======================================
                store_data_json = []
                store_details = []
                if hyperlocal == False and storelisting == False:
                    zone_id = ""
                    store_id = ""
                elif hyperlocal == True and storelisting == True:
                    store_details.append(str(store_id))
                    store_data_json.append(ObjectId(store_id))
                if hyperlocal == True and storelisting == False:
                    store_data = db.stores.find(
                        {
                            "categoryId": str(store_category_id),
                            "serviceZones.zoneId": zone_id,
                            "status": 1,
                        }
                    )
                    for store in store_data:
                        store_details.append(str(store["_id"]))
                        store_data_json.append(ObjectId(store["_id"]))
                else:
                    pass
                try:
                    resData = []
                    if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                        skip = page * 40 - 40
                        limit = skip + 40
                    else:
                        skip = page * 6 - 6
                        limit = skip + 6

                    query = {}
                    if store_category_id:
                        query["store_category_id"] = store_category_id

                    if hyperlocal == False and storelisting == False:
                        pass
                    elif hyperlocal == True and storelisting == False:
                        query["zone_id"] = zone_id
                    elif hyperlocal == True and storelisting == True:
                        query["store_id"] = store_id
                    else:
                        pass
                    data = {"central_product_id": 1, "_id": 0}

                    product_data = list(
                        db.popularProducts.find(query, data).sort([("popular_score", -1)])
                    )
                    if product_data is None or len(product_data) == 0:
                        product_data = list(
                            db.zoneWisePopularProducts.find(query, data).sort([("popular_score", -1)])
                            )
                    product_data = [product["central_product_id"] for product in product_data]
                    product_ids = list(set(product_data))
                    if len(product_ids) > 0:
                        must_query = []
                        must_not = []
                        should_query = []
                        must_query.append({"terms": {"_id": product_ids}})
                        must_query.append({"terms": {"productFor": product_for}})
                        must_query.append({"match": {"status": 1}})
                        must_query.append({"match": {"storeCategoryId": store_category_id}})
                        # ===================check price is available or not=======================
                        if int(login_type) != 2:
                            must_not.append(
                                {"match": {"units.b2cPricing.b2cproductSellingPrice": 0}}
                            )
                        else:
                            must_not.append(
                                {"match": {"units.b2bPricing.b2bproductSellingPrice": 0}}
                            )

                        sort_query = [{"units.floatValue": {"order": "asc"}}]
                        if int(integration_type) != 3:
                            elatic_search_query = {
                                "query": {"bool": {"must": must_query, "must_not": must_not}},
                                "size": len(product_ids),
                                "from": skip * limit,
                                "sort": sort_query,
                            }
                        else:
                            elatic_search_query = {
                                "query": {"bool": {"must": must_query}},
                                "size": len(product_ids),
                                "from": skip * limit,
                                "sort": sort_query,
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
                        language = language
                        popular_supplier_data = []
                        try:
                            for product in res["hits"]["hits"][skip:limit]:
                                is_dc_linked = False
                                variant_data = []
                                best_supplier = {}
                                if "units" in product["_source"]:
                                    if "suppliers" in product["_source"]["units"][0]:
                                        best_supplier, product_tag = best_supplier_function(
                                            product["_id"],
                                            product["_source"]["units"][0]["suppliers"],
                                            store_id,
                                            remove_central,
                                            zone_id,
                                            store_category_id,
                                            PHARMACY_STORE_CATEGORY_ID,
                                        )
                                    else:
                                        pass
                                else:
                                    pass

                                if len(best_supplier) > 0:
                                    if "storeName" in best_supplier:
                                        best_supplier["storeName"] = (
                                            best_supplier["storeName"][language]
                                            if language in best_supplier["storeName"]
                                            else best_supplier["storeName"]["en"]
                                        )
                                    popular_supplier_data.append(
                                        ObjectId(best_supplier["productId"])
                                    )
                        except:
                            pass
                        if len(popular_supplier_data) > 0:
                            full_product_data = db.childProducts.find(
                                {"_id": {"$in": popular_supplier_data}, "status": 1}
                            )
                            for child_product_details in full_product_data:
                                try:
                                    available_qty = child_product_details["units"][0][
                                        "availableQuantity"
                                    ]
                                except:
                                    available_qty = 0
                                    # ===============================offer data======================================
                                offers_details = []
                                offer_details_data = []
                                if "offer" in child_product_details:
                                    for offer in child_product_details["offer"]:
                                        offer_terms = db.offers.find_one(
                                            {
                                                "_id": ObjectId(offer["offerId"]),
                                                "status": 1,
                                                "storeCategoryId": store_category_id,
                                                "offerFor": {"$in": product_for},
                                            }
                                        )
                                        if offer_terms is not None:
                                            if offer_terms["startDateTime"] <= int(time.time()):
                                                offer["termscond"] = offer_terms["termscond"]
                                                offer["name"] = offer_terms["name"]["en"]
                                                offer["discountValue"] = offer_terms[
                                                    "discountValue"
                                                ]
                                                offer["discountType"] = offer_terms["offerType"]
                                                offers_details.append(offer)
                                                offer_details_data.append(
                                                    {
                                                        "offerId": offer["offerId"],
                                                        "offerName": offer["offerName"]["en"],
                                                        "webimages": offer["webimages"]["image"],
                                                        "mobimage": offer["images"]["image"],
                                                        "discountValue": offer["discountValue"],
                                                    }
                                                )
                                            else:
                                                pass
                                        else:
                                            pass
                                else:
                                    pass

                                if len(offers_details) > 0:
                                    best_offer = max(
                                        offers_details, key=lambda x: x["discountValue"]
                                    )
                                    currdate = datetime.datetime.now().replace(
                                        hour=23, minute=59, second=59, microsecond=59
                                    )
                                    eastern = timezone(timezonename)
                                    currlocal = eastern.localize(currdate)
                                    best_offer["endDateTimeISO"] = (
                                        int(((currlocal).timestamp())) * 1000
                                    )
                                else:
                                    best_offer = {}

                                # ======================================product seo======================================================
                                if "productSeo" in child_product_details:
                                    try:
                                        if len(child_product_details["productSeo"]["title"]) > 0:
                                            title = (
                                                child_product_details["productSeo"]["title"][
                                                    language
                                                ]
                                                if language
                                                in child_product_details["productSeo"]["title"]
                                                else child_product_details["productSeo"]["title"][
                                                    "en"
                                                ]
                                            )
                                        else:
                                            title = ""
                                    except:
                                        title = ""

                                    try:
                                        if (
                                            len(child_product_details["productSeo"]["description"])
                                            > 0
                                        ):
                                            description = (
                                                child_product_details["productSeo"]["description"][
                                                    language
                                                ]
                                                if language
                                                in child_product_details["productSeo"][
                                                    "description"
                                                ]
                                                else child_product_details["productSeo"][
                                                    "description"
                                                ]["en"]
                                            )
                                        else:
                                            description = ""
                                    except:
                                        description = ""

                                    try:
                                        if len(child_product_details["productSeo"]["metatags"]) > 0:
                                            metatags = (
                                                child_product_details["productSeo"]["metatags"][
                                                    language
                                                ]
                                                if language
                                                in child_product_details["productSeo"]["metatags"]
                                                else child_product_details["productSeo"][
                                                    "metatags"
                                                ]["en"]
                                            )
                                        else:
                                            metatags = ""
                                    except:
                                        metatags = ""

                                    try:
                                        if len(child_product_details["productSeo"]["slug"]) > 0:
                                            slug = (
                                                child_product_details["productSeo"]["slug"][
                                                    language
                                                ]
                                                if language
                                                in child_product_details["productSeo"]["slug"]
                                                else child_product_details["productSeo"]["slug"][
                                                    "en"
                                                ]
                                            )
                                        else:
                                            slug = ""
                                    except:
                                        slug = ""
                                    product_seo = {
                                        "title": title,
                                        "description": description,
                                        "metatags": metatags,
                                        "slug": slug,
                                    }
                                else:
                                    product_seo = {
                                        "title": "",
                                        "description": "",
                                        "metatags": "",
                                        "slug": "",
                                    }
                                tax_value = []

                                # =========================================pharmacy details=========================================
                                if "prescriptionRequired" in child_product_details:
                                    try:
                                        if int(child_product_details["prescriptionRequired"]) == 0:
                                            prescription_required = False
                                        else:
                                            prescription_required = True
                                    except:
                                        prescription_required = False

                                else:
                                    prescription_required = False

                                if "saleOnline" in child_product_details:
                                    if child_product_details["saleOnline"] == 0:
                                        sales_online = False
                                    else:
                                        sales_online = True
                                else:
                                    sales_online = False

                                if "uploadProductDetails" in child_product_details:
                                    upload_details = child_product_details["uploadProductDetails"]
                                else:
                                    upload_details = ""

                                # ========================= for the get the linked the unit data====================================
                                for link_unit in child_product_details["units"][0]["attributes"]:
                                    try:
                                        for attrlist in link_unit["attrlist"]:
                                            try:
                                                if attrlist is None:
                                                    pass
                                                else:
                                                    if attrlist["linkedtounit"] == 1:
                                                        if attrlist["measurementUnit"] == "":
                                                            attr_name = (
                                                                str(attrlist["value"][language])
                                                                if language in attrlist["value"]
                                                                else str(attrlist["value"]["en"])
                                                            )
                                                        else:
                                                            attr_name = (
                                                                str(attrlist["value"][language])
                                                                + " "
                                                                + attrlist["measurementUnit"]
                                                                if language in attrlist["value"]
                                                                else str(attrlist["value"]["en"])
                                                                + " "
                                                                + attrlist["measurementUnit"]
                                                            )
                                                        variant_data.append(
                                                            {
                                                                "attrname": attrlist["attrname"][
                                                                    "en"
                                                                ],
                                                                "value": str(attr_name),
                                                                "name": attrlist["attrname"]["en"],
                                                            }
                                                        )
                                                    else:
                                                        pass
                                            except:
                                                pass
                                    except:
                                        pass
                                # =========================for max quantity=================================================
                                if "maxQuantity" in child_product_details:
                                    if child_product_details["maxQuantity"] != "":
                                        max_quantity = int(child_product_details["maxQuantity"])
                                    else:
                                        max_quantity = 30
                                else:
                                    max_quantity = 30
                                # ==========================================================================================
                                if "allowOrderOutOfStock" in child_product_details:
                                    allow_out_of_order = child_product_details[
                                        "allowOrderOutOfStock"
                                    ]
                                else:
                                    allow_out_of_order = False

                                mobile_images = []

                                if "productType" in child_product_details:
                                    if child_product_details["productType"] == 2:
                                        combo_product = True
                                    else:
                                        combo_product = False
                                else:
                                    combo_product = False

                                minimum_order_qty = 1
                                unit_package_type = "Box"
                                unit_moq_type = "Box"
                                if login_type == 1:
                                    try:
                                        base_price = child_product_details["units"][0][
                                            "b2cPricing"
                                        ][0]["b2cproductSellingPrice"]
                                    except:
                                        base_price = child_product_details["units"][0]["floatValue"]
                                else:
                                    if "b2bminimumOrderQty" in child_product_details["units"][0]:
                                        minimum_order_qty = child_product_details["units"][0][
                                            "b2bminimumOrderQty"
                                        ]
                                    else:
                                        pass
                                    if "b2bunitPackageType" in child_product_details["units"][0]:
                                        try:
                                            if (
                                                type(
                                                    child_product_details["units"][0][
                                                        "b2bunitPackageType"
                                                    ]["en"]
                                                )
                                                == str
                                            ):
                                                unit_package_type = child_product_details["units"][
                                                    0
                                                ]["b2bunitPackageType"]["en"]
                                                unit_moq_type = child_product_details["units"][0][
                                                    "b2bunitPackageType"
                                                ]["en"]
                                            else:
                                                unit_package_type = child_product_details["units"][
                                                    0
                                                ]["b2bunitPackageType"]["en"]["en"]
                                                unit_moq_type = child_product_details["units"][0][
                                                    "b2bunitPackageType"
                                                ]["en"]["en"]
                                        except:
                                            unit_package_type = "Box"
                                    else:
                                        pass
                                    try:
                                        base_price = child_product_details["units"][0][
                                            "b2bPricing"
                                        ][0]["b2bproductSellingPrice"]
                                    except:
                                        base_price = child_product_details["units"][0]["floatValue"]

                                # ==================================get currecny rate============================
                                try:
                                    currency_rate = currency_exchange_rate[
                                        str(child_product_details["currency"])
                                        + "_"
                                        + str(currency_code)
                                    ]
                                except:
                                    currency_rate = 0
                                currency_details = db.currencies.find_one(
                                    {"currencyCode": currency_code}
                                )
                                if currency_details is not None:
                                    currency_symbol = currency_details["currencySymbol"]
                                    currency = currency_details["currencyCode"]
                                else:
                                    currency_symbol = (
                                        child_product_details["currencySymbol"]
                                        if child_product_details["currencySymbol"] is not None
                                        else "₹"
                                    )
                                    currency = child_product_details["currency"]
                                if float(currency_rate) > 0:
                                    base_price = base_price * float(currency_rate)

                                tax_price = 0
                                if "tax" in child_product_details:
                                    if len(child_product_details["tax"]) == 0:
                                        tax_price = 0
                                    else:
                                        for amount in child_product_details["tax"]:
                                            if "taxValue" in amount:
                                                tax_price = tax_price + (int(amount["taxValue"]))
                                            if "value" in amount:
                                                tax_price = tax_price + (int(amount["value"]))
                                            else:
                                                tax_price = tax_price + 0
                                else:
                                    tax_price = 0

                                if len(best_offer) > 0:
                                    discount_type = (
                                        int(best_offer["discountType"])
                                        if "discountType" in best_offer
                                        else 1
                                    )
                                    discount_value = (
                                        best_offer["discountValue"]
                                        if "discountValue" in best_offer
                                        else 0
                                    )
                                else:
                                    discount_type = 2
                                    discount_value = 0

                                if discount_type == 0:
                                    percentage = 0
                                else:
                                    percentage = int(discount_value)

                                base_price = base_price + ((float(base_price) * tax_price) / 100)

                                # ==============calculate discount price =============================
                                if discount_type == 0:
                                    discount_price = float(discount_value)
                                elif discount_type == 1:
                                    discount_price = (
                                        float(base_price) * float(discount_value)
                                    ) / 100
                                else:
                                    discount_price = 0
                                final_price = base_price - discount_price

                                # try:
                                #     product_name = child_product_details['pName']["en"] if "pName" in child_product_details else child_product_details['pPName']['en']
                                # except:
                                product_name = child_product_details["units"][0]["unitName"]["en"]

                                next_availbale_time = ""
                                if available_qty <= 0:
                                    outOfStock = True
                                else:
                                    outOfStock = False
                                # ===================================variant count======================================
                                seller_data = []
                                # variant_count_data = product['_source']['variantCount'] if "variantCount" in product['_source'] else 1
                                # ===================================variant count======================================
                                variant_query = {"parentProductId": product["_id"], "status": 1}
                                if len(store_data_json) > 0:
                                    variant_query["storeId"] = {"$in": store_data_json}
                                else:
                                    variant_query["storeId"] = ObjectId(
                                        child_product_details["storeId"]
                                    )
                                # product['_source']['variantCount'] if "variantCount" in product['_source'] else 1
                                variant_count_data = db.childProducts.find(variant_query).count()
                                if variant_count_data > 1:
                                    variant_count = True
                                else:
                                    variant_count = False
                                isShoppingList = False

                                if "containsMeat" in child_product_details:
                                    contains_Meat = child_product_details["containsMeat"]
                                else:
                                    contains_Meat = False

                                if "needsIdProof" in child_product_details:
                                    if not child_product_details["needsIdProof"]:
                                        needsIdProof = False
                                    else:
                                        needsIdProof = True
                                else:
                                    needsIdProof = False

                                parent_product_data = db.products.find_one(
                                    {"_id": ObjectId(product["_id"])}
                                )  # for get the average rating of product
                                # =====================from here need to send dc supplier id for the product===============
                                # get product type, is normal or combo or special product
                                product_type = combo_special_type_validation(
                                    str(child_product_details["_id"])
                                )
                                avg_rating = product_avg_rating(
                                    child_product_details["parentProductId"]
                                )

                                addition_info = []
                                if "THC" in child_product_details["units"][0]:
                                    addition_info.append(
                                        {
                                            "seqId": 2,
                                            "attrname": "THC",
                                            "value": str(child_product_details["units"][0]["THC"])
                                            + " %",
                                            "id": "",
                                        }
                                    )
                                else:
                                    pass

                                if "CBD" in child_product_details["units"][0]:
                                    addition_info.append(
                                        {
                                            "seqId": 1,
                                            "attrname": "CBD",
                                            "value": str(child_product_details["units"][0]["CBD"])
                                            + " %",
                                            "id": "",
                                        }
                                    )
                                else:
                                    pass

                                # =================================================canniber product type========================
                                if "cannabisProductType" in child_product_details["units"][0]:
                                    if (
                                        child_product_details["units"][0]["cannabisProductType"]
                                        != ""
                                    ):
                                        cannabis_type_details = db.cannabisProductType.find_one(
                                            {
                                                "_id": ObjectId(
                                                    child_product_details["units"][0][
                                                        "cannabisProductType"
                                                    ]
                                                ),
                                                "status": 1,
                                            }
                                        )
                                        if cannabis_type_details is not None:
                                            addition_info.append(
                                                {
                                                    "seqId": 3,
                                                    "attrname": "Type",
                                                    "value": cannabis_type_details["productType"][
                                                        "en"
                                                    ],
                                                    "id": child_product_details["units"][0][
                                                        "cannabisProductType"
                                                    ],
                                                }
                                            )
                                        else:
                                            pass
                                else:
                                    pass

                                if len(addition_info) > 0:
                                    additional_info = sorted(
                                        addition_info, key=lambda k: k["seqId"], reverse=True
                                    )
                                else:
                                    additional_info = []

                                if (
                                    outOfStock == True
                                    and child_product_details["storeCategoryId"]
                                    == MEAT_STORE_CATEGORY_ID
                                ):
                                    pass
                                else:
                                    model_data = []
                                    if "modelImage" in child_product_details["units"][0]:
                                        if len(child_product_details["units"][0]["modelImage"]) > 0:
                                            model_data = child_product_details["units"][0][
                                                "modelImage"
                                            ]
                                        else:
                                            pass
                                    else:
                                        pass

                                    resData.append(
                                        {
                                            "maxQuantity": max_quantity,
                                            "isComboProduct": combo_product,
                                            "productType": product_type,
                                            "childProductId": str(child_product_details["_id"]),
                                            "availableQuantity": available_qty,
                                            "offerDetailsData": offer_details_data,
                                            "productName": product_name,
                                            "parentProductId": child_product_details[
                                                "parentProductId"
                                            ],
                                            "suppliers": best_supplier,
                                            "supplier": best_supplier,
                                            "containsMeat": contains_Meat,
                                            "isShoppingList": isShoppingList,
                                            "tax": tax_value,
                                            "linkedAttribute": variant_data,
                                            "allowOrderOutOfStock": allow_out_of_order,
                                            "moUnit": "Pcs",
                                            "outOfStock": outOfStock,
                                            "variantData": variant_data,
                                            "addOnsCount": 0,
                                            "variantCount": variant_count,
                                            "prescriptionRequired": prescription_required,
                                            "saleOnline": sales_online,
                                            "uploadProductDetails": upload_details,
                                            "productSeo": product_seo,
                                            "brandName": child_product_details["brandTitle"][
                                                language
                                            ]
                                            if language in child_product_details["brandTitle"]
                                            else child_product_details["brandTitle"]["en"],
                                            "manufactureName": child_product_details[
                                                "manufactureName"
                                            ][language]
                                            if language in child_product_details["manufactureName"]
                                            else "",
                                            "TotalStarRating": round(avg_rating, 2),
                                            "currencySymbol": currency_symbol,
                                            "extraAttributeDetails": additional_info,
                                            "mobileImage": [],
                                            "modelImage": model_data,
                                            "currency": currency,
                                            "storeCategoryId": child_product_details[
                                                "storeCategoryId"
                                            ]
                                            if "storeCategoryId" in child_product_details
                                            else "",
                                            "images": child_product_details["images"],
                                            "mobimages": mobile_images,
                                            "units": child_product_details["units"],
                                            "finalPriceList": {
                                                "basePrice": round(base_price, 2),
                                                "finalPrice": round(final_price, 2),
                                                "discountPrice": round(discount_price, 2),
                                                "discountType": discount_type,
                                                "discountPercentage": percentage,
                                            },
                                            "price": int(final_price),
                                            "isDcAvailable": is_dc_linked,
                                            "discountType": discount_type,
                                            "needsIdProof": needsIdProof,
                                            "unitId": str(
                                                child_product_details["units"][0]["unitId"]
                                            ),
                                            "offer": best_offer,
                                            "MOQData": {
                                                "minimumOrderQty": minimum_order_qty,
                                                "unitPackageType": unit_package_type,
                                                "unitMoqType": unit_moq_type,
                                                "MOQ": str(minimum_order_qty)
                                                + " "
                                                + unit_package_type,
                                            },
                                            "nextSlotTime": next_availbale_time,
                                        }
                                    )
                            else:
                                pass
                        if len(resData) > 0:
                            if store_category_id != MEAT_STORE_CATEGORY_ID:
                                newlist = sorted(
                                    resData, key=lambda k: k["availableQuantity"], reverse=True
                                )
                            else:
                                newlist = sorted(
                                    resData, key=lambda k: k["availableQuantity"], reverse=True
                                )
                            response = {
                                "id": "",
                                "catName": "Popular Items",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": len(product_ids),
                                "categoryData": newlist,
                                "type": 9,
                                "seqId": 5,
                            }
                            last_json_response.append(response)
                            last_response = {"data": {"list": last_json_response}}
                            return JsonResponse(last_response, safe=False, status=200)
                        else:
                            response = {
                                "id": "",
                                "catName": "Popular Items",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": [],
                                "type": 9,
                                "seqId": 5,
                            }
                            last_json_response.append(response)
                            last_response = {"data": {"list": last_json_response}}
                            return JsonResponse(last_response, safe=False, status=404)
                    else:
                        response = {
                            "id": "",
                            "catName": "Popular Items",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "categoryData": [],
                            "type": 9,
                            "seqId": 5,
                        }
                        last_json_response.append(response)
                        last_response = {"data": {"list": last_json_response}}
                        return JsonResponse(last_response, safe=False, status=404)
                except Exception as ex:
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    print(
                        "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                        type(ex).__name__,
                        ex,
                    )
                    response = {
                        "id": "",
                        "catName": "Popular Items",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": [],
                        "type": 9,
                        "seqId": 5,
                    }
                    last_json_response.append(response)
                    last_response = {"data": {"list": last_json_response}}
                    return JsonResponse(last_response, safe=False, status=500)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class BestDeals(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the data for best deals",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="storeId",
                default="5e20914ac348027af2f9028e",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store id from which store we need to get the data",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5df8b6ea8798dc19d926bd28",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular zone",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default=MEAT_STORE_CATEGORY_ID,
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="integrationType",
                default="0",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for filter out the products base on product setup configuration, value should be"
                "0 for All products, "
                "1 for Only Magento Products, "
                "2 for Only Shopify Products, "
                "3 for Only Roadyo or shopar products",
            ),
            openapi.Parameter(
                name="loginType",
                default="1",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="login type of the user. value should be 1 for retailer and 2 for distributor",
            ),
            openapi.Parameter(
                name="cityId",
                default="5df7b7218798dc2c1114e6bf",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="city id from which city we need to show price, for city pricing, mainly we are using for meat flow",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            start_time = time.time()
            store_id = request.GET.get("storeId", "")
            zone_id = request.GET.get("zoneId", "")
            integration_type = int(request.GET.get("integrationType", 0))
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            if zone_id == "":
                zone_id = str(request.META["HTTP_ZONEID"]) if "HTTP_ZONEID" in request.META else ""

            city_id = request.GET.get("cityId", "")
            language = (
                str(request.META["HTTP_LANGUAGE"]) if "HTTP_LANGUAGE" in request.META else "en"
            )
            token = request.META["HTTP_AUTHORIZATION"]
            last_json_response = []
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            zone_id = zone_id.replace("%7D", "")
            # =======================for remove central=================================
            from_redis_data = True
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            else:
                try:
                    print(json.loads(token)["metaData"]["institutionType"])
                    login_type = json.loads(token)["metaData"]["institutionType"]
                except:
                    login_type = 1
                product_for = []
                if login_type == 1:
                    product_for.append("1")
                else:
                    product_for.append("2")
                print('product_for--',product_for)
                must_query = [
                    {"match": {"status": 1}},
                    {"match": {"offer.status": 1}},
                    {"exists": {"field": "offer"}},
                    {"terms": {"productFor": product_for}},
                    {"match": {"storeCategoryId": str(store_category_id)}},
                ]
                if store_id == "":
                    if zone_id == "" or store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                        pass
                    else:
                        if store_category_id == MEAT_STORE_CATEGORY_ID:
                            store_data_details = ["0"]
                            zone_details = zone_find({"_id": ObjectId(zone_id)})
                            try:
                                store_query = {"_id": ObjectId(zone_details["DCStoreId"])}
                            except:
                                store_query = {"status": 100}
                            store_data = store_find(store_query)
                        else:
                            store_data_details = []
                            if zone_id != "" and store_category_id == PHARMACY_STORE_CATEGORY_ID:
                                store_query = {
                                    "categoryId": str(store_category_id),
                                    "serviceZones.zoneId": zone_id,
                                }
                            elif zone_id != "" and store_category_id == MEAT_STORE_CATEGORY_ID:
                                store_query = {
                                    "categoryId": str(store_category_id),
                                    "cityId": city_id,
                                }
                            else:
                                store_query = {
                                    "categoryId": str(store_category_id),
                                    "serviceZones.zoneId": zone_id,
                                    "storeFrontTypeId": {"$ne": 5},
                                    "status": 1,
                                }
                            store_data = store_find(store_query)

                        if store_data.count() > 0:
                            for store in store_data:
                                store_data_details.append(str(store["_id"]))
                            offer_data = db.offers.find(
                                {"storeId": {"$in": store_data_details}, "status": 1}
                            )
                            offer_ids = []
                            if offer_data.count() > 0:
                                for o_id in offer_data:
                                    offer_ids.append(str(o_id["_id"]))
                            if len(offer_ids) > 0:
                                must_query.append({"terms": {"offer.offerId": offer_ids}})
                            must_query.append({"terms": {"storeId": store_data_details}})
                        else:
                            must_query.append({"match": {"zoneId": zone_id}})
                else:
                    offer_data = db.offers.find({"storeId": {"$in": [store_id]}, "status": 1})
                    offer_ids = []
                    if offer_data.count() > 0:
                        for o_id in offer_data:
                            offer_ids.append(str(o_id["_id"]))
                    # if len(offer_ids) > 0:
                    #     must_query.append({"terms": {"offer.offerId": offer_ids}})
                    must_query.append({"match": {"storeId": store_id}})

                sort_query = [
                    {"isCentral": {"order": "desc"}},
                    {"isInStock": {"order": "desc"}},
                    {"units.discountPrice": {"order": "asc"}},
                ]

                must_query.append({"match": {"status": 1}})
                must_not = []
                should_query = []
                must_not.append({"match": {"storeId": "0"}})

                if int(login_type) != 2:
                    must_not.append({"match": {"units.b2cPricing.b2cproductSellingPrice": 0}})
                else:
                    must_not.append({"match": {"units.b2bPricing.b2bproductSellingPrice": 0}})

                if int(integration_type) == 0:
                    pass
                elif int(integration_type) == 1:
                    must_not.append({"match": {"magentoId": -1}})
                    must_query.append({"exists": {"field": "magentoId"}})
                elif int(integration_type) == 2:
                    must_not.append({"term": {"shopify_variant_id.keyword": {"value": ""}}})
                    must_query.append({"exists": {"field": "shopify_variant_id"}})
                elif int(integration_type) == 3:
                    must_query.append({"match": {"magentoId": -1}})
                    must_query.append({"term": {"shopify_variant_id.keyword": ""}})
                aggs_json = {
                    "group_by_sub_category": {
                        "terms": {
                            "field": "parentProductId.keyword",
                            "order": {"avg_score": "desc"},
                            "size": 10,
                        },
                        "aggs": {
                            "avg_score": {"max": {"script": "doc.isInStock"}},
                            "top_sales_hits": {
                                "top_hits": {
                                    "sort": sort_query,
                                    "_source": {
                                        "includes": [
                                            "_id",
                                            "_score",
                                            "pName",
                                            "prescriptionRequired",
                                            "needsIdProof",
                                            "saleOnline",
                                            "uploadProductDetails",
                                            "storeId",
                                            "parentProductId",
                                            "currencySymbol",
                                            "currency",
                                            "pPName",
                                            "tax",
                                            "offer",
                                            "brandTitle",
                                            "categoryList",
                                            "images",
                                            "avgRating",
                                            "units",
                                            "storeCategoryId",
                                            "manufactureName",
                                            "maxQuantity",
                                        ]
                                    },
                                    "size": 1,
                                }
                            },
                        },
                    }
                }

                search_item_query = {
                    "query": {"bool": {"must": must_query, "must_not": must_not}},
                    "track_total_hits": True,
                    "sort": sort_query,
                    "aggs": aggs_json,
                }


                rjId = ""
                if store_id != "":
                    if rjId == "":
                        rjId = str(store_id)
                    else:
                        rjId = rjId + "_" + str(store_id)

                if zone_id != "":
                    if rjId == "":
                        rjId = str(zone_id)
                    else:
                        rjId = rjId + "_" + str(zone_id)

                if integration_type != "":
                    if rjId == "":
                        rjId = str(integration_type)
                    else:
                        rjId = rjId + "_" + str(integration_type)

                if store_category_id != "":
                    if rjId == "":
                        rjId = str(store_category_id)
                    else:
                        rjId = rjId + "_" + str(store_category_id)

                if city_id != "":
                    if rjId == "":
                        rjId = str(city_id)
                    else:
                        rjId = rjId + "_" + str(city_id)

                if currency_code != "":
                    if rjId == "":
                        rjId = str(currency_code)
                    else:
                        rjId = rjId + "_" + str(currency_code)

                print("search_item_query", search_item_query)
                try:
                    redis_response_data = RJ_DEALS.jsonget(rjId)
                except:
                    redis_response_data = {}
                if redis_response_data is None:
                    from_redis_data = False
                    redis_response_data = {}
                redis_response_data = {}
                if not len(redis_response_data):
                    res = es.search(index=index_products, body=search_item_query)
                    from_redis_data = False
                else:
                    res = {"hits": {"total": {"value": 0}}}
                    from_redis_data = True

                print("time for the query in es ##############", time.time()- start_time)
                try:
                    if "value" in res["hits"]["total"]:
                        if res["hits"]["total"] == 0 or "hits" not in res["hits"] and not len(redis_response_data):
                            response = {
                                "id": "",
                                "catName": "",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": [],
                                "type": 3,
                                "seqId": 4,
                            }
                            last_json_response.append(response)
                            last_response = {"data": {"list": last_json_response}}
                            return JsonResponse(last_response, safe=False, status=404)
                    else:
                        if res["hits"]["total"] == 0 or "hits" not in res["hits"] and not len(redis_response_data):
                            response = {
                                "id": "",
                                "catName": "",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": [],
                                "type": 3,
                                "seqId": 4,
                            }
                            last_json_response.append(response)
                            last_response = {"data": {"list": last_json_response}}
                            return JsonResponse(last_response, safe=False, status=404)
                except:
                    if res["hits"]["total"] == 0 or "hits" not in res["hits"] and not len(redis_response_data):
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "categoryData": [],
                            "type": 3,
                            "seqId": 4,
                        }
                        last_json_response.append(response)
                        last_response = {"data": {"list": last_json_response}}
                        return JsonResponse(last_response, safe=False, status=404)
                start_new = time.time()
                main_sellers = []
                if zone_id != "":
                    store_details = db.stores.find(
                        {
                            "serviceZones.zoneId": zone_id,
                            "storeFrontTypeId": {"$ne": 5},
                            "status": 1,
                        }
                    )
                    for seller in store_details:
                        main_sellers.append(str(seller["_id"]))
                if zone_id != "":
                    driver_roaster = next_availbale_driver_roaster(zone_id)
                else:
                    driver_roaster = {}
                if not len(redis_response_data):
                    print("ressss",res["hits"]["total"])
                    print("ressss11",res["aggregations"]["group_by_sub_category"]["buckets"])
                    resData = product_modification(
                        res["aggregations"]["group_by_sub_category"]["buckets"],
                        language,
                        "",
                        zone_id,
                        currency_code,
                        store_category_id,
                        login_type,
                        True,
                        main_sellers,
                        driver_roaster,
                        city_id,
                        rjId
                    )
                else:
                    resData = redis_response_data
                print("############best deals#############", time.time() - start_new)
                if len(resData) == 0:
                    response = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": [],
                        "type": 3,
                        "seqId": 4,
                    }
                    last_json_response.append(response)
                    last_response = {"data": {"list": last_json_response}}
                    return JsonResponse(last_response, safe=False, status=404)
                else:
                    if len(resData) > 0:
                        dataframe = pd.DataFrame(resData)
                        dataframe = dataframe.drop_duplicates(subset="parentProductId", keep="last")
                        details = dataframe.to_json(orient="records")
                        data = json.loads(details)
                        recent_data = validate_units_data(data, True)
                        newlist = sorted(
                            recent_data, key=lambda k: k["availableQuantity"], reverse=True
                        )
                    else:
                        newlist = []
                    if len(newlist) > 0:
                        response = {
                            "id": "",
                            "catName": "Deals of the day",  # "Ofertas del día",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 20,
                            "categoryData": newlist,
                            "type": 3,
                            "seqId": 4,
                        }
                        last_json_response.append(response)
                        last_response = {"data": {"list": last_json_response}}
                        return JsonResponse(last_response, safe=False, status=200)
                    else:
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "categoryData": [],
                            "type": 3,
                            "seqId": 4,
                        }
                        last_json_response.append(response)
                        last_response = {"data": {"list": last_json_response}}
                        return JsonResponse(last_response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            last_json_response = []
            error = {
                "id": "",
                "catName": "Deals of the Day",
                "imageUrl": "",
                "bannerImageUrl": "",
                "websiteImageUrl": "",
                "websiteBannerImageUrl": "",
                "offers": [],
                "penCount": 0,
                "categoryData": [],
                "type": 3,
                "seqId": 4,
            }
            last_json_response.append(error)
            last_response = {"data": {"list": last_json_response}}
            return JsonResponse(last_response, safe=False, status=404)


class SubCategoryList(APIView):
    def get(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            categoryId = request.GET["categoryId"] if "categoryId" in request.GET else ""
            sub_category_id = request.GET["subCategoryId"] if "subCategoryId" in request.GET else ""
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            category_list = []
            banner_deatils_data = []
            # ================== for the logs============================================================================
            if token == "":
                response_data = {
                    "message": "Token Error",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)

            if categoryId != "" and sub_category_id != "":
                sub_cat_data = []
                sub_category_details = category_find({"parentId": ObjectId(categoryId)}, 0, 50)
                sub_cat_query = {"parentId": ObjectId(categoryId)}
                sub_sub_cat_count = category_find_count(sub_cat_query)
                if sub_category_details.count() > 0:
                    category_list = []
                    for sub_sub_cat in sub_category_details:
                        sub_sub_cat_query = {"categoryId": ObjectId(cat["_id"])}
                        sub_cat_data.append(
                            {
                                "id": str(sub_sub_cat["_id"]),
                                "subCategoryName": sub_sub_cat["categoryName"][language],
                                "imageUrl": sub_sub_cat["imageUrl"],
                                "subCatCount": sub_sub_cat_count,
                            }
                        )
                    category_list.append(
                        {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "subCategory": sub_cat_data,
                            "penCount": sub_sub_cat_count,
                        }
                    )
                    response = {"data": category_list, "message": "data found"}
                    return JsonResponse(response, safe=False, status=200)
                else:
                    response = {"data": [], "message": "data not found"}
                    return JsonResponse(response, safe=False, status=404)

            parent_query = {"status": 1, "_id": ObjectId(categoryId)}
            parent_category_data = category_find_one(parent_query)
            # ========================for the category======================================
            cat_query = {"status": 1, "parentId": ObjectId(categoryId), "storeId": "0"}
            category_data = category_find(cat_query, 0, 50)
            # ===============for the second category========================================
            sub_cat_query = {"parentId": ObjectId(categoryId), "status": 1}
            sub_cat_count = category_find_count(sub_cat_query)
            # ===========================end ===============================================
            sub_cat_data = []
            if category_data.count() > 0:
                for cat in category_data:
                    sub_sub_cat_query = {"categoryId": ObjectId(cat["_id"])}
                    sub_sub_cat_count = category_find_count(sub_sub_cat_query)
                    sub_cat_data.append(
                        {
                            "id": str(cat["_id"]),
                            "subCategoryName": cat["categoryName"][language],
                            "imageUrl": cat["mobileIcon"],
                            "subCatCount": sub_sub_cat_count,
                        }
                    )

                category_list.append(
                    {
                        "id": str(categoryId),
                        "catName": parent_category_data["categoryName"][language],
                        "imageUrl": parent_category_data["mobileIcon"],
                        "bannerImageUrl": parent_category_data["mobileIcon"]
                        if "mobileIcon" in parent_category_data
                        else "",
                        "websiteImageUrl": parent_category_data["websiteImage"]
                        if "websiteImage" in parent_category_data
                        else "",
                        "websiteBannerImageUrl": parent_category_data["websiteImage"]
                        if "websiteImage" in parent_category_data
                        else "",
                        "subCategory": sub_cat_data,
                        "penCount": sub_cat_count,
                    }
                )
                response = {
                    "data": category_list,
                    "bannerData": banner_deatils_data,
                    "message": "data found",
                }
                return JsonResponse(response, safe=False, status=200)
            else:
                response = {"data": [], "message": "data not found"}
                return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class SubCategoryProducts(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the category, sub-category, sub-sub-category",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            # openapi.Parameter(
            #     name='currencySymbol', default="INR", required=True, in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, description="currencySymbol for the currency..INR, INR"),
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="loginType",
                default="1",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="login type of the user. value should be 1 for retailer and 2 for distributor",
            ),
            openapi.Parameter(
                name="skip",
                default="0",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="pagination from which page data need",
            ),
            openapi.Parameter(
                name="size",
                default="10",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="pagination for how much data need",
            ),
            openapi.Parameter(
                name="cityId",
                default="5df7b7218798dc2c1114e6bf",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular city",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default=ECOMMERCE_STORE_CATEGORY_ID,
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5df8b6ea8798dc19d926bd28",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="zone id for getting the products from only that zone",
            ),
            openapi.Parameter(
                name="integrationType",
                default="0",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for filter out the products base on product setup configuration, value should be"
                "0 for All products, "
                "1 for Only Magento Products, "
                "2 for Only Shopify Products, "
                "3 for Only Roadyo or shopar products",
            ),
            openapi.Parameter(
                name="storeId",
                default="5df8b7ad8798dc19da1a4b0e",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store id for getting the products from only that store",
            ),
        ],
        responses={
            200: "successfully. subcategory data found",
            404: "data not found. it might be subcategory wise data not found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            start_time = time.time()
            token = request.META["HTTP_AUTHORIZATION"]
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            zone_id = str(request.META["HTTP_ZONEID"]) if "HTTP_ZONEID" in request.META else ""
            store_id = str(request.META["HTTP_STOREID"]) if "HTTP_STOREID" in request.META else ""
            login_type = (
                int(request.META["HTTP_LOGINTYPE"]) if "HTTP_LOGINTYPE" in request.META else 1
            )
            limit = int(request.META["HTTP_SKIP"]) if "HTTP_SKIP" in request.META else 0
            size = int(request.META["HTTP_SIZE"]) if "HTTP_SIZE" in request.META else 100
            integration_type = int(request.GET.get("integrationType", 0))
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            is_ecommerce, remove_central, hide_recent_view, store_listing = validate_store_category(
                store_category_id, ECOMMERCE_STORE_CATEGORY_ID
            )
            try:
                user_id = json.loads(token)["userId"]
            except:
                user_id=""
            if store_listing is True:
                zone_id = ""

            if is_ecommerce is True:
                store_id = ""
                zone_id = ""
            else:
                pass

            if zone_id != "":
                driver_roaster = next_availbale_driver_roaster(zone_id)
                next_availbale_driver_time = driver_roaster["productText"]
            else:
                next_availbale_driver_time = ""

            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            # {"match": {"units.isPrimary": True}}
            must_not = []
            should_query = []
            must_query = [
                {"match": {"status": 1}},
                {"match": {"storeCategoryId": str(store_category_id)}},
                # {"exists": {"field": "categoryList"}},
            ]
            if store_id != "":
                must_query.append({"match": {"storeId": str(store_id)}})
            elif zone_id != "":
                store_data_details = store_validation_function(store_category_id, zone_id)
                # store_data_details.append("0")
                must_query.append({"terms": {"storeId": store_data_details}})

            sub_query = [
                {"isCentral": {"order": "desc"}},
                {"isInStock": {"order": "desc"}},
                {"units.discountPrice": {"order": "asc"}},
            ]
            # if store_id != "":
            must_not.append({"match": {"storeId": "0"}})
            must_not.append({"match": {"firstCategoryName": ""}})
            must_not.append({"match": {"secondCategoryName": ""}})
            must_not.append({"match": {"units.b2cPricing.b2cproductSellingPrice": 0}})
            aggs_query = {
                "group_by_sub_category": {
                    "terms": {
                        "field": "secondCategoryName.keyword",
                        "size": int(size),
                        "order": {"avg_score": "desc"},
                    },
                    "aggs": {
                        "avg_score": {"sum": {"field": "doc.isInStock"}},
                        "top_hits": {
                            "terms": {"field": "parentProductId.keyword", "size": 6},
                            "aggs": {
                                "top_sales_hits": {
                                    "top_hits": {
                                        "sort": [
                                            {"isCentral": {"order": "desc"}},
                                            {"isInStock": {"order": "desc"}},
                                            {"units.discountPrice": {"order": "asc"}},
                                        ],
                                        "_source": {
                                            "includes": [
                                                "_id",
                                                "pName",
                                                "storeId",
                                                "isInStock",
                                                "parentProductId",
                                                "currencySymbol",
                                                "currency",
                                                "pPName",
                                                "needsIdProof",
                                                "tax",
                                                "brandTitle",
                                                "categoryList",
                                                "images",
                                                "avgRating",
                                                "units",
                                                "storeCategoryId",
                                                "manufactureName",
                                                "maxQuantity",
                                            ]
                                        },
                                        "size": 1,
                                    }
                                }
                            },
                        },
                    },
                }
            }

            if int(integration_type) == 0:
                pass
            elif int(integration_type) == 1:
                must_not.append({"match": {"magentoId": -1}})
                must_query.append({"exists": {"field": "magentoId"}})
            elif int(integration_type) == 2:
                must_not.append({"term": {"shopify_variant_id.keyword": {"value": ""}}})
                must_query.append({"exists": {"field": "shopify_variant_id"}})
            elif int(integration_type) == 3:
                must_query.append({"match": {"magentoId": -1}})
                must_query.append({"term": {"shopify_variant_id.keyword": ""}})

            # if int(integration_type) != 3:
            query = {
                "sort": sub_query,
                "query": {"bool": {"must": must_query, "must_not": must_not}},
                "aggs": aggs_query,
            }
            categoty_data_json = []
            print(query)
            res = child_product_es_aggrigate_data(query)
            store_list_json = []
            # ===========================================get the store data========================================
            if zone_id != "" and store_category_id == PHARMACY_STORE_CATEGORY_ID:
                store_query = {"status": 1, "serviceZones.zoneId": zone_id}
                stores_list = store_find(store_query)
                for s in stores_list:
                    store_list_json.append(str(s["_id"]))
            else:
                pass
            # ==================================================end================================================
            main_sellers = []
            if zone_id != "":
                store_details = db.stores.find(
                    {"serviceZones.zoneId": zone_id, "storeFrontTypeId": {"$ne": 5}, "status": 1}
                )
                for seller in store_details:
                    main_sellers.append(str(seller["_id"]))

            if zone_id != "":
                driver_roaster = next_availbale_driver_roaster(zone_id)
            else:
                driver_roaster = {}
            # [int(limit) : int(size)]
            for res_res in res["aggregations"]["group_by_sub_category"]["buckets"][
                int(limit) : int(size)
            ]:
                start_time_res = time.time()
                sub_cat_id = res_res["key"]
                try:
                    cat_name = res_res["top_hits"]["buckets"][0]["top_sales_hits"]["hits"]["hits"][
                        0
                    ]["_source"]["categoryList"][0]["parentCategory"]["categoryName"]["en"]
                    catlist = res_res["top_hits"]["buckets"][0]["top_sales_hits"]["hits"]["hits"][
                        0
                    ]["_source"]["categoryList"]
                except:
                    cat_name = ""
                    catlist = ""
                if sub_cat_id != "":
                    product_data = []
                    all_product_ids = []
                    for main_bucket in res_res["top_hits"]["buckets"]:
                        child_product_data_mongo = db.childProducts.find_one(
                            {
                                "_id": ObjectId(
                                    main_bucket["top_sales_hits"]["hits"]["hits"][0]["_id"]
                                ),
                                "status": 1,
                            }
                        )
                        if child_product_data_mongo is None:


                            child_product_details = main_bucket["top_sales_hits"]["hits"]["hits"][
                                0
                            ]["_source"]
                            child_product_details["_id"] = main_bucket["top_sales_hits"]["hits"]["hits"][0]["_id"]
                        else:
                            child_product_details = child_product_data_mongo
                        # =====================this block for check the product is available in dc or not================
                        all_dc_list = []
                        is_dc_linked = False
                        hard_limit = 0
                        pre_order = False
                        procurementTime = 0
                        if "seller" in child_product_details:
                            if len(child_product_details["seller"]) > 0:
                                is_dc_linked = True
                                for seller in child_product_details["seller"]:
                                    if seller["storeId"] in main_sellers:
                                        if seller["preOrder"]:
                                            all_dc_list.append(seller)
                                        else:
                                            pass
                                    else:
                                        pass

                                if len(all_dc_list) == 0:
                                    for new_seller in child_product_details["seller"]:
                                        if new_seller["storeId"] in main_sellers:
                                            all_dc_list.append(new_seller)
                                        else:
                                            pass
                                else:
                                    pass
                            else:
                                pass
                        else:
                            pass
                        # =====================end block for dc check================
                        # ==========dc==============
                        if is_dc_linked:
                            if len(all_dc_list) > 0:
                                best_seller_product = min(
                                    all_dc_list, key=lambda x: x["procurementTime"]
                                )
                            else:
                                best_seller_product = {}
                        else:
                            best_seller_product = {}
                        main_product_details = None
                        if len(best_seller_product) > 0:
                            hard_limit = best_seller_product["hardLimit"]
                            pre_order = best_seller_product["preOrder"]
                            procurementTime = best_seller_product["procurementTime"]
                            main_product_details = db.childProducts.find_one(
                                {
                                    "parentProductId": child_product_details["parentProductId"],
                                    "units.unitId": child_product_details["units"][0]["unitId"],
                                    "storeId": ObjectId(best_seller_product["storeId"]),
                                }
                            )
                            if main_product_details is not None:
                                child_product_id = str(main_product_details["_id"])
                            else:
                                child_product_id = str(
                                    main_bucket["top_sales_hits"]["hits"]["hits"][0]["_id"]
                                )
                        else:
                            child_product_id = str(
                                main_bucket["top_sales_hits"]["hits"]["hits"][0]["_id"]
                            )

                        if "productSeo" in child_product_details:
                            try:
                                if len(child_product_details["productSeo"]["title"]) > 0:
                                    title = (
                                        child_product_details["productSeo"]["title"][language]
                                        if language in child_product_details["productSeo"]["title"]
                                        else child_product_details["productSeo"]["title"]["en"]
                                    )
                                else:
                                    title = ""
                            except:
                                title = ""

                            try:
                                if len(child_product_details["productSeo"]["description"]) > 0:
                                    description = (
                                        child_product_details["productSeo"]["description"][language]
                                        if language
                                        in child_product_details["productSeo"]["description"]
                                        else child_product_details["productSeo"]["description"][
                                            "en"
                                        ]
                                    )
                                else:
                                    description = ""
                            except:
                                description = ""

                            try:
                                if len(child_product_details["productSeo"]["metatags"]) > 0:
                                    metatags = (
                                        child_product_details["productSeo"]["metatags"][language]
                                        if language
                                        in child_product_details["productSeo"]["metatags"]
                                        else child_product_details["productSeo"]["metatags"]["en"]
                                    )
                                else:
                                    metatags = ""
                            except:
                                metatags = ""

                            try:
                                if len(child_product_details["productSeo"]["slug"]) > 0:
                                    slug = (
                                        child_product_details["productSeo"]["slug"][language]
                                        if language in child_product_details["productSeo"]["slug"]
                                        else child_product_details["productSeo"]["slug"]["en"]
                                    )
                                else:
                                    slug = ""
                            except:
                                slug = ""

                            product_seo = {
                                "title": title,
                                "description": description,
                                "metatags": metatags,
                                "slug": slug,
                            }
                        else:
                            product_seo = {
                                "title": "",
                                "description": "",
                                "metatags": "",
                                "slug": "",
                            }
                        if "prescriptionRequired" in child_product_details:
                            try:
                                if int(child_product_details["prescriptionRequired"]) == 0:
                                    prescription_required = False
                                else:
                                    prescription_required = True
                            except:
                                prescription_required = False

                        else:
                            prescription_required = False

                        if "saleOnline" in child_product_details:
                            if child_product_details["saleOnline"] == 0:
                                sales_online = False
                            else:
                                sales_online = True
                        else:
                            sales_online = False

                        if "needsIdProof" in child_product_details:
                            if child_product_details["needsIdProof"] is False:
                                needsIdProof = False
                            else:
                                needsIdProof = True
                        else:
                            needsIdProof = False

                        if "uploadProductDetails" in child_product_details:
                            upload_details = child_product_details["uploadProductDetails"]
                        else:
                            upload_details = ""
                        try:
                            currency_rate = currency_exchange_rate[
                                str(child_product_details["currency"]) + "_" + str(currency_code)
                            ]
                        except:
                            currency_rate = 0
                        currency_details = db.currencies.find_one({"currencyCode": currency_code})
                        if currency_details is not None:
                            currency_symbol = currency_details["currencySymbol"]
                            currency = currency_details["currencyCode"]
                        else:
                            currency_symbol = child_product_details["currencySymbol"]
                            currency = child_product_details["currency"]

                        tax_value = []
                        if child_product_details is not None:
                            if type(child_product_details["tax"]) == list:
                                for tax in child_product_details["tax"]:
                                    if "taxValue" in tax:
                                        tax_value.append({"value": tax["taxValue"]})
                                    else:
                                        pass
                            else:
                                if child_product_details["tax"] is not None:
                                    if "taxValue" in child_product_details["tax"]:
                                        tax_value.append(
                                            {"value": child_product_details["tax"]["taxValue"]}
                                        )
                                    else:
                                        tax_value.append({"value": child_product_details["tax"]})
                                else:
                                    pass
                        else:
                            tax_value = []

                        query = {
                            "parentProductId": str(child_product_details["parentProductId"]),
                            "status": 1,
                        }
                        try:
                            if store_category_id == MEAT_STORE_CATEGORY_ID:
                                variant_query["storeId"] = {
                                    "$in": [ObjectId(child_product_details["storeId"])]
                                }
                            else:
                                if str(child_product_details["storeId"]) == "0":
                                    variant_query["storeId"] = child_product_details["storeId"]
                                else:
                                    variant_query["storeId"] = ObjectId(
                                        child_product_details["storeId"]
                                    )
                        except:
                            query["storeId"] = child_product_details["storeId"]
                        variant_count_data = db.childProducts.find(query).count()
                        if variant_count_data > 1:
                            variant_count = True
                        else:
                            variant_count = False
                        try:
                            mobile_images = child_product_details["images"][0]
                        except:
                            try:
                                mobile_images = child_product_details["images"]
                            except:
                                mobile_images = child_product_details["image"]

                        model_data = []
                        try:
                            if "modelImage" in child_product_details["units"][0]:
                                if len(child_product_details["units"][0]["modelImage"]) > 0:
                                    model_data = child_product_details["units"][0]["modelImage"]
                                else:
                                    model_data = [
                                        {
                                            "extraLarge": "",
                                            "medium": "",
                                            "altText": "",
                                            "large": "",
                                            "small": "",
                                        }
                                    ]
                            else:
                                model_data = [
                                    {
                                        "extraLarge": "",
                                        "medium": "",
                                        "altText": "",
                                        "large": "",
                                        "small": "",
                                    }
                                ]
                        except:
                            model_data = [
                                {
                                    "extraLarge": "",
                                    "medium": "",
                                    "altText": "",
                                    "large": "",
                                    "small": "",
                                }
                            ]

                        addition_info = []
                        if "THC" in child_product_details["units"][0]:
                            addition_info.append(
                                {
                                    "seqId": 2,
                                    "attrname": "THC",
                                    "value": str(child_product_details["units"][0]["THC"]) + " %",
                                    "id": "",
                                }
                            )
                        else:
                            addition_info.append(
                                {
                                    "seqId": 2,
                                    "attrname": "THC",
                                    "value": "0 %",
                                    "id": "",
                                }
                            )

                        if "CBD" in child_product_details["units"][0]:
                            addition_info.append(
                                {
                                    "seqId": 1,
                                    "attrname": "CBD",
                                    "value": str(child_product_details["units"][0]["CBD"]) + " %",
                                    "id": "",
                                }
                            )
                        else:
                            addition_info.append(
                                {
                                    "seqId": 1,
                                    "attrname": "CBD",
                                    "value": "0.02 %",
                                    "id": "",
                                }
                            )

                        # =================================================canniber product type========================
                        if "cannabisProductType" in child_product_details["units"][0]:
                            if child_product_details["units"][0]["cannabisProductType"] != "":
                                cannabis_type_details = db.cannabisProductType.find_one(
                                    {
                                        "_id": ObjectId(
                                            child_product_details["units"][0]["cannabisProductType"]
                                        ),
                                        "status": 1,
                                    }
                                )
                                if cannabis_type_details is not None:
                                    addition_info.append(
                                        {
                                            "seqId": 3,
                                            "attrname": "Type",
                                            "value": cannabis_type_details["productType"]["en"],
                                            "id": child_product_details["units"][0][
                                                "cannabisProductType"
                                            ],
                                        }
                                    )
                                else:
                                    pass
                        else:
                            addition_info.append(
                                {
                                    "seqId": 3,
                                    "attrname": "Type",
                                    "value": "Hybrid",
                                    "id": "60c3247a75560000230055cd",
                                }
                            )
                        if len(addition_info) > 0:
                            additional_info = sorted(
                                addition_info, key=lambda k: k["seqId"], reverse=True
                            )
                        else:
                            additional_info = []
                        try:
                            if language in child_product_details["units"][0]["unitName"]:
                                product_name = child_product_details["units"][0]["unitName"][
                                    language
                                ]
                            else:
                                product_name = child_product_details["units"][0]["unitName"]["en"]
                        except:
                            product_name = ""
                        isFavourite = False
                        try:
                            response_casandra = session.execute(
                                """SELECT * FROM favouriteproductsuserwise where userid=%(userid)s AND childproductid=%(productid)s ALLOW FILTERING""",
                                {
                                    "userid": user_id,
                                    "productid": child_product_id,
                                },
                            )
                            if not response_casandra:
                                isFavourite = False
                            else:
                                isFavourite = True
                        except Exception as e:
                            print(e)
                            isFavourite = False
                        if product_name != "":
                            product_data.append(
                                {
                                    "_id": str(child_product_details["_id"]),
                                    "maxQuantity": child_product_details["maxQuantity"]
                                    if "maxQuantity" in child_product_details
                                    else 0,
                                    "isComboProduct": child_product_details["isComboProduct"]
                                    if "isComboProduct" in child_product_details
                                    else False,
                                    "currencyRate": currency_rate,
                                    "productStatus": "",
                                    "hardLimit": hard_limit,
                                    "preOrder": pre_order,
                                    "procurementTime": procurementTime,
                                    "extraAttributeDetails": additional_info,
                                    "childProductId": str(child_product_id),
                                    "tax": tax_value,
                                    "productName": product_name,
                                    "parentProductId": child_product_details["parentProductId"],
                                    "storeCategoryId": child_product_details["storeCategoryId"],
                                    "allowOrderOutOfStock": child_product_details[
                                        "allowOrderOutOfStock"
                                    ]
                                    if "allowOrderOutOfStock" in child_product_details
                                    else False,
                                    "prescriptionRequired": prescription_required,
                                    "saleOnline": sales_online,
                                    "uploadProductDetails": upload_details,
                                    "nextSlotTime": next_availbale_driver_time,
                                    "productSeo": product_seo,
                                    "mobimages": mobile_images,
                                    "modelImage": model_data,
                                    "needsIdProof": needsIdProof,
                                    "variantCount": variant_count,
                                    "brandName": child_product_details["brandTitle"][language]
                                    if language in child_product_details["brandTitle"]
                                    else child_product_details["brandTitle"]["en"],
                                    "manufactureName": child_product_details["manufactureName"][
                                        language
                                    ]
                                    if language in child_product_details["manufactureName"]
                                    else "",
                                    "currencySymbol": currency_symbol,
                                    "currency": currency,
                                    "images": child_product_details["images"],
                                    "productTag": "",
                                    "units": child_product_details["units"],
                                    "storeId": str(child_product_details["storeId"]),
                                    "unitId": child_product_details["units"][0]["unitId"],
                                    "offerData": child_product_details["offer"]
                                    if "offer" in child_product_details
                                    else [],
                                    "childProductDetails": child_product_details,
                                    "isDcLinked": is_dc_linked,
                                    "mainProductDetails": main_product_details,
                                    "isFavourite":isFavourite

                                }
                            )
                    if len(product_data) > 0:
                        product_dataframe = pd.DataFrame(product_data)
                        try:
                            product_dataframe = product_dataframe.apply(
                                get_avaialable_quantity,
                                next_availbale_driver_time=next_availbale_driver_time,
                                driver_roaster=driver_roaster,
                                zone_id=zone_id,
                                axis=1,
                            )
                        except: pass
                        try:
                            product_dataframe = product_dataframe[product_dataframe.storeExists != False]
                        except: pass
                        try:
                            product_dataframe = product_dataframe.apply(
                                best_offer_function_validate,
                                zone_id=zone_id,
                                logintype=login_type,
                                axis=1,
                            )
                        except: pass
                        try:
                            product_dataframe["suppliers"] = product_dataframe.apply(
                                best_supplier_function_cust, axis=1
                            )
                        except: pass
                        try:
                            product_dataframe["productType"] = product_dataframe.apply(
                                product_type_validation, axis=1
                            )
                        except: pass
                        try:
                            product_dataframe["TotalStarRating"] = product_dataframe.apply(
                                cal_star_rating_product, axis=1
                            )
                        except: pass
                        try:
                            product_dataframe["linkedAttribute"] = product_dataframe.apply(
                                linked_unit_attribute, axis=1
                            )
                        except: pass
                        try:
                            product_dataframe["variantData"] = product_dataframe.apply(
                                linked_variant_data, language=language, axis=1
                            )
                        except: pass
                        try:
                            product_dataframe["unitsData"] = product_dataframe.apply(
                                home_units_data,
                                lan=language,
                                sort=0,
                                status=0,
                                axis=1,
                                logintype=login_type,
                                store_category_id=store_category_id,
                                margin_price=True,
                                city_id="",
                            )
                        except: pass
                        try:
                            del product_dataframe["childProductDetails"]
                            del product_dataframe["mainProductDetails"]
                        except: pass
                        details = product_dataframe.to_json(orient="records")
                        resData = json.loads(details)
                    else:
                        resData = []
                    categoty_data = validate_units_data(resData, False)
                    if len(categoty_data) > 0:
                        offer_dataframe = pd.DataFrame(categoty_data)
                        offers_data = offer_dataframe.to_dict(orient="records")
                        # newlist = offers_data
                        newlist = sorted(
                            offers_data, key=lambda k: (k["outOfStock"]), reverse=False
                        )
                        # newlist = newlist[int(limit) : int(size)]
                    else:
                        newlist = []
                    if len(newlist) > 0 and cat_name != "":
                        first_category_id = ""
                        second_category_id = ""
                        # second_category_id_list = []
                        # third_category_id = ""
                        second_category_id = ""
                        third_category_id = []
                        find_cat = catlist[0]
                        first_category_id = find_cat['parentCategory']['categoryId']
                        for i in find_cat['parentCategory']['childCategory']:
                            if i['categoryName']['en'] == sub_cat_id:
                                second_category_id = i["categoryId"]
                            else:
                                third_category_id.append(i["categoryId"])
                        third_category_id_data = third_category_id[0] if len(third_category_id) > 0 else ""
                        # main_category_details = db.category.find_one({"categoryName.en": sub_cat_id, "storeId": "0"})
                        # if main_category_details is not None:
                        #     if "parentId" in main_category_details:
                        #         second_category_id = str(main_category_details['parentId'])
                        #         third_category_id = str(main_category_details['_id'])
                        #     else:
                        #         second_category_id = str(main_category_details['_id'])
                        # else:
                        #     pass

                        # first_category_details = db.category.find_one({"categoryName.en": cat_name, "storeId": "0"})
                        # if first_category_details is not None:
                        #     first_category_id = str(first_category_details['_id'])
                        # else:
                        #     pass
                        categoty_data_json.append(
                            {
                                "subCategory": newlist,
                                "firstCategoryId": first_category_id,
                                "secondCategoryId": second_category_id,
                                "thirdCategoryId": third_category_id_data,
                                "subCategoryName": sub_cat_id,
                                "catName": cat_name,
                            }
                        )
                else:
                    pass
            if len(categoty_data_json) > 0:
                categ_data = categoty_data_json
                response = {
                    "id": "",
                    "catName": "",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "categoryData": categ_data,
                    "offers": [],
                    "type": 7,
                    "seqId": 7,
                }
                return JsonResponse(response, safe=False, status=200)
            else:
                response = {
                    "id": "",
                    "catName": "",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "categoryData": [],
                    "offers": [],
                    "type": 7,
                    "seqId": 7,
                }
                return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {
                "id": "",
                "catName": "",
                "imageUrl": "",
                "bannerImageUrl": "",
                "websiteImageUrl": "",
                "websiteBannerImageUrl": "",
                "categoryData": [],
                "offers": [],
                "type": 7,
                "seqId": 7,
                "message": message,
            }
            return JsonResponse(error, safe=False, status=500)


class AllProductList(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the data for all products on scroll",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="storeId",
                default="5e20914ac348027af2f9028e",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for HyperLocal TRUE and StoreListing TRUE. Exmaple-> Dining, Grocery"
                "store id from which store we need to get the data. if we need to get for any particular store that time need to send storeId(mandatory)",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5fcf541871d7bd51e6008c94",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for HyperLocal TRUE and StoreListing FALSE. Exmaple-> Shop Around, Pharmacy"
                "zone id from which zone we need to get the data. if we need to get for any particular zone that time need to send zoneId(mandatory)",
            ),
            openapi.Parameter(
                name="page",
                default="1",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="pagination from which page we need to get the data",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default="602e73ee8cd00052be5d6fa3",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            store_id = request.GET.get("storeId", "")
            zone_id = request.GET.get("zoneId", "")
            page = int(request.GET.get("page", 1))
            from_data = int(page * 10) - 10
            to_data_product = int(page * 10)
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            language = (
                str(request.META["HTTP_LANGUAGE"]) if "HTTP_LANGUAGE" in request.META else "en"
            )
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            token = request.META["HTTP_AUTHORIZATION"]
            last_json_response = []
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            else:
                sub_query = [{"_id": {"order": "desc"}}]
                must_query = [
                    {"match": {"status": 1}},
                    {"match": {"units.isPrimary": True}},
                    {"match": {"storeCategoryId": str(store_category_id)}},
                ]
                if store_id == "" and zone_id != "":
                    if zone_id == "" or store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                        pass
                    else:
                        store_data_details = []
                        store_query = {
                            "categoryId": str(store_category_id),
                            "serviceZones.zoneId": zone_id,
                        }
                        store_data = store_find(store_query)

                        if store_data.count() > 0:
                            for store in store_data:
                                store_data_details.append(str(store["_id"]))
                            must_query.append({"terms": {"storeId": store_data_details}})
                        else:
                            pass
                elif store_id != "":
                    must_query.append({"match": {"storeId": store_id}})
                else:
                    pass

                query = {
                    "sort": sub_query,
                    "size": to_data_product,
                    "from": from_data,
                    "query": {
                        "bool": {"must": must_query, "must_not": [{"match": {"storeId": "0"}}]}
                    },
                }
                # calling product es function for the get data from es
                res_filter_parameters = child_product_es_search_data(query)
                if res_filter_parameters["hits"]["total"]["value"] == 0:
                    response = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": [],
                        "type": 3,
                        "seqId": 3,
                    }
                    last_json_response.append(response)
                    last_response = {"data": {"list": last_json_response}}
                    return JsonResponse(last_response, safe=False, status=404)
                else:
                    resData = []
                    for product in res_filter_parameters["hits"]["hits"]:
                        variant_data = []
                        best_supplier = {
                            "productId": product["_id"],
                            "id": product["_source"]["storeId"],
                        }
                        child_product_details = db.childProducts.find_one(
                            {"_id": ObjectId(best_supplier["productId"])}
                        )
                        try:
                            if child_product_details is not None:
                                available_qty = (
                                    child_product_details["units"][0]["availableQuantity"]
                                    if "availableQuantity" in child_product_details["units"][0]
                                    else 0
                                )
                                # ===============================offer data======================================
                                offers_details = []
                                offer_details_data = []
                                if "offer" in child_product_details:
                                    for offer in child_product_details["offer"]:
                                        offer_count = db.offers.find(
                                            {
                                                "_id": ObjectId(offer["offerId"]),
                                                "status": 1,
                                                "storeCategoryId": store_category_id,
                                            }
                                        ).count()
                                        if offer_count > 0:
                                            if offer["status"] == 1:
                                                offer_terms = db.offers.find_one(
                                                    {
                                                        "_id": ObjectId(offer["offerId"]),
                                                        "storeCategoryId": store_category_id,
                                                    },
                                                    {
                                                        "termscond": 1,
                                                        "name": 1,
                                                        "discountValue": 1,
                                                        "offerType": 1,
                                                    },
                                                )
                                                if offer_terms is not None:
                                                    offer["termscond"] = offer_terms["termscond"]
                                                else:
                                                    offer["termscond"] = ""
                                                offer["name"] = offer_terms["name"]["en"]
                                                offer["discountValue"] = offer_terms[
                                                    "discountValue"
                                                ]
                                                offer["discountType"] = offer_terms["offerType"]
                                                offers_details.append(offer)
                                                offer_details_data.append(
                                                    {
                                                        "offerId": offer["offerId"],
                                                        "offerName": offer["offerName"]["en"],
                                                        "webimages": offer["webimages"]["image"],
                                                        "mobimage": offer["images"]["image"],
                                                        "discountValue": offer["discountValue"],
                                                    }
                                                )
                                            else:
                                                pass
                                        else:
                                            pass
                                else:
                                    pass

                                if len(offers_details) > 0:
                                    best_offer = max(
                                        offers_details, key=lambda x: x["discountValue"]
                                    )
                                    currdate = datetime.datetime.now().replace(
                                        hour=23, minute=59, second=59, microsecond=59
                                    )
                                    eastern = timezone(timezonename)
                                    currlocal = eastern.localize(currdate)
                                    best_offer["endDateTimeISO"] = (
                                        int((currlocal.timestamp())) * 1000
                                    )
                                else:
                                    best_offer = {}
                                # ======================================product seo======================================================
                                if "productSeo" in child_product_details:
                                    try:
                                        if len(child_product_details["productSeo"]["title"]) > 0:
                                            title = (
                                                child_product_details["productSeo"]["title"][
                                                    language
                                                ]
                                                if language
                                                in child_product_details["productSeo"]["title"]
                                                else child_product_details["productSeo"]["title"][
                                                    "en"
                                                ]
                                            )
                                        else:
                                            title = ""
                                    except:
                                        title = ""

                                    try:
                                        if (
                                            len(child_product_details["productSeo"]["description"])
                                            > 0
                                        ):
                                            description = (
                                                child_product_details["productSeo"]["description"][
                                                    language
                                                ]
                                                if language
                                                in child_product_details["productSeo"][
                                                    "description"
                                                ]
                                                else child_product_details["productSeo"][
                                                    "description"
                                                ]["en"]
                                            )
                                        else:
                                            description = ""
                                    except:
                                        description = ""

                                    try:
                                        if len(child_product_details["productSeo"]["metatags"]) > 0:
                                            metatags = (
                                                child_product_details["productSeo"]["metatags"][
                                                    language
                                                ]
                                                if language
                                                in child_product_details["productSeo"]["metatags"]
                                                else child_product_details["productSeo"][
                                                    "metatags"
                                                ]["en"]
                                            )
                                        else:
                                            metatags = ""
                                    except:
                                        metatags = ""

                                    try:
                                        if len(child_product_details["productSeo"]["slug"]) > 0:
                                            slug = (
                                                child_product_details["productSeo"]["slug"][
                                                    language
                                                ]
                                                if language
                                                in child_product_details["productSeo"]["slug"]
                                                else child_product_details["productSeo"]["slug"][
                                                    "en"
                                                ]
                                            )
                                        else:
                                            slug = ""
                                    except:
                                        slug = ""

                                    product_seo = {
                                        "title": title,
                                        "description": description,
                                        "metatags": metatags,
                                        "slug": slug,
                                    }
                                else:
                                    product_seo = {
                                        "title": "",
                                        "description": "",
                                        "metatags": "",
                                        "slug": "",
                                    }
                                tax_value = []

                                # =========================================pharmacy details=========================================
                                if "prescriptionRequired" in child_product_details:
                                    if child_product_details["prescriptionRequired"] == 0:
                                        prescription_required = False
                                    else:
                                        prescription_required = True
                                else:
                                    prescription_required = False

                                if "saleOnline" in child_product_details:
                                    if child_product_details["saleOnline"] == 0:
                                        sales_online = False
                                    else:
                                        sales_online = True
                                else:
                                    sales_online = False

                                if "uploadProductDetails" in child_product_details:
                                    upload_details = child_product_details["uploadProductDetails"]
                                else:
                                    upload_details = ""

                                # ========================= for the get the linked the unit data====================================
                                for link_unit in child_product_details["units"][0]["attributes"]:
                                    try:
                                        for attrlist in link_unit["attrlist"]:
                                            try:
                                                if attrlist is None:
                                                    pass
                                                else:
                                                    if attrlist["linkedtounit"] == 1:
                                                        if attrlist["measurementUnit"] == "":
                                                            attr_name = (
                                                                str(attrlist["value"][language])
                                                                if language in attrlist["value"]
                                                                else str(attrlist["value"]["en"])
                                                            )
                                                        else:
                                                            attr_name = (
                                                                str(attrlist["value"][language])
                                                                + " "
                                                                + attrlist["measurementUnit"]
                                                                if language in attrlist["value"]
                                                                else str(attrlist["value"]["en"])
                                                                + " "
                                                                + attrlist["measurementUnit"]
                                                            )
                                                        variant_data.append(
                                                            {
                                                                "attrname": attrlist["attrname"][
                                                                    "en"
                                                                ],
                                                                "value": str(attr_name),
                                                                "name": attrlist["attrname"]["en"],
                                                            }
                                                        )
                                                    else:
                                                        pass
                                            except:
                                                pass
                                    except:
                                        pass
                                # =========================for max quantity=================================================
                                if "maxQuantity" in child_product_details:
                                    if child_product_details["maxQuantity"] != "":
                                        max_quantity = int(child_product_details["maxQuantity"])
                                    else:
                                        max_quantity = 30
                                else:
                                    max_quantity = 30
                                # ==========================================================================================
                                if "allowOrderOutOfStock" in child_product_details:
                                    allow_out_of_order = child_product_details[
                                        "allowOrderOutOfStock"
                                    ]
                                else:
                                    allow_out_of_order = False

                                mobile_images = []

                                if "productType" in child_product_details:
                                    if child_product_details["productType"] == 2:
                                        combo_product = True
                                    else:
                                        combo_product = False
                                else:
                                    combo_product = False

                                try:
                                    base_price = child_product_details["units"][0]["b2cPricing"][0][
                                        "b2cproductSellingPrice"
                                    ]
                                except:
                                    base_price = child_product_details["units"][0]["floatValue"]

                                # ==================================get currecny rate============================
                                try:
                                    currency_rate = currency_exchange_rate[
                                        str(child_product_details["currency"])
                                        + "_"
                                        + str(currency_code)
                                    ]
                                except:
                                    currency_rate = 0
                                currency_details = db.currencies.find_one(
                                    {"currencyCode": currency_code}
                                )
                                if currency_details is not None:
                                    currency_symbol = currency_details["currencySymbol"]
                                    currency = currency_details["currencyCode"]
                                else:
                                    currency_symbol = child_product_details["currencySymbol"]
                                    currency = child_product_details["currency"]

                                if float(currency_rate) > 0:
                                    base_price = base_price * float(currency_rate)

                                tax_price = 0
                                if "tax" in child_product_details:
                                    if len(child_product_details["tax"]) == 0:
                                        tax_price = 0
                                    else:
                                        for amount in child_product_details["tax"]:
                                            if "taxValue" in amount:
                                                tax_price = tax_price + (int(amount["taxValue"]))
                                            if "value" in amount:
                                                tax_price = tax_price + (int(amount["value"]))
                                            else:
                                                tax_price = tax_price + 0
                                else:
                                    tax_price = 0

                                if len(best_offer) > 0:
                                    discount_type = (
                                        int(best_offer["discountType"])
                                        if "discountType" in best_offer
                                        else 1
                                    )
                                    discount_value = (
                                        best_offer["discountValue"]
                                        if "discountValue" in best_offer
                                        else 0
                                    )
                                else:
                                    discount_type = 2
                                    discount_value = 0

                                if discount_type == 0:
                                    percentage = 0
                                else:
                                    percentage = int(discount_value)

                                base_price = base_price + ((float(base_price) * tax_price) / 100)

                                # ==============calculate discount price =============================
                                if discount_type == 0:
                                    discount_price = float(discount_value)
                                elif discount_type == 1:
                                    discount_price = (
                                        float(base_price) * float(discount_value)
                                    ) / 100
                                else:
                                    discount_price = 0
                                final_price = base_price - discount_price

                                try:
                                    product_name = (
                                        child_product_details["pName"]["en"]
                                        if "pName" in child_product_details
                                        else child_product_details["pPName"]["en"]
                                    )
                                except:
                                    product_name = child_product_details["units"][0]["unitName"][
                                        "en"
                                    ]
                                next_availbale_time = ""
                                if available_qty <= 0:
                                    outOfStock = True
                                else:
                                    outOfStock = False

                                variant_query = {"parentProductId": product["_id"], "status": 1}
                                if best_supplier["id"] == "0":
                                    variant_query["storeId"] = best_supplier["id"]
                                else:
                                    variant_query["storeId"] = ObjectId(best_supplier["id"])
                                # product['_source']['variantCount'] if "variantCount" in product['_source'] else 1
                                variant_count_data = db.childProducts.find(variant_query).count()
                                if variant_count_data > 1:
                                    variant_count = True
                                else:
                                    variant_count = False
                                isShoppingList = False

                                if "containsMeat" in child_product_details:
                                    contains_Meat = child_product_details["containsMeat"]
                                else:
                                    contains_Meat = False

                                parent_product_data = db.products.find_one(
                                    {"_id": ObjectId(product["_source"]["parentProductId"])}
                                )  # for get the average rating of product
                                # =====================from here need to send dc supplier id for the product===============
                                # get product type, is normal or combo or special product
                                product_type = combo_special_type_validation(
                                    str(best_supplier["_id"])
                                )
                                resData.append(
                                    {
                                        "maxQuantity": max_quantity,
                                        "isComboProduct": combo_product,
                                        "childProductId": str(best_supplier["productId"]),
                                        "availableQuantity": available_qty,
                                        "productType": product_type,
                                        "offerDetailsData": offer_details_data,
                                        "productName": product_name,
                                        "parentProductId": product["_id"],
                                        "suppliers": best_supplier,
                                        "supplier": best_supplier,
                                        "containsMeat": contains_Meat,
                                        "isShoppingList": isShoppingList,
                                        "tax": tax_value,
                                        "linkedAttribute": variant_data,
                                        "allowOrderOutOfStock": allow_out_of_order,
                                        "moUnit": "Pcs",
                                        "outOfStock": outOfStock,
                                        "variantData": variant_data,
                                        "addOnsCount": 0,
                                        "variantCount": variant_count,
                                        "prescriptionRequired": prescription_required,
                                        "saleOnline": sales_online,
                                        "uploadProductDetails": upload_details,
                                        "productSeo": product_seo,
                                        "brandName": child_product_details["brandTitle"][language]
                                        if language in child_product_details["brandTitle"]
                                        else child_product_details["brandTitle"]["en"],
                                        "manufactureName": child_product_details["manufactureName"][
                                            language
                                        ]
                                        if language in child_product_details["manufactureName"]
                                        else "",
                                        "TotalStarRating": parent_product_data["avgRating"]
                                        if "avgRating" in parent_product_data
                                        else 0,
                                        "currencySymbol": currency_symbol,
                                        "mobileImage": [],
                                        "currency": currency,
                                        "storeCategoryId": child_product_details["storeCategoryId"]
                                        if "storeCategoryId" in child_product_details
                                        else "",
                                        "images": child_product_details["images"],
                                        "mobimages": mobile_images,
                                        "units": child_product_details["units"],
                                        "finalPriceList": {
                                            "basePrice": round(base_price, 2),
                                            "finalPrice": round(final_price, 2),
                                            "discountPrice": round(discount_price, 2),
                                            "discountType": discount_type,
                                            "discountPercentage": percentage,
                                        },
                                        "price": int(final_price),
                                        "discountType": discount_type,
                                        "unitId": str(child_product_details["units"][0]["unitId"]),
                                        "offer": best_offer,
                                        #  "offers": best_offer,
                                        "nextSlotTime": next_availbale_time,
                                    }
                                )
                            else:
                                pass
                        except Exception as ex:
                            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                            message = template.format(type(ex).__name__, ex.args)
                            print(
                                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                                type(ex).__name__,
                                ex,
                            )
                            pass

                    if len(resData) == 0:
                        response = {
                            "id": "",
                            "catName": "Deals of the Day",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": res_filter_parameters["hits"]["total"]["value"],
                            "categoryData": [],
                            "type": 3,
                            "seqId": 3,
                        }
                        last_json_response.append(response)
                        last_response = {"data": {"list": last_json_response}}
                        return JsonResponse(last_response, safe=False, status=404)
                    else:
                        dataframe = pd.DataFrame(resData)
                        details = dataframe.to_json(orient="records")
                        data = json.loads(details)
                        newlist = data  # validate_units_data(data)
                        if len(newlist) > 0:
                            res_data_dataframe = pd.DataFrame(newlist)
                            res_data_dataframe = res_data_dataframe.drop_duplicates(
                                "productName", keep="first"
                            )
                            newlist_json = res_data_dataframe.to_dict(orient="records")
                        else:
                            newlist_json = []
                        response = {
                            "id": "",
                            "catName": "Deals of the Day",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": res_filter_parameters["hits"]["total"]["value"],
                            "categoryData": newlist_json,
                            "type": 3,
                            "seqId": 3,
                        }
                        last_json_response.append(response)
                        last_response = {"data": {"list": last_json_response}}
                        return JsonResponse(last_response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            last_json_response = []
            error = {
                "id": "",
                "catName": "Deals of the Day",
                "imageUrl": "",
                "bannerImageUrl": "",
                "websiteImageUrl": "",
                "websiteBannerImageUrl": "",
                "offers": [],
                "penCount": 0,
                "categoryData": [],
                "type": 3,
                "seqId": 3,
            }
            last_json_response.append(error)
            last_response = {"data": {"list": last_json_response}}
            return JsonResponse(last_response, safe=False, status=500)


"""
    API for the get the all the wish list (favourite products) for the user
    sort_type : 0 for default, 1 for price low to high and 2 for high to low
"""


class PostAddProductsList(APIView):
    @swagger_auto_schema(
        method="post",
        tags=["Post Product List"],
        operation_description="API for getting list of the products which linked with post",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
        ],
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            required=["productIds"],
            properties={
                "productIds": openapi.Schema(
                    type=openapi.TYPE_ARRAY,
                    description="array of the data or object(parentProductId, childProductId, offerId, productName)",
                    items=openapi.Items(
                        type=openapi.TYPE_OBJECT,
                        required=["parentProductId", "childProductId"],
                        properties={
                            "parentProductId": openapi.Schema(
                                type=openapi.TYPE_STRING,
                                default="5df85105e80e605065d3cdfe",
                                description="central product id of the product. ex.5df8efcae2c05798aa40e67f",
                            ),
                            "childProductId": openapi.Schema(
                                type=openapi.TYPE_STRING,
                                default="5df85105e80e605065d3cdff",
                                description="child product id of the product. ex.5df8efcae2c05798aa40e67f",
                            ),
                        },
                    ),
                ),
                "integrationType": openapi.Schema(
                    type=openapi.TYPE_INTEGER,
                    description="for filter out the products base on product setup configuration, value should be"
                    "0 for All products, "
                    "1 for Only Magento Products, "
                    "2 for Only Shopify Products, "
                    "3 for Only Roadyo or shopar products",
                    example=0,
                    default=0,
                ),
            },
        ),
        responses={
            200: "successfully. found favourite products for the user",
            404: "data not found. it might be product not found for the user",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["post"])
    def post(self, request):
        try:
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            print("currency code", currency_code)
            token = request.META["HTTP_AUTHORIZATION"]
            if token == "":
                response_data = {"message": "unauthorized", "data": []}
                return JsonResponse(response_data, safe=False, status=401)
            user_id = json.loads(token)["userId"]
            # user_id = "5d92f959fc2045620ce36c92"
            response_casandra = request.data
            if "productIds" not in response_casandra:
                response_data = {"message": "product ids are missing", "data": []}
                return JsonResponse(response_data, safe=False, status=422)
            else:
                fav_products = []
                recent_data = []
                resData = []
                integration_type = (
                    int(response_casandra["integrationType"])
                    if "integrationType" in response_casandra
                    else 0
                )
                try:
                    for product in response_casandra["productIds"]:
                        if "childProductId" in product:
                            central_query = {"_id": ObjectId(product["childProductId"])}
                        else:
                            central_query = {
                                "storeId": ObjectId(product["sellerId"]),
                                "units.isPrimary": True,
                                "parentProductId": str(product["parentProductId"]),
                                "status": 1,
                            }
                        if int(integration_type) == 0:
                            pass
                        elif int(integration_type) == 1:
                            central_query["magentoId"] = {"$ne": -1}
                            central_query["magentoId"] = {"$exists": True}
                        elif int(integration_type) == 2:
                            central_query["shopify_variant_id"] = {"$ne": ""}
                            central_query["shopify_variant_id"] = {"$exists": True}
                        elif int(integration_type) == 3:
                            or_mongo_query = []
                            or_mongo_query.append({"magentoId": ""})
                            or_mongo_query.append({"shopify_variant_id": ""})
                            or_mongo_query.append({"magentoId": {"$exists": False}})
                            or_mongo_query.append({"shopify_variant_id": {"$exists": False}})
                            central_query["$or"] = or_mongo_query

                        product_details = db.childProducts.find_one(central_query)
                        contains_Meat = False
                        if product_details is not None:
                            if "containsMeat" in product_details:
                                contains_Meat = product_details["containsMeat"]
                            else:
                                contains_Meat = False
                            supplier_list = []
                            try:
                                best_supplier = {
                                    "productId": str(product_details["_id"]),
                                    "id": str(product_details["storeId"]),
                                    "retailerQty": product_details["units"][0]["availableQuantity"],
                                }
                                if best_supplier["retailerQty"] > 0:
                                    outOfStock = False
                                    availableQuantity = best_supplier["retailerQty"]
                                else:
                                    outOfStock = True
                                    availableQuantity = 0

                                offers_details = []
                                if "offer" in product_details:
                                    for offer in product_details["offer"]:
                                        if offer["status"] == 1:
                                            offers_details.append(offer)
                                        else:
                                            pass

                                if len(offers_details) > 0:
                                    best_offer = max(
                                        offers_details, key=lambda x: x["discountValue"]
                                    )
                                else:
                                    best_offer = {}

                                product_id = str(product_details["_id"])
                                rating_count = db.reviewRatings.find(
                                    {
                                        "productId": product_details["parentProductId"],
                                        "rating": {"$ne": 0},
                                    }
                                ).count()
                                tax_value = []
                                if type(product_details["tax"]) == list:
                                    for tax in product_details["tax"]:
                                        tax_value.append({"value": tax["taxValue"]})
                                else:
                                    if product_details["tax"] is not None:
                                        if "taxValue" in product_details["tax"]:
                                            tax_value.append(
                                                {"value": product_details["tax"]["taxValue"]}
                                            )
                                        else:
                                            tax_value.append({"value": product_details["tax"]})
                                    else:
                                        tax_value = []

                                seller_details = db.stores.find_one(
                                    {"_id": ObjectId(product_details["storeId"])}
                                )
                                if seller_details is not None:
                                    seller_name = seller_details["storeName"]["en"]
                                else:
                                    seller_name = ""
                                # ==================================get currecny rate============================
                                try:
                                    currency_rate = currency_exchange_rate[
                                        str(product_details["currency"]) + "_" + str(currency_code)
                                    ]
                                except:
                                    currency_rate = 0
                                currency_details = db.currencies.find_one(
                                    {"currencyCode": currency_code}
                                )
                                if currency_details is not None:
                                    currency_symbol = currency_details["currencySymbol"]
                                    currency = currency_details["currencyCode"]
                                else:
                                    currency_symbol = product_details["currencySymbol"]
                                    currency = product_details["currency"]
                                # get product type, is normal or combo or special product
                                product_type = combo_special_type_validation(str(product_id))

                                model_data = []
                                if "modelImage" in product_details["units"][0]:
                                    if len(product_details["units"][0]["modelImage"]) > 0:
                                        model_data = product_details["units"][0]["modelImage"]
                                    else:
                                        pass
                                else:
                                    pass

                                resData.append(
                                    {
                                        "childProductId": product_id,
                                        "currencyRate": currency_rate,
                                        "productType": product_type,
                                        "availableQuantity": availableQuantity,
                                        "productName": product_details["units"][0]["unitName"][
                                            language
                                        ]
                                        if language in product_details["units"][0]["unitName"]
                                        else product_details["units"][0]["unitName"]["en"],
                                        "parentProductId": product_details["parentProductId"],
                                        "brandName": product_details["brandTitle"][language]
                                        if language in product_details["brandTitle"]
                                        else product_details["brandTitle"]["en"],
                                        "outOfStock": outOfStock,
                                        # product['createdtimestamp'],
                                        "timestamp": product_details["createdtimestamp"]
                                        if "createdtimestamp" in product_details
                                        else 0,
                                        "suppliers": best_supplier,
                                        "storeName": seller_name,
                                        "tax": tax_value,
                                        "variantData": [],
                                        "currencySymbol": currency_symbol,
                                        "currency": currency,
                                        "images": product_details["images"]
                                        if "images" in product_details
                                        else product_details["units"][0]["image"],
                                        "modelImage": model_data,
                                        "finalPriceList": product_details["units"],
                                        "units": product_details["units"],
                                        "unitId": product_details["units"][0]["unitId"],
                                        "offer": best_offer,
                                        "avgRating": product_details["avgRating"]
                                        if "avgRating" in product_details
                                        else 0,
                                        "totalRatingCount": rating_count,
                                        "containsMeat" : contains_Meat
                                    }
                                )
                            except Exception as ex:
                                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                                message = template.format(type(ex).__name__, ex.args)
                                print(
                                    "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                                    type(ex).__name__,
                                    ex,
                                )
                        else:
                            pass
                except Exception as ex:
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    print(
                        "1Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                        type(ex).__name__,
                        ex,
                    )
                if len(resData) > 0:
                    dataframe = pd.DataFrame(resData)
                    dataframe["unitsData"] = dataframe.apply(
                        home_units_data,
                        lan=language,
                        sort=0,
                        status=0,
                        axis=1,
                        store_category_id=ECOMMERCE_STORE_CATEGORY_ID,
                        logintype=1,
                        margin_price=True,
                        city_id="",
                    )
                    # dataframe["unitsData"] = dataframe.apply(home_units_data, lan=language, sort=0, status=0, axis=1, store_category_id=ECOMMERCE_STORE_CATEGORY_ID, logintype=1)
                    details = dataframe.to_json(orient="records")
                    data = json.loads(details)
                    for k in data:
                        if len(k["offer"]) != 0:
                            if "discountType" in k["offer"]:
                                if k["offer"]["discountType"] == 0:
                                    percentage = 0
                                else:
                                    percentage = int(k["offer"]["discountValue"])
                            else:
                                percentage = 0
                        else:
                            percentage = 0

                        mou = ""
                        mou_unit = ""
                        minimum_qty = 0

                        recent_data.append(
                            {
                                "outOfStock": k["outOfStock"],
                                "unitId": k["unitId"],
                                "containsMeat": k["containsMeat"],
                                "parentProductId": k["parentProductId"],
                                "childProductId": k["childProductId"],
                                "productName": k["productName"],
                                "availableQuantity": k["availableQuantity"],
                                "images": k["images"],
                                "modelImage": k["modelImage"] if "modelImage" in k else [],
                                "timestamp": k["timestamp"],
                                "supplier": k["suppliers"],
                                "brandName": k["brandName"],
                                "variantData": k["variantData"],
                                "discountType": k["offer"]["discountType"]
                                if "discountType" in k["offer"]
                                else 0,
                                "productType": k["productType"] if "productType" in k else 1,
                                "finalPrice": k["unitsData"]["finalPrice"],
                                "finalPriceList": {
                                    "basePrice": k["unitsData"]["basePrice"],
                                    "finalPrice": k["unitsData"]["finalPrice"],
                                    "discountPrice": k["unitsData"]["discountPrice"],
                                    "discountPercentage": percentage,
                                },
                                "totalRatingCount": k["totalRatingCount"],
                                "mouData": {
                                    "mou": mou,
                                    "mouUnit": mou_unit,
                                    "mouQty": minimum_qty,
                                    "minimumPurchaseUnit": "",
                                },
                                "currencySymbol": k["currencySymbol"],
                                "currency": k["currency"],
                                "avgRating": k["avgRating"],
                                "offers": k["offer"],
                            }
                        )

                    sorted_data = sorted(recent_data, key=lambda k: k["finalPrice"], reverse=True)
                    if len(sorted_data) > 0:
                        response = {"data": sorted_data, "message": "data found...!!!"}
                        return_response = {"data": response}
                        return JsonResponse(return_response, safe=False, status=200)
                    else:
                        response = {"data": [], "message": "data not found...!!!"}
                        return_response = {"data": response}
                        return JsonResponse(return_response, safe=False, status=404)
                else:
                    response = {"data": [], "penCount": 0, "message": "data not found...!!!"}
                    return_response = {"data": response}
                    return JsonResponse(return_response, safe=False, status=404)
        except Exception as ex:
            template = "an exception of type {0} occurred. arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            response_data = {
                "message": translator.translate(str(message), dest=language).text,
                "penCount": 0,
                "data": [],
            }
            return_response = {"data": response_data}
            return JsonResponse(return_response, safe=False, status=500)


"""
    API for the get the all the wish list (favourite products) for the user
    sort_type : 0 for default, 1 for price low to high and 2 for high to low
"""


class BannerList(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Banner List"],
        operation_description="API for getting the list of banners",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default="608674d9e8abbb0c434782b4",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
        ],
        responses={
            200: "successfully. review or rating added",
            404: "data not found. it might be user not found, product not found",
            401: "Unauthorized. token expired",
            422: "Feilds are missing. required Feilds are missing",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            token = request.META["HTTP_AUTHORIZATION"]
            if token == "":
                response_data = {"message": "unauthorized", "data": []}
                return JsonResponse(response_data, safe=False, status=401)
            store_category_id = request.GET.get("storeCategoryId", ECOMMERCE_STORE_CATEGORY_ID)
            start_time = time.time()
            banner_deatils_data = []
            category_name = ""
            sub_category_name = ""
            banner_details = db.banner.find(
                {"status": 1, "storeCategoryId": store_category_id},
                {"image_web": 1, "image_mobile": 1, "type": 1, "data": 1},
            )
            if banner_details.count() > 0:
                for i in banner_details:
                    try:
                        category_name = ""
                        sub_category_name = ""
                        sub_sub_category_name = ""
                        if int(i["type"]) == 0:
                            banner_type = "Send Package"
                        elif int(i["type"]) == 1:
                            banner_type = "offer"
                        elif int(i["type"]) == 2:
                            banner_type = "brands"
                        elif int(i["type"]) == 3:
                            banner_type = "category"
                        elif int(i["type"]) == 4:
                            banner_type = "stores"
                        elif int(i["type"]) == 5:
                            banner_type = "subcategory"
                        elif int(i["type"]) == 6:
                            banner_type = "subsubcategory"
                        elif int(i["type"]) == 7:
                            banner_type = "supplier"
                        elif int(i["type"]) == 8:
                            banner_type = "products"
                        elif int(i["type"]) == 9:
                            banner_type = "url"
                        if int(i["type"]) == 3:
                            base_category = db.category.find_one(
                                {"_id": ObjectId(str(i["data"][0]["id"]).replace(" ", ""))},
                                {"parentId": 1, "categoryName": 1},
                            )
                            if base_category != None:
                                if "parentId" in base_category:
                                    second_category = db.category.find_one(
                                        {"_id": ObjectId(base_category["parentId"])},
                                        {"parentId": 1, "categoryName": 1},
                                    )
                                    if second_category != None:
                                        if "parentId" in second_category:
                                            sub_sub_category_name = base_category["categoryName"][
                                                "en"
                                            ]
                                            sub_category_name = second_category["categoryName"][
                                                "en"
                                            ]
                                            first_category = db.category.find_one(
                                                {"_id": ObjectId(second_category["parentId"])},
                                                {"categoryName": 1},
                                            )
                                            if first_category != None:
                                                category_name = first_category["categoryName"]["en"]
                                            else:
                                                category_name = ""
                                        else:
                                            category_name = second_category["categoryName"]["en"]
                                            sub_category_name = base_category["categoryName"]["en"]
                                            sub_sub_category_name = ""
                                    else:
                                        first_category = db.category.find_one(
                                            {
                                                "_id": ObjectId(
                                                    str(i["data"][0]["id"]).replace(" ", "")
                                                )
                                            },
                                            {"parentId": 1, "categoryName": 1},
                                        )
                                        if first_category != None:
                                            category_name = first_category["categoryName"]["en"]
                                        else:
                                            category_name = ""
                                            sub_category_name = ""
                                            sub_sub_category_name = ""
                                else:
                                    first_category = db.category.find_one(
                                        {"_id": ObjectId(str(i["data"][0]["id"]).replace(" ", ""))},
                                        {"parentId": 1, "categoryName": 1},
                                    )
                                    if first_category != None:
                                        category_name = first_category["categoryName"]["en"]
                                    else:
                                        category_name = ""
                                        sub_category_name = ""
                                        sub_sub_category_name = ""
                            else:
                                category_name = ""
                                sub_category_name = ""
                                sub_sub_category_name = ""

                        if sub_category_name != "" and sub_sub_category_name == "":
                            banner_deatils_data.append(
                                {
                                    "type": 5,
                                    "bannerTypeMsg": "subcategory",
                                    "catName": category_name,
                                    "offerName": "",
                                    "name": i["data"][0]["name"]["en"],
                                    "imageWeb": i["image_web"],
                                    "imageMobile": i["image_mobile"],
                                }
                            )
                        elif sub_category_name != "" and sub_sub_category_name != "":
                            banner_deatils_data.append(
                                {
                                    "type": 6,
                                    "bannerTypeMsg": "subsubcategory",
                                    "offerName": "",
                                    "catName": category_name,
                                    "subCatName": sub_category_name,
                                    "name": i["data"][0]["name"]["en"],
                                    "imageWeb": i["image_web"],
                                    "imageMobile": i["image_mobile"],
                                }
                            )
                        elif int(i["type"]) == 1:
                            offer_details = db.offers.find_one(
                                {"_id": ObjectId(i["data"][0]["id"]), "status": 1}, {"name": 1}
                            )
                            if offer_details != None:
                                child_product_count = db.products.find(
                                    {
                                        "offer.status": 1,
                                        "offer.offerId": str(offer_details["_id"]),
                                        "status": 1,
                                    }
                                ).count()
                                if child_product_count > 0:
                                    banner_deatils_data.append(
                                        {
                                            "type": i["type"],
                                            "bannerTypeMsg": banner_type,
                                            "catName": "",
                                            "subCatName": "",
                                            "offerName": offer_details["name"]["en"],
                                            "name": str(offer_details["_id"]),
                                            "imageWeb": i["image_web"],
                                            "imageMobile": i["image_mobile"],
                                        }
                                    )
                        elif int(i["type"]) == 8:
                            product_details = db.products.find_one(
                                {"_id": ObjectId(i["data"][0]["id"]), "status": 1}, {"units": 1}
                            )
                            if product_details != None:
                                supplier_list = []
                                if "suppliers" in product_details["units"][0]:
                                    for s in product_details["units"][0]["suppliers"]:
                                        if s["id"] != "0":
                                            child_product_count = db.childProducts.find(
                                                {"_id": ObjectId(s["productId"]), "status": 1}
                                            ).count()
                                            if child_product_count > 0:
                                                supplier_list.append(s)
                                        else:
                                            pass

                                if len(supplier_list) > 0:
                                    best_supplier = min(
                                        supplier_list, key=lambda x: x["retailerPrice"]
                                    )
                                    if best_supplier["retailerQty"] == 0:
                                        best_supplier = max(
                                            supplier_list, key=lambda x: x["retailerQty"]
                                        )
                                    else:
                                        best_supplier = best_supplier
                                    if len(best_supplier) > 0:
                                        central_product_id = str(product_details["_id"])
                                        child_product_id = str(best_supplier["productId"])
                                        banner_deatils_data.append(
                                            {
                                                "type": i["type"],
                                                "bannerTypeMsg": banner_type,
                                                "catName": "",
                                                "subCatName": "",
                                                "offerName": "/python/product/details?&parentProductId="
                                                + central_product_id
                                                + "&productId="
                                                + child_product_id,
                                                "name": "/python/product/details?&parentProductId="
                                                + central_product_id
                                                + "&productId="
                                                + child_product_id,
                                                "imageWeb": i["image_web"],
                                                "imageMobile": i["image_mobile"],
                                            }
                                        )
                        elif int(i["type"]) == 9:
                            banner_deatils_data.append(
                                {
                                    "type": i["type"],
                                    "bannerTypeMsg": banner_type,
                                    "catName": "",
                                    "subCatName": "",
                                    "offerName": i["data"][0]["name"]["en"],
                                    "name": i["data"][0]["name"]["en"],
                                    "imageWeb": i["image_web"],
                                    "imageMobile": i["image_mobile"],
                                }
                            )
                        elif int(i["type"]) == 0:
                            banner_deatils_data.append(
                                {
                                    "type": i["type"],
                                    "bannerTypeMsg": banner_type,
                                    "catName": "",
                                    "subCatName": "",
                                    "offerName": "",
                                    "name": "",
                                    "imageWeb": i["image_web"],
                                    "imageMobile": i["image_mobile"],
                                }
                            )
                        else:
                            try:
                                store_details = json.loads(i["data"][0]["name"])
                            except:
                                try:
                                    store_details = i["data"][0]["name"]
                                except:
                                    store_details = {"en": ""}
                            try:
                                if store_details["en"] != "":
                                    banner_deatils_data.append(
                                        {
                                            "type": i["type"],
                                            "bannerTypeMsg": banner_type,
                                            "offerName": "",
                                            "name": store_details["en"],
                                            "imageWeb": i["image_web"],
                                            "imageMobile": i["image_mobile"],
                                        }
                                    )
                            except:
                                pass
                    except:
                        pass
                if len(banner_deatils_data) > 0:
                    response = {"message": "data found", "data": banner_deatils_data}
                    return JsonResponse(response, safe=False, status=200)
                else:
                    response = {"message": "data not found", "data": []}
                    return JsonResponse(response, safe=False, status=404)
            else:
                response = {"message": "data not found", "data": []}
                return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "an exception of type {0} occurred. arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            response_data = {
                "message": translator.translate(str(message), dest=language).text,
                "penCount": 0,
                "data": [],
            }
            return_response = {"data": response_data}
            return JsonResponse(return_response, safe=False, status=500)


"""
    API for the get the all seller list for product
"""


class ProductSellerList(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Seller List"],
        operation_description="API for getting the list of seller",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="productId",
                default="5ffc5861c4235db708b18156",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="product id for which product need to get the seller, NOTE NEED TO SEND PARENT PRODUCT ID",
            ),
        ],
        responses={
            200: "successfully. review or rating added",
            404: "data not found. it might be user not found, product not found",
            401: "Unauthorized. token expired",
            422: "Feilds are missing. required Feilds are missing",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            token = request.META["HTTP_AUTHORIZATION"]
            if token == "":
                response_data = {"message": "unauthorized", "data": []}
                return JsonResponse(response_data, safe=False, status=401)
            product_id = request.GET.get("productId", "")
            if product_id == "":
                response_data = {"message": "product id is missing", "data": []}
                return JsonResponse(response_data, safe=False, status=422)
            else:
                product_data = []
                product_details = db.products.find_one({"_id": ObjectId(product_id)}, {"units": 1})
                if product_details is not None:
                    if "suppliers" in product_details["units"][0]:
                        for supplier in product_details["units"][0]["suppliers"]:
                            if supplier["id"] != "0":
                                child_product_count = db.childProducts.find(
                                    {"_id": ObjectId(supplier["productId"]), "status": 1}
                                ).count()
                                if child_product_count > 0:
                                    avg_rating_value = 0
                                    seller_rating = db.sellerReviewRatings.aggregate(
                                        [
                                            {"$match": {"sellerId": str(supplier["id"])}},
                                            {
                                                "$group": {
                                                    "_id": "$sellerId",
                                                    "avgRating": {"$avg": "$rating"},
                                                }
                                            },
                                        ]
                                    )
                                    for avg_rating in seller_rating:
                                        avg_rating_value = avg_rating["avgRating"]
                                    if "storeName" in supplier:
                                        product_data.append(
                                            {
                                                "parentProductId": str(product_details["_id"]),
                                                "avgRating": avg_rating_value,
                                                "childProductId": supplier["productId"],
                                                "storeName": supplier["storeName"]["en"],
                                                "sellerId": supplier["id"],
                                            }
                                        )
                                    else:
                                        pass
                                else:
                                    pass
                            else:
                                pass
                    else:
                        pass
                else:
                    pass

                if len(product_data) > 0:
                    response = {"message": "data found", "data": product_data}
                    return JsonResponse(response, safe=False, status=200)
                else:
                    response = {"message": "data not found", "data": []}
                    return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "an exception of type {0} occurred. arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            response_data = {
                "message": translator.translate(str(message), dest=language).text,
                "penCount": 0,
                "data": [],
            }
            return_response = {"data": response_data}
            return JsonResponse(return_response, safe=False, status=500)


class HomePageNew2(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the data on home page in app and website",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="loginType",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="getting the data for retailer or distributor. "
                "1 for retailer \n"
                "2 for distributor",
                default="1",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default=MEAT_STORE_CATEGORY_ID,
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5fcf541871d7bd51e6008c94",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular zone",
            ),
            openapi.Parameter(
                name="storeId",
                default="5f06d68bb18ddc49df328de5",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular store",
            ),
            openapi.Parameter(
                name="lat",
                required=True,
                default="13.05176",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="latitude of the user's location",
            ),
            openapi.Parameter(
                name="long",
                default="77.580448",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="longtitue of the user's location",
            ),
            openapi.Parameter(
                name="cityId",
                default="5df7b7218798dc2c1114e6bf",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="city id from which city we need to show price, for city pricing, mainly we are using for meat flow",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:

            start_time_date = time.time()
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            last_json_response = []
            notification_count = 0
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            zone_id = str(request.META["HTTP_ZONEID"]) if "HTTP_ZONEID" in request.META else ""
            store_id = str(request.META["HTTP_STOREID"]) if "HTTP_STOREID" in request.META else ""
            city_id = request.GET.get("cityId", "5df7b7218798dc2c1114e6bf")
            token = request.META["HTTP_AUTHORIZATION"]
            try:
                institution_type = json.loads(token)["metaData"]["institutionType"]
            except:
                institution_type = 1

            try:
                user_id = json.loads(token)["userId"]
            except:
                user_id = "5df87244cb12e160bf694309"
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)

            is_ecommerce, remove_central, hide_recent_view, store_listing = validate_store_category(
                store_category_id, ECOMMERCE_STORE_CATEGORY_ID
            )

            if is_ecommerce == True and store_category_id != MEAT_STORE_CATEGORY_ID:
                store_id = ""
                zone_id = ""
            else:
                if store_id != "":
                    zone_id = ""
            # zone_id = "5fcf541871d7bd51e6008c94"
            try:
                if zone_id != "":
                    driver_roaster = next_availbale_driver_roaster(zone_id)
                    next_delivery_slot = driver_roaster["text"]
                    next_availbale_driver_time = driver_roaster["productText"]
                else:
                    driver_roaster = {}
                    next_delivery_slot = ""
                    next_availbale_driver_time = ""
            except:
                driver_roaster = {}
                next_delivery_slot = ""
                next_availbale_driver_time = ""

            # ==============================================get product details===================================
            start_time = time.time()
            language = language
            store_data_details = ["0"]
            store_data_json = []
            response_json = []
            must_query = [
                {"terms": {"status": [1, 2]}},
                {"match": {"storeCategoryId": str(store_category_id)}},
                {"terms": {"productFor": [0, str(institution_type)]}},
            ]
            store_query = None
            seller_dc_id = ""
            if zone_id != "":
                store_data_details = ["0"]
                zone_details = zone_find({"_id": ObjectId(zone_id)})
                try:
                    seller_dc_id = zone_details["DCStoreId"]
                    store_data_json.append(ObjectId(zone_details["DCStoreId"]))
                    store_data_details.append(str(zone_details["DCStoreId"]))
                except:
                    pass
            else:
                store_query = None
            if len(store_data_details) > 0:
                must_query.append({"terms": {"storeId": store_data_details}})
            else:
                pass
            category_data_time = time.time()
            sort_query = [
                {"isCentral": {"order": "desc"}},
                {"isInStock": {"order": "desc"}},
                {"units.discountPrice": {"order": "asc"}},
                {"_score": {"order": "desc"}},
            ]
            if institution_type == 2:
                b2b_pricing = "b2bPricing"
            else:
                b2b_pricing = "b2cPricing"
            query = {
                "query": {
                    "bool": {
                        "must": must_query,
                        "must_not": [
                            {"match": {"units." + b2b_pricing + ".b2bproductSellingPrice": 0}}
                        ],
                    }
                },
                "aggs": {
                    "group_by_catName": {
                        "terms": {
                            "field": "categoryList.parentCategory.categoryName.en.keyword",
                            "size": 10,
                        },
                        "aggs": {
                            "top_hits": {
                                "terms": {
                                    "field": "parentProductId.keyword",
                                    "size": 30,
                                    "order": {"avg_score": "desc"},
                                },
                                "aggs": {
                                    "avg_score": {"sum": {"field": "isInStock"}},
                                    "top_sub_category_hits": {
                                        "top_hits": {
                                            "sort": sort_query,
                                            "_source": {
                                                "includes": [
                                                    "_id",
                                                    "pName",
                                                    "storeId",
                                                    "parentProductId",
                                                    "pPName",
                                                    "categoryList",
                                                    "images",
                                                    "avgRating",
                                                    "units",
                                                    "storeCategoryId",
                                                    "manufactureName",
                                                    "maxQuantity",
                                                ]
                                            },
                                            "size": 1,
                                        }
                                    },
                                },
                            }
                        },
                    }
                },
            }
            res = es.search(index=CHILD_PRODUCT_INDEX, body=query)
            if len(res["aggregations"]["group_by_catName"]["buckets"]) == 0:
                response = {
                    "list": [],
                }
                for product in response["list"]:
                    last_json_response.append(product)
            else:
                pass
            # =================get all the zone dc list=====================================================
            store_details = db.stores.find(
                {"serviceZones.zoneId": zone_id, "storeFrontTypeId": {"$ne": 5}, "status": 1}
            )
            dc_seller_list = []
            seller_list_object_ids = []
            main_sellers = []
            for seller in store_details:
                main_sellers.append(str(seller["_id"]))
            if seller_dc_id != "":
                dc_seller_list.append(str(seller_dc_id))
                seller_list_object_ids.append(ObjectId(seller_dc_id))
            else:
                for dc_seller in store_details:
                    dc_seller_list.append(str(dc_seller["_id"]))
                    seller_list_object_ids.append(ObjectId(dc_seller["_id"]))
            if "group_by_catName" in res["aggregations"]:
                for i in res["aggregations"]["group_by_catName"]["buckets"]:
                    resData = []
                    category_name = i["key"]
                    doc_count = i["doc_count"]
                    all_product_ids = []
                    alldcSellers_list = []
                    for bucket in i["top_hits"]["buckets"]:
                        pro = bucket["top_sub_category_hits"]["hits"]["hits"][0]
                        child_product_count = db.childProducts.find(
                            {
                                "units.unitId": pro["_source"]["units"][0]["unitId"],
                                "storeId": {"$in": seller_list_object_ids},
                                "status": 1,
                            }
                        ).count()
                        if child_product_count > 0:
                            all_product_ids.append(ObjectId(pro["_id"]))
                        else:
                            pass

                        if pro["_source"]["storeId"] != "0":
                            alldcSellers_list.append(ObjectId(pro["_source"]["storeId"]))

                    dc_product_ids = []
                    product_ids = []
                    for inner in res["aggregations"]["group_by_catName"]["buckets"]:
                        if inner["key"] == category_name:
                            for inner_bucket in inner["top_hits"]["buckets"]:
                                for product in inner_bucket["top_sub_category_hits"]["hits"][
                                    "hits"
                                ]:
                                    dc_product_ids.append(ObjectId(product["_id"]))
                                    try:
                                        supplier_data = []
                                        get_child_product = db.childProducts.find_one(
                                            {
                                                "units.unitId": product["_source"]["units"][0][
                                                    "unitId"
                                                ],
                                                "storeId": {"$in": seller_list_object_ids},
                                                "status": 1,
                                            }
                                        )
                                        if get_child_product is not None:
                                            try:
                                                price = get_child_product["units"][0]["b2cPricing"][
                                                    0
                                                ]["b2cproductSellingPrice"]
                                            except:
                                                price = get_child_product["units"][0]["floatValue"]
                                            supplier_data.append(
                                                {
                                                    "id": str(get_child_product["storeId"]),
                                                    "retailerPrice": price,
                                                    "retailerQty": get_child_product["units"][0][
                                                        "availableQuantity"
                                                    ],
                                                    "productId": str(get_child_product["_id"]),
                                                }
                                            )
                                        else:
                                            try:
                                                price = product["_source"]["units"][0][
                                                    "b2cPricing"
                                                ][0]["b2cproductSellingPrice"]
                                            except:
                                                price = product["_source"]["units"][0]["floatValue"]
                                            supplier_data.append(
                                                {
                                                    "id": product["_source"]["storeId"],
                                                    "retailerPrice": price,
                                                    "retailerQty": product["_source"]["units"][0][
                                                        "availableQuantity"
                                                    ],
                                                    "productId": product["_id"],
                                                }
                                            )

                                        if len(supplier_data) > 0:
                                            best_supplier = min(
                                                supplier_data, key=lambda x: x["retailerPrice"]
                                            )
                                            if best_supplier["retailerQty"] == 0:
                                                best_supplier = max(
                                                    supplier_data, key=lambda x: x["retailerQty"]
                                                )
                                            else:
                                                best_supplier = best_supplier
                                        else:
                                            best_supplier = {}
                                        if len(best_supplier) > 0:
                                            product_ids.append(ObjectId(best_supplier["productId"]))
                                        else:
                                            pass
                                    except Exception as ex:
                                        print(
                                            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                                            type(ex).__name__,
                                            ex,
                                        )
                                        pass

                    child_product_data_info = list(
                        db.childProducts.find({"_id": {"$in": product_ids}})
                    )
                    for child_product_details in child_product_data_info:
                        all_dc_list = []
                        is_dc_linked = False
                        dc_id = ""
                        dc_child_product = db.childProducts.find_one(
                            {
                                "units.unitId": child_product_details["units"][0]["unitId"],
                                "storeId": {"$in": store_data_json},
                                "status": 1,
                            }
                        )
                        hard_limit = 0
                        pre_order = False
                        procurementTime = 0
                        if dc_child_product is not None:
                            if "seller" in dc_child_product:
                                if len(dc_child_product["seller"]) > 0:
                                    is_dc_linked = True
                                    dc_id = str(dc_child_product["_id"])
                                    for seller in dc_child_product["seller"]:
                                        if seller["storeId"] in main_sellers:
                                            if seller["preOrder"] == True:
                                                all_dc_list.append(seller)
                                            else:
                                                pass
                                        else:
                                            pass
                                else:
                                    pass
                            else:
                                pass

                            if len(all_dc_list) == 0:
                                if "seller" in dc_child_product:
                                    if len(dc_child_product["seller"]) > 0:
                                        for new_seller in dc_child_product["seller"]:
                                            if new_seller["storeId"] in main_sellers:
                                                all_dc_list.append(new_seller)
                                            else:
                                                pass
                                    else:
                                        pass
                                else:
                                    pass
                            else:
                                pass
                        else:
                            pass

                        if is_dc_linked:
                            if len(all_dc_list) > 0:
                                best_seller_product = min(
                                    all_dc_list, key=lambda x: x["procurementTime"]
                                )
                            else:
                                best_seller_product = {}
                        else:
                            best_seller_product = {}
                        main_product_details = None
                        if len(best_seller_product) > 0:
                            hard_limit = best_seller_product["hardLimit"]
                            pre_order = best_seller_product["preOrder"]
                            procurementTime = best_seller_product["procurementTime"]
                            main_product_details = db.childProducts.find_one(
                                {
                                    "parentProductId": child_product_details["parentProductId"],
                                    "units.unitId": child_product_details["units"][0]["unitId"],
                                    "storeId": ObjectId(best_seller_product["storeId"]),
                                }
                            )
                            if main_product_details is not None:
                                child_product_id = str(main_product_details["_id"])
                            else:
                                child_product_id = str(child_product_details["_id"])
                        else:
                            child_product_id = str(child_product_details["_id"])

                        variant_data = []
                        if is_dc_linked:
                            try:
                                available_qty = dc_child_product["units"][0]["availableQuantity"]
                            except:
                                available_qty = 0
                        else:
                            try:
                                available_qty = child_product_details["units"][0][
                                    "availableQuantity"
                                ]
                            except:
                                available_qty = 0
                        # ===============================offer data======================================
                        offers_details = []
                        offer_details_data = []
                        if dc_id != "":
                            child_offer_details = db.childProducts.find_one(
                                {"_id": ObjectId(dc_id)}
                            )
                            if child_offer_details is not None:
                                if "offer" in child_offer_details:
                                    for offer in child_offer_details["offer"]:
                                        if offer["status"] == 1:
                                            offer_count = db.offers.find_one(
                                                {"_id": ObjectId(offer["offerId"])}
                                            )
                                            if offer_count is not None:
                                                if offer_count["startDateTime"] <= int(time.time()):
                                                    offer["termscond"] = ""
                                                    offers_details.append(offer)
                                                    offer_details_data.append(
                                                        {
                                                            "offerId": offer["offerId"],
                                                            "offerName": offer["offerName"]["en"],
                                                            "webimages": offer["webimages"][
                                                                "image"
                                                            ],
                                                            "mobimage": offer["images"]["image"],
                                                            "discountValue": offer["discountValue"],
                                                        }
                                                    )
                                        else:
                                            pass
                                else:
                                    pass
                            else:
                                pass
                        else:
                            if "offer" in child_product_details:
                                for offer in child_product_details["offer"]:
                                    if offer["status"] == 1:
                                        # if offer['offerId'] in offer_json:
                                        offer_terms = db.offers.find_one(
                                            {"_id": ObjectId(offer["offerId"])}
                                        )
                                        if offer_terms is not None:
                                            if offer_terms["startDateTime"] <= int(time.time()):
                                                offer["termscond"] = offer_terms["termscond"]
                                                offer["name"] = offer_terms["name"]["en"]
                                                offer["discountValue"] = offer_terms[
                                                    "discountValue"
                                                ]
                                                offer["discountType"] = offer_terms["offerType"]
                                                offers_details.append(offer)
                                                offer_details_data.append(
                                                    {
                                                        "offerId": offer["offerId"],
                                                        "offerName": offer["offerName"]["en"],
                                                        "webimages": offer["webimages"]["image"],
                                                        "mobimage": offer["images"]["image"],
                                                        "discountValue": offer["discountValue"],
                                                    }
                                                )
                                    else:
                                        pass
                            else:
                                pass
                        if len(offers_details) > 0:
                            best_offer = max(offers_details, key=lambda x: x["discountValue"])
                            currdate = datetime.datetime.now().replace(
                                hour=23, minute=59, second=59, microsecond=59
                            )
                            eastern = timezone(timezonename)
                            currlocal = eastern.localize(currdate)
                            best_offer["endDateTimeISO"] = int(((currlocal).timestamp())) * 1000
                        else:
                            best_offer = {}
                        # ======================================product seo======================================================
                        try:
                            if "productSeo" in child_product_details:
                                if len(child_product_details["productSeo"]["title"]) > 0:
                                    title = (
                                        child_product_details["productSeo"]["title"][language]
                                        if language in child_product_details["productSeo"]["title"]
                                        else child_product_details["productSeo"]["title"]["en"]
                                    )
                                else:
                                    title = ""

                                if len(child_product_details["productSeo"]["description"]) > 0:
                                    description = (
                                        child_product_details["productSeo"]["description"][language]
                                        if language
                                        in child_product_details["productSeo"]["description"]
                                        else child_product_details["productSeo"]["description"][
                                            "en"
                                        ]
                                    )
                                else:
                                    description = ""

                                if len(child_product_details["productSeo"]["metatags"]) > 0:
                                    metatags = (
                                        child_product_details["productSeo"]["metatags"][language]
                                        if language
                                        in child_product_details["productSeo"]["metatags"]
                                        else child_product_details["productSeo"]["metatags"]["en"]
                                    )
                                else:
                                    metatags = ""

                                if len(child_product_details["productSeo"]["slug"]) > 0:
                                    slug = (
                                        child_product_details["productSeo"]["slug"][language]
                                        if language in child_product_details["productSeo"]["slug"]
                                        else child_product_details["productSeo"]["slug"]["en"]
                                    )
                                else:
                                    slug = ""

                                product_seo = {
                                    "title": title,
                                    "description": description,
                                    "metatags": metatags,
                                    "slug": slug,
                                }
                            else:
                                product_seo = {
                                    "title": "",
                                    "description": "",
                                    "metatags": "",
                                    "slug": "",
                                }
                        except:
                            product_seo = {
                                "title": "",
                                "description": "",
                                "metatags": "",
                                "slug": "",
                            }
                        tax_value = []

                        # =========================================pharmacy details=========================================
                        if "prescriptionRequired" in child_product_details:
                            if child_product_details["prescriptionRequired"] == 0:
                                prescription_required = False
                            else:
                                prescription_required = True
                        else:
                            prescription_required = False

                        if "saleOnline" in child_product_details:
                            if child_product_details["saleOnline"] == 0:
                                sales_online = False
                            else:
                                sales_online = True
                        else:
                            sales_online = False

                        if "uploadProductDetails" in child_product_details:
                            upload_details = child_product_details["uploadProductDetails"]
                        else:
                            upload_details = ""

                        # ========================= for the get the linked the unit data====================================
                        try:
                            for link_unit in child_product_details["units"][0]["attributes"]:
                                try:
                                    for attrlist in link_unit["attrlist"]:
                                        try:
                                            if attrlist is None:
                                                pass
                                            else:
                                                if attrlist["linkedtounit"] == 1:
                                                    if attrlist["measurementUnit"] == "":
                                                        attr_name = (
                                                            str(attrlist["value"][language])
                                                            if language in attrlist["value"]
                                                            else str(attrlist["value"]["en"])
                                                        )
                                                    else:
                                                        attr_name = (
                                                            str(attrlist["value"][language])
                                                            + " "
                                                            + attrlist["measurementUnit"]
                                                            if language in attrlist["value"]
                                                            else str(attrlist["value"]["en"])
                                                            + " "
                                                            + attrlist["measurementUnit"]
                                                        )
                                                    variant_data.append(
                                                        {
                                                            "attrname": attrlist["attrname"]["en"],
                                                            "value": str(attr_name),
                                                            "name": attrlist["attrname"]["en"],
                                                        }
                                                    )
                                                else:
                                                    pass
                                        except:
                                            pass
                                except:
                                    pass
                        except:
                            pass
                        # =========================for max quantity=================================================
                        if "maxQuantity" in child_product_details:
                            if child_product_details["maxQuantity"] != "":
                                max_quantity = int(child_product_details["maxQuantity"])
                            else:
                                max_quantity = 30
                        else:
                            max_quantity = 30
                        # ==========================================================================================
                        if "allowOrderOutOfStock" in child_product_details:
                            allow_out_of_order = child_product_details["allowOrderOutOfStock"]
                        else:
                            allow_out_of_order = False
                        mobile_images = []

                        if "productType" in child_product_details:
                            if child_product_details["productType"] == 2:
                                combo_product = True
                            else:
                                combo_product = False
                        else:
                            combo_product = False

                        minimum_order_qty = 1
                        unit_package_type = "Box"
                        unit_moq_type = "Box"
                        moq_data = ""

                        # ================get the city pricing for the products=====================
                        (
                            base_price,
                            minimum_order_qty,
                            unit_package_type,
                            unit_moq_type,
                            moq_data,
                            seller_price,
                        ) = cal_product_city_pricing(
                            institution_type, city_id, child_product_details
                        )
                        tax_price = 0
                        if "tax" in child_product_details:
                            if len(child_product_details["tax"]) == 0:
                                tax_price = 0
                            else:
                                for amount in child_product_details["tax"]:
                                    if "taxValue" in amount:
                                        tax_price = tax_price + (int(amount["taxValue"]))
                                    if "value" in amount:
                                        tax_price = tax_price + (int(amount["value"]))
                                    else:
                                        tax_price = tax_price + 0
                        else:
                            tax_price = 0

                        if len(best_offer) > 0:
                            discount_type = (
                                int(best_offer["discountType"])
                                if "discountType" in best_offer
                                else 1
                            )
                            discount_value = (
                                best_offer["discountValue"] if "discountValue" in best_offer else 0
                            )
                        else:
                            discount_type = 2
                            discount_value = 0

                        if discount_type == 0:
                            percentage = 0
                        else:
                            percentage = int(discount_value)

                        try:
                            currency_rate = currency_exchange_rate[
                                str(child_product_details["currency"]) + "_" + str(currency_code)
                            ]
                        except:
                            currency_rate = 0
                        currency_details = db.currencies.find_one({"currencyCode": currency_code})
                        if currency_details is not None:
                            currency_symbol = currency_details["currencySymbol"]
                            currency = currency_details["currencyCode"]
                        else:
                            currency_symbol = child_product_details["currencySymbol"]
                            currency = child_product_details["currency"]

                        if float(currency_rate) > 0:
                            base_price = base_price * float(currency_rate)
                        base_price = base_price + ((float(base_price) * tax_price) / 100)

                        # ==============calculate discount price =============================
                        if discount_type == 0:
                            discount_price = float(discount_value)
                        elif discount_type == 1:
                            discount_price = (float(base_price) * float(discount_value)) / 100
                        else:
                            discount_price = 0
                        final_price = base_price - discount_price

                        try:
                            product_name = (
                                child_product_details["pName"]["en"]
                                if "pName" in child_product_details
                                else child_product_details["pPName"]["en"]
                            )
                        except:
                            product_name = child_product_details["units"][0]["unitName"]["en"]
                        # ===================================variant count======================================
                        variant_query = {
                            "parentProductId": child_product_details["parentProductId"],
                            "status": 1,
                        }
                        # if str(child_product_details["storeId"]) == "0":
                        #     variant_query["storeId"] = child_product_details["storeId"]
                        # else:
                        #     if store_category_id == MEAT_STORE_CATEGORY_ID:
                        #         variant_query["storeId"] = {
                        #             "$in": [ObjectId(child_product_details["storeId"])]
                        #         }
                        #     else:
                        #         variant_query["storeId"] = ObjectId(
                        #             child_product_details["storeId"]
                        #         )
                        variant_count_data = db.childProducts.find(variant_query).count()
                        if variant_count_data > 1:
                            variant_count = True
                        else:
                            variant_count = False

                        # =============================calculate shift and avability check for meat flow================
                        outOfStock, next_availbale_time = meat_availability_check(
                            main_product_details
                            if main_product_details is not None
                            else child_product_details,
                            available_qty,
                            is_dc_linked,
                            next_availbale_driver_time,
                            hard_limit,
                            pre_order,
                            driver_roaster,
                            dc_child_product,
                            zone_id,
                            procurementTime,
                        )
                        isShoppingList = False

                        if "containsMeat" in child_product_details:
                            contains_Meat = child_product_details["containsMeat"]
                        else:
                            contains_Meat = False
                        avg_rating = (
                            child_product_details["avgRating"]
                            if "avgRating" in child_product_details
                            else 0
                        )
                        # =====================from here need to send dc supplier id for the product===============
                        resData.append(
                            {
                                "maxQuantity": max_quantity,
                                "isComboProduct": combo_product,
                                "variantCount": variant_count,
                                "childProductId": str(child_product_id),
                                "availableQuantity": available_qty,
                                "offerDetailsData": offer_details_data,
                                "productName": product_name,
                                "parentProductId": child_product_details["parentProductId"],
                                "suppliers": {
                                    "id": str(child_product_details["storeId"]),
                                    "productId": str(child_product_id),
                                },
                                "supplier": {
                                    "id": str(child_product_details["storeId"]),
                                    "productId": str(child_product_id),
                                },
                                "containsMeat": contains_Meat,
                                "isShoppingList": isShoppingList,
                                "tax": tax_value,
                                "linkedAttribute": variant_data,
                                "allowOrderOutOfStock": allow_out_of_order,
                                "moUnit": "Pcs",
                                "outOfStock": outOfStock,
                                "variantData": variant_data,
                                "addOnsCount": 0,
                                "prescriptionRequired": prescription_required,
                                "saleOnline": sales_online,
                                "uploadProductDetails": upload_details,
                                "productSeo": product_seo,
                                "brandName": child_product_details["brandTitle"][language]
                                if language in child_product_details["brandTitle"]
                                else child_product_details["brandTitle"]["en"],
                                "manufactureName": child_product_details["manufactureName"][
                                    language
                                ]
                                if language in child_product_details["manufactureName"]
                                else "",
                                "TotalStarRating": avg_rating,
                                "currencySymbol": currency_symbol,
                                "mobileImage": [],
                                "currency": currency,
                                "storeCategoryId": child_product_details["storeCategoryId"]
                                if "storeCategoryId" in child_product_details
                                else "",
                                "images": child_product_details["images"],
                                "mobimages": mobile_images,
                                "units": child_product_details["units"],
                                "finalPriceList": {
                                    "basePrice": round(base_price, 2),
                                    "finalPrice": round(final_price, 2),
                                    "discountPrice": round(discount_price, 2),
                                    "discountType": discount_type,
                                    "discountPercentage": percentage,
                                },
                                "price": int(final_price),
                                "isDcAvailable": is_dc_linked,
                                "discountType": discount_type,
                                "unitId": str(child_product_details["units"][0]["unitId"]),
                                "offer": best_offer,
                                "MOQData": {
                                    "minimumOrderQty": minimum_order_qty,
                                    "unitPackageType": unit_package_type,
                                    "unitMoqType": unit_moq_type,
                                    "MOQ": moq_data,  # str(minimum_order_qty) + " " + unit_package_type,
                                },
                                # "offers": best_offer,
                                "nextSlotTime": next_availbale_time,
                            }
                        )

                    if len(resData) > 0:
                        newlist = sorted(resData, key=lambda k: k["isDcAvailable"], reverse=True)
                        res_data_dataframe = pd.DataFrame(newlist)
                        res_data_dataframe = res_data_dataframe.drop_duplicates(
                            "productName", keep="first"
                        )
                        newlist = res_data_dataframe.to_dict(orient="records")
                        offers_data = []
                        category_count = db.category.find_one(
                            {
                                "categoryName.en": category_name,
                                "status": 1,
                                "storeId": store_id if store_id != "" else "0",
                                "storeCategory.storeCategoryId": store_category_id,
                            }
                        )
                        if category_count is not None:
                            if len(newlist) > 0:
                                response_json.append(
                                    {
                                        "id": str(category_count['_id']),
                                        "catName": category_name,
                                        "imageUrl": "",
                                        "bannerImageUrl": "",
                                        "websiteImageUrl": "",
                                        "websiteBannerImageUrl": "",
                                        "offers": offers_data,
                                        "penCount": doc_count,
                                        "catSeq": category_count["seqId"],
                                        "categoryData": newlist,
                                        "type": 4,
                                        "seqId": 6,
                                    }
                                )
                if len(response_json) > 0:
                    newlist = sorted(response_json, key=lambda k: k["catSeq"], reverse=True)
                    response = {
                        "list": newlist,
                    }
                    for product in response["list"]:
                        last_json_response.append(product)
                else:
                    response = {
                        "list": [],
                    }
                    for product in response["list"]:
                        last_json_response.append(product)
            else:
                response = {
                    "list": [],
                }
                for product in response["list"]:
                    last_json_response.append(product)

            # ===============================================category data======================================
            seq_id = 1
            cat_start_time = time.time()
            try:
                category_json = []
                to_data = 20
                if store_id == "":
                    if zone_id == "":
                        category_query = {
                            "parentId": {"$exists": False},
                            "status": 1,
                            "storeCategory.storeCategoryId": str(store_category_id),
                            "storeId": "0",
                        }
                        category_data_db = category_find(category_query, 0, to_data)
                    else:
                        store_data_details = []
                        if zone_id != "" and store_category_id == PHARMACY_STORE_CATEGORY_ID:
                            store_query = {"categoryId": str(store_category_id), "cityId": city_id}
                        else:
                            store_query = {
                                "categoryId": str(store_category_id),
                                "serviceZones.zoneId": zone_id,
                            }
                        store_data = store_find(store_query)
                        if store_data.count() > 0:
                            for store in store_data:
                                store_data_details.append(str(store["_id"]))
                            category_query = {
                                "status": 1,
                                "parentId": {"$exists": False},
                                "storeCategory.storeCategoryId": str(store_category_id),
                                "storeId": "0"
                                # "$or": [{"storeid": {"$in": store_data_details}}, {"storeId": {"$in": store_data_details}}]
                            }
                            category_data_db = category_find(category_query, 0, to_data)
                            if category_data_db.count() == 0:
                                response = {
                                    "id": "",
                                    "catName": "",
                                    "imageUrl": "",
                                    "bannerImageUrl": "",
                                    "websiteImageUrl": "",
                                    "websiteBannerImageUrl": "",
                                    "offers": [],
                                    "penCount": 0,
                                    "categoryData": [],
                                    "type": 2,
                                    "seqId": seq_id,
                                }
                                last_json_response.append(response)
                        else:
                            response = {
                                "id": "",
                                "catName": "",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": [],
                                "type": 2,
                                "seqId": seq_id,
                            }
                            last_json_response.append(response)
                else:
                    category_query = {
                        "status": 1,
                        "parentId": {"$exists": False},
                        "storeCategory.storeCategoryId": str(store_category_id),
                        "storeid": {"$in": [store_id]},
                    }
                    category_data_db = category_find(category_query, 0, to_data)
                    if category_data_db.count() == 0:
                        category_query = {
                            "status": 1,
                            "parentId": {"$exists": False},
                            "storeCategory.storeCategoryId": str(store_category_id),
                            "storeId": store_id,
                        }
                        category_data_db = category_find(category_query, 0, to_data)

                if category_data_db.count() != 0:
                    for cate_gory in category_data_db:
                        category_json.append(
                            {
                                "id": str(cate_gory["_id"]),
                                "catName": cate_gory["categoryName"][language]
                                if language in cate_gory["categoryName"]
                                else cate_gory["categoryName"]["en"],
                                "imageUrl": cate_gory["mobileImage"]
                                if "mobileImage" in cate_gory
                                else "",
                                "bannerImageUrl": cate_gory["mobileImage"]
                                if "mobileImage" in cate_gory
                                else "",
                                "websiteImageUrl": cate_gory["websiteImage"]
                                if "websiteImage" in cate_gory
                                else "",
                                "websiteBannerImageUrl": cate_gory["websiteImage"]
                                if "websiteImage" in cate_gory
                                else "",
                            }
                        )
                    dataframe = pd.DataFrame(category_json)
                    dataframe = dataframe.drop_duplicates(subset="catName", keep="last")
                    details = dataframe.to_json(orient="records")
                    data = json.loads(details)
                    response = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": data,
                        "seqId": seq_id,
                        "type": 2,
                    }
                    last_json_response.append(response)
                else:
                    response = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": category_json,
                        "type": 2,
                        "seqId": seq_id,
                    }
                    last_json_response.append(response)
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                print(
                    "Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex
                )
                error = {
                    "id": "",
                    "catName": "",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "offers": [],
                    "penCount": 0,
                    "categoryData": [],
                    "type": 2,
                    "seqId": seq_id,
                }
                last_json_response.append(error)
            # thread10 = threading.Thread(target=home_page_seo, args=(language, store_category_id, res29))
            # ===============================for home page seo=================================================
            language = language
            response = {}
            home_page_data = home_page_find_one()
            if home_page_data is not None:
                if "homepage" in home_page_data:
                    response = {
                        "facebookIcon": home_page_data["homepage"]["facebookIcon"]
                        if "facebookIcon" in home_page_data["homepage"]
                        else "",
                        "facebookLink": home_page_data["homepage"]["facebookLink"]
                        if "facebookLink" in home_page_data["homepage"]
                        else "",
                        "twitterIcon": home_page_data["homepage"]["twitterIcon"]
                        if "twitterIcon" in home_page_data["homepage"]
                        else "",
                        "twitterLink": home_page_data["homepage"]["twitterLink"]
                        if "twitterLink" in home_page_data["homepage"]
                        else "",
                        "instagramIcon": home_page_data["homepage"]["instagramIcon"]
                        if "instagramIcon" in home_page_data["homepage"]
                        else "",
                        "instagramLink": home_page_data["homepage"]["instagramLink"]
                        if "instagramLink" in home_page_data["homepage"]
                        else "",
                        "youtubeIcon": home_page_data["homepage"]["youtubeIcon"]
                        if "youtubeIcon" in home_page_data["homepage"]
                        else "",
                        "youtubeLink": home_page_data["homepage"]["youtubeLink"]
                        if "youtubeLink" in home_page_data["homepage"]
                        else "",
                        "linkedInIcon": home_page_data["homepage"]["linkedInIcon"]
                        if "linkedInIcon" in home_page_data["homepage"]
                        else "",
                        "linkedInLink": home_page_data["homepage"]["linkedInLink"]
                        if "linkedInLink" in home_page_data["homepage"]
                        else "",
                        "copyRight": home_page_data["homepage"]["copyRight"][language]
                        if language in home_page_data["homepage"]["copyRight"]
                        else home_page_data["homepage"]["copyRight"]["en"],
                    }
            home_page_seo_response = {"data": response}
            store_response_data = []
            total_data_count = 0

            # ===========================================banner details=========================================if store_category_id == MEAT_STORE_CATEGORY_ID:
            seq_id = 2
            banner_time = time.time()
            try:
                banner_deatils_data = []
                banner_query = {
                    "status": 1,
                    "storeCategoryId": store_category_id,
                    "zones.zoneId": zone_id,
                }
                banner_type = ""
                banner_details = banner_find(banner_query)
                if banner_details.count() > 0:
                    for i in banner_details:
                        # try:
                        category_name = ""
                        sub_category_name = ""
                        sub_sub_category_name = ""
                        if int(i["type"]) == 1:
                            banner_type = "offer"
                        elif int(i["type"]) == 2:
                            banner_type = "brands"
                        elif int(i["type"]) == 3:
                            banner_type = "category"
                        elif int(i["type"]) == 4:
                            banner_type = "stores"
                        elif int(i["type"]) == 5:
                            banner_type = "subcategory"
                        elif int(i["type"]) == 6:
                            banner_type = "subsubcategory"
                        elif int(i["type"]) == 7:
                            banner_type = "supplier"

                        if int(i["type"]) == 3:
                            base_category = category_find_one(
                                {"_id": ObjectId(str(i["data"][0]["id"]).replace(" ", ""))}
                            )
                            if base_category is not None:
                                if "parentId" in base_category:
                                    second_category = category_find_one(
                                        {"_id": ObjectId(base_category["parentId"])}
                                    )
                                    if second_category is not None:
                                        if "parentId" in second_category:
                                            sub_sub_category_name = base_category["categoryName"][
                                                "en"
                                            ]
                                            sub_category_name = second_category["categoryName"][
                                                "en"
                                            ]
                                            first_category = category_find_one(
                                                {"_id": ObjectId(second_category["parentId"])}
                                            )
                                            if first_category is not None:
                                                category_name = first_category["categoryName"]["en"]
                                            else:
                                                category_name = ""
                                        else:
                                            category_name = second_category["categoryName"]["en"]
                                            sub_category_name = base_category["categoryName"]["en"]
                                            sub_sub_category_name = ""
                                    else:
                                        first_category = category_find_one(
                                            {
                                                "_id": ObjectId(
                                                    str(i["data"][0]["id"]).replace(" ", "")
                                                )
                                            }
                                        )
                                        if first_category is not None:
                                            category_name = first_category["categoryName"]["en"]
                                        else:
                                            category_name = ""
                                            sub_category_name = ""
                                            sub_sub_category_name = ""
                                else:
                                    first_category = category_find_one(
                                        {"_id": ObjectId(str(i["data"][0]["id"]).replace(" ", ""))}
                                    )
                                    if first_category is not None:
                                        category_name = first_category["categoryName"]["en"]
                                    else:
                                        category_name = ""
                                        sub_category_name = ""
                                        sub_sub_category_name = ""
                            else:
                                category_name = ""
                                sub_category_name = ""
                                sub_sub_category_name = ""

                        if sub_category_name != "" and sub_sub_category_name == "":
                            banner_deatils_data.append(
                                {
                                    "type": 5,
                                    "bannerTypeMsg": "subcategory",
                                    "catName": category_name,
                                    "offerName": "",
                                    "name": i["data"][0]["name"]["en"],
                                    "imageWeb": i["image_web"],
                                    "imageMobile": i["image_mobile"],
                                }
                            )
                        elif sub_category_name != "" and sub_sub_category_name != "":
                            banner_deatils_data.append(
                                {
                                    "type": 6,
                                    "bannerTypeMsg": "subsubcategory",
                                    "offerName": "",
                                    "catName": category_name,
                                    "subCatName": sub_category_name,
                                    "name": i["data"][0]["name"]["en"],
                                    "imageWeb": i["image_web"],
                                    "imageMobile": i["image_mobile"],
                                }
                            )
                        elif int(i["type"]) == 1:
                            offer_query = {"_id": ObjectId(i["data"][0]["id"]), "status": 1}
                            offer_details = offer_find_one(offer_query)
                            if offer_details is not None:
                                product_query = {
                                    "offer.status": 1,
                                    "offer.offerId": str(offer_details["_id"]),
                                    "status": 1,
                                }
                                child_product_count = product_find_count(product_query)
                                if child_product_count > 0:
                                    banner_deatils_data.append(
                                        {
                                            "type": i["type"],
                                            "bannerTypeMsg": banner_type,
                                            "catName": "",
                                            "subCatName": "",
                                            "offerName": offer_details["name"]["en"],
                                            "name": str(offer_details["_id"]),
                                            "imageWeb": i["image_web"],
                                            "imageMobile": i["image_mobile"],
                                        }
                                    )
                        else:
                            try:
                                store_details = json.loads(i["data"][0]["name"])
                            except:
                                try:
                                    store_details = i["data"][0]["name"]
                                except:
                                    store_details = {"en": ""}
                            try:
                                if store_details["en"] != "":
                                    banner_deatils_data.append(
                                        {
                                            "type": i["type"],
                                            "bannerTypeMsg": banner_type,
                                            "offerName": "",
                                            "name": store_details["en"],
                                            "imageWeb": i["image_web"],
                                            "imageMobile": i["image_mobile"],
                                        }
                                    )
                            except:
                                pass
                    # except:
                    #     pass
                    response = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "penCount": 0,
                        "offers": [],
                        "type": 1,
                        "seqId": seq_id,
                        "categoryData": banner_deatils_data,
                    }
                    last_json_response.append(response)
                else:
                    response = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "type": 1,
                        "seqId": seq_id,
                        "categoryData": [],
                    }
                    last_json_response.append(response)
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                print(
                    "Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex
                )
                error = {
                    "id": "",
                    "catName": "",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "offers": [],
                    "penCount": 0,
                    "seqId": 1,
                    "type": 1,
                    "categoryData": [],
                }
                last_json_response.append(response)
            # ==========================================hot deals===============================================
            error = {
                "id": "",
                "catName": "Deals of the Day",
                "imageUrl": "",
                "bannerImageUrl": "",
                "websiteImageUrl": "",
                "websiteBannerImageUrl": "",
                "offers": [],
                "penCount": 0,
                "categoryData": [],
                "type": 3,
                "seqId": 3,
            }
            last_json_response.append(error)
            # ====================================last response=================================================
            newlist = sorted(last_json_response, key=lambda k: k["seqId"])
            # =================================get notification count for user===============================
            try:
                notifictaion_data = session2.execute(
                    """SELECT COUNT(*) FROM notificationlogs where app_name=%(app_name)s AND userid=%(userid)s ALLOW FILTERING""",
                    {"app_name": APP_NAME, "userid": str(user_id)},
                )
                for count in notifictaion_data:
                    for j in count:
                        notification_count = j
            except Exception as e:
                print(e)
                notification_count = 0
            last_response = {
                "data": {
                    "notificationCount": notification_count,
                    "list": newlist,
                    "totalCatCount": total_data_count,
                    "seoData": home_page_seo_response["data"],
                    "storeData": store_response_data,
                    "nextDeliverySlot": next_delivery_slot,
                }
            }
            print(
                "********************total time for maety home page******************",
                time.time() - start_time_date,
            )
            return JsonResponse(last_response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


"""
    type 0  for website
    type 1 for app
    deviceType:
                0 for website
                1 for IOS
                2 for Android
    LoginType:
                0 for retailer
                1 for distributor
    Type in Response:
                1 for recently bought
                2 for banner
                3 for recently viewed
                4 for category

"""


class HomePageNew3(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the data on home page in app and website",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default="5df8766e8798dc2f236c95fa",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5fcf541871d7bd51e6008c94",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular zone",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            last_json_response = []
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            zone_id = str(request.META["HTTP_ZONEID"]) if "HTTP_ZONEID" in request.META else ""
            token = request.META["HTTP_AUTHORIZATION"]
            try:
                user_id = json.loads(token)["userId"]
            except:
                user_id = "609ccd3d635c3f68308be75f"
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif zone_id == "":
                response_data = {
                    "message": "zone id is missing",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # ==================================store details page======================================
                store_data_json = []
                store_details = []
                if zone_id != "":
                    store_data = db.stores.find(
                        {
                            "categoryId": str(store_category_id),
                            "serviceZones.zoneId": zone_id,
                            "status": 1,
                        }
                    )
                    for store in store_data:
                        store_details.append(str(store["_id"]))
                        store_data_json.append(ObjectId(store["_id"]))
                else:
                    pass
                # ==============================================get product details===================================
                try:
                    store_order_data = db.storeOrder.aggregate(
                        [
                            {"$match": {"status.status": 7}},
                            {"$match": {"createdBy.userId": user_id}},
                            {"$match": {"storeCategoryId": store_category_id}},
                            {"$project": {"products": 1}},
                            {"$unwind": "$products"},
                            {"$sort": {"createdTimeStamp": -1}},
                            {"$skip": 0},
                            {"$limit": 5},
                            {"$group": {"products": {"$push": "$products.productId"}, "_id": None}},
                        ]
                    )
                    total_product_data = db.storeOrder.aggregate(
                        [
                            {"$match": {"status.status": 7}},
                            {"$match": {"createdBy.userId": user_id}},
                            {"$match": {"storeCategoryId": store_category_id}},
                            {"$project": {"products": 1}},
                            {"$unwind": "$products"},
                            {"$count": "total_products"},
                        ]
                    )
                    total_product_count = 0
                    for count in total_product_data:
                        total_product_count = count["total_products"]
                    recently_bought = []
                    for product in store_order_data:
                        must_query = [
                            {"match": {"status": 1}},
                            {"match": {"storeCategoryId": str(store_category_id)}},
                            {"terms": {"storeId": store_details}},
                            {"terms": {"_id": product["products"]}},
                        ]
                        sort_query = [
                            {"isInStock": {"order": "desc"}},
                            {"units.discountPrice": {"order": "asc"}},
                        ]

                        must_query.append({"match": {"status": 1}})

                        search_item_query = {
                            "query": {
                                "bool": {
                                    "must": must_query,
                                    "must_not": [{"match": {"storeId": "0"}}],
                                }
                            },
                            "track_total_hits": True,
                            "sort": sort_query,
                            "aggs": {
                                "group_by_sub_category": {
                                    "terms": {
                                        "field": "parentProductId.keyword",
                                        "order": {"avg_score": "desc"},
                                        "size": 20,
                                    },
                                    "aggs": {
                                        "avg_score": {"max": {"script": "doc.isInStock"}},
                                        "top_sales_hits": {
                                            "top_hits": {
                                                "sort": sort_query,
                                                "_source": {
                                                    "includes": [
                                                        "_id",
                                                        "_score",
                                                        "pName",
                                                        "prescriptionRequired",
                                                        "needsIdProof",
                                                        "saleOnline",
                                                        "uploadProductDetails",
                                                        "storeId",
                                                        "parentProductId",
                                                        "currencySymbol",
                                                        "currency",
                                                        "pPName",
                                                        "tax",
                                                        "offer",
                                                        "brandTitle",
                                                        "categoryList",
                                                        "images",
                                                        "avgRating",
                                                        "units",
                                                        "storeCategoryId",
                                                        "manufactureName",
                                                        "maxQuantity",
                                                    ]
                                                },
                                                "size": 1,
                                            }
                                        },
                                    },
                                }
                            },
                        }
                        res = es.search(index=index_products, body=search_item_query)
                        try:
                            if "value" in res["hits"]["total"]:
                                if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                                    response = {
                                        "id": "",
                                        "catName": "",
                                        "imageUrl": "",
                                        "bannerImageUrl": "",
                                        "websiteImageUrl": "",
                                        "websiteBannerImageUrl": "",
                                        "offers": [],
                                        "penCount": 0,
                                        "categoryData": [],
                                        "type": 3,
                                        "seqId": 1,
                                    }
                                    last_json_response.append(response)
                            else:
                                if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                                    response = {
                                        "id": "",
                                        "catName": "",
                                        "imageUrl": "",
                                        "bannerImageUrl": "",
                                        "websiteImageUrl": "",
                                        "websiteBannerImageUrl": "",
                                        "offers": [],
                                        "penCount": 0,
                                        "categoryData": [],
                                        "type": 3,
                                        "seqId": 1,
                                    }
                                    last_json_response.append(response)
                        except:
                            if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                                response = {
                                    "id": "",
                                    "catName": "",
                                    "imageUrl": "",
                                    "bannerImageUrl": "",
                                    "websiteImageUrl": "",
                                    "websiteBannerImageUrl": "",
                                    "offers": [],
                                    "penCount": 0,
                                    "categoryData": [],
                                    "type": 3,
                                    "seqId": 1,
                                }
                                last_json_response.append(response)
                        main_sellers = []
                        if zone_id != "":
                            store_details = db.stores.find(
                                {
                                    "serviceZones.zoneId": zone_id,
                                    "storeFrontTypeId": {"$ne": 5},
                                    "status": 1,
                                }
                            )
                            for seller in store_details:
                                main_sellers.append(str(seller["_id"]))
                        if zone_id != "":
                            driver_roaster = next_availbale_driver_roaster(zone_id)
                        else:
                            driver_roaster = {}

                        recently_bought = product_modification(
                            res["aggregations"]["group_by_sub_category"]["buckets"],
                            language,
                            "",
                            zone_id,
                            currency_code,
                            store_category_id,
                            0,
                            False,
                            main_sellers,
                            driver_roaster,
                            "",
                        )
                        if len(recently_bought) == 0:
                            response = {
                                "id": "",
                                "catName": "",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": [],
                                "type": 3,
                                "seqId": 1,
                            }
                            last_json_response.append(response)
                        else:
                            if len(recently_bought) > 0:
                                dataframe = pd.DataFrame(recently_bought)
                                dataframe = dataframe.drop_duplicates(
                                    subset="childProductId", keep="last"
                                )
                                details = dataframe.to_json(orient="records")
                                data = json.loads(details)
                                recent_data = validate_units_data(data, False)
                                newlist = sorted(
                                    recent_data, key=lambda k: k["createdTimestamp"], reverse=True
                                )
                            else:
                                newlist = []

                            response = {
                                "id": "",
                                "catName": "Recently Bought",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": total_product_count,
                                "categoryData": newlist,
                                "type": 3,
                                "seqId": 1,
                            }
                            last_json_response.append(response)
                except:
                    response = {
                        "id": "",
                        "catName": "Recently Bought",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": [],
                        "type": 3,
                        "seqId": 1,
                    }
                    last_json_response.append(response)
                # ===========================================banner details=========================================
                try:
                    banner_deatils_data = []
                    store_data_details = []
                    store_data = store_find(
                        {
                            "categoryId": str(store_category_id),
                            "serviceZones.zoneId": zone_id,
                            "status": 1,
                        }
                    )
                    if store_data.count() > 0:
                        for store in store_data:
                            store_data_details.append(str(store["_id"]))
                    banner_query = {
                        "status": 1,
                        "storeCategoryId": store_category_id,
                        "zones.zoneId": zone_id,
                    }
                    banner_type = ""
                    banner_details = banner_find(banner_query)
                    if banner_details.count() > 0:
                        for i in banner_details:
                            # try:
                            category_name = ""
                            sub_category_name = ""
                            sub_sub_category_name = ""
                            if int(i["type"]) == 1:
                                banner_type = "offer"
                            elif int(i["type"]) == 2:
                                banner_type = "brands"
                            elif int(i["type"]) == 3:
                                banner_type = "category"
                            elif int(i["type"]) == 4:
                                banner_type = "stores"
                            elif int(i["type"]) == 5:
                                banner_type = "subcategory"
                            elif int(i["type"]) == 6:
                                banner_type = "subsubcategory"
                            elif int(i["type"]) == 7:
                                banner_type = "supplier"
                            elif int(i["type"]) == 8:
                                banner_type = "products"
                            elif int(i["type"]) == 9:
                                banner_type = "url"
                            if int(i["type"]) == 3:
                                base_category = category_find_one(
                                    {"_id": ObjectId(str(i["data"][0]["id"]).replace(" ", ""))}
                                )
                                if base_category is not None:
                                    if "parentId" in base_category:
                                        second_category = category_find_one(
                                            {"_id": ObjectId(base_category["parentId"])}
                                        )
                                        if second_category is not None:
                                            if "parentId" in second_category:
                                                sub_sub_category_name = base_category[
                                                    "categoryName"
                                                ]["en"]
                                                sub_category_name = second_category["categoryName"][
                                                    "en"
                                                ]
                                                first_category = category_find_one(
                                                    {"_id": ObjectId(second_category["parentId"])}
                                                )
                                                if first_category is not None:
                                                    category_name = first_category["categoryName"][
                                                        "en"
                                                    ]
                                                else:
                                                    category_name = ""
                                            else:
                                                category_name = second_category["categoryName"][
                                                    "en"
                                                ]
                                                sub_category_name = base_category["categoryName"][
                                                    "en"
                                                ]
                                                sub_sub_category_name = ""
                                        else:
                                            first_category = category_find_one(
                                                {
                                                    "_id": ObjectId(
                                                        str(i["data"][0]["id"]).replace(" ", "")
                                                    )
                                                }
                                            )
                                            if first_category is not None:
                                                category_name = first_category["categoryName"]["en"]
                                            else:
                                                category_name = ""
                                                sub_category_name = ""
                                                sub_sub_category_name = ""
                                    else:
                                        first_category = category_find_one(
                                            {
                                                "_id": ObjectId(
                                                    str(i["data"][0]["id"]).replace(" ", "")
                                                )
                                            }
                                        )
                                        if first_category is not None:
                                            category_name = first_category["categoryName"]["en"]
                                        else:
                                            category_name = ""
                                            sub_category_name = ""
                                            sub_sub_category_name = ""
                                else:
                                    category_name = ""
                                    sub_category_name = ""
                                    sub_sub_category_name = ""

                            if sub_category_name != "" and sub_sub_category_name == "":
                                banner_deatils_data.append(
                                    {
                                        "type": 5,
                                        "bannerTypeMsg": "subcategory",
                                        "catName": category_name,
                                        "offerName": "",
                                        "name": i["data"][0]["name"]["en"],
                                        "imageWeb": i["image_web"],
                                        "imageMobile": i["image_mobile"],
                                    }
                                )
                            elif sub_category_name != "" and sub_sub_category_name != "":
                                banner_deatils_data.append(
                                    {
                                        "type": 6,
                                        "bannerTypeMsg": "subsubcategory",
                                        "offerName": "",
                                        "catName": category_name,
                                        "subCatName": sub_category_name,
                                        "name": i["data"][0]["name"]["en"],
                                        "imageWeb": i["image_web"],
                                        "imageMobile": i["image_mobile"],
                                    }
                                )
                            elif int(i["type"]) == 1:
                                offer_query = {"_id": ObjectId(i["data"][0]["id"]), "status": 1}
                                offer_details = offer_find_one(offer_query)
                                if offer_details is not None:
                                    product_query = {
                                        "offer.status": 1,
                                        "offer.offerId": str(offer_details["_id"]),
                                        "status": 1,
                                    }
                                    child_product_count = product_find_count(product_query)
                                    if child_product_count > 0:
                                        banner_deatils_data.append(
                                            {
                                                "type": i["type"],
                                                "bannerTypeMsg": banner_type,
                                                "catName": "",
                                                "subCatName": "",
                                                "offerName": offer_details["name"]["en"],
                                                "name": str(offer_details["_id"]),
                                                "imageWeb": i["image_web"],
                                                "imageMobile": i["image_mobile"],
                                            }
                                        )
                            elif int(i["type"]) == 8:
                                product_details = db.products.find_one(
                                    {"_id": ObjectId(i["data"][0]["id"]), "status": 1}, {"units": 1}
                                )
                                if product_details is not None:
                                    supplier_list = []
                                    if "suppliers" in product_details["units"][0]:
                                        for s in product_details["units"][0]["suppliers"]:
                                            if s["id"] != "0":
                                                child_product_count = db.childProducts.find(
                                                    {"_id": ObjectId(s["productId"]), "status": 1}
                                                ).count()
                                                if child_product_count > 0:
                                                    supplier_list.append(s)
                                            else:
                                                pass
                                    if len(supplier_list) > 0:
                                        best_supplier = min(
                                            supplier_list, key=lambda x: x["retailerPrice"]
                                        )
                                        if best_supplier["retailerQty"] == 0:
                                            best_supplier = max(
                                                supplier_list, key=lambda x: x["retailerQty"]
                                            )
                                        else:
                                            best_supplier = best_supplier
                                        if len(best_supplier) > 0:
                                            central_product_id = str(product_details["_id"])
                                            child_product_id = str(best_supplier["productId"])
                                            banner_deatils_data.append(
                                                {
                                                    "type": i["type"],
                                                    "bannerTypeMsg": banner_type,
                                                    "catName": "",
                                                    "parentProductId": central_product_id,
                                                    "childProductId": child_product_id,
                                                    "subCatName": "",
                                                    "offerName": "/python/product/details?&parentProductId="
                                                    + central_product_id
                                                    + "&productId="
                                                    + child_product_id,
                                                    "name": "/python/product/details?&parentProductId="
                                                    + central_product_id
                                                    + "&productId="
                                                    + child_product_id,
                                                    "imageWeb": i["image_web"],
                                                    "imageMobile": i["image_mobile"],
                                                }
                                            )
                            elif int(i["type"]) == 9:
                                banner_deatils_data.append(
                                    {
                                        "type": i["type"],
                                        "bannerTypeMsg": banner_type,
                                        "catName": "",
                                        "subCatName": "",
                                        "offerName": i["data"][0]["name"]["en"],
                                        "name": i["data"][0]["name"]["en"],
                                        "imageWeb": i["image_web"],
                                        "imageMobile": i["image_mobile"],
                                    }
                                )
                            else:
                                try:
                                    store_details_name = json.loads(i["data"][0]["name"])
                                except:
                                    try:
                                        store_details_name = i["data"][0]["name"]
                                    except:
                                        store_details_name = {"en": ""}
                                try:
                                    if store_details_name["en"] != "":
                                        banner_deatils_data.append(
                                            {
                                                "type": i["type"],
                                                "bannerTypeMsg": banner_type,
                                                "offerName": "",
                                                "name": store_details_name["en"],
                                                "imageWeb": i["image_web"],
                                                "imageMobile": i["image_mobile"],
                                            }
                                        )
                                except:
                                    pass
                        # except:
                        #     pass
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "penCount": 0,
                            "offers": [],
                            "type": 1,
                            "seqId": 2,
                            "categoryData": banner_deatils_data,
                        }
                        last_json_response.append(response)
                    else:
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "type": 1,
                            "seqId": 2,
                            "categoryData": [],
                        }
                        last_json_response.append(response)
                except Exception as ex:
                    print(
                        "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                        type(ex).__name__,
                        ex,
                    )
                    error = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "seqId": 2,
                        "type": 1,
                        "categoryData": [],
                    }
                    last_json_response.append(error)
                # ============================================recent view====================================
                try:
                    resData = []
                    mongo_query = {
                        "userid": user_id,
                        "store_category_id": store_category_id,
                        "storeid": {"$in": store_details},
                    }
                    recent_product_details = (
                        db.userRecentView.find(mongo_query)
                        .sort([("createdtimestamp", -1)])
                        .skip(0)
                        .limit(6)
                    )
                    recent_total_count = db.userRecentView.find(mongo_query).count()
                    if recent_product_details.count() == 0:
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "categoryData": [],
                            "type": 5,
                            "seqId": 3,
                        }
                        last_json_response.append(response)
                    else:
                        for pro_data in recent_product_details:
                            query = {"_id": ObjectId(pro_data["productId"])}
                            product = db.childProducts.find_one(query)
                            if product is not None:
                                # try:
                                if product["storeId"] != "0":
                                    variant_data = []
                                    best_supplier = {
                                        "productId": str(product["_id"]),
                                        "id": str(product["storeId"]),
                                    }
                                    product_tag = ""
                                    # ================================get the details from childProducts collection=====================
                                    if len(best_supplier) > 0:
                                        query = {
                                            "parentProductId": str(product["parentProductId"]),
                                            "status": 1,
                                        }
                                        try:
                                            if best_supplier["id"] == "0":
                                                query["storeId"] = best_supplier["id"]
                                            else:
                                                query["storeId"] = ObjectId(best_supplier["id"])
                                        except:
                                            query["storeId"] = best_supplier["id"]
                                        variant_count_data = db.childProducts.find(query).count()
                                        if variant_count_data > 1:
                                            variant_count = True
                                        else:
                                            variant_count = False

                                        if len(best_supplier) > 0:
                                            child_product_id = best_supplier["productId"]
                                        else:
                                            child_product_id = i["_id"]

                                        if "availableQuantity" in product["units"][0]:
                                            if product["units"][0]["availableQuantity"] > 0:
                                                outOfStock = False
                                                availableQuantity = product["units"][0][
                                                    "availableQuantity"
                                                ]
                                            else:
                                                outOfStock = True
                                                availableQuantity = 0
                                        else:
                                            outOfStock = True
                                            availableQuantity = 0

                                        if product_tag != "":
                                            outOfStock = True
                                        else:
                                            pass
                                        offers_details = []
                                        offer_details_data = []
                                        if "offer" in product:
                                            for offer in product["offer"]:
                                                if offer["status"] == 1:
                                                    offers_details.append(offer)
                                                    offer_details_data.append(
                                                        {
                                                            "offerId": offer["offerId"],
                                                            "offerName": offer["offerName"]["en"],
                                                            "webimages": offer["webimages"][
                                                                "image"
                                                            ],
                                                            "mobimage": offer["images"]["image"],
                                                            "discountValue": offer["discountValue"],
                                                        }
                                                    )
                                                else:
                                                    pass
                                        else:
                                            pass

                                        if len(offers_details) > 0:
                                            best_offer = max(
                                                offers_details, key=lambda x: x["discountValue"]
                                            )
                                            offer_details = db.offers.find(
                                                {
                                                    "_id": ObjectId(best_offer["offerId"]),
                                                    "storeCategoryId": store_category_id,
                                                }
                                            ).count()
                                            if offer_details != 0:
                                                best_offer = best_offer
                                                currdate = datetime.datetime.now().replace(
                                                    hour=23, minute=59, second=59, microsecond=59
                                                )
                                                eastern = timezone(timezonename)
                                                currlocal = eastern.localize(currdate)
                                                best_offer["endDateTimeISO"] = (
                                                    int(((currlocal).timestamp())) * 1000
                                                )
                                            else:
                                                best_offer = {}
                                        else:
                                            best_offer = {}
                                        # ======================================product seo======================================================
                                        if "productSeo" in product:
                                            try:
                                                if len(product["productSeo"]["title"]) > 0:
                                                    title = (
                                                        product["productSeo"]["title"][language]
                                                        if language
                                                        in product["productSeo"]["title"]
                                                        else product["productSeo"]["title"]["en"]
                                                    )
                                                else:
                                                    title = ""

                                                if len(product["productSeo"]["description"]) > 0:
                                                    description = (
                                                        product["productSeo"]["description"][
                                                            language
                                                        ]
                                                        if language
                                                        in product["productSeo"]["description"]
                                                        else product["productSeo"]["description"][
                                                            "en"
                                                        ]
                                                    )
                                                else:
                                                    description = ""

                                                if len(product["productSeo"]["metatags"]) > 0:
                                                    metatags = (
                                                        product["productSeo"]["metatags"][language]
                                                        if language
                                                        in product["productSeo"]["metatags"]
                                                        else product["productSeo"]["metatags"]["en"]
                                                    )
                                                else:
                                                    metatags = ""

                                                if len(product["productSeo"]["slug"]) > 0:
                                                    slug = (
                                                        product["productSeo"]["slug"][language]
                                                        if language in product["productSeo"]["slug"]
                                                        else product["productSeo"]["slug"]["en"]
                                                    )
                                                else:
                                                    slug = ""

                                                product_seo = {
                                                    "title": title,
                                                    "description": description,
                                                    "metatags": metatags,
                                                    "slug": slug,
                                                }
                                            except:
                                                product_seo = {
                                                    "title": "",
                                                    "description": "",
                                                    "metatags": "",
                                                    "slug": "",
                                                }
                                        else:
                                            product_seo = {
                                                "title": "",
                                                "description": "",
                                                "metatags": "",
                                                "slug": "",
                                            }
                                        tax_value = []

                                        # =========================================pharmacy details=========================================
                                        if "prescriptionRequired" in product:
                                            if product["prescriptionRequired"] == 0:
                                                prescription_required = False
                                            else:
                                                prescription_required = True
                                        else:
                                            prescription_required = False

                                        if "saleOnline" in product:
                                            if product["saleOnline"] == 0:
                                                sales_online = False
                                            else:
                                                sales_online = True
                                        else:
                                            sales_online = False

                                        if "uploadProductDetails" in product:
                                            upload_details = product["uploadProductDetails"]
                                        else:
                                            upload_details = ""
                                        # ==================================================================================================

                                        if len(best_supplier) == 0:
                                            tax_value = []
                                        else:
                                            if product is not None:
                                                if type(product["tax"]) == list:
                                                    for tax in product["tax"]:
                                                        tax_value.append({"value": tax["taxValue"]})
                                                else:
                                                    if product["tax"] is not None:
                                                        if "taxValue" in product["tax"]:
                                                            tax_value.append(
                                                                {
                                                                    "value": product["tax"][
                                                                        "taxValue"
                                                                    ]
                                                                }
                                                            )
                                                        else:
                                                            tax_value.append(
                                                                {"value": product["tax"]}
                                                            )
                                                    else:
                                                        pass
                                            else:
                                                tax_value = []

                                        # ========================= for the get the linked the unit data====================================
                                        for link_unit in product["units"][0]["attributes"]:
                                            try:
                                                for attrlist in link_unit["attrlist"]:
                                                    try:
                                                        if attrlist is None:
                                                            pass
                                                        else:
                                                            if attrlist["linkedtounit"] == 1:
                                                                if (
                                                                    attrlist["measurementUnit"]
                                                                    == ""
                                                                ):
                                                                    attr_name = (
                                                                        str(
                                                                            attrlist["value"][
                                                                                language
                                                                            ]
                                                                        )
                                                                        if language
                                                                        in attrlist["value"]
                                                                        else str(
                                                                            attrlist["value"]["en"]
                                                                        )
                                                                    )
                                                                else:
                                                                    attr_name = (
                                                                        str(
                                                                            attrlist["value"][
                                                                                language
                                                                            ]
                                                                        )
                                                                        + " "
                                                                        + attrlist[
                                                                            "measurementUnit"
                                                                        ]
                                                                        if language
                                                                        in attrlist["value"]
                                                                        else str(
                                                                            attrlist["value"]["en"]
                                                                        )
                                                                        + " "
                                                                        + attrlist[
                                                                            "measurementUnit"
                                                                        ]
                                                                    )
                                                                variant_data.append(
                                                                    {
                                                                        "value": str(attr_name),
                                                                        "name": attrlist[
                                                                            "attrname"
                                                                        ]["en"],
                                                                    }
                                                                )
                                                            else:
                                                                pass
                                                    except:
                                                        pass
                                            except:
                                                pass
                                        # =========================for max quantity=================================================
                                        if "maxQuantity" in product:
                                            if product["maxQuantity"] != "":
                                                max_quantity = int(product["maxQuantity"])
                                            else:
                                                max_quantity = 30
                                        else:
                                            max_quantity = 30
                                        # ==========================================================================================
                                        if "allowOrderOutOfStock" in product:
                                            allow_out_of_order = product["allowOrderOutOfStock"]
                                        else:
                                            allow_out_of_order = False
                                        try:
                                            mobile_images = product["images"]
                                        except:
                                            try:
                                                mobile_images = product["images"]
                                            except:
                                                mobile_images = product["image"]
                                        linked_attribute = get_linked_unit_attribute(
                                            product["units"]
                                        )
                                        currency = product["currency"]
                                        product_status = product["status"]
                                        product_tag = ""
                                        if "productType" in product:
                                            if product["productType"] == 2:
                                                combo_product = True
                                            else:
                                                combo_product = False
                                        else:
                                            combo_product = False
                                        currency_symbol = product["currencySymbol"]
                                        if currency_symbol is None:
                                            currency_symbol = "₹"

                                        try:
                                            currency_rate = currency_exchange_rate[
                                                str(product["currencySymbol"])
                                                + "_"
                                                + str(currency_code)
                                            ]
                                        except:
                                            currency_rate = 0
                                        currency_details = db.currencies.find_one(
                                            {"currencyCode": currency_code}
                                        )
                                        if currency_details is not None:
                                            currency_symbol = currency_details["currencySymbol"]
                                            currency = currency_details["currencyCode"]
                                        else:
                                            currency_symbol = product["currencySymbol"]
                                            currency = product["currency"]

                                        # ==================================get currecny rate============================
                                        # currency_rate = 0
                                        if "needsIdProof" in product:
                                            if not product["needsIdProof"]:
                                                needsIdProof = False
                                            else:
                                                needsIdProof = True
                                        else:
                                            needsIdProof = False
                                        # get product type, is normal or combo or special product
                                        product_type = combo_special_type_validation(
                                            str(product["_id"])
                                        )
                                        resData.append(
                                            {
                                                "maxQuantity": max_quantity,
                                                "isComboProduct": combo_product,
                                                "currencyRate": currency_rate,
                                                "productType": product_type,
                                                "needsIdProof": needsIdProof,
                                                "childProductId": str(product["_id"]),
                                                "productStatus": product_status,
                                                "productTag": product_tag,
                                                "availableQuantity": availableQuantity,
                                                "productName": product["pName"][language]
                                                if language in product["pName"]
                                                else child_product_details["pName"]["en"],
                                                "parentProductId": product["parentProductId"],
                                                "suppliers": best_supplier,
                                                "tax": tax_value,
                                                "linkedAttribute": linked_attribute,
                                                "allowOrderOutOfStock": allow_out_of_order,
                                                "outOfStock": outOfStock,
                                                "offerDetailsData": offer_details_data,
                                                "variantData": variant_data,
                                                "variantCount": variant_count,
                                                "prescriptionRequired": prescription_required,
                                                "saleOnline": sales_online,
                                                "uploadProductDetails": upload_details,
                                                "productSeo": product_seo,
                                                "brandName": product["brandTitle"][language]
                                                if language in product["brandTitle"]
                                                else product["brandTitle"]["en"],
                                                "manufactureName": product["manufactureName"][
                                                    language
                                                ]
                                                if language in product["manufactureName"]
                                                else "",
                                                "TotalStarRating": product["avgRating"]
                                                if "avgRating" in product
                                                else 0,
                                                "currencySymbol": currency_symbol,
                                                "currency": currency,
                                                "storeCategoryId": product["storeCategoryId"]
                                                if "storeCategoryId" in product
                                                else "",
                                                "images": product["images"],
                                                "mobimages": mobile_images,
                                                "finalPriceList": product["units"],
                                                "units": product["units"],
                                                "unitId": product["units"][0]["unitId"],
                                                "offer": best_offer,
                                                "createdTimestamp": pro_data["createdtimestamp"],
                                                "nextSlotTime": "",
                                            }
                                        )
                                else:
                                    pass
                                # except:
                                #     pass
                            else:
                                pass
                        if len(resData) > 0:
                            dataframe = pd.DataFrame(resData)
                            dataframe["unitsData"] = dataframe.apply(
                                home_units_data,
                                lan=language,
                                sort=0,
                                status=0,
                                axis=1,
                                logintype=1,
                                store_category_id=store_category_id,
                                margin_price=True,
                                city_id="",
                            )
                            dataframe = dataframe.drop_duplicates(
                                subset="parentProductId", keep="last"
                            )
                            details = dataframe.to_json(orient="records")
                            data = json.loads(details)
                            recent_data = validate_units_data(data, False)
                            newlist = sorted(
                                recent_data, key=lambda k: k["createdTimestamp"], reverse=True
                            )
                        else:
                            newlist = []
                        if len(newlist) > 0:
                            response = {
                                "id": "",
                                "catName": "Recently Viewed",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": recent_total_count,
                                "categoryData": newlist,
                                "type": 5,
                                "seqId": 3,
                            }
                            last_json_response.append(response)
                        else:
                            response = {
                                "id": "",
                                "catName": "Recently Viewed",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": [],
                                "type": 5,
                                "seqId": 3,
                            }
                            last_json_response.append(response)
                except Exception as ex:
                    print(
                        "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                        type(ex).__name__,
                        ex,
                    )
                    error = {
                        "id": "",
                        "catName": "Recently Viewed",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": [],
                        "type": 5,
                        "seqId": 3,
                    }
                    last_json_response.append(error)
                # ==========================================category data====================================
                sub_category_details = []
                sub_category_count = []
                store_details_1 = []
                store_details_1.append("0")
                store_data_json.append("0")
                category_data = db.category.aggregate(
                    [
                        {
                            "$match": {
                                "status": 1,
                                "productCount": {"$gt": 0},
                                "parentId": {"$exists": False},
                                "isNauCategory" : True,
                                "storeid": {"$in": store_details_1},
                                "storeCategory.storeCategoryId": store_category_id,
                                "_id": {"$type": "objectId"},
                            }
                        },
                        {"$project": {"categoryName": 1}},
                        {
                            "$lookup": {
                                "from": "category",
                                "let": {"parent_id": "$_id"},
                                "pipeline": [
                                    {
                                        "$match": {
                                            "$expr": {
                                                "$and": [
                                                    {"$eq": ["$parentId", "$$parent_id"]},
                                                    {"$eq": ["$status", 1]},
                                                    # {"$eq": ["$level", 2]},
                                                    {"$gt": ["$productCount", 0]},
                                                ]
                                            }
                                        }
                                    },
                                    {
                                        "$project": {
                                            "parentId": 1,
                                            "categoryName": 1,
                                        }
                                    },
                                ],
                                "as": "subCategories",
                            }
                        },
                    ]
                )
                sub_categories = []
                for cat in category_data:
                    for subcat in cat["subCategories"]:
                        category_count = db.category.find(
                            {
                                "parentId": ObjectId(subcat["_id"]),
                                "status": 1,
                                "productCount": {"$ne": 0},
                            }
                        ).count()
                        # sub_categories.append(subcat['_id'])
                        if category_count == 0:
                            category_count = db.category.find(
                                {
                                    "parentId": ObjectId(subcat["parentId"]),
                                    "status": 1,
                                    "productCount": {"$ne": 0},
                                }
                            ).count()
                            if category_count != 0:
                                sub_category_details.append(
                                    {"id": str(subcat["parentId"]), "count": category_count}
                                )
                        else:
                            sub_category_details.append(
                                {"id": str(subcat["_id"]), "count": category_count}
                            )
                # ====================================last response==============================================
                newlist = sorted(last_json_response, key=lambda k: k["seqId"])

                if len(sub_category_details) > 0:
                    dataframe_sub_cat = pd.DataFrame(sub_category_details)
                    dataframe_sub_cat = dataframe_sub_cat.drop_duplicates(subset="id", keep="first")
                    sub_cat_details = dataframe_sub_cat.to_json(orient="records")
                    new_sub_category_details = json.loads(sub_cat_details)
                else:
                    new_sub_category_details = []

                last_response = {
                    "data": {
                        "list": newlist,
                        "categoryData": new_sub_category_details,
                        # total_data_count,
                        "totalCatCount": sum(sub_category_count),
                        # total_data_count,
                        "totalPage": int(sum(sub_category_count) / 4) + 1,
                    }
                }
                return JsonResponse(last_response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class CategoryProducts(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the sub-category and sub-category wise products",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            # openapi.Parameter(
            #     name='currencySymbol', default="INR", required=True, in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, description="currencySymbol for the currency..INR, INR"),
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="page",
                default="1",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="pagination from which page data need",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5fcf541871d7bd51e6008c94",
                in_=openapi.IN_QUERY,
                required=True,
                type=openapi.TYPE_STRING,
                description="zone id for getting the products from only that zone",
            ),
            openapi.Parameter(
                name="categoryId",
                default="5f33bcba6b5f51ac3b7cb635",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="category id from which category we need to get sub category and products",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default=GROCERY_STORE_CATEGORY_ID,
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store category id from which store category we need to get sub category and products",
            ),
        ],
        responses={
            200: "successfully. subcategory data found",
            404: "data not found. it might be subcategory wise data not found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        # try:
        start_time = time.time()
        token = request.META["HTTP_AUTHORIZATION"]
        zone_id = request.GET.get("zoneId", "")
        category_id = request.GET.get("categoryId", "")
        currency_code = (
            request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
        )
        store_category_id = request.GET.get("storeCategoryId", "")
        language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
        page = int(request.GET.get("page", "1"))
        category_limit = int(page) * 4
        category_skip = category_limit - 4
        product_limit = int(page) * 3
        product_skip = product_limit - 3
        if token == "":
            response_data = {
                "message": "unauthorized",
                "data": [],
            }
            return JsonResponse(response_data, safe=False, status=401)
        elif zone_id == "":
            response_data = {
                "message": "zone id is missing",
                "data": [],
            }
            return JsonResponse(response_data, safe=False, status=422)
        elif zone_id == "":
            response_data = {
                "message": "zone id is missing",
                "data": [],
            }
            return JsonResponse(response_data, safe=False, status=422)
        elif category_id == "":
            response_data = {
                "message": "category id is missing",
                "data": [],
            }
            return JsonResponse(response_data, safe=False, status=422)
        else:
            # =======================================category data==================================================
            store_data_json = []
            store_details = ["0"]
            if zone_id != "":
                store_data = db.stores.find(
                    {
                        "categoryId": str(store_category_id),
                        "serviceZones.zoneId": zone_id,
                        "status": 1,
                    }
                )
                for store in store_data:
                    store_details.append(str(store["_id"]))
                    store_data_json.append(ObjectId(store["_id"]))
            else:
                pass
            aggregate_result = db.category.aggregate(
                [
                    {
                        "$match": {
                            "status": 1,
                            "productCount": {"$gt": 0},
                            "storeid": {"$in": store_details},
                            "parentId": ObjectId(category_id),
                            "_id": {"$type": "objectId"},
                        }
                    },
                    {"$skip": category_skip},
                    {"$limit": category_limit},
                    {
                        "$project": {
                            "categoryName": 1,
                            "mobileImage": 1,
                            "mobileIcon": 1,
                            "websiteImage": 1,
                            "websiteIcon": 1,
                        }
                    },
                ]
            )
            parent_category_details = db.category.find_one({"_id": ObjectId(category_id)})
            categoty_json = []
            sub_category_data = []
            for parent_category in aggregate_result:
                second_category_id = str(parent_category["_id"])
                second_category_name = (
                    parent_category["categoryName"][language]
                    if language in parent_category["categoryName"]
                    else parent_category["categoryName"]["en"]
                )
                mobileIcon = (
                    parent_category["mobileIcon"] if "mobileIcon" in parent_category else ""
                )
                sub_category_data.append(
                    {
                        "id": second_category_id,
                        "subCategoryName": second_category_name,
                        "imageUrl": mobileIcon,
                        "childCount": 0,
                        "penCount": 0,
                    }
                )

            if len(sub_category_data) > 0:
                dataframe_sub_cat = pd.DataFrame(sub_category_data)
                dataframe_sub_cat = dataframe_sub_cat.drop_duplicates(
                    subset="subCategoryName", keep="first"
                )
                sub_cat_details = dataframe_sub_cat.to_json(orient="records")
                sub_cat_json_data = json.loads(sub_cat_details)
            else:
                sub_cat_json_data = []

            # =====================================query for product get========================================
            # store_details.append("0")
            product_aggrigate_result = db.category.aggregate(
                [
                    {
                        "$match": {
                            "status": 1,
                            "productCount": {"$gt": 0},
                            "storeid": {"$in": store_details},
                            "parentId": ObjectId(category_id),
                            "_id": {"$type": "objectId"},
                        }
                    },
                    {
                        "$sort": {
                            # "popularityScore": -1
                            "productCount": -1
                        }
                    },
                    {"$skip": product_skip},
                    {"$limit": product_limit},
                    {
                        "$project": {
                            "categoryName": 1,
                            "mobileImage": 1,
                            "mobileIcon": 1,
                            "popularityScore": 1,
                            "websiteImage": 1,
                            "websiteIcon": 1,
                        }
                    },
                ]
            )
            product_category_data = []
            product_category_ids = []
            for child_category in product_aggrigate_result:
                product_category_ids.append(str(child_category["_id"]))
                product_category_data.append(child_category["categoryName"]["en"])
            if len(product_category_data) > 0:
                must_query = [
                    {"match": {"status": 1}},
                    {"terms": {"storeId": store_details}},
                    {
                        "terms": {
                            # "linkedProductCategory.categoryId": product_category_data
                            # "categoryList.parentCategory.childCategory.categoryId": product_category_data
                            "categoryList.parentCategory.childCategory.categoryName.en.keyword": product_category_data
                        }
                    },
                    {"match": {"storeCategoryId": str(store_category_id)}},
                    {"exists": {"field": "linkedProductCategory"}},
                ]
                sub_query = [
                    {"isCentral": {"order": "desc"}},
                    {"isInStock": {"order": "desc"}},
                    {"units.floatValue": {"order": "asc"}},
                ]
                query = {
                    "sort": sub_query,
                    "query": {
                        "bool": {
                            "must": must_query,
                        }
                    },
                    "aggs": {
                        "group_by_sub_category": {
                            "terms": {
                                # "field": "linkedProductCategory.categoryId.keyword",
                                "field": "categoryList.parentCategory.childCategory.categoryId.keyword",
                                "size": 50,
                            },
                            "aggs": {
                                "top_hits": {
                                    "terms": {"field": "parentProductId.keyword", "size": 5},
                                    "aggs": {
                                        "top_sales_hits": {
                                            "top_hits": {
                                                "sort": sub_query,
                                                "_source": {
                                                    "includes": [
                                                        "_id",
                                                        "pName",
                                                        "storeId",
                                                        "parentProductId",
                                                        "currencySymbol",
                                                        "currency",
                                                        "pPName",
                                                        "needsIdProof",
                                                        "tax",
                                                        "brandTitle",
                                                        "categoryList",
                                                        "images",
                                                        "avgRating",
                                                        "offer",
                                                        "units",
                                                        "storeCategoryId",
                                                        "manufactureName",
                                                        "maxQuantity",
                                                    ]
                                                },
                                                "size": 1,
                                            }
                                        }
                                    },
                                }
                            },
                        }
                    },
                }
                categoty_data_json = []
                res = child_product_es_aggrigate_data(query)
                # ==================================================end================================================
                main_sellers = []
                if zone_id != "":
                    store_details = db.stores.find(
                        {
                            "serviceZones.zoneId": zone_id,
                            "storeFrontTypeId": {"$ne": 5},
                            "status": 1,
                        }
                    )
                    for seller in store_details:
                        main_sellers.append(str(seller["_id"]))

                if zone_id != "":
                    driver_roaster = next_availbale_driver_roaster(zone_id)
                else:
                    driver_roaster = {}

                for res_res in res["aggregations"]["group_by_sub_category"]["buckets"][
                    int(product_skip) : int(product_limit)
                ]:
                    sub_cat_id = res_res["key"]
                    if sub_cat_id != "":
                        if sub_cat_id in product_category_ids:
                            resData = product_modification(
                                res_res["top_hits"]["buckets"],
                                language,
                                "",
                                zone_id,
                                currency_code,
                                store_category_id,
                                1,
                                False,
                                main_sellers,
                                driver_roaster,
                                "",
                            )
                            categoty_data = validate_units_data(resData, False)
                            if len(categoty_data) > 0:
                                offer_dataframe = pd.DataFrame(categoty_data)
                                offer_dataframe = offer_dataframe.drop_duplicates(
                                    "childProductId", keep="last"
                                )
                                offers_data = offer_dataframe.to_dict(orient="records")
                                newlist = sorted(
                                    offers_data,
                                    key=lambda k: k["availableQuantity"],
                                    reverse=True,
                                )
                            else:
                                newlist = []
                            if len(newlist) > 0:
                                sub_category_data = db.category.find_one(
                                    {"_id": ObjectId(sub_cat_id)}
                                )
                                parent_count = db.category.find(
                                    {
                                        "_id": ObjectId(parent_category_details["_id"]),
                                        "parentId": {"$exists": True},
                                    }
                                ).count()
                                if parent_count > 0:
                                    categoty_data_json.append(
                                        {
                                            "subCategory": newlist,
                                            "subCategoryName": sub_category_data["categoryName"][
                                                language
                                            ]
                                            if language in sub_category_data["categoryName"]
                                            else sub_category_data["categoryName"]["en"],
                                            "catName": parent_category_details["categoryName"][
                                                language
                                            ]
                                            if language in parent_category_details["categoryName"]
                                            else parent_category_details["categoryName"]["en"],
                                            "popularityScore": sub_category_data["popularityScore"]
                                            if "popularityScore" in sub_category_data
                                            else 0,
                                        }
                                    )
                                else:
                                    categoty_data_json.append(
                                        {
                                            "subCategory": newlist,
                                            "subCategoryName": sub_category_data["categoryName"][
                                                language
                                            ]
                                            if language in sub_category_data["categoryName"]
                                            else sub_category_data["categoryName"]["en"],
                                            "catName": sub_category_data["categoryName"][language]
                                            if language in sub_category_data["categoryName"]
                                            else sub_category_data["categoryName"]["en"],
                                            "popularityScore": sub_category_data["popularityScore"]
                                            if "popularityScore" in sub_category_data
                                            else 0,
                                        }
                                    )
                            else:
                                pass
                        else:
                            pass
                    else:
                        pass

                if len(categoty_data_json) > 0:
                    categoty_data_json_dataframe = pd.DataFrame(categoty_data_json)
                    categoty_data_json_data = categoty_data_json_dataframe.to_dict(orient="records")
                    categoty_data_json_newlist = sorted(
                        categoty_data_json_data,
                        key=lambda k: k["popularityScore"],
                        reverse=True,
                    )
                else:
                    categoty_data_json_newlist = []

                if len(categoty_data_json_newlist) > 0:
                    categoty_json.append(
                        {
                            "id": str(category_id),
                            "catName": parent_category_details["categoryName"][language]
                            if language in parent_category_details["categoryName"]
                            else parent_category_details["categoryName"]["en"],
                            "imageUrl": parent_category_details["mobileImage"]
                            if "mobileImage" in parent_category_details
                            else "",
                            "bannerImageUrl": parent_category_details["mobileImage"]
                            if "mobileImage" in parent_category_details
                            else "",
                            "websiteImageUrl": parent_category_details["websiteImage"]
                            if "websiteImage" in parent_category_details
                            else "",
                            "websiteBannerImageUrl": parent_category_details["websiteImage"]
                            if "websiteImage" in parent_category_details
                            else "",
                            "categoryData": sub_cat_json_data,
                            "offers": [],
                            "products": categoty_data_json_newlist,
                            "type": 4,
                            "seqId": 6,
                        }
                    )
                else:
                    pass
            else:
                pass
            if len(categoty_json) > 0:
                categ_data = categoty_json
                response = {
                    "id": "",
                    "catName": "",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "categoryData": categ_data,
                    "offers": [],
                    "type": 7,
                    "seqId": 7,
                }
                return JsonResponse(response, safe=False, status=200)
            else:
                response = {
                    "id": "",
                    "catName": "",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "categoryData": [],
                    "offers": [],
                    "type": 7,
                    "seqId": 7,
                }
                return JsonResponse(response, safe=False, status=404)

    # except Exception as ex:
    #     template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    #     message = template.format(type(ex).__name__, ex.args)
    #     print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
    #     error = {
    #         "id": "",
    #         "catName": "",
    #         "imageUrl": "",
    #         "bannerImageUrl": "",
    #         "websiteImageUrl": "",
    #         "websiteBannerImageUrl": "",
    #         "categoryData": [],
    #         "offers": [],
    #         "type": 7,
    #         "seqId": 7,
    #         "message": message,
    #     }
    #     return JsonResponse(error, safe=False, status=500)


class MegaMenu(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Mega Menu"],
        operation_description="API for getting the mega menu for the service",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5fcf541871d7bd51e6008c94",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="zone id for getting the products from only that zone",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default=ECOMMERCE_STORE_CATEGORY_ID,
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store category id from which store category we need to get mega menu",
            ),
            openapi.Parameter(
                name="storeId",
                default="5f1af591c9aba51af8fac4c0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store id from which store we need to get mega menu",
            ),
        ],
        responses={
            200: "successfully. subcategory data found",
            404: "data not found. it might be subcategory wise data not found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            start_time = time.time()
            token = request.META["HTTP_AUTHORIZATION"]
            zone_id = request.GET.get("zoneId", "")
            category_id = request.GET.get("categoryId", "")
            store_id = request.GET.get("storeId", "")
            store_category_id = request.GET.get("storeCategoryId", "")
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif store_category_id == "":
                response_data = {
                    "message": "store category id is missing",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # =======================================category data==================================================
                mega_menu_query = {"storeCategoryId": store_category_id, "status": 1}
                if store_id != "":
                    mega_menu_query["storeId"] = store_id
                else:
                    pass
                if store_id == "" or store_id == "0":
                    try:
                        mega_menu_json = json.loads(
                            redis_mega_menu.get("megamenu_" + str(store_category_id))
                        )
                    except:
                        mega_menu_json = []
                else:
                    try:
                        mega_menu_json = json.loads(
                            redis_mega_menu.get(
                                "megamenu_" + str(store_category_id) + "_" + str(store_id)
                            )
                        )
                    except:
                        mega_menu_json = []
                if len(mega_menu_json) > 0:
                    response_data = {
                        "message": "data found",
                        "data": mega_menu_json,
                    }
                    print("@@@@@@@@@@@@@@ end time for mega menu @@@@@@@", time.time() - start_time)
                    return JsonResponse(response_data, safe=False, status=200)
                else:
                    response_data = {
                        "message": "data not found",
                        "data": [],
                    }
                    return JsonResponse(response_data, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"message": message}
            return JsonResponse(error, safe=False, status=500)


class MegaMenuColumn(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Mega Menu"],
        operation_description="API for getting the mega menu for the service",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="menuId",
                default="60ccaa0e391f33ba5300a77e",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="menu id from which we need to get data",
            ),
        ],
        responses={
            200: "successfully. subcategory data found",
            404: "data not found. it might be subcategory wise data not found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            start_time = time.time()
            token = request.META["HTTP_AUTHORIZATION"]
            menu_id = request.GET.get("menuId", "")
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif menu_id == "":
                response_data = {
                    "message": "column id is missing",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # =======================================category data==================================================
                mega_menu_query = {"menuId": menu_id}
                mega_menu_column = db.ecomMegaMenuColumn.find(mega_menu_query).sort([("seqId", 1)])
                mega_row_query = {"columnId": menu_id}
                mega_menu_row = db.ecomMegaMenuRows.find(mega_row_query).sort([("seqId", 1)])
                mega_menu_column_json = []
                count = 0
                if mega_menu_column.count() > 0:
                    for mega_column in mega_menu_column:
                        mega_menu_rows = db.ecomMegaMenuRows.find(
                            {"columnId": str(mega_column["_id"]), "level": 2}
                        ).sort([("seqId", -1)])
                        if mega_menu_rows.count() > 0:
                            for row in mega_menu_rows:
                                entity_data = []
                                more_count = 0
                                for entity in row["entity"]:
                                    entity_data.append(
                                        {
                                            "id": entity["id"],
                                            "name": entity["name"][language]
                                            if language in entity["name"]
                                            else entity["name"]["en"],
                                        }
                                    )
                                if "firstCategoryName" in row:
                                    if language in row["firstCategoryName"]:
                                        first_category_name = row["firstCategoryName"][language]
                                    else:
                                        first_category_name = ""
                                else:
                                    first_category_name = ""

                                if "secondCategoryName" in row:
                                    if language in row["secondCategoryName"]:
                                        second_category_name = row["secondCategoryName"][language]
                                    else:
                                        second_category_name = ""
                                else:
                                    second_category_name = ""

                                if "thirdCategoryName" in row:
                                    if language in row["thirdCategoryName"]:
                                        third_category_name = row["thirdCategoryName"][language]
                                    else:
                                        third_category_name = ""
                                else:
                                    third_category_name = ""
                                if row["entityLinkedWith"] == 2:
                                    more_count = db.category.find(
                                        {"parentId": ObjectId(entity_data[0]["id"])}
                                    ).count()
                                else:
                                    pass
                                count = count + 1
                                mega_menu_column_json.append(
                                    {
                                        "height": row["height"],
                                        "childCount": more_count,
                                        "dataType": row["dataType"],
                                        "columnId": str(mega_column["_id"]),
                                        "rowId": str(row["_id"]),
                                        "textType": int(row["textType"]),
                                        "text": row["text"][language]
                                        if language in row["text"]
                                        else row["text"]["en"],
                                        "dataType": mega_column["dataType"],
                                        "firstCategoryName": first_category_name,
                                        "secondCategoryName": second_category_name,
                                        "thirdCategoryName": third_category_name,
                                        "level": row["level"] if "level" in row else 0,
                                        "entityLinkedWith": row["entityLinkedWith"],
                                        "rows": mega_column["rows"],
                                        "entity": entity_data,
                                        "webImages": row["webImages"],
                                        "mobileImage": row["mobileImage"],
                                        "menuId": mega_column["menuId"],
                                        "count": count,
                                    }
                                )
                elif mega_menu_row.count() > 0:
                    for mega_column in mega_menu_row:
                        mega_menu_rows = db.ecomMegaMenuRows.find(
                            {"_id": ObjectId(mega_column["_id"])}
                        )
                        if mega_menu_rows.count() > 0:
                            for row in mega_menu_rows:
                                entity_data = []
                                more_count = 0
                                for entity in row["entity"]:
                                    entity_data.append(
                                        {
                                            "id": entity["id"],
                                            "name": entity["name"][language]
                                            if language in entity["name"]
                                            else entity["name"]["en"],
                                        }
                                    )
                                if "firstCategoryName" in row:
                                    if language in row["firstCategoryName"]:
                                        first_category_name = row["firstCategoryName"][language]
                                    else:
                                        first_category_name = ""
                                else:
                                    first_category_name = ""

                                if "secondCategoryName" in row:
                                    if language in row["secondCategoryName"]:
                                        second_category_name = row["secondCategoryName"][language]
                                    else:
                                        second_category_name = ""
                                else:
                                    second_category_name = ""

                                if "thirdCategoryName" in row:
                                    if language in row["thirdCategoryName"]:
                                        third_category_name = row["thirdCategoryName"][language]
                                    else:
                                        third_category_name = ""
                                else:
                                    third_category_name = ""
                                if row["entityLinkedWith"] == 2:
                                    more_count = db.category.find(
                                        {"parentId": ObjectId(entity_data[0]["id"])}
                                    ).count()
                                else:
                                    pass
                                count = count + 1
                                mega_menu_column_json.append(
                                    {
                                        "height": row["height"],
                                        "childCount": more_count,
                                        "dataType": row["dataType"],
                                        "columnId": str(mega_column["_id"]),
                                        "rowId": str(row["_id"]),
                                        "textType": int(row["textType"]),
                                        "text": row["text"][language]
                                        if language in row["text"]
                                        else row["text"]["en"],
                                        "dataType": mega_column["dataType"],
                                        "firstCategoryName": first_category_name,
                                        "secondCategoryName": second_category_name,
                                        "thirdCategoryName": third_category_name,
                                        "level": row["level"] if "level" in row else 0,
                                        "entityLinkedWith": row["entityLinkedWith"],
                                        "rows": mega_column["rows"],
                                        "entity": entity_data,
                                        "webImages": row["webImages"],
                                        "mobileImage": row["mobileImage"],
                                        "menuId": "",
                                        "count": count,
                                    }
                                )
                else:
                    pass

                if len(mega_menu_column_json) > 0:
                    response_data = {
                        "message": "data found",
                        "data": mega_menu_column_json,
                    }
                    return JsonResponse(response_data, safe=False, status=200)
                else:
                    response_data = {
                        "message": "data not found",
                        "data": [],
                    }
                    return JsonResponse(response_data, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"message": message}
            return JsonResponse(error, safe=False, status=500)


"""
    type 0  for website
    type 1 for app
    deviceType:
                0 for website
                1 for IOS
                2 for Android
    LoginType:
                0 for retailer
                1 for distributor
    Type in Response:
                1 for recently bought
                2 for banner
                3 for recently viewed
                4 for category

"""


class HomePageV4(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the data on home page in app and website",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default="5df8766e8798dc2f236c95fa",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="storeId",
                default="5e20914ac348027af2f9028e",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular store",
            ),
            openapi.Parameter(
                name="requestFrom",
                default="1",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="fetching the data from which type, for website value"
                "should be 1, from tablet value should be 2"
                "from mobile value should be 3",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            start_time = time.time()
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            last_json_response = []
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            store_category_id = request.GET.get("storeCategoryId", "")
            store_id = request.GET.get("storeId", "")
            request_from = request.GET.get("requestFrom", "1")
            token = request.META["HTTP_AUTHORIZATION"]
            zone_id = request.GET.get('zoneId', "")
            try:
                user_id = json.loads(token)["userId"]
            except:
                user_id = "609ccd3d635c3f68308be75f"
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif store_category_id == "":
                response_data = {
                    "message": "store category id is missing",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # ==================================store details page======================================
                home_page_query = {"storeCategoryId": store_category_id, "contentType": 2}
                if store_id != "":
                    home_page_query["storeId"] = store_id
                home_page_header_data = db.ecomHomePage.find_one(
                    {"storeCategoryId": store_category_id, "contentType": 1}
                )
                home_page_footer_data = db.ecomHomePage.find_one(
                    {"storeCategoryId": store_category_id, "contentType": 3}
                )
                home_page_content_insta = db.ecomHomePage.find_one(home_page_query)
                instagram_feeds = db.instagramPostFeed.find({}).sort([("_id", -1)]).limit(36)

                if zone_id != "":
                    try:
                        datas = "homepage_" + str(zone_id) + "_" + str(request_from)
                        print(datas)
                        home_page_json = json.loads(
                            r.get("homepage_" + str(zone_id) + "_" + str(request_from))
                        )
                    except:
                        print("*************error******************")
                        home_page_json = []
                else:
                    try:
                        home_page_json = json.loads(
                            r.get("homepage_" + str(store_category_id) + "_" + str(request_from))
                        )
                    except:
                        print("*************error******************")
                        home_page_json = []

                header_details = {}
                if home_page_header_data is not None:
                    header_details["description"] = (
                        home_page_header_data["description"][language]
                        if language in home_page_header_data["description"]
                        else home_page_header_data["description"]["en"]
                    )
                    header_details["title"] = (
                        home_page_header_data["title"][language]
                        if language in home_page_header_data["description"]
                        else home_page_header_data["title"]["en"]
                    )
                    try:
                        header_details["afterLoginTitle"] = (
                            home_page_header_data["afterLoginTitle"][language]
                            if language in home_page_header_data["afterLoginTitle"]
                            else home_page_header_data["afterLoginTitle"]["en"]
                        )
                    except:
                        header_details["afterLoginTitle"] = ""
                    header_details["images"] = (
                        home_page_header_data["images"] if "images" in home_page_header_data else []
                    )
                    header_details["headerFooterTextColor"] = (
                        home_page_header_data["headerFooterTextColor"]
                        if "headerFooterTextColor" in home_page_header_data
                        else ""
                    )
                    header_details["headerFooterBackGroundColor"] = (
                        home_page_header_data["headerFooterBackGroundColor"]
                        if "headerFooterBackGroundColor" in home_page_header_data
                        else ""
                    )
                else:
                    pass

                footer_details = {}
                if home_page_footer_data is not None:
                    footer_details["description"] = (
                        home_page_footer_data["description"][language]
                        if language in home_page_footer_data["description"]
                        else home_page_footer_data["description"]["en"]
                    )
                    footer_details["title"] = (
                        home_page_footer_data["title"][language]
                        if language in home_page_footer_data["description"]
                        else home_page_footer_data["title"]["en"]
                    )
                    footer_details["images"] = (
                        home_page_footer_data["images"] if "images" in home_page_footer_data else []
                    )
                    footer_details["headerFooterTextColor"] = (
                        home_page_footer_data["headerFooterTextColor"]
                        if "headerFooterTextColor" in home_page_footer_data
                        else ""
                    )
                    footer_details["headerFooterBackGroundColor"] = (
                        home_page_footer_data["headerFooterBackGroundColor"]
                        if "headerFooterBackGroundColor" in home_page_footer_data
                        else ""
                    )
                else:
                    pass

                home_page_seo = {}
                home_page_seo_details = db.homepage.find_one(
                    {"storeId": "0"}, {"seo": 1, "homepage": 1}
                )
                if home_page_seo_details is not None:
                    if "seo" in home_page_seo_details:
                        home_page_seo["metatags"] = (
                            home_page_seo_details["seo"]["title"]["en"]
                            if "title" in home_page_seo_details["seo"]
                            else ""
                        )  # home_page_seo_details["metaTags"] if "metaTags" in home_page_seo_details else ""
                        home_page_seo["metatagsdesc"] = (
                            home_page_seo_details["seo"]["description"]["en"]
                            if "description" in home_page_seo_details["seo"]
                            else ""
                        )
                        home_page_seo["title"] = (
                            home_page_seo_details["seo"]["title"]["en"]
                            if "title" in home_page_seo_details["seo"]
                            else ""
                        )
                        home_page_seo["copyRight"] = (
                            home_page_seo_details["homepage"]["copyRight"]["en"]
                            if "copyRight" in home_page_seo_details["homepage"]
                            else ""
                        )
                    else:
                        home_page_seo["metatags"] = "Online Shopping Site for Lifestyle & More"
                        home_page_seo["title"] = "Online Shopping Site for Lifestyle & More"
                        home_page_seo[
                            "metatagsdesc"
                        ] = "Online Shopping Site for Lifestyle & Fashion in India. Buy Shoes, Clothing, Accessories and lifestyle products for women & men. Best Online Fashion Store *COD* Easy returns and exchanges*"
                        home_page_seo["copyRight"] = "Copyright © 2021 Roadyo. All rights reserved"
                else:
                    home_page_seo["metatags"] = "Online Shopping Site for Lifestyle & More"
                    home_page_seo["title"] = "Online Shopping Site for Lifestyle & More"
                    home_page_seo[
                        "metatagsdesc"
                    ] = "Online Shopping Site for Lifestyle & Fashion in India. Buy Shoes, Clothing, Accessories and lifestyle products for women & men. Best Online Fashion Store *COD* Easy returns and exchanges*"
                    home_page_seo["copyRight"] = "Copyright © 2021 Roadyo. All rights reserved"

                # instagram feeds banner
                if home_page_content_insta is not None:

                    insta_feed_template = {
                        "_id": "",
                        "title": "MEOLAA'S INSTAGRAM",
                        "afterLoginTitle": "",
                        "description": "",
                        "linkedWith": 8,
                        "visible": True,
                        "type": 7,
                        "sectionType": 1,
                        "buttonText": "",
                        "image": [],
                        "cellType": 2,
                        "numberOfRows": {
                            "row": 2,
                            "cellCount": 6
                        },
                        "entity": [],
                        "seqId": 1
                    }

                    if "instagramMode" in home_page_content_insta:
                        if home_page_content_insta["instagramMode"] == "1" or home_page_content_insta["instagramMode"] == 1:
                            try:
                                insta_feed_template["linkedWith"] = int(home_page_content_insta["linkedWith"][0])
                            except:
                                insta_feed_template["linkedWith"] = home_page_content_insta["linkedWith"]

                            insta_feed_template["_id"] = str(home_page_content_insta["_id"])
                            insta_feed_template["title"] = home_page_content_insta["title"]["en"]
                            insta_feed_template["visible"] = home_page_content_insta["visible"]
                            insta_feed_template["type"] = home_page_content_insta["type"]
                            insta_feed_template["sectionType"] = home_page_content_insta["sectionType"]
                            insta_feed_template["description"] = home_page_content_insta["description"]["en"]
                            insta_feed_template["cellType"] = int(home_page_content_insta["cellType"])
                            insta_feed_template["numberOfRows"] = home_page_content_insta["numberOfRows"]
                            insta_feed_template["seqId"] = home_page_content_insta["seqId"]

                    insta_feed_entity = {
                        "name": "",
                        "categoryId": "",
                        "buttonText": "",
                        "linkedWith": 8,
                        "value": "",
                        "currency": "",
                        "currencySymbol": "",
                        "images": [
                            {
                                "altText": "InstagramFeed",
                                "extraLarge": "",
                                "large": "",
                                "medium": "",
                                "small": ""
                            }
                        ],
                        "id": "",
                        "link": "",
                        "firstCategoryName": "",
                        "secondCategoryName": "",
                        "thirdCategoryName": "",
                        "level": 0,
                        "seqId": 0,
                        "discountPrice": "0",
                        "price": "0",
                        "sellerId": "",
                        "sellerTypeId": 0,
                        "storeCategoryId": "",
                        "storeFrontTypeId": 0,
                        "desc": "  ",
                        "firstCategoryId": "",
                        "secondCategoryId": "",
                        "thirdCategoryId": ""
                    }
                    insta_feed_temp_list = []
                    for insta_f in instagram_feeds:
                        ins_feed_temp = insta_feed_entity
                        ins_feed_temp["id"] = str(insta_f["_id"])
                        ins_feed_temp["name"] = insta_f["title"]["en"]
                        ins_feed_temp["link"] = insta_f["socialMediaLink"]
                        ins_feed_temp["images"] = insta_f["images"]

                        ins_feed_temp["createOn"] = insta_f["createOn"]
                        ins_feed_temp["createTimestap"] = insta_f["createTimestap"]
                        ins_feed_temp["status"] = insta_f["status"]
                        ins_feed_temp["postBy"] = insta_f["postBy"]
                        ins_feed_temp["productLink"] = insta_f["productLink"]

                        insta_feed_temp_list.append(ins_feed_temp)

                    if insta_feed_temp_list:
                        insta_feed_template["entity"] = insta_feed_temp_list
                        home_page_json.append(insta_feed_template)

                last_response = {
                    "homePageSeo": home_page_seo,
                    "data": home_page_json,
                    "footer": footer_details,
                    "header": header_details,
                }
                print("***********home page response time************", time.time() - start_time)
                return JsonResponse(last_response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class ZoneAlerts(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Zones"],
        operation_description="api for get the alrets for zones",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default="5df871188798dc4ce17e484a",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5fcf541871d7bd51e6008c94",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="get the alert from particualar zone",
            ),
            openapi.Parameter(
                name="timezone",
                default="330",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="offset of the timezone",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            store_category_id = request.GET.get("storeCategoryId", "")
            zone_id = request.GET.get("zoneId", "")
            time_zone = request.GET.get("timezone", "")
            token = request.META["HTTP_AUTHORIZATION"]
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif store_category_id == "":
                response_data = {
                    "message": "store category id is missing",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            elif zone_id == "":
                response_data = {
                    "message": "zone id is missing",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            elif time_zone == "":
                response_data = {
                    "message": "time zone is missing",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # ==================================store details page======================================
                currenct_date = datetime.datetime.now().timestamp()
                next_time = int(currenct_date + int(time_zone))
                zone_query = {
                    "storeCategories.categoryId": store_category_id,
                    "status": "ACTIVE",
                    "zoneId": zone_id,
                    "startsTimeStemp": {"$lte": next_time},
                    "endTimeStemp": {"$gte": next_time},
                }
                zone_alert_data = db.zoneAlert.find(zone_query)
                zone_alert_details = []
                if zone_alert_data.count() > 0:
                    for alert in zone_alert_data:
                        zone_alert_details.append(
                            {
                                "alertId": str(alert["_id"]),
                                "message": alert["message"],
                                "startsTimeStemp": alert["startsTimeStemp"],
                                "endTimeStemp": alert["endTimeStemp"],
                            }
                        )
                else:
                    pass

                if len(zone_alert_details) > 0:
                    last_response = {
                        "message": "alert found",
                        "totalCount": len(zone_alert_details),
                        "data": zone_alert_details,
                    }
                    return JsonResponse(last_response, safe=False, status=200)
                else:
                    last_response = {
                        "message": "alert not found",
                        "totalCount": 0,
                        "data": [],
                    }
                    return JsonResponse(last_response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class VariantList(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Variant"],
        operation_description="api for get the variant list for the products",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="parentProoductId",
                default="60ebbb127cc78aa89e996796",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="parent product id from which parent product we need to fetch the variant",
            ),
            openapi.Parameter(
                name="childProoductId",
                default="60ebc82c7cc78aa89e9967e1",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="child product id of the product, which are currently selected",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            parent_product_id = request.GET.get("parentProoductId", "")
            child_product_id = request.GET.get("childProoductId", "")
            token = request.META["HTTP_AUTHORIZATION"]
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif parent_product_id == "":
                response_data = {
                    "message": "parent product id is missing",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            elif child_product_id == "":
                response_data = {
                    "message": "child product id is missing",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # ==================================product variant get api======================================
                product_data = []
                child_product_details = db.childProducts.find_one(
                    {"_id": ObjectId(child_product_id)}
                )
                if child_product_details is not None:
                    product_query = {"parentProductId": parent_product_id, "status": 1}

                    if child_product_details["storeId"] == "0":
                        product_query["storeId"] = child_product_details["storeId"]
                    else:
                        product_query["storeId"] = ObjectId(child_product_details["storeId"])
                    all_variant_data = db.childProducts.find(product_query)
                    if all_variant_data.count() > 0:
                        for variant in all_variant_data:
                            attribute_data = []
                            if str(variant["_id"]) == child_product_id:
                                is_primary = True
                            else:
                                is_primary = False
                            try:
                                for link_unit in variant["units"][0]["attributes"]:
                                    try:
                                        for attrlist in link_unit["attrlist"]:
                                            try:
                                                if attrlist is None:
                                                    pass
                                                else:
                                                    if attrlist["linkedtounit"] == 1:
                                                        if attrlist["measurementUnit"] == "":
                                                            attr_name = (
                                                                str(attrlist["value"][language])
                                                                if language in attrlist["value"]
                                                                else str(attrlist["value"]["en"])
                                                            )
                                                        else:
                                                            attr_name = (
                                                                str(attrlist["value"][language])
                                                                + " "
                                                                + attrlist["measurementUnit"]
                                                                if language in attrlist["value"]
                                                                else str(attrlist["value"]["en"])
                                                                + " "
                                                                + attrlist["measurementUnit"]
                                                            )
                                                        attribute_data.append(
                                                            {
                                                                "value": str(attr_name),
                                                                "name": attrlist["attrname"]["en"],
                                                            }
                                                        )
                                                    else:
                                                        pass
                                            except:
                                                pass
                                    except:
                                        pass
                            except:
                                pass

                            if "unitSizeGroupValue" in variant["units"][0]:
                                if "en" in variant["units"][0]["unitSizeGroupValue"]:
                                    primary_child_product_size = variant["units"][0][
                                        "unitSizeGroupValue"
                                    ]["en"]
                                    if primary_child_product_size != "":
                                        attribute_data.append(
                                            {"value": primary_child_product_size, "name": "Size"}
                                        )
                                    else:
                                        pass
                                else:
                                    pass

                            if "colorName" in variant["units"][0]:
                                primary_child_product_colour = variant["units"][0]["colorName"]
                                if primary_child_product_colour == "":
                                    pass
                                else:
                                    attribute_data.append(
                                        {"value": primary_child_product_colour, "name": "Color"}
                                    )
                            else:
                                pass

                            offers_details = []
                            if "offer" in variant:
                                for offer in variant["offer"]:
                                    if offer["status"] == 1:
                                        offer_terms = db.offers.find_one(
                                            {"_id": ObjectId(offer["offerId"])}
                                        )
                                        if offer_terms is not None:
                                            if offer_terms["startDateTime"] <= int(time.time()):
                                                offers_details.append(offer)
                                            else:
                                                pass
                                        else:
                                            pass
                                    else:
                                        pass
                            else:
                                pass

                            # ==================tax calculation===============================
                            tax_price = 0
                            if "tax" in variant:
                                if len(variant["tax"]) == 0:
                                    tax_price = 0
                                else:
                                    for amount in variant["tax"]:
                                        if "taxValue" in amount:
                                            tax_price = tax_price + (int(amount["taxValue"]))
                                        if "value" in amount:
                                            tax_price = tax_price + (int(amount["value"]))
                                        else:
                                            tax_price = tax_price + 0
                            else:
                                tax_price = 0

                            if len(offers_details) > 0:
                                best_offer = max(offers_details, key=lambda x: x["discountValue"])
                                offer_details = db.offers.find(
                                    {
                                        "_id": ObjectId(best_offer["offerId"]),
                                        "status": 1,
                                        "storeId": ObjectId(variant["storeId"]),
                                    }
                                ).count()
                                if offer_details != 0:
                                    best_offer = best_offer
                                else:
                                    best_offer = {}
                                if len(best_offer) > 0:
                                    # ==================================offers part=========================================
                                    discount_type = (
                                        int(best_offer["discountType"])
                                        if "discountType" in best_offer
                                        else 0
                                    )
                                    discount_value = (
                                        best_offer["discountValue"]
                                        if "discountValue" in best_offer
                                        else 0
                                    )
                                else:
                                    best_offer = {}
                                    discount_type = 0
                                    discount_value = 0
                            else:
                                best_offer = {}
                                discount_type = 0
                                discount_value = 0

                            # ===============================price get the details======================================

                            try:
                                try:
                                    base_price = variant["units"][0]["b2cPricing"][0][
                                        "b2cproductSellingPrice"
                                    ]
                                except:
                                    base_price = variant["units"][0]["floatValue"]
                            except:
                                base_price = 0
                            base_price = base_price + ((base_price * tax_price) / 100)

                            if discount_type == 0:
                                discount_price = float(discount_value)
                            elif discount_type == 1:
                                discount_price = (float(base_price) * float(discount_value)) / 100
                            else:
                                discount_price = 0

                            final_price = float(base_price) - discount_price

                            if final_price == 0 or base_price == 0:
                                discount_price = 0
                            else:
                                discount_price = discount_price

                            product_data.append(
                                {
                                    "parentProductId": str(variant["parentProductId"]),
                                    "childProductId": str(variant["_id"]),
                                    "finalPriceList": {
                                        "basePrice": round(int(float(base_price)), 2),
                                        "discountPrice": round(discount_price, 2),
                                        "finalPrice": round(final_price, 2),
                                        "discountPercentage": discount_value,
                                    },
                                    "unitId": str(variant["units"][0]["unitId"]),
                                    "variantSpecs": attribute_data,
                                    "isPrimary": is_primary,
                                }
                            )

                        if len(product_data) > 0:
                            last_response = {
                                "message": "data found",
                                "data": product_data,
                            }
                            return JsonResponse(last_response, safe=False, status=200)
                        else:
                            last_response = {
                                "message": "data not found",
                                "data": [],
                            }
                            return JsonResponse(last_response, safe=False, status=404)
                    else:
                        last_response = {
                            "message": "data not found",
                            "data": [],
                        }
                        return JsonResponse(last_response, safe=False, status=404)
                else:
                    last_response = {
                        "message": "data not found",
                        "data": [],
                    }
                    return JsonResponse(last_response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class SeoDetails(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Seo Details"],
        operation_description="api for get the seo details for stores, like store category id etc",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="storeId",
                default="5e20914ac348027af2f9028e",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store id from which store we need to get the details",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            store_id = request.GET.get("storeId", "")
            token = request.META["HTTP_AUTHORIZATION"]
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif store_id == "":
                response_data = {
                    "message": "store id is missing",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # ==================================product variant get api======================================
                store_query = {"_id": ObjectId(store_id)}
                store_details = db.stores.find_one(store_query)
                if store_details is not None:
                    store_category_id = store_details["categoryId"]
                    store_category_details = db.storeCategory.find_one(
                        {"_id": ObjectId(store_category_id)}
                    )
                    if store_category_details is not None:
                        last_response = {
                            "type": store_category_details["type"],
                            "scheduleBooking": store_category_details["scheduleBooking"],
                            "shiftSelection": store_category_details["shiftSelection"],
                            "nowBooking": store_category_details["nowBooking"],
                            "storeCategoryId": store_category_id,
                            "categoryName": store_details["categoryName"][language],
                            "message": "data found",
                        }
                        return JsonResponse(last_response, safe=False, status=200)
                    else:
                        last_response = {
                            "message": "data not found",
                            "data": [],
                        }
                        return JsonResponse(last_response, safe=False, status=404)
                else:
                    last_response = {
                        "message": "data not found",
                        "data": [],
                    }
                    return JsonResponse(last_response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class MegaMenuOfferList(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="api for get the offer list for the mega menu section",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default="5cc0846e087d924456427975",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store category id from which store category we need to get the data",
            ),
            openapi.Parameter(
                name="storeId",
                default="5e20914ac348027af2f9028e",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store id from which store we need to get the details",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            store_id = request.GET.get("storeId", "")
            store_category_id = request.GET.get("storeCategoryId", "")
            token = request.META["HTTP_AUTHORIZATION"]
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif store_category_id == "":
                response_data = {
                    "message": "store category id is missing",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # ==================================product variant get api======================================
                offer_query = {
                    "storeCategoryId": store_category_id,
                    "contentType": 4,
                    "testMode": "false",
                    "status": 1,
                }
                if store_id != "":
                    offer_query["storeId"] = store_id
                offer_details = db.ecomHomePage.find(offer_query)
                offer_data = []
                if offer_details.count() > 0:
                    for offer in offer_details:
                        entity_data = []
                        for entity in offer["entity"]:
                            entity_data.append(
                                {
                                    "name": entity["name"],
                                    "offerId": entity["offerId"],
                                    "linkedWith": entity["linkedWith"],
                                    "images": entity["images"],
                                    "seqId": entity["seqId"],
                                }
                            )
                        if offer["visible"] == "false":
                            visible = False
                        else:
                            visible = True
                        if len(entity_data) > 0:
                            newlist = sorted(entity_data, key=lambda k: k["seqId"], reverse=True)
                            offer_data.append(
                                {
                                    "id": str(offer["_id"]),
                                    "title": offer["title"][language]
                                    if language in offer["title"]
                                    else "",
                                    "description": offer["description"][language]
                                    if language in offer["description"]
                                    else "",
                                    "visible": visible,
                                    "type": offer["type"],
                                    "cellType": offer["cellType"],
                                    "numberOfRows": offer["numberOfRows"],
                                    "entity": newlist,
                                }
                            )
                        else:
                            pass
                    if len(offer_data) > 0:
                        last_response = {
                            "message": "data found",
                            "data": offer_data,
                        }
                        return JsonResponse(last_response, safe=False, status=200)
                    else:
                        last_response = {
                            "message": "data not found",
                            "data": [],
                        }
                        return JsonResponse(last_response, safe=False, status=404)
                else:
                    last_response = {
                        "message": "data not found",
                        "data": [],
                    }
                    return JsonResponse(last_response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


"""
    type 0  for website
    type 1 for app
    deviceType:
                0 for website
                1 for IOS
                2 for Android
    LoginType:
                0 for retailer
                1 for distributor
    Type in Response:
                1 for recently bought
                2 for banner
                3 for recently viewed
                4 for category

"""


class HomePageIngredienta(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="API for getting the data on home page in app and website",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token",
            ),
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="timezone",
                default="Asia/Calcutta",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="offset of the timezone",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default="618cee1356e1122719470754",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5fcf541871d7bd51e6008c94",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular zone",
            ),
        ],
        responses={
            200: "successfully. data found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            last_json_response = []
            timezone_data = request.GET.get("timezone", "")
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            zone_id = str(request.META["HTTP_ZONEID"]) if "HTTP_ZONEID" in request.META else ""
            token = request.META["HTTP_AUTHORIZATION"]
            try:
                user_id = json.loads(token)["userId"]
            except:
                user_id = "609ccd3d635c3f68308be75f"
            category_seq_id = 1
            banner_seq_id = 2
            product_seq_id = 7
            next_availbale_driver_time = ""
            login_type = 1
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif zone_id == "":
                response_data = {
                    "message": "zone id is missing",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # ==================================store details page======================================
                is_temp_close = False
                store_tag = ""
                store_is_open = False
                if zone_id != "":
                    dc_details = db.zones.find_one({"_id": ObjectId(zone_id)}, {"DCStoreId": 1})
                else:
                    dc_details = None
                if dc_details is not None:
                    if "DCStoreId" in dc_details:
                        if dc_details["DCStoreId"] == "":
                            dc_store_details = None
                        else:
                            dc_store_details = db.stores.find_one(
                                {"_id": ObjectId(dc_details["DCStoreId"])}
                            )
                        if dc_store_details is not None:
                            # =====================================about store tags=================================
                            if "storeIsOpen" in dc_store_details:
                                store_is_open = dc_store_details["storeIsOpen"]
                            else:
                                store_is_open = False

                            if "nextCloseTime" in dc_store_details:
                                next_close_time = dc_store_details["nextCloseTime"]
                            else:
                                next_close_time = ""

                            if "nextOpenTime" in dc_store_details:
                                next_open_time = dc_store_details["nextOpenTime"]
                            else:
                                next_open_time = ""

                            try:
                                if "timeZoneWorkingHour" in dc_store_details["_source"]:
                                    timeZoneWorkingHour = dc_store_details["_source"][
                                        "timeZoneWorkingHour"
                                    ]
                                else:
                                    timeZoneWorkingHour = ""
                            except:
                                timeZoneWorkingHour = ""

                            is_delivery = True
                            if next_close_time == "" and next_open_time == "":
                                is_temp_close = True
                                store_tag = "Store is temporarily closed"
                            elif next_open_time != "" and store_is_open == False:
                                is_temp_close = False
                                # next_open_time = int(next_open_time + timezone_data * 60)
                                next_open_time = time_zone_converter(
                                    timezone_data, next_open_time, timeZoneWorkingHour
                                )
                                local_time = datetime.datetime.fromtimestamp(next_open_time)
                                next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                                next_day_midnight = next_day.replace(hour=0, minute=0, second=0)
                                next_day_midnight_timestamp = int(next_day_midnight.timestamp())
                                if next_day_midnight_timestamp > next_open_time:
                                    open_time = local_time.strftime("%I:%M %p")
                                    store_tag = "Store is closed, opens next at " + open_time
                                else:
                                    open_time = local_time.strftime("%I:%M %p")
                                    store_tag = (
                                        "Store is closed, next opens tomorrow At " + open_time
                                    )
                            else:
                                is_temp_close = False
                                store_tag = ""
                store_data_json = []
                store_details = []
                if zone_id != "":
                    store_data = db.stores.find(
                        {
                            "categoryId": str(store_category_id),
                            "serviceZones.zoneId": zone_id,
                            "status": 1,
                        }
                    )
                    for store in store_data:
                        store_details.append(str(store["_id"]))
                        store_data_json.append(ObjectId(store["_id"]))
                else:
                    pass
                # ===============================================category data======================================
                category_time = time.time()
                try:
                    category_json = []
                    to_data = 4
                    category_store_data_details = []
                    if zone_id != "" and store_category_id == PHARMACY_STORE_CATEGORY_ID:
                        store_query = {"categoryId": str(store_category_id), "cityId": city_id}
                    else:
                        store_query = {
                            "categoryId": str(store_category_id),
                            "serviceZones.zoneId": zone_id,
                        }
                    store_data = store_find(store_query)
                    if store_data.count() > 0:
                        for store in store_data:
                            category_store_data_details.append(str(store["_id"]))
                        category_query = {
                            "status": 1,
                            "parentId": {"$exists": False},
                            "storeCategory.storeCategoryId": str(store_category_id),
                            "$or": [
                                {"storeid": {"$in": category_store_data_details}},
                                {"storeId": {"$in": category_store_data_details}},
                            ],
                        }
                        category_data_db = category_find(category_query, 0, to_data)
                        if category_data_db.count() == 0:
                            response = {
                                "id": "",
                                "catName": "",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": [],
                                "type": 2,
                                "seqId": category_seq_id,
                            }
                            last_json_response.append(response)
                    else:
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "categoryData": [],
                            "type": 2,
                            "seqId": category_seq_id,
                        }
                        last_json_response.append(response)
                    try:
                        if category_data_db.count() != 0:
                            for cate_gory in category_data_db:
                                category_json.append(
                                    {
                                        "id": str(cate_gory["_id"]),
                                        "catName": cate_gory["categoryName"][language]
                                        if language in cate_gory["categoryName"]
                                        else cate_gory["categoryName"]["en"],
                                        "imageUrl": cate_gory["mobileImage"]
                                        if "mobileImage" in cate_gory
                                        else "",
                                        "bannerImageUrl": cate_gory["mobileImage"]
                                        if "mobileImage" in cate_gory
                                        else "",
                                        "websiteImageUrl": cate_gory["websiteImage"]
                                        if "websiteImage" in cate_gory
                                        else "",
                                        "websiteBannerImageUrl": cate_gory["websiteImage"]
                                        if "websiteImage" in cate_gory
                                        else "",
                                    }
                                )

                            dataframe = pd.DataFrame(category_json)
                            dataframe = dataframe.drop_duplicates(subset="catName", keep="last")
                            product_list = dataframe.to_dict(orient="records")
                            newlist = product_list  # sorted(
                            # product_list, key=lambda k: k['catName'], reverse=True)

                            response = {
                                "id": "",
                                "catName": "",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": newlist,
                                "seqId": category_seq_id,
                                "type": 2,
                            }
                            last_json_response.append(response)
                        else:
                            response = {
                                "id": "",
                                "catName": "",
                                "imageUrl": "",
                                "bannerImageUrl": "",
                                "websiteImageUrl": "",
                                "websiteBannerImageUrl": "",
                                "offers": [],
                                "penCount": 0,
                                "categoryData": category_json,
                                "type": 2,
                                "seqId": category_seq_id,
                            }
                            last_json_response.append(response)
                    except:
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "categoryData": category_json,
                            "type": 2,
                            "seqId": category_seq_id,
                        }
                        last_json_response.append(response)
                except Exception as ex:
                    print(
                        "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                        type(ex).__name__,
                        ex,
                    )
                    error = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": [],
                        "type": 2,
                        "seqId": category_seq_id,
                    }
                    last_json_response.append(error)

                # ===========================================banner details=========================================
                try:
                    banner_deatils_data = []
                    store_data_details = []
                    store_data = store_find(
                        {
                            "categoryId": str(store_category_id),
                            "serviceZones.zoneId": zone_id,
                            "status": 1,
                        }
                    )
                    if store_data.count() > 0:
                        for store in store_data:
                            store_data_details.append(str(store["_id"]))
                    banner_query = {
                        "status": 1,
                        "storeCategoryId": store_category_id,
                        "zones.zoneId": zone_id,
                    }
                    banner_type = ""
                    banner_details = banner_find(banner_query)
                    if banner_details.count() > 0:
                        for i in banner_details:
                            try:
                                category_name = ""
                                sub_category_name = ""
                                sub_sub_category_name = ""
                                if int(i["type"]) == 1:
                                    banner_type = "offer"
                                elif int(i["type"]) == 2:
                                    banner_type = "brands"
                                elif int(i["type"]) == 3:
                                    banner_type = "category"
                                elif int(i["type"]) == 4:
                                    banner_type = "stores"
                                elif int(i["type"]) == 5:
                                    banner_type = "subcategory"
                                elif int(i["type"]) == 6:
                                    banner_type = "subsubcategory"
                                elif int(i["type"]) == 7:
                                    banner_type = "supplier"
                                elif int(i["type"]) == 8:
                                    banner_type = "products"
                                elif int(i["type"]) == 9:
                                    banner_type = "url"
                                if int(i["type"]) == 3:
                                    base_category = category_find_one(
                                        {"_id": ObjectId(str(i["data"][0]["id"]).replace(" ", ""))}
                                    )
                                    if base_category is not None:
                                        if "parentId" in base_category:
                                            second_category = category_find_one(
                                                {"_id": ObjectId(base_category["parentId"])}
                                            )
                                            if second_category is not None:
                                                if "parentId" in second_category:
                                                    sub_sub_category_name = base_category[
                                                        "categoryName"
                                                    ]["en"]
                                                    sub_category_name = second_category[
                                                        "categoryName"
                                                    ]["en"]
                                                    first_category = category_find_one(
                                                        {
                                                            "_id": ObjectId(
                                                                second_category["parentId"]
                                                            )
                                                        }
                                                    )
                                                    if first_category is not None:
                                                        category_name = first_category[
                                                            "categoryName"
                                                        ]["en"]
                                                    else:
                                                        category_name = ""
                                                else:
                                                    category_name = second_category["categoryName"][
                                                        "en"
                                                    ]
                                                    sub_category_name = base_category[
                                                        "categoryName"
                                                    ]["en"]
                                                    sub_sub_category_name = ""
                                            else:
                                                first_category = category_find_one(
                                                    {
                                                        "_id": ObjectId(
                                                            str(i["data"][0]["id"]).replace(" ", "")
                                                        )
                                                    }
                                                )
                                                if first_category is not None:
                                                    category_name = first_category["categoryName"][
                                                        "en"
                                                    ]
                                                else:
                                                    category_name = ""
                                                    sub_category_name = ""
                                                    sub_sub_category_name = ""
                                        else:
                                            first_category = category_find_one(
                                                {
                                                    "_id": ObjectId(
                                                        str(i["data"][0]["id"]).replace(" ", "")
                                                    )
                                                }
                                            )
                                            if first_category is not None:
                                                category_name = first_category["categoryName"]["en"]
                                            else:
                                                category_name = ""
                                                sub_category_name = ""
                                                sub_sub_category_name = ""
                                    else:
                                        category_name = ""
                                        sub_category_name = ""
                                        sub_sub_category_name = ""

                                if sub_category_name != "" and sub_sub_category_name == "":
                                    banner_deatils_data.append(
                                        {
                                            "type": 5,
                                            "bannerTypeMsg": "subcategory",
                                            "catName": category_name,
                                            "offerName": "",
                                            "name": i["data"][0]["name"]["en"],
                                            "imageWeb": i["image_web"],
                                            "imageMobile": i["image_mobile"],
                                        }
                                    )
                                elif sub_category_name != "" and sub_sub_category_name != "":
                                    banner_deatils_data.append(
                                        {
                                            "type": 6,
                                            "bannerTypeMsg": "subsubcategory",
                                            "offerName": "",
                                            "catName": category_name,
                                            "subCatName": sub_category_name,
                                            "name": i["data"][0]["name"]["en"],
                                            "imageWeb": i["image_web"],
                                            "imageMobile": i["image_mobile"],
                                        }
                                    )
                                elif int(i["type"]) == 1:
                                    offer_query = {"_id": ObjectId(i["data"][0]["id"]), "status": 1}
                                    offer_details = offer_find_one(offer_query)
                                    if offer_details is not None:
                                        product_query = {
                                            "offer.status": 1,
                                            "offer.offerId": str(offer_details["_id"]),
                                            "status": 1,
                                        }
                                        child_product_count = product_find_count(product_query)
                                        if child_product_count > 0:
                                            banner_deatils_data.append(
                                                {
                                                    "type": i["type"],
                                                    "bannerTypeMsg": banner_type,
                                                    "catName": "",
                                                    "subCatName": "",
                                                    "offerName": offer_details["name"]["en"],
                                                    "name": str(offer_details["_id"]),
                                                    "imageWeb": i["image_web"],
                                                    "imageMobile": i["image_mobile"],
                                                }
                                            )
                                elif int(i["type"]) == 8:
                                    product_details = db.products.find_one(
                                        {"_id": ObjectId(i["data"][0]["id"]), "status": 1},
                                        {"units": 1},
                                    )
                                    if product_details is not None:
                                        supplier_list = []
                                        if "suppliers" in product_details["units"][0]:
                                            for s in product_details["units"][0]["suppliers"]:
                                                if s["id"] != "0":
                                                    child_product_count = db.childProducts.find(
                                                        {
                                                            "_id": ObjectId(s["productId"]),
                                                            "status": 1,
                                                        }
                                                    ).count()
                                                    if child_product_count > 0:
                                                        supplier_list.append(s)
                                                else:
                                                    pass
                                        if len(supplier_list) > 0:
                                            best_supplier = min(
                                                supplier_list, key=lambda x: x["retailerPrice"]
                                            )
                                            if best_supplier["retailerQty"] == 0:
                                                best_supplier = max(
                                                    supplier_list, key=lambda x: x["retailerQty"]
                                                )
                                            else:
                                                best_supplier = best_supplier
                                            if len(best_supplier) > 0:
                                                central_product_id = str(product_details["_id"])
                                                child_product_id = str(best_supplier["productId"])
                                                banner_deatils_data.append(
                                                    {
                                                        "type": i["type"],
                                                        "bannerTypeMsg": banner_type,
                                                        "catName": "",
                                                        "parentProductId": central_product_id,
                                                        "childProductId": child_product_id,
                                                        "subCatName": "",
                                                        "offerName": "/python/product/details?&parentProductId="
                                                        + central_product_id
                                                        + "&productId="
                                                        + child_product_id,
                                                        "name": "/python/product/details?&parentProductId="
                                                        + central_product_id
                                                        + "&productId="
                                                        + child_product_id,
                                                        "imageWeb": i["image_web"],
                                                        "imageMobile": i["image_mobile"],
                                                    }
                                                )
                                elif int(i["type"]) == 9:
                                    banner_deatils_data.append(
                                        {
                                            "type": i["type"],
                                            "bannerTypeMsg": banner_type,
                                            "catName": "",
                                            "subCatName": "",
                                            "offerName": i["data"][0]["name"]["en"],
                                            "name": i["data"][0]["name"]["en"],
                                            "imageWeb": i["image_web"],
                                            "imageMobile": i["image_mobile"],
                                        }
                                    )
                                else:
                                    try:
                                        store_details_name = json.loads(i["data"][0]["name"])
                                    except:
                                        try:
                                            store_details_name = i["data"][0]["name"]
                                        except:
                                            store_details_name = {"en": ""}
                                    try:
                                        if store_details_name["en"] != "":
                                            banner_deatils_data.append(
                                                {
                                                    "type": i["type"],
                                                    "bannerTypeMsg": banner_type,
                                                    "offerName": "",
                                                    "name": store_details_name["en"],
                                                    "imageWeb": i["image_web"],
                                                    "imageMobile": i["image_mobile"],
                                                }
                                            )
                                    except:
                                        pass
                            except:
                                pass
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "penCount": 0,
                            "offers": [],
                            "type": 1,
                            "seqId": banner_seq_id,
                            "categoryData": banner_deatils_data,
                        }
                        last_json_response.append(response)
                    else:
                        response = {
                            "id": "",
                            "catName": "",
                            "imageUrl": "",
                            "bannerImageUrl": "",
                            "websiteImageUrl": "",
                            "websiteBannerImageUrl": "",
                            "offers": [],
                            "penCount": 0,
                            "type": 1,
                            "seqId": banner_seq_id,
                            "categoryData": [],
                        }
                        last_json_response.append(response)
                except Exception as ex:
                    print(
                        "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                        type(ex).__name__,
                        ex,
                    )
                    error = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "seqId": banner_seq_id,
                        "type": 1,
                        "categoryData": [],
                    }
                    last_json_response.append(error)
                # ==========================================category data====================================
                sub_category_details = []
                sub_category_count = []
                must_not = []
                should_query = []
                must_query = [
                    {"match": {"status": 1}},
                    {"match": {"storeCategoryId": str(store_category_id)}},
                    {"exists": {"field": "categoryList"}},
                ]
                store_data_details = store_validation_function(store_category_id, zone_id)
                must_query.append({"terms": {"storeId": store_data_details}})

                sub_query = [{"_id": {"order": "desc"}}]
                must_not.append({"match": {"storeId": "0"}})
                must_not.append({"match": {"firstCategoryName": ""}})
                aggs_query = {
                    "group_by_sub_category": {
                        "terms": {"field": "firstCategoryName.keyword", "size": 4},
                        "aggs": {
                            "top_hits": {
                                "terms": {"field": "parentProductId.keyword", "size": 6},
                                "aggs": {
                                    "top_sales_hits": {
                                        "top_hits": {
                                            "sort": [
                                                {"isCentral": {"order": "desc"}},
                                                {"isInStock": {"order": "desc"}},
                                                {"units.discountPrice": {"order": "asc"}},
                                            ],
                                            "_source": {
                                                "includes": [
                                                    "_id",
                                                    "pName",
                                                    "storeId",
                                                    "parentProductId",
                                                    "currencySymbol",
                                                    "currency",
                                                    "pPName",
                                                    "needsIdProof",
                                                    "tax",
                                                    "brandTitle",
                                                    "categoryList",
                                                    "images",
                                                    "avgRating",
                                                    "units",
                                                    "storeCategoryId",
                                                    "manufactureName",
                                                    "maxQuantity",
                                                ]
                                            },
                                            "size": 1,
                                        }
                                    }
                                },
                            }
                        },
                    }
                }
                query = {
                    "sort": sub_query,
                    "query": {"bool": {"must": must_query, "must_not": must_not}},
                    "aggs": aggs_query,
                }
                categoty_data_json = []
                res = child_product_es_aggrigate_data(query)
                store_list_json = []
                # ===========================================get the store data========================================
                if zone_id != "" and store_category_id == PHARMACY_STORE_CATEGORY_ID:
                    store_query = {"status": 1, "serviceZones.zoneId": zone_id}
                    stores_list = store_find(store_query)
                    for s in stores_list:
                        store_list_json.append(str(s["_id"]))
                else:
                    pass
                # ==================================================end================================================
                main_sellers = []
                if zone_id != "":
                    store_details_inside = db.stores.find(
                        {
                            "serviceZones.zoneId": zone_id,
                            "storeFrontTypeId": {"$ne": 5},
                            "status": 1,
                        }
                    )
                    for seller in store_details_inside:
                        main_sellers.append(str(seller["_id"]))

                if zone_id != "":
                    driver_roaster = next_availbale_driver_roaster(zone_id)
                else:
                    driver_roaster = {}

                for res_res in res["aggregations"]["group_by_sub_category"]["buckets"][0:4]:
                    start_time_res = time.time()
                    cat_name = res_res["key"]
                    parent_product_cat_id = res_res["top_hits"]["buckets"][0]["top_sales_hits"][
                        "hits"
                    ]["hits"][0]["_source"]["categoryList"][0]["parentCategory"]["categoryId"]
                    if cat_name != "":
                        product_data = []
                        all_product_ids = []
                        for main_bucket in res_res["top_hits"]["buckets"]:
                            child_product_details = main_bucket["top_sales_hits"]["hits"]["hits"][
                                0
                            ]["_source"]
                            # =====================this block for check the product is available in dc or not================
                            all_dc_list = []
                            is_dc_linked = False
                            hard_limit = 0
                            pre_order = False
                            procurementTime = 0
                            # =====================end block for dc check================
                            best_seller_product = {}
                            main_product_details = None
                            child_product_id = str(
                                main_bucket["top_sales_hits"]["hits"]["hits"][0]["_id"]
                            )

                            if "productSeo" in child_product_details:
                                try:
                                    if len(child_product_details["productSeo"]["title"]) > 0:
                                        title = (
                                            child_product_details["productSeo"]["title"][language]
                                            if language
                                            in child_product_details["productSeo"]["title"]
                                            else child_product_details["productSeo"]["title"]["en"]
                                        )
                                    else:
                                        title = ""
                                except:
                                    title = ""

                                try:
                                    if len(child_product_details["productSeo"]["description"]) > 0:
                                        description = (
                                            child_product_details["productSeo"]["description"][
                                                language
                                            ]
                                            if language
                                            in child_product_details["productSeo"]["description"]
                                            else child_product_details["productSeo"]["description"][
                                                "en"
                                            ]
                                        )
                                    else:
                                        description = ""
                                except:
                                    description = ""

                                try:
                                    if len(child_product_details["productSeo"]["metatags"]) > 0:
                                        metatags = (
                                            child_product_details["productSeo"]["metatags"][
                                                language
                                            ]
                                            if language
                                            in child_product_details["productSeo"]["metatags"]
                                            else child_product_details["productSeo"]["metatags"][
                                                "en"
                                            ]
                                        )
                                    else:
                                        metatags = ""
                                except:
                                    metatags = ""

                                try:
                                    if len(child_product_details["productSeo"]["slug"]) > 0:
                                        slug = (
                                            child_product_details["productSeo"]["slug"][language]
                                            if language
                                            in child_product_details["productSeo"]["slug"]
                                            else child_product_details["productSeo"]["slug"]["en"]
                                        )
                                    else:
                                        slug = ""
                                except:
                                    slug = ""

                                product_seo = {
                                    "title": title,
                                    "description": description,
                                    "metatags": metatags,
                                    "slug": slug,
                                }
                            else:
                                product_seo = {
                                    "title": "",
                                    "description": "",
                                    "metatags": "",
                                    "slug": "",
                                }
                            if "prescriptionRequired" in child_product_details:
                                if child_product_details["prescriptionRequired"] == 0:
                                    prescription_required = False
                                else:
                                    prescription_required = True
                            else:
                                prescription_required = False

                            if "saleOnline" in child_product_details:
                                if child_product_details["saleOnline"] == 0:
                                    sales_online = False
                                else:
                                    sales_online = True
                            else:
                                sales_online = False

                            if "needsIdProof" in child_product_details:
                                if child_product_details["needsIdProof"] is False:
                                    needsIdProof = False
                                else:
                                    needsIdProof = True
                            else:
                                needsIdProof = False

                            if "uploadProductDetails" in child_product_details:
                                upload_details = child_product_details["uploadProductDetails"]
                            else:
                                upload_details = ""
                            try:
                                currency_rate = currency_exchange_rate[
                                    str(child_product_details["currency"])
                                    + "_"
                                    + str(currency_code)
                                ]
                            except:
                                currency_rate = 0
                            currency_details = db.currencies.find_one(
                                {"currencyCode": currency_code}
                            )
                            if currency_details is not None:
                                currency_symbol = currency_details["currencySymbol"]
                                currency = currency_details["currencyCode"]
                            else:
                                currency_symbol = child_product_details["currencySymbol"]
                                currency = child_product_details["currency"]

                            tax_value = []
                            if child_product_details is not None:
                                if type(child_product_details["tax"]) == list:
                                    for tax in child_product_details["tax"]:
                                        tax_value.append({"value": tax["taxValue"]})
                                else:
                                    if child_product_details["tax"] is not None:
                                        if "taxValue" in child_product_details["tax"]:
                                            tax_value.append(
                                                {"value": child_product_details["tax"]["taxValue"]}
                                            )
                                        else:
                                            tax_value.append(
                                                {"value": child_product_details["tax"]}
                                            )
                                    else:
                                        pass
                            else:
                                tax_value = []

                            query = {
                                "parentProductId": str(child_product_details["parentProductId"]),
                                "status": 1,
                            }
                            try:
                                if store_category_id == MEAT_STORE_CATEGORY_ID:
                                    variant_query["storeId"] = {
                                        "$in": [ObjectId(child_product_details["storeId"])]
                                    }
                                else:
                                    if str(child_product_details["storeId"]) == "0":
                                        variant_query["storeId"] = child_product_details["storeId"]
                                    else:
                                        variant_query["storeId"] = ObjectId(
                                            child_product_details["storeId"]
                                        )
                            except:
                                query["storeId"] = child_product_details["storeId"]
                            variant_count_data = db.childProducts.find(query).count()
                            if variant_count_data > 1:
                                variant_count = True
                            else:
                                variant_count = False
                            try:
                                mobile_images = child_product_details["images"][0]
                            except:
                                try:
                                    mobile_images = child_product_details["images"]
                                except:
                                    mobile_images = child_product_details["image"]

                            model_data = []
                            if "modelImage" in child_product_details["units"][0]:
                                if len(child_product_details["units"][0]["modelImage"]) > 0:
                                    model_data = child_product_details["units"][0]["modelImage"]
                                else:
                                    model_data = [
                                        {
                                            "extraLarge": "",
                                            "medium": "",
                                            "altText": "",
                                            "large": "",
                                            "small": "",
                                        }
                                    ]
                            else:
                                model_data = [
                                    {
                                        "extraLarge": "",
                                        "medium": "",
                                        "altText": "",
                                        "large": "",
                                        "small": "",
                                    }
                                ]

                            addition_info = []
                            if "THC" in child_product_details["units"][0]:
                                addition_info.append(
                                    {
                                        "seqId": 2,
                                        "attrname": "THC",
                                        "value": str(child_product_details["units"][0]["THC"])
                                        + " %",
                                        "id": "",
                                    }
                                )
                            else:
                                addition_info.append(
                                    {
                                        "seqId": 2,
                                        "attrname": "THC",
                                        "value": "0 %",
                                        "id": "",
                                    }
                                )

                            if "CBD" in child_product_details["units"][0]:
                                addition_info.append(
                                    {
                                        "seqId": 1,
                                        "attrname": "CBD",
                                        "value": str(child_product_details["units"][0]["CBD"])
                                        + " %",
                                        "id": "",
                                    }
                                )
                            else:
                                addition_info.append(
                                    {
                                        "seqId": 1,
                                        "attrname": "CBD",
                                        "value": "0.02 %",
                                        "id": "",
                                    }
                                )

                            # =================================================canniber product type========================
                            if "cannabisProductType" in child_product_details["units"][0]:
                                if child_product_details["units"][0]["cannabisProductType"] != "":
                                    cannabis_type_details = db.cannabisProductType.find_one(
                                        {
                                            "_id": ObjectId(
                                                child_product_details["units"][0][
                                                    "cannabisProductType"
                                                ]
                                            ),
                                            "status": 1,
                                        }
                                    )
                                    if cannabis_type_details is not None:
                                        addition_info.append(
                                            {
                                                "seqId": 3,
                                                "attrname": "Type",
                                                "value": cannabis_type_details["productType"]["en"],
                                                "id": child_product_details["units"][0][
                                                    "cannabisProductType"
                                                ],
                                            }
                                        )
                                    else:
                                        pass
                            else:
                                addition_info.append(
                                    {
                                        "seqId": 3,
                                        "attrname": "Type",
                                        "value": "Hybrid",
                                        "id": "60c3247a75560000230055cd",
                                    }
                                )
                            if len(addition_info) > 0:
                                additional_info = sorted(
                                    addition_info, key=lambda k: k["seqId"], reverse=True
                                )
                            else:
                                additional_info = []

                            product_data.append(
                                {
                                    "maxQuantity": child_product_details["maxQuantity"]
                                    if "maxQuantity" in child_product_details
                                    else 0,
                                    "isComboProduct": child_product_details["isComboProduct"]
                                    if "isComboProduct" in child_product_details
                                    else False,
                                    "currencyRate": currency_rate,
                                    "productStatus": "",
                                    "hardLimit": hard_limit,
                                    "preOrder": pre_order,
                                    "procurementTime": procurementTime,
                                    "extraAttributeDetails": additional_info,
                                    "childProductId": str(child_product_id),
                                    "tax": tax_value,
                                    "productName": child_product_details["units"][0]["unitName"][
                                        language
                                    ]
                                    if language in child_product_details["units"][0]["unitName"]
                                    else child_product_details["units"][0]["unitName"]["en"],
                                    "parentProductId": child_product_details["parentProductId"],
                                    "storeCategoryId": child_product_details["storeCategoryId"],
                                    "allowOrderOutOfStock": child_product_details[
                                        "allowOrderOutOfStock"
                                    ]
                                    if "allowOrderOutOfStock" in child_product_details
                                    else False,
                                    "prescriptionRequired": prescription_required,
                                    "saleOnline": sales_online,
                                    "uploadProductDetails": upload_details,
                                    "nextSlotTime": next_availbale_driver_time,
                                    "productSeo": product_seo,
                                    "mobimages": mobile_images,
                                    "modelImage": model_data,
                                    "needsIdProof": needsIdProof,
                                    "variantCount": variant_count,
                                    "brandName": child_product_details["brandTitle"][language]
                                    if language in child_product_details["brandTitle"]
                                    else child_product_details["brandTitle"]["en"],
                                    "manufactureName": child_product_details["manufactureName"][
                                        language
                                    ]
                                    if language in child_product_details["manufactureName"]
                                    else "",
                                    "currencySymbol": currency_symbol,
                                    "currency": currency,
                                    "images": child_product_details["images"],
                                    "productTag": "",
                                    "units": child_product_details["units"],
                                    "storeId": str(child_product_details["storeId"]),
                                    "unitId": child_product_details["units"][0]["unitId"],
                                    "offerData": child_product_details["offer"]
                                    if "offer" in child_product_details
                                    else [],
                                    "childProductDetails": child_product_details,
                                    "isDcLinked": is_dc_linked,
                                    "mainProductDetails": main_product_details,
                                }
                            )
                        if len(product_data) > 0:
                            product_dataframe = pd.DataFrame(product_data)
                            product_dataframe = product_dataframe.apply(
                                get_avaialable_quantity,
                                next_availbale_driver_time="",
                                driver_roaster=driver_roaster,
                                zone_id=zone_id,
                                axis=1,
                            )
                            product_dataframe = product_dataframe.apply(
                                best_offer_function_validate,
                                zone_id=zone_id,
                                logintype=login_type,
                                axis=1,
                            )
                            product_dataframe["suppliers"] = product_dataframe.apply(
                                best_supplier_function_cust, axis=1
                            )
                            product_dataframe["productType"] = product_dataframe.apply(
                                product_type_validation, axis=1
                            )
                            product_dataframe["TotalStarRating"] = product_dataframe.apply(
                                cal_star_rating_product, axis=1
                            )
                            product_dataframe["linkedAttribute"] = product_dataframe.apply(
                                linked_unit_attribute, axis=1
                            )
                            product_dataframe["variantData"] = product_dataframe.apply(
                                linked_variant_data, language=language, axis=1
                            )
                            product_dataframe["unitsData"] = product_dataframe.apply(
                                home_units_data,
                                lan=language,
                                sort=0,
                                status=0,
                                axis=1,
                                logintype=login_type,
                                store_category_id=store_category_id,
                                margin_price=True,
                                city_id="",
                            )
                            del product_dataframe["childProductDetails"]
                            del product_dataframe["mainProductDetails"]
                            details = product_dataframe.to_json(orient="records")
                            resData = json.loads(details)
                        else:
                            resData = []
                        categoty_last_data = validate_units_data(resData, False)
                        if len(categoty_last_data) > 0:
                            offer_dataframe = pd.DataFrame(categoty_last_data)
                            offers_data = offer_dataframe.to_dict(orient="records")
                            newcategorylist = sorted(
                                offers_data, key=lambda k: k["availableQuantity"], reverse=True
                            )
                        else:
                            newcategorylist = []
                        if len(newcategorylist) > 0:
                            product_category_details = db.category.find_one(
                                {"_id": ObjectId(parent_product_cat_id)}
                            )
                            if product_category_details is not None:
                                mobile_image = product_category_details["mobileImage"]
                                website_image = product_category_details["websiteImage"]
                            else:
                                mobile_image = ""
                                website_image = ""
                            categoty_data_json.append(
                                {
                                    "websiteImage": website_image,
                                    "mobileImage": mobile_image,
                                    "productData": newcategorylist,
                                    "catName": cat_name,
                                }
                            )
                    else:
                        pass
                if len(categoty_data_json) > 0:
                    categ_data = categoty_data_json
                    response = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "categoryData": categ_data,
                        "offers": [],
                        "type": 7,
                        "seqId": product_seq_id,
                    }
                    last_json_response.append(response)
                else:
                    response = {
                        "id": "",
                        "catName": "",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "categoryData": [],
                        "offers": [],
                        "type": 7,
                        "seqId": product_seq_id,
                    }
                    last_json_response.append(response)
                # ====================================last response==============================================
                newlist = sorted(last_json_response, key=lambda k: k["seqId"])

                if len(sub_category_details) > 0:
                    dataframe_sub_cat = pd.DataFrame(sub_category_details)
                    dataframe_sub_cat = dataframe_sub_cat.drop_duplicates(subset="id", keep="first")
                    sub_cat_details = dataframe_sub_cat.to_json(orient="records")
                    new_sub_category_details = json.loads(sub_cat_details)
                else:
                    new_sub_category_details = []

                # ===========================total count data========================================================
                must_query = [
                    {"match": {"status": 1}},
                    {"match": {"storeCategoryId": str(store_category_id)}},
                ]
                store_data_details = []
                store_data = store_find(
                    {"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id}
                )
                if store_data.count() > 0:
                    for store in store_data:
                        store_data_details.append(str(store["_id"]))
                    must_query.append({"terms": {"units.suppliers.id": store_data_details}})
                else:
                    must_query.append({"match": {"zoneId": zone_id}})

                query = {
                    "query": {"bool": {"must": must_query}},
                    "aggs": {
                        "top_hits": {
                            "terms": {
                                "field": "firstCategoryName.keyword",
                                "size": 200,
                            }
                        }
                    },
                }
                res = product_es_aggrigate_data(query)
                try:
                    total_data_count = len(res["aggregations"]["top_hits"]["buckets"])
                except:
                    total_data_count = 0

                last_response = {
                    "data": {
                        "storeIsOpen": store_is_open,
                        "storeTag": store_tag,
                        "isTempClose": is_temp_close,
                        "list": newlist,
                        "categoryData": new_sub_category_details,
                        "totalCatCount": total_data_count,
                        "totalPage": int(total_data_count / 4),
                    }
                }
                return JsonResponse(last_response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)

class ServiceHomePage(APIView):
    def get(self, request):
        language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
        store_category_id = str(request.META["HTTP_STORECATEGORYID"])
        zone_id = str(request.META["HTTP_ZONEID"]) if "HTTP_ZONEID" in request.META else ""
        token = request.META["HTTP_AUTHORIZATION"]
        timezone_data = request.GET.get("timezone", "")
        if token == "":
            response_data = {
                "message": "unauthorized",
                "totalCount": 0,
                "data": [],
            }
            return JsonResponse(response_data, safe=False, status=401)
        fina_data = {}
        store_details = []
        cat_data = []
        if zone_id != "":
            store_data = db.stores.find(
                {
                    "categoryId": str(store_category_id),
                    "serviceZones.zoneId": zone_id,
                    "status": 1,
                }
            )
            for store in store_data:
                store_details.append(str(store["_id"]))
        else:
            response_data = {"messgae": "zoneId not Found"}
            return JsonResponse(response_data, status=412)
        ''' get all cateogry '''
        get_category = db.category.find({"storeid":{"$in": store_details},"parentId": {"$exists": False}, "storeId": "0", "storeCategory.storeCategoryId": store_category_id, "status": 1})
        if get_category is None:
            cat_data.append({
                "id": "",
                "catName": "",
                "mobileIcon": "",
                "websiteIcon": "",
                "mobileImage": "",
                "websiteImage": "",
                "categoryType": ""

            })
        else:
            for cat in get_category:
                try:
                    cat_data.append(
                        {
                            "id": str(cat['_id']),
                            "catName": cat['categoryName'][language] if language in cat['categoryName'] else cat['categoryName']['en'],
                            "mobileIcon": cat['mobileIcon'],
                            "websiteIcon": cat['mobileIcon'],
                            "mobileImage": cat['mobileIcon'],
                            "websiteImage": cat['mobileIcon']

                        }
                    )
                except Exception as e:
                    print(e)
                    pass
        fina_data['allCatData'] = cat_data
        ''' get sub cat of category '''
        match_query = {"storeid": {"$in":store_details}, "storeId":"0","parentId": {"$exists": False}}
        and_query = [{"$eq": ["$parentId","$$parent_id"]},{"$eq": ["$status",1]}]
        sub_cat = db.category.aggregate(
                    [{"$match": match_query},
                    {"$project": {"_id":1, "categoryName": 1, "storeCategory": 1}},
                    {"$lookup": {
                        "from": "category",
                        "let": {"parent_id": "$_id"},
                        "pipeline": [
                                {"$match": {"$expr": { "$and": and_query }}},
                                {"$project": {
                                    "seqId": 1,
                                    "categoryName": 1,
                                    "mobileImage": 1,
                                    "mobileIcon": 1,
                                    "websiteImage": 1,
                                    "websiteIcon": 1
                                  }},
                                {"$limit": 10}],
                              "as": "subCategories"}
                     }])
        if sub_cat is None:
            fcat_data = []
            fcat_data.append(
                {
                    "id": "",
                    "catName": "",
                    "subCat": []
                }
            )
        else:
            fcat_data = []
            banner_get_id = []
            for fcat in sub_cat:
                banner_get_id = []
                if len(fcat['subCategories']) > 0:
                    main_cat_id = str(fcat['_id'])
                    main_cat_name = fcat['categoryName'][language] if language in fcat['categoryName'] else fcat['categoryName']['en']
                    sub_cat_data = []
                    for scat in fcat['subCategories']:
                        product_count = db.childProducts.find({
                            "categoryList.parentCategory.childCategory.categoryId": str(scat['_id']),
                            "status": 1,
                            "storeId": {"$ne": "0"}
                        }).count()
                        sub_cat_count = db.category.find({"parentId": ObjectId(str(scat['_id']))}).count()
                        if product_count > 0 or sub_cat_count >0:
                            sub_cat_data.append(
                                {
                                    "id": str(scat['_id']),
                                    "websiteIcon": scat['websiteIcon'],
                                    "mobileIcon": scat['mobileIcon'],
                                    "mobileImage": scat['mobileImage'],
                                    "websiteImage": scat['websiteImage'],
                                    "catName": scat['categoryName'][language] if language in scat['categoryName'] else scat['categoryName']['en'],
                                    "subCatcount": sub_cat_count
                                }
                            )
                            banner_get_id.append(str(scat['_id']))
                    banner_get_id.append(main_cat_id)
                    final_banner_id = list(set(banner_get_id))
                    ''' get category banner '''
                    banner_query = {
                        "status": 1,
                        "data.id": {"$in": final_banner_id},
                        "type": 3,
                        "storeCategoryId": store_category_id
                    }
                    if store_category_id != ECOMMERCE_STORE_CATEGORY_ID:
                        banner_query['zones.zoneId'] = zone_id
                    banner_detail = banner_find(banner_query)
                    sub_banner_data = []
                    if banner_detail.count() > 0:
                        for i in banner_detail:
                            sub_category_name = ""
                            sub_sub_category_name = ""
                            sub_sub_category_id = ""
                            category_name = ""
                            base_category_id = ""
                            if int(i["type"]) == 3:
                                base_category = category_find_one(
                                    {"_id": ObjectId(str(i["data"][0]["id"]).replace(" ", ""))}
                                )
                                if base_category is not None:
                                    if "parentId" in base_category:
                                        second_category = category_find_one(
                                            {"_id": ObjectId(base_category["parentId"])}
                                        )
                                        if second_category is not None:
                                            if "parentId" in second_category:
                                                sub_sub_category_name = base_category["categoryName"]["en"]
                                                sub_sub_category_id = str(base_category["_id"])
                                                sub_category_name = second_category["categoryName"]["en"]
                                                sub_category_id = str(second_category["_id"])
                                                sub_sub_category_name = base_category[
                                                    "categoryName"
                                                ]["en"]
                                                sub_category_name = second_category["categoryName"][
                                                    "en"
                                                ]
                                                first_category = category_find_one(
                                                    {"_id": ObjectId(second_category["parentId"])}
                                                )
                                                if first_category is not None:
                                                    category_name = first_category["categoryName"][
                                                        "en"
                                                    ]
                                                    base_category_id = str(first_category["_id"])
                                                else:
                                                    category_name = ""
                                            else:
                                                category_name = second_category["categoryName"][
                                                    "en"
                                                ]
                                                base_category_id = str(second_category["_id"])
                                                sub_category_name = base_category["categoryName"][
                                                    "en"
                                                ]
                                                sub_category_id = str(base_category["_id"])
                                                sub_sub_category_name = ""
                                        else:
                                            first_category = category_find_one(
                                                {
                                                    "_id": ObjectId(
                                                        str(i["data"][0]["id"]).replace(" ", "")
                                                    )
                                                }
                                            )
                                            if first_category is not None:
                                                base_category_id = str(first_category["_id"])
                                                category_name = first_category["categoryName"]["en"]
                                            else:
                                                category_name = ""
                                                sub_category_name = ""
                                                sub_sub_category_name = ""
                                    else:
                                        first_category = category_find_one(
                                            {
                                                "_id": ObjectId(
                                                    str(i["data"][0]["id"]).replace(" ", "")
                                                )
                                            }
                                        )
                                        if first_category is not None:
                                            category_name = first_category["categoryName"]["en"]
                                            base_category_id = str(first_category["_id"])
                                        else:
                                            category_name = ""
                                            sub_category_name = ""
                                            sub_sub_category_name = ""
                                else:
                                    category_name = ""
                                    sub_category_name = ""
                                    sub_sub_category_name = ""
                            if sub_category_name != "" and sub_sub_category_name == "":
                                sub_banner_data.append(
                                    {
                                        "type": 5,
                                        "bannerTypeMsg": "subcategory",
                                        "firstCategoryId": base_category_id,
                                        "secondCategoryId": sub_category_id,
                                        "thirdCategoryId": sub_sub_category_id,
                                        "catName": category_name,
                                        "offerName": "",
                                        "name": i["data"][0]["name"]["en"],
                                        "imageWeb": i["image_web"],
                                        "imageMobile": i["image_mobile"],
                                    }
                                )
                            elif sub_category_name != "" and sub_sub_category_name != "":
                                sub_banner_data.append(
                                    {
                                        "bannerTypeMsg": "subsubcategory",
                                        "offerName": "",
                                        "catName": category_name,
                                        "subCatName": sub_category_name,
                                        "firstCategoryId": base_category_id,
                                        "secondCategoryId": sub_category_id,
                                        "thirdCategoryId": sub_sub_category_id,
                                        "name": i["data"][0]["name"]["en"],
                                        "imageWeb": i["image_web"],
                                        "imageMobile": i["image_mobile"],
                                    }
                                )
                            elif sub_category_name == "" and sub_sub_category_name == "" and category_name != "":
                                sub_banner_data.append(
                                    {
                                        "bannerTypeMsg": "parentCategory",
                                        "offerName": "",
                                        "catName": category_name,
                                        "subCatName": "",
                                        "firstCategoryId": base_category_id,
                                        "secondCategoryId": "",
                                        "thirdCategoryId": "",
                                        "name": i["data"][0]["name"]["en"],
                                        "imageWeb": i["image_web"],
                                        "imageMobile": i["image_mobile"],
                                    }
                                )

                    else:
                        sub_banner_data = []
                    fcat_data.append(
                        {
                            "id": main_cat_id,
                            "catName": main_cat_name,
                            "subCatData": sub_cat_data,
                            "bannerData": sub_banner_data
                        }
                    )

        fina_data['catData'] =  fcat_data

        ''' get all banner detail'''
        try:
            banner_deatils_data = []
            last_json_response = []
            if zone_id == "":
                banner_query = {"status": 1, "storeCategoryId": store_category_id}
            else:
                store_data_details = []
                store_data = store_find(
                    {
                        "categoryId": str(store_category_id),
                        "serviceZones.zoneId": zone_id,
                    }
                )
                if store_data.count() > 0:
                    for store in store_data:
                        store_data_details.append(str(store["_id"]))
                banner_query = {
                    "status": 1,
                    "storeCategoryId": store_category_id,
                    "zones.zoneId": zone_id,
                }
                # banner_query = {"status": 1, "storeCategoryId": store_category_id, "storeId": {"$in": store_data_details}}

            banner_type = ""
            print("banner_query", banner_query)
            banner_details = banner_find(banner_query)
            if banner_details.count() > 0:
                for i in banner_details:
                    base_category_id = ""
                    brand_id = ""
                    sub_category_name = ""
                    sub_category_id = ""
                    sub_sub_category_name = ""
                    sub_sub_category_id = ""
                    # try:
                    category_name = ""
                    sub_category_name = ""
                    sub_sub_category_name = ""
                    if int(i["type"]) == 1:
                        banner_type = "offer"
                    elif int(i["type"]) == 2:
                        banner_type = "brands"
                    elif int(i["type"]) == 3:
                        banner_type = "category"
                    elif int(i["type"]) == 4:
                        banner_type = "stores"
                    elif int(i["type"]) == 5:
                        banner_type = "subcategory"
                    elif int(i["type"]) == 6:
                        banner_type = "subsubcategory"
                    elif int(i["type"]) == 7:
                        banner_type = "supplier"
                    elif int(i["type"]) == 8:
                        banner_type = "products"
                    elif int(i["type"]) == 9:
                        banner_type = "url"
                    if int(i["type"]) == 3:

                        base_category = category_find_one(
                            {"_id": ObjectId(str(i["data"][0]["id"]).replace(" ", ""))}
                        )
                        if base_category is not None:
                            if "parentId" in base_category:
                                second_category = category_find_one(
                                    {"_id": ObjectId(base_category["parentId"])}
                                )
                                if second_category is not None:
                                    if "parentId" in second_category:
                                        sub_sub_category_name = base_category["categoryName"]["en"]
                                        sub_sub_category_id = str(base_category["_id"])
                                        sub_category_name = second_category["categoryName"]["en"]
                                        sub_category_id = str(second_category["_id"])
                                        sub_sub_category_name = base_category[
                                            "categoryName"
                                        ]["en"]
                                        sub_category_name = second_category["categoryName"][
                                            "en"
                                        ]
                                        first_category = category_find_one(
                                            {"_id": ObjectId(second_category["parentId"])}
                                        )
                                        if first_category is not None:
                                            category_name = first_category["categoryName"][
                                                "en"
                                            ]
                                            base_category_id = str(first_category["_id"])
                                        else:
                                            category_name = ""
                                    else:
                                        category_name = second_category["categoryName"][
                                            "en"
                                        ]
                                        base_category_id = str(second_category["_id"])
                                        sub_category_name = base_category["categoryName"][
                                            "en"
                                        ]
                                        sub_category_id = str(base_category["_id"])
                                        sub_sub_category_name = ""
                                else:
                                    first_category = category_find_one(
                                        {
                                            "_id": ObjectId(
                                                str(i["data"][0]["id"]).replace(" ", "")
                                            )
                                        }
                                    )
                                    if first_category is not None:
                                        base_category_id = str(first_category["_id"])
                                        category_name = first_category["categoryName"]["en"]
                                    else:
                                        category_name = ""
                                        sub_category_name = ""
                                        sub_sub_category_name = ""
                            else:
                                first_category = category_find_one(
                                    {
                                        "_id": ObjectId(
                                            str(i["data"][0]["id"]).replace(" ", "")
                                        )
                                    }
                                )
                                if first_category is not None:
                                    category_name = first_category["categoryName"]["en"]
                                    base_category_id = str(first_category["_id"])
                                else:
                                    category_name = ""
                                    sub_category_name = ""
                                    sub_sub_category_name = ""
                        else:
                            category_name = ""
                            sub_category_name = ""
                            sub_sub_category_name = ""

                    if sub_category_name != "" and sub_sub_category_name == "":
                        banner_deatils_data.append(
                            {
                                "type": 5,
                                "bannerTypeMsg": "subcategory",
                                "firstCategoryId": base_category_id,
                                "secondCategoryId": sub_category_id,
                                "thirdCategoryId": sub_sub_category_id,
                                "catName": category_name,
                                "offerName": "",
                                "name": i["data"][0]["name"]["en"],
                                "imageWeb": i["image_web"],
                                "imageMobile": i["image_mobile"],
                            }
                        )
                    elif sub_category_name != "" and sub_sub_category_name != "":
                        banner_deatils_data.append(
                            {
                                "bannerTypeMsg": "subsubcategory",
                                "offerName": "",
                                "catName": category_name,
                                "subCatName": sub_category_name,
                                "firstCategoryId": base_category_id,
                                    "secondCategoryId": sub_category_id,
                                "thirdCategoryId": sub_sub_category_id,
                                "name": i["data"][0]["name"]["en"],
                                "imageWeb": i["image_web"],
                                "imageMobile": i["image_mobile"],
                            }
                        )
                    elif int(i["type"]) == 1:
                        offer_id = ""
                        for o_id in i["data"]:
                            offer_inner = offer_find_one(
                                {"_id": ObjectId(o_id["id"]), "status": 1}
                            )
                            if offer_inner is not None:
                                if offer_id != "":
                                    offer_id = offer_id + "," + o_id["id"]
                                else:
                                    offer_id = o_id["id"]
                            else:
                                pass
                        offer_query = {"_id": ObjectId(i["data"][0]["id"]), "status": 1}
                        offer_details = offer_find_one(offer_query)
                        if offer_details is not None:
                            product_query = {
                                "offer.status": 1,
                                "offer.offerId": str(offer_details["_id"]),
                                "status": 1,
                            }
                            child_product_count = product_find_count(product_query)
                            if child_product_count > 0:
                                banner_deatils_data.append(
                                    {
                                        "type": i["type"],
                                        "bannerTypeMsg": banner_type,
                                        "catName": "",
                                        "subCatName": "",
                                        "firstCategoryId": "",
                                        "secondCategoryId": "",
                                        "thirdCategoryId": "",
                                        "offerName": offer_details["name"]["en"],
                                        "name": str(offer_id),
                                        "imageWeb": i["image_web"],
                                        "imageMobile": i["image_mobile"],
                                    }
                                )
                    elif int(i["type"]) == 8:
                        product_details = db.products.find_one(
                            {"_id": ObjectId(i["data"][0]["id"]), "status": 1}, {"units": 1}
                        )
                        if product_details is not None:
                            supplier_list = []
                            if "suppliers" in product_details["units"][0]:
                                for s in product_details["units"][0]["suppliers"]:
                                    if s["id"] != "0":
                                        child_product_count = db.childProducts.find(
                                            {"_id": ObjectId(s["productId"]), "status": 1}
                                        ).count()
                                        if child_product_count > 0:
                                            supplier_list.append(s)
                                    else:
                                        pass
                            if len(supplier_list) > 0:
                                best_supplier = min(
                                    supplier_list, key=lambda x: x["retailerPrice"]
                                )
                                if best_supplier["retailerQty"] == 0:
                                    best_supplier = max(
                                        supplier_list, key=lambda x: x["retailerQty"]
                                    )
                                else:
                                    best_supplier = best_supplier
                                if len(best_supplier) > 0:
                                    central_product_id = str(product_details["_id"])
                                    child_product_id = str(best_supplier["productId"])
                                    banner_deatils_data.append(
                                        {
                                            "type": i["type"],
                                            "bannerTypeMsg": banner_type,
                                            "firstCategoryId": "",
                                            "secondCategoryId": "",
                                            "thirdCategoryId": "",
                                            "catName": "",
                                            "parentProductId": central_product_id,
                                            "childProductId": child_product_id,
                                            "subCatName": "",
                                            "offerName": "/python/product/details?&parentProductId="
                                                         + central_product_id
                                                         + "&productId="
                                                         + child_product_id,
                                            "name": "/python/product/details?&parentProductId="
                                                    + central_product_id
                                                    + "&productId="
                                                    + child_product_id,
                                            "imageWeb": i["image_web"],
                                            "imageMobile": i["image_mobile"],
                                        }
                                    )
                    elif int(i["type"]) == 9:
                        banner_deatils_data.append(
                            {
                                "type": i["type"],
                                "bannerTypeMsg": banner_type,
                                "firstCategoryId": base_category_id,
                                "secondCategoryId": sub_category_id,
                                "thirdCategoryId": sub_sub_category_id,
                                "catName": "",
                                "subCatName": "",
                                "offerName": i["data"][0]["name"]["en"],
                                "name": i["data"][0]["name"]["en"],
                                "imageWeb": i["image_web"],
                                "imageMobile": i["image_mobile"],
                            }
                        )
                    else:
                        try:
                            store_details_name = json.loads(i["data"][0]["name"])
                            id = json.loads(i["data"][0]["id"])
                        except:
                            try:
                                store_details_name = i["data"][0]["name"]
                                id = i["data"][0]["id"]
                            except:
                                store_details_name = {"en": ""}
                                id = ""
                        try:
                            if store_details_name["en"] != "":
                                banner_deatils_data.append(
                                    {
                                        "type": i["type"],
                                        "bannerTypeMsg": banner_type,
                                        "offerName": "",
                                        "id": id,
                                        "firstCategoryId": base_category_id,
                                        "secondCategoryId": sub_category_id,
                                        "thirdCategoryId": sub_sub_category_id,
                                        "name": store_details_name["en"],
                                        "imageWeb": i["image_web"],
                                        "imageMobile": i["image_mobile"],
                                    }
                                )
                        except:
                            pass
                # except:
                #     pass
                response = {
                    "id": "",
                    "catName": "",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "penCount": 0,
                    "offers": [],
                    "type": 1,
                    "categoryData": banner_deatils_data,
                }
                last_json_response.append(response)
            else:
                response = {
                    "id": "",
                    "catName": "",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "offers": [],
                    "penCount": 0,
                    "type": 1,
                    "categoryData": [],
                }
                last_json_response.append(response)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(
                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                type(ex).__name__,
                ex,
            )
            error = {
                "id": "",
                "catName": "",
                "imageUrl": "",
                "bannerImageUrl": "",
                "websiteImageUrl": "",
                "websiteBannerImageUrl": "",
                "offers": [],
                "penCount": 0,
                "seqId": 1,
                "type": 1,
                "categoryData": [],
            }
        fina_data['bannerData'] = banner_deatils_data
        response_data = {"data": fina_data, "message": "Data found"}
        return JsonResponse(response_data, status=200)

class RicevanStores(APIView):
    def get(self, request):
        token = request.META["HTTP_AUTHORIZATION"]
        language = request.GET.get('language', "en")
        sort_data = request.GET.get('sort', 1)
        lat = float(request.GET.get('lat', 13.0287))
        long = float(request.GET.get('long', 77.58958))
        if token == "":
            response_data = {
                "message": "unauthorized",
                "totalCount": 0,
                "data": [],
            }
            return JsonResponse(response_data, safe=False, status=401)
        try:
            user_id = json.loads(token)["userId"]
            if user_id == "":
                response_data = {'message': 'user not found'}
                return JsonResponse(response_data, status=404)
        except Exception as e:
            print(e)
            user_id = "63dd09a0944c18faf866e0f8"
        print(user_id)
        condition = {
            "status": 1,
            "storeId": 0,
            "polygons": {
                "$geoIntersects": {
                    "$geometry": {"type": "Point", "coordinates": [float(long), float(lat)]}
                }
            },
        }
        zone_details = db.zones.find_one(
            condition,
            {
                "_id": 1
            },
            )
        customer_store = db.customer.find_one({"_id": ObjectId(user_id)})
        if 'linkedSellers' in customer_store:
            if len(customer_store['linkedSellers']) == 0:
                response_data = {'message': 'store is not found'}
                return JsonResponse(response_data, status=404)
            else:
                store_details = []
                for s_id in customer_store['linkedSellers']:
                    store_details.append(s_id['storeId'])
                if len(store_details) != 0:
                    store_query = {
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "match": {
                                            "status": 1
                                        }
                                    },
                                    {
                                        "match": {
                                            "serviceZones.zoneId": str(zone_details['_id'])
                                        }
                                    },
                                    {
                                        "terms": {
                                            "_id": store_details
                                        }
                                    }
                                ]
                            }
                        },
                        "size": 500,
                        "from": 0,
                        "sort": [
                            {
                                "storeIsOpen": {
                                    "order": "desc"
                                }
                            },
                            {
                                "_geo_distance": {
                                    "distance_type": "plane",
                                    "location": {
                                        "lat": lat,
                                        "lon": long
                                    },
                                    "order": "asc",
                                    "unit": "km"
                                }
                            }
                        ]
                    }
                    print(store_query)
                    store_details = es.search(index=index_store, body=store_query, scroll="1m")
                    if len(store_details['hits']['hits']) != 0:
                        open_store = []
                        close_store = []
                        temporary_close = []
                        fav_open = []
                        fav_tem_close = []
                        fav_close = []
                        cusine_name = ""
                        final_response = {"message": "data found"}
                        for store in store_details['hits']['hits']:
                            if int(sort_data) == 0 or int(sort_data) == 1:
                                distance_km = round(store['sort'][1], 2)
                                distance_miles = round(distance_km * conv_fac, 2)
                            else:
                                try:
                                    distance_km = round(store['sort'][2], 2)
                                    distance_miles = round(distance_km * conv_fac, 2)
                                except:
                                    try:
                                        distance_km = round(store['sort'][1], 2)
                                        distance_miles = round(distance_km * conv_fac, 2)
                                    except:
                                        distance_km = round(store['sort'][0], 2)
                                        distance_miles = round(distance_km * conv_fac, 2)
                            print('distance_km--',distance_km)
                            specialities_data = []
                            if "specialities" in store["_source"]:
                                if len(store["_source"]['specialities']):
                                    for spec in store["_source"]['specialities']:
                                        spec_data = DbHelper.get_single_speciality({"_id": ObjectId(spec)},
                                                                                   {"specialityName": 1, "image": 1})
                                        if spec_data != None:
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
                            offer_details = DbHelper.get_offer_details(str(store['_id']))
                            offer_json = []
                            for offer in offer_details:
                                offer_json.append({
                                    "offerName": offer['name'][language],
                                    "offerId": str(offer['_id']),
                                    "offerType": offer['offerType'],
                                    "discountValue": offer['discountValue']
                                })

                            if len(offer_json) > 0:
                                best_offer_store = max(offer_json, key=lambda x: x['discountValue'])
                                if best_offer_store['offerType'] == 1:
                                    percentage_text = str(best_offer_store['discountValue']) + "%" + " " + "off"
                                else:
                                    try:
                                        percentage_text = seller['_source']['currencySymbol'] + str(
                                            best_offer_store['discountValue']) + " off"
                                    except:
                                        percentage_text = "₹" + str(best_offer_store['discountValue']) + " off"
                                offer_name = best_offer_store['offerName']
                            else:
                                offer_name = ""
                                percentage_text = ""
                            if "shopifyStoreDetails" in store['_source']:
                                if "enable" in store["_source"]["shopifyStoreDetails"]:
                                    shopify_enable = store["_source"]["shopifyStoreDetails"]['enable']
                                else:
                                    shopify_enable = False
                            else:
                                shopify_enable = False
                            spe_data = []
                            spe_name = []
                            try:
                                payment_methode = db.cities.find_one({"_id": ObjectId(store['cityId'])})
                                if payment_methode is None:
                                    payment_methode = {}
                            except:
                                payment_methode = []
                            if "favouriteUsers" in store and user_id in store['favouriteUsers']:
                                if user_id != "":
                                    if store['storeIsOpen'] == True:
                                        ''' get store data'''
                                        for spe in list(set(store['specialities'])):
                                            spe_data.append(ObjectId(spe))
                                        specialities = db.specialities.find({"_id": {"$in": spe_data}})
                                        for s_data in specialities:
                                            spe_name.append(s_data['specialityName'][language])
                                        contact_phone = store['contactPhone']['countryCode'] + " " + \
                                                        store['contactPhone']['number']
                                        fav_open.append({
                                            "id": str(store['_id']),
                                            "safetyStandardsDynamicContent": store["_source"][
                                                'safetyStandardsDynamicContent'],
                                            "storeName": store["_source"]['storeName'][language] if language in
                                                                                                    store["_source"][
                                                                                                        'storeName'] else
                                            store["_source"]['storeName']["en"],
                                            "safetyStandards": store["_source"]['safetyStandards'],
                                            "avgRating": round(store["_source"]['avgRating'], 2) if "avgRating" in
                                                                                                    store[
                                                                                                        '_source'] else 0,
                                            "averageCostForMealForTwo": store["_source"][
                                                'averageCostForMealForTwo'] if 'averageCostForMealForTwo' in store[
                                                "_source"] else "",
                                            "shopifyId": store['_source']['shopifyId'] if 'shopifyId' in store[
                                                '_source'] else "",
                                            "currencyCode": store["_source"]['currencyCode'] if "currencyCode" in store[
                                                "_source"] else "INR",
                                            "logoImages": store["_source"]['logoImages'],
                                            "freeDeliveryAbove": store["_source"][
                                                'freeDeliveryAbove'] if "freeDeliveryAbove" in store["_source"] else 0,
                                            "averageDeliveryTime": str(store["_source"][
                                                                           'averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in
                                                                                                                           store[
                                                                                                                               "_source"] else "",
                                            "currencySymbol": store["_source"][
                                                'currencySymbol'] if "currencySymbol" in store[
                                                "_source"] else "INR",
                                            "currency": store["_source"]['currencyCode'] if "currencyCode" in store[
                                                "_source"] else "INR",
                                            "isExpressDelivery": store["_source"][
                                                'isExpressDelivery'] if "isExpressDelivery" in store else 0,
                                            "specialities": spe_name,
                                            "businessLocationAddress": store["_source"][
                                                'businessLocationAddress'] if "businessLocationAddress" in store else {},
                                            "billingAddress": store["_source"][
                                                'billingAddress'] if 'billingAddress' in store else {},
                                            "headOffice": store["_source"][
                                                'headOffice'] if 'headOffice' in store else {},
                                            "paymentMethods": {
                                                "cashOnPickUp": store["_source"]['cashOnPickUp'] if "cashOnPickUp" in
                                                                                                    store[
                                                                                                        "_source"] else False,
                                                "pickUpPrePaymentCard": store["_source"][
                                                    'pickUpPrePaymentCard'] if "pickUpPrePaymentCard" in store[
                                                    "_source"] else False,
                                                "cashOnDelivery": store["_source"][
                                                    'cashOnDelivery'] if "cashOnDelivery" in store[
                                                    "_source"] else False,
                                                "deliveryPrePaymentCard": store["_source"][
                                                    'deliveryPrePaymentCard'] if "deliveryPrePaymentCard" in store[
                                                    "_source"] else False,
                                                "cardOnDelivery": store["_source"][
                                                    'cardOnDelivery'] if "cardOnDelivery" in store[
                                                    "_source"] else False,
                                                "acceptsCashOnDelivery": store["_source"][
                                                    'acceptsCashOnDelivery'] if "acceptsCashOnDelivery" in store[
                                                    "_source"] else False,
                                                "acceptsCard": store["_source"]['acceptsCard'] if "acceptsCard" in
                                                                                                  store[
                                                                                                      "_source"] else False,
                                                "acceptsWallet": store["_source"]['acceptsWallet'] if "acceptsWallet" in
                                                                                                      store[
                                                                                                          "_source"] else False,
                                            },
                                            "galleryImages": store["_source"]['galleryImages'],
                                            "minimumOrderValue": str(store["_source"]['minimumOrder']) if
                                            store["_source"]['minimumOrder'] != 0 else "No Minimum",
                                            "cityId": store["_source"]['cityId'],
                                            "citiesOfOperation": store["_source"]['citiesOfOperation'],
                                            "isExpressDelivery": int(
                                                store["_source"]['isExpressDelivery']) if "isExpressDelivery" in store[
                                                "_source"] else 0,
                                            "parentstoreIdOrSupplierId": store["_source"][
                                                'parentstoreIdOrSupplierId'] if "parentstoreIdOrSupplierId" in store[
                                                '_source'] else "",
                                            "storeName": store["_source"]['storeName'][language] if language in
                                                                                                    store["_source"][
                                                                                                        'storeName'] else
                                            store["_source"]['storeName']['en'],
                                            "address": store["_source"]['headOffice'][
                                                'headOfficeAddress'] if "headOfficeAddress" in store["_source"][
                                                'headOffice'] else "",
                                            "storeTypeId": store["_source"]['storeTypeId'],
                                            "supportedOrderTypes": store["_source"][
                                                'supportedOrderTypes'] if "supportedOrderTypes" in store[
                                                '_source'] else 3,
                                            "storeType": store["_source"]['storeType'],
                                            "averageDeliveryTimeInMins": store["_source"][
                                                'averageDeliveryTimeInMins'] if "averageDeliveryTimeInMins" in store[
                                                '_source'] else 0,
                                            "storeFrontTypeId": store["_source"]['storeFrontTypeId'],
                                            "storeFrontType": store["_source"]['storeFrontType'],
                                            "driverTypeId": store["_source"]['driverTypeId'] if "driverTypeId" in store[
                                                "_source"] else 0,
                                            "driverType": store["_source"]['driverType'] if "driverType" in store[
                                                "_source"] else 0,
                                            "phoneNumber": contact_phone,
                                            "cuisines": cusine_name,
                                            "distance": distance_km,
                                            "distanceMiles": distance_miles,
                                            "offerName": offer_name,
                                            "percentageText": percentage_text,
                                            "nextCloseTime": store["_source"]['nextCloseTime'] if "nextCloseTime" in
                                                                                                  store[
                                                                                                      '_source'] else "",
                                            "nextOpenTime": store["_source"]['nextOpenTime'] if "nextOpenTime" in store[
                                                '_source'] else "",
                                            "status": store["_source"]['status'],
                                            "shopifyEnable": shopify_enable,
                                            "address": store["_source"]['businessLocationAddress'][
                                                'address'] if "address" in store["_source"][
                                                'businessLocationAddress'] else "",
                                            "locality": store["_source"]['businessLocationAddress'][
                                                'locality'] if "locality" in store["_source"][
                                                'businessLocationAddress'] else "",
                                            "postCode": store["_source"]['businessLocationAddress'][
                                                'postCode'] if "postCode" in store["_source"][
                                                'businessLocationAddress'] else "",
                                            "addressArea": store["_source"]['businessLocationAddress'][
                                                'addressArea'] if "addressArea" in store["_source"][
                                                'businessLocationAddress'] else "",
                                            "state": store["_source"]['businessLocationAddress']['state'] if "state" in
                                                                                                             store[
                                                                                                                 "_source"][
                                                                                                                 'businessLocationAddress'] else "",
                                            "country": store["_source"]['businessLocationAddress'][
                                                'country'] if "country" in store["_source"][
                                                'businessLocationAddress'] else "",
                                            "city": store["_source"]['businessLocationAddress']['city'] if "city" in
                                                                                                           store[
                                                                                                               "_source"][
                                                                                                               'businessLocationAddress'] else ""
                                        })
                                    else:
                                        ''' closed store '''
                                        if store['nextCloseTime'] == "" and store['nextOpenTime'] == "":
                                            ''' temporory close '''
                                            for spe in list(set(store['specialities'])):
                                                spe_data.append(ObjectId(spe))
                                            specialities = db.specialities.find({"_id": {"$in": spe_data}})
                                            for s_data in specialities:
                                                spe_name.append(s_data['specialityName'][language])
                                            contact_phone = store['contactPhone']['countryCode'] + " " + \
                                                            store['contactPhone'][
                                                                'number']

                                            fav_tem_close.append({
                                                "id": str(store['_id']),
                                                "safetyStandardsDynamicContent": store["_source"][
                                                    'safetyStandardsDynamicContent'],
                                                "storeName": store["_source"]['storeName'][language] if language in
                                                                                                        store[
                                                                                                            "_source"][
                                                                                                            'storeName'] else
                                                store["_source"]['storeName']["en"],
                                                "safetyStandards": store["_source"]['safetyStandards'],
                                                "avgRating": round(store["_source"]['avgRating'], 2) if "avgRating" in
                                                                                                        store[
                                                                                                            '_source'] else 0,
                                                "averageCostForMealForTwo": store["_source"][
                                                    'averageCostForMealForTwo'] if 'averageCostForMealForTwo' in store[
                                                    "_source"] else "",
                                                "shopifyId": store['_source']['shopifyId'] if 'shopifyId' in store[
                                                    '_source'] else "",
                                                "currencyCode": store["_source"]['currencyCode'] if "currencyCode" in
                                                                                                    store[
                                                                                                        "_source"] else "INR",
                                                "logoImages": store["_source"]['logoImages'],
                                                "freeDeliveryAbove": store["_source"][
                                                    'freeDeliveryAbove'] if "freeDeliveryAbove" in store[
                                                    "_source"] else 0,
                                                "averageDeliveryTime": str(store["_source"][
                                                                               'averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in
                                                                                                                               store[
                                                                                                                                   "_source"] else "",
                                                "currencySymbol": store["_source"][
                                                    'currencySymbol'] if "currencySymbol" in store[
                                                    "_source"] else "INR",
                                                "currency": store["_source"]['currencyCode'] if "currencyCode" in store[
                                                    "_source"] else "INR",
                                                "isExpressDelivery": store["_source"][
                                                    'isExpressDelivery'] if "isExpressDelivery" in store else 0,
                                                "specialities": spe_name,
                                                "businessLocationAddress": store["_source"][
                                                    'businessLocationAddress'] if "businessLocationAddress" in store else {},
                                                "billingAddress": store["_source"][
                                                    'billingAddress'] if 'billingAddress' in store else {},
                                                "headOffice": store["_source"][
                                                    'headOffice'] if 'headOffice' in store else {},
                                                "paymentMethods": {
                                                    "cashOnPickUp": store["_source"][
                                                        'cashOnPickUp'] if "cashOnPickUp" in store[
                                                        "_source"] else False,
                                                    "pickUpPrePaymentCard": store["_source"][
                                                        'pickUpPrePaymentCard'] if "pickUpPrePaymentCard" in store[
                                                        "_source"] else False,
                                                    "cashOnDelivery": store["_source"][
                                                        'cashOnDelivery'] if "cashOnDelivery" in store[
                                                        "_source"] else False,
                                                    "deliveryPrePaymentCard": store["_source"][
                                                        'deliveryPrePaymentCard'] if "deliveryPrePaymentCard" in store[
                                                        "_source"] else False,
                                                    "cardOnDelivery": store["_source"][
                                                        'cardOnDelivery'] if "cardOnDelivery" in store[
                                                        "_source"] else False,
                                                    "acceptsCashOnDelivery": store["_source"][
                                                        'acceptsCashOnDelivery'] if "acceptsCashOnDelivery" in store[
                                                        "_source"] else False,
                                                    "acceptsCard": store["_source"]['acceptsCard'] if "acceptsCard" in
                                                                                                      store[
                                                                                                          "_source"] else False,
                                                    "acceptsWallet": store["_source"][
                                                        'acceptsWallet'] if "acceptsWallet" in store[
                                                        "_source"] else False,
                                                },
                                                "galleryImages": store["_source"]['galleryImages'],
                                                "minimumOrderValue": str(store["_source"]['minimumOrder']) if
                                                store["_source"]['minimumOrder'] != 0 else "No Minimum",
                                                "cityId": store["_source"]['cityId'],
                                                "citiesOfOperation": store["_source"]['citiesOfOperation'],
                                                "isExpressDelivery": int(
                                                    store["_source"]['isExpressDelivery']) if "isExpressDelivery" in
                                                                                              store["_source"] else 0,
                                                "parentstoreIdOrSupplierId": store["_source"][
                                                    'parentstoreIdOrSupplierId'] if "parentstoreIdOrSupplierId" in
                                                                                    store['_source'] else "",
                                                "storeName": store["_source"]['storeName'][language] if language in
                                                                                                        store[
                                                                                                            "_source"][
                                                                                                            'storeName'] else
                                                store["_source"]['storeName']['en'],
                                                "address": store["_source"]['headOffice'][
                                                    'headOfficeAddress'] if "headOfficeAddress" in store["_source"][
                                                    'headOffice'] else "",
                                                "storeTypeId": store["_source"]['storeTypeId'],
                                                "supportedOrderTypes": store["_source"][
                                                    'supportedOrderTypes'] if "supportedOrderTypes" in store[
                                                    '_source'] else 3,
                                                "storeType": store["_source"]['storeType'],
                                                "averageDeliveryTimeInMins": store["_source"][
                                                    'averageDeliveryTimeInMins'] if "averageDeliveryTimeInMins" in
                                                                                    store['_source'] else 0,
                                                "storeFrontTypeId": store["_source"]['storeFrontTypeId'],
                                                "storeFrontType": store["_source"]['storeFrontType'],
                                                "driverTypeId": store["_source"]['driverTypeId'] if "driverTypeId" in
                                                                                                    store[
                                                                                                        "_source"] else 0,
                                                "driverType": store["_source"]['driverType'] if "driverType" in store[
                                                    "_source"] else 0,
                                                "phoneNumber": contact_phone,
                                                "cuisines": cusine_name,
                                                "distance": distance_km,
                                                "distanceMiles": distance_miles,
                                                "offerName": offer_name,
                                                "percentageText": percentage_text,
                                                "nextCloseTime": store["_source"]['nextCloseTime'] if "nextCloseTime" in
                                                                                                      store[
                                                                                                          '_source'] else "",
                                                "nextOpenTime": store["_source"]['nextOpenTime'] if "nextOpenTime" in
                                                                                                    store[
                                                                                                        '_source'] else "",
                                                "status": store["_source"]['status'],
                                                "shopifyEnable": shopify_enable,
                                                "address": store["_source"]['businessLocationAddress'][
                                                    'address'] if "address" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "locality": store["_source"]['businessLocationAddress'][
                                                    'locality'] if "locality" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "postCode": store["_source"]['businessLocationAddress'][
                                                    'postCode'] if "postCode" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "addressArea": store["_source"]['businessLocationAddress'][
                                                    'addressArea'] if "addressArea" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "state": store["_source"]['businessLocationAddress'][
                                                    'state'] if "state" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "country": store["_source"]['businessLocationAddress'][
                                                    'country'] if "country" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "city": store["_source"]['businessLocationAddress']['city'] if "city" in
                                                                                                               store[
                                                                                                                   "_source"][
                                                                                                                   'businessLocationAddress'] else ""
                                            })
                                        elif store['nextOpenTime'] != "":
                                            for spe in list(set(store['specialities'])):
                                                spe_data.append(ObjectId(spe))
                                            specialities = db.specialities.find({"_id": {"$in": spe_data}})
                                            for s_data in specialities:
                                                spe_name.append(s_data['specialityName'][language])
                                            contact_phone = store['contactPhone']['countryCode'] + " " + \
                                                            store['contactPhone'][
                                                                'number']
                                            timeZoneWorkingHour = store['timeZoneWorkingHour']
                                            next_open_time = store['nextOpenTime']
                                            next_open_time = time_zone_converter(timezone, next_open_time,
                                                                                 timeZoneWorkingHour)
                                            local_time = datetime.datetime.fromtimestamp(next_open_time)
                                            next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                                            next_day_midnight = next_day.replace(hour=0, minute=0, second=0)
                                            next_day_midnight_timestamp = int(next_day_midnight.timestamp())
                                            store_tag = ""
                                            if next_day_midnight_timestamp > next_open_time:
                                                open_time = local_time.strftime("%I:%M %p")
                                                store_tag = "Opens Next At " + open_time
                                            else:
                                                open_time = local_time.strftime("%I:%M %p")
                                                store_tag = "Opens Tomorrow At " + open_time
                                            fav_close.append({
                                                "id": str(store['_id']),
                                                "safetyStandardsDynamicContent": store["_source"][
                                                    'safetyStandardsDynamicContent'],
                                                "storeName": store["_source"]['storeName'][language] if language in
                                                                                                        store[
                                                                                                            "_source"][
                                                                                                            'storeName'] else
                                                store["_source"]['storeName']["en"],
                                                "safetyStandards": store["_source"]['safetyStandards'],
                                                "avgRating": round(store["_source"]['avgRating'], 2) if "avgRating" in
                                                                                                        store[
                                                                                                            '_source'] else 0,
                                                "averageCostForMealForTwo": store["_source"][
                                                    'averageCostForMealForTwo'] if 'averageCostForMealForTwo' in store[
                                                    "_source"] else "",
                                                "shopifyId": store['_source']['shopifyId'] if 'shopifyId' in store[
                                                    '_source'] else "",
                                                "currencyCode": store["_source"]['currencyCode'] if "currencyCode" in
                                                                                                    store[
                                                                                                        "_source"] else "INR",
                                                "logoImages": store["_source"]['logoImages'],
                                                "freeDeliveryAbove": store["_source"][
                                                    'freeDeliveryAbove'] if "freeDeliveryAbove" in store[
                                                    "_source"] else 0,
                                                "averageDeliveryTime": str(store["_source"][
                                                                               'averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in
                                                                                                                               store[
                                                                                                                                   "_source"] else "",
                                                "currencySymbol": store["_source"][
                                                    'currencySymbol'] if "currencySymbol" in store[
                                                    "_source"] else "INR",
                                                "currency": store["_source"]['currencyCode'] if "currencyCode" in store[
                                                    "_source"] else "INR",
                                                "isExpressDelivery": store["_source"][
                                                    'isExpressDelivery'] if "isExpressDelivery" in store else 0,
                                                "specialities": spe_name,
                                                "businessLocationAddress": store["_source"][
                                                    'businessLocationAddress'] if "businessLocationAddress" in store else {},
                                                "billingAddress": store["_source"][
                                                    'billingAddress'] if 'billingAddress' in store else {},
                                                "headOffice": store["_source"][
                                                    'headOffice'] if 'headOffice' in store else {},
                                                "paymentMethods": {
                                                    "cashOnPickUp": store["_source"][
                                                        'cashOnPickUp'] if "cashOnPickUp" in store[
                                                        "_source"] else False,
                                                    "pickUpPrePaymentCard": store["_source"][
                                                        'pickUpPrePaymentCard'] if "pickUpPrePaymentCard" in store[
                                                        "_source"] else False,
                                                    "cashOnDelivery": store["_source"][
                                                        'cashOnDelivery'] if "cashOnDelivery" in store[
                                                        "_source"] else False,
                                                    "deliveryPrePaymentCard": store["_source"][
                                                        'deliveryPrePaymentCard'] if "deliveryPrePaymentCard" in store[
                                                        "_source"] else False,
                                                    "cardOnDelivery": store["_source"][
                                                        'cardOnDelivery'] if "cardOnDelivery" in store[
                                                        "_source"] else False,
                                                    "acceptsCashOnDelivery": store["_source"][
                                                        'acceptsCashOnDelivery'] if "acceptsCashOnDelivery" in store[
                                                        "_source"] else False,
                                                    "acceptsCard": store["_source"]['acceptsCard'] if "acceptsCard" in
                                                                                                      store[
                                                                                                          "_source"] else False,
                                                    "acceptsWallet": store["_source"][
                                                        'acceptsWallet'] if "acceptsWallet" in store[
                                                        "_source"] else False,
                                                },
                                                "galleryImages": store["_source"]['galleryImages'],
                                                "minimumOrderValue": str(store["_source"]['minimumOrder']) if
                                                store["_source"]['minimumOrder'] != 0 else "No Minimum",
                                                "cityId": store["_source"]['cityId'],
                                                "citiesOfOperation": store["_source"]['citiesOfOperation'],
                                                "isExpressDelivery": int(
                                                    store["_source"]['isExpressDelivery']) if "isExpressDelivery" in
                                                                                              store["_source"] else 0,
                                                "parentstoreIdOrSupplierId": store["_source"][
                                                    'parentstoreIdOrSupplierId'] if "parentstoreIdOrSupplierId" in
                                                                                    store['_source'] else "",
                                                "storeName": store["_source"]['storeName'][language] if language in
                                                                                                        store[
                                                                                                            "_source"][
                                                                                                            'storeName'] else
                                                store["_source"]['storeName']['en'],
                                                "address": store["_source"]['headOffice'][
                                                    'headOfficeAddress'] if "headOfficeAddress" in store["_source"][
                                                    'headOffice'] else "",
                                                "storeTypeId": store["_source"]['storeTypeId'],
                                                "supportedOrderTypes": store["_source"][
                                                    'supportedOrderTypes'] if "supportedOrderTypes" in store[
                                                    '_source'] else 3,
                                                "storeType": store["_source"]['storeType'],
                                                "averageDeliveryTimeInMins": store["_source"][
                                                    'averageDeliveryTimeInMins'] if "averageDeliveryTimeInMins" in
                                                                                    store['_source'] else 0,
                                                "storeFrontTypeId": store["_source"]['storeFrontTypeId'],
                                                "storeFrontType": store["_source"]['storeFrontType'],
                                                "driverTypeId": store["_source"]['driverTypeId'] if "driverTypeId" in
                                                                                                    store[
                                                                                                        "_source"] else 0,
                                                "driverType": store["_source"]['driverType'] if "driverType" in store[
                                                    "_source"] else 0,
                                                "phoneNumber": contact_phone,
                                                "cuisines": cusine_name,
                                                "distance": distance_km,
                                                "distanceMiles": distance_miles,
                                                "offerName": offer_name,
                                                "percentageText": percentage_text,
                                                "nextCloseTime": store["_source"]['nextCloseTime'] if "nextCloseTime" in
                                                                                                      store[
                                                                                                          '_source'] else "",
                                                "nextOpenTime": store["_source"]['nextOpenTime'] if "nextOpenTime" in
                                                                                                    store[
                                                                                                        '_source'] else "",
                                                "status": store["_source"]['status'],
                                                "shopifyEnable": shopify_enable,
                                                "address": store["_source"]['businessLocationAddress'][
                                                    'address'] if "address" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "locality": store["_source"]['businessLocationAddress'][
                                                    'locality'] if "locality" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "postCode": store["_source"]['businessLocationAddress'][
                                                    'postCode'] if "postCode" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "addressArea": store["_source"]['businessLocationAddress'][
                                                    'addressArea'] if "addressArea" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "state": store["_source"]['businessLocationAddress'][
                                                    'state'] if "state" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "country": store["_source"]['businessLocationAddress'][
                                                    'country'] if "country" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "city": store["_source"]['businessLocationAddress']['city'] if "city" in
                                                                                                               store[
                                                                                                                   "_source"][
                                                                                                                   'businessLocationAddress'] else "",
                                                "nextOpen": store_tag
                                            })
                                        else:
                                            pass
                                else:
                                    pass
                            else:
                                try:
                                    if store["_source"]['storeIsOpen'] == True:
                                        ''' get store data'''
                                        for spe in list(set(store["_source"]['specialities'])):
                                            spe_data.append(ObjectId(spe))
                                        specialities = db.specialities.find({"_id": {"$in": spe_data}})
                                        for s_data in specialities:
                                            spe_name.append(s_data['specialityName'][language])
                                        contact_phone = store["_source"]['contactPhone']['countryCode'] + " " + store['contactPhone'][
                                            'number']
                                        open_store.append({
                                            "id": str(store['_id']),
                                            "safetyStandardsDynamicContent": store["_source"][
                                                'safetyStandardsDynamicContent'],
                                            "storeName": store["_source"]['storeName'][language] if language in
                                                                                                    store["_source"][
                                                                                                        'storeName'] else
                                            store["_source"]['storeName']["en"],
                                            "safetyStandards": store["_source"]['safetyStandards'],
                                            "avgRating": round(store["_source"]['avgRating'], 2) if "avgRating" in
                                                                                                    store[
                                                                                                        '_source'] else 0,
                                            "averageCostForMealForTwo": store["_source"][
                                                'averageCostForMealForTwo'] if 'averageCostForMealForTwo' in store[
                                                "_source"] else "",
                                            "shopifyId": store['_source']['shopifyId'] if 'shopifyId' in store[
                                                '_source'] else "",
                                            "currencyCode": store["_source"]['currencyCode'] if "currencyCode" in store[
                                                "_source"] else "INR",
                                            "logoImages": store["_source"]['logoImages'],
                                            "freeDeliveryAbove": store["_source"][
                                                'freeDeliveryAbove'] if "freeDeliveryAbove" in store["_source"] else 0,
                                            "averageDeliveryTime": str(store["_source"][
                                                                           'averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in
                                                                                                                           store[
                                                                                                                               "_source"] else "",
                                            "currencySymbol": store["_source"][
                                                'currencySymbol'] if "currencySymbol" in store[
                                                "_source"] else "INR",
                                            "currency": store["_source"]['currencyCode'] if "currencyCode" in store[
                                                "_source"] else "INR",
                                            "isExpressDelivery": store["_source"][
                                                'isExpressDelivery'] if "isExpressDelivery" in store else 0,
                                            "specialities": spe_name,
                                            "businessLocationAddress": store["_source"][
                                                'businessLocationAddress'] if "businessLocationAddress" in store else {},
                                            "billingAddress": store["_source"][
                                                'billingAddress'] if 'billingAddress' in store else {},
                                            "headOffice": store["_source"][
                                                'headOffice'] if 'headOffice' in store else {},
                                            "paymentMethods": {
                                                "cashOnPickUp": store["_source"]['cashOnPickUp'] if "cashOnPickUp" in
                                                                                                    store[
                                                                                                        "_source"] else False,
                                                "pickUpPrePaymentCard": store["_source"][
                                                    'pickUpPrePaymentCard'] if "pickUpPrePaymentCard" in store[
                                                    "_source"] else False,
                                                "cashOnDelivery": store["_source"][
                                                    'cashOnDelivery'] if "cashOnDelivery" in store[
                                                    "_source"] else False,
                                                "deliveryPrePaymentCard": store["_source"][
                                                    'deliveryPrePaymentCard'] if "deliveryPrePaymentCard" in store[
                                                    "_source"] else False,
                                                "cardOnDelivery": store["_source"][
                                                    'cardOnDelivery'] if "cardOnDelivery" in store[
                                                    "_source"] else False,
                                                "acceptsCashOnDelivery": store["_source"][
                                                    'acceptsCashOnDelivery'] if "acceptsCashOnDelivery" in store[
                                                    "_source"] else False,
                                                "acceptsCard": store["_source"]['acceptsCard'] if "acceptsCard" in
                                                                                                  store[
                                                                                                      "_source"] else False,
                                                "acceptsWallet": store["_source"]['acceptsWallet'] if "acceptsWallet" in
                                                                                                      store[
                                                                                                          "_source"] else False,
                                            },
                                            "galleryImages": store["_source"]['galleryImages'],
                                            "minimumOrderValue": str(store["_source"]['minimumOrder']) if
                                            store["_source"]['minimumOrder'] != 0 else "No Minimum",
                                            "cityId": store["_source"]['cityId'],
                                            "citiesOfOperation": store["_source"]['citiesOfOperation'],
                                            "isExpressDelivery": int(
                                                store["_source"]['isExpressDelivery']) if "isExpressDelivery" in store[
                                                "_source"] else 0,
                                            "parentstoreIdOrSupplierId": store["_source"][
                                                'parentstoreIdOrSupplierId'] if "parentstoreIdOrSupplierId" in store[
                                                '_source'] else "",
                                            "storeName": store["_source"]['storeName'][language] if language in
                                                                                                    store["_source"][
                                                                                                        'storeName'] else
                                            store["_source"]['storeName']['en'],
                                            "address": store["_source"]['headOffice'][
                                                'headOfficeAddress'] if "headOfficeAddress" in store["_source"][
                                                'headOffice'] else "",
                                            "storeTypeId": store["_source"]['storeTypeId'],
                                            "supportedOrderTypes": store["_source"][
                                                'supportedOrderTypes'] if "supportedOrderTypes" in store[
                                                '_source'] else 3,
                                            "storeType": store["_source"]['storeType'],
                                            "averageDeliveryTimeInMins": store["_source"][
                                                'averageDeliveryTimeInMins'] if "averageDeliveryTimeInMins" in store[
                                                '_source'] else 0,
                                            "storeFrontTypeId": store["_source"]['storeFrontTypeId'],
                                            "storeFrontType": store["_source"]['storeFrontType'],
                                            "driverTypeId": store["_source"]['driverTypeId'] if "driverTypeId" in store[
                                                "_source"] else 0,
                                            "driverType": store["_source"]['driverType'] if "driverType" in store[
                                                "_source"] else 0,
                                            "phoneNumber": contact_phone,
                                            "cuisines": cusine_name,
                                            "distance": distance_km,
                                            "distanceMiles": distance_miles,
                                            "offerName": offer_name,
                                            "percentageText": percentage_text,
                                            "nextCloseTime": store["_source"]['nextCloseTime'] if "nextCloseTime" in
                                                                                                  store[
                                                                                                      '_source'] else "",
                                            "nextOpenTime": store["_source"]['nextOpenTime'] if "nextOpenTime" in store[
                                                '_source'] else "",
                                            "status": store["_source"]['status'],
                                            "shopifyEnable": shopify_enable,
                                            "address": store["_source"]['businessLocationAddress'][
                                                'address'] if "address" in store["_source"][
                                                'businessLocationAddress'] else "",
                                            "locality": store["_source"]['businessLocationAddress'][
                                                'locality'] if "locality" in store["_source"][
                                                'businessLocationAddress'] else "",
                                            "postCode": store["_source"]['businessLocationAddress'][
                                                'postCode'] if "postCode" in store["_source"][
                                                'businessLocationAddress'] else "",
                                            "addressArea": store["_source"]['businessLocationAddress'][
                                                'addressArea'] if "addressArea" in store["_source"][
                                                'businessLocationAddress'] else "",
                                            "state": store["_source"]['businessLocationAddress']['state'] if "state" in store["_source"]['businessLocationAddress'] else "",
                                            "country": store["_source"]['businessLocationAddress'][
                                                'country'] if "country" in store["_source"][
                                                'businessLocationAddress'] else "",
                                            "city": store["_source"]['businessLocationAddress']['city'] if "city" in store["_source"]['businessLocationAddress'] else ""
                                        })
                                    else:
                                        ''' closed store '''
                                        if store["_source"]['nextCloseTime'] == "" and store["_source"]['nextOpenTime'] == "":
                                            ''' temporory close '''
                                            for spe in list(set(store["_source"]['specialities'])):
                                                spe_data.append(ObjectId(spe))
                                            specialities = db.specialities.find({"_id": {"$in": spe_data}})
                                            for s_data in specialities:
                                                spe_name.append(s_data['specialityName'][language])
                                            contact_phone = store["_source"]['contactPhone']['countryCode'] + " " + \
                                                            store["_source"]['contactPhone'][
                                                                'number']

                                            temporary_close.append({
                                                "id": str(store['_id']),
                                                "safetyStandardsDynamicContent": store["_source"][
                                                    'safetyStandardsDynamicContent'],
                                                "storeName": store["_source"]['storeName'][language] if language in store["_source"]['storeName'] else
                                                store["_source"]['storeName']["en"],
                                                "safetyStandards": store["_source"]['safetyStandards'],
                                                "avgRating": round(store["_source"]['avgRating'], 2) if "avgRating" in store['_source'] else 0,
                                                "averageCostForMealForTwo": store["_source"]['averageCostForMealForTwo'] if 'averageCostForMealForTwo' in store["_source"] else "",
                                                "shopifyId": store['_source']['shopifyId'] if 'shopifyId' in store['_source'] else "",
                                                "currencyCode": store["_source"]['currencyCode'] if "currencyCode" in store["_source"] else "INR",
                                                "logoImages": store["_source"]['logoImages'],
                                                "freeDeliveryAbove": store["_source"]['freeDeliveryAbove'] if "freeDeliveryAbove" in store["_source"] else 0,
                                                "averageDeliveryTime": str(store["_source"]['averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in store["_source"] else "",
                                                "currencySymbol": store["_source"][
                                                    'currencySymbol'] if "currencySymbol" in store[
                                                    "_source"] else "INR",
                                                "currency": store["_source"]['currencyCode'] if "currencyCode" in store[
                                                    "_source"] else "INR",
                                                "isExpressDelivery": store["_source"][
                                                    'isExpressDelivery'] if "isExpressDelivery" in store else 0,
                                                "specialities": spe_name,
                                                "businessLocationAddress": store["_source"][
                                                    'businessLocationAddress'] if "businessLocationAddress" in store else {},
                                                "billingAddress": store["_source"][
                                                    'billingAddress'] if 'billingAddress' in store else {},
                                                "headOffice": store["_source"][
                                                    'headOffice'] if 'headOffice' in store else {},
                                                "paymentMethods": {
                                                                "cashOnPickUp": store["_source"]['cashOnPickUp'] if "cashOnPickUp" in store[
                                                                    "_source"] else False,
                                                                "pickUpPrePaymentCard": store["_source"][
                                                                    'pickUpPrePaymentCard'] if "pickUpPrePaymentCard" in store["_source"] else False,
                                                                "cashOnDelivery": store["_source"]['cashOnDelivery'] if "cashOnDelivery" in store[
                                                                    "_source"] else False,
                                                                "deliveryPrePaymentCard": store["_source"][
                                                                    'deliveryPrePaymentCard'] if "deliveryPrePaymentCard" in store[
                                                                    "_source"] else False,
                                                                "cardOnDelivery": store["_source"]['cardOnDelivery'] if "cardOnDelivery" in store[
                                                                    "_source"] else False,
                                                                "acceptsCashOnDelivery": store["_source"][
                                                                    'acceptsCashOnDelivery'] if "acceptsCashOnDelivery" in store["_source"] else False,
                                                                "acceptsCard": store["_source"]['acceptsCard'] if "acceptsCard" in store[
                                                                    "_source"] else False,
                                                                "acceptsWallet": store["_source"]['acceptsWallet'] if "acceptsWallet" in store[
                                                                    "_source"] else False,
                                                            },
                                                "galleryImages": store["_source"]['galleryImages'],
                                                "minimumOrderValue": str(store["_source"]['minimumOrder']) if store["_source"]['minimumOrder'] != 0 else "No Minimum",
                                                "cityId": store["_source"]['cityId'],
                                                "citiesOfOperation": store["_source"]['citiesOfOperation'],
                                                "isExpressDelivery": int(store["_source"]['isExpressDelivery']) if "isExpressDelivery" in store["_source"] else 0,
                                                "parentstoreIdOrSupplierId": store["_source"]['parentstoreIdOrSupplierId'] if "parentstoreIdOrSupplierId" in store['_source'] else "",
                                                "storeName": store["_source"]['storeName'][language] if language in store["_source"][ 'storeName'] else store["_source"]['storeName']['en'],
                                                "address": store["_source"]['headOffice']['headOfficeAddress'] if "headOfficeAddress" in store["_source"]['headOffice'] else "",
                                                "storeTypeId": store["_source"]['storeTypeId'],
                                                "supportedOrderTypes": store["_source"]['supportedOrderTypes'] if "supportedOrderTypes" in store['_source'] else 3,
                                                "storeType": store["_source"]['storeType'],
                                                "averageDeliveryTimeInMins": store["_source"]['averageDeliveryTimeInMins'] if "averageDeliveryTimeInMins" in store['_source'] else 0,
                                                "storeFrontTypeId": store["_source"]['storeFrontTypeId'],
                                                "storeFrontType": store["_source"]['storeFrontType'],
                                                "driverTypeId": store["_source"]['driverTypeId'] if "driverTypeId" in store["_source"] else 0,
                                                "driverType": store["_source"]['driverType'] if "driverType" in store[
                                                    "_source"] else 0,
                                                "phoneNumber": contact_phone,
                                                "cuisines": cusine_name,
                                                "distance": distance_km,
                                                "distanceMiles": distance_miles,
                                                "offerName": offer_name,
                                                "percentageText": percentage_text,
                                                "nextCloseTime": store["_source"]['nextCloseTime'] if "nextCloseTime" in store['_source'] else "",
                                                "nextOpenTime": store["_source"]['nextOpenTime'] if "nextOpenTime" in store['_source'] else "",
                                                "status": store["_source"]['status'],
                                                "shopifyEnable": shopify_enable,
                                                "address": store["_source"]['businessLocationAddress']['address'] if "address" in store["_source"]['businessLocationAddress'] else "",
                                                "locality": store["_source"]['businessLocationAddress']['locality'] if "locality" in store["_source"]['businessLocationAddress'] else "",
                                                "postCode": store["_source"]['businessLocationAddress']['postCode'] if "postCode" in store["_source"]['businessLocationAddress'] else "",
                                                "addressArea": store["_source"]['businessLocationAddress']['addressArea'] if "addressArea" in  store["_source"]['businessLocationAddress'] else "",
                                                "state": store["_source"]['businessLocationAddress']['state'] if "state" in store["_source"]['businessLocationAddress'] else "",
                                                "country": store["_source"]['businessLocationAddress']['country'] if "country" in store["_source"]['businessLocationAddress'] else "",
                                                "city": store["_source"]['businessLocationAddress']['city'] if "city" in store["_source"]['businessLocationAddress'] else ""
                                            })
                                        elif store["_source"]['nextOpenTime'] != "":
                                            for spe in list(set(store["_source"]['specialities'])):
                                                spe_data.append(ObjectId(spe))
                                            specialities = db.specialities.find({"_id": {"$in": spe_data}})
                                            for s_data in specialities:
                                                spe_name.append(s_data['specialityName'][language])
                                            contact_phone = store["_source"]['contactPhone']['countryCode'] + " " + \
                                                            store["_source"]['contactPhone'][
                                                                'number']
                                            timeZoneWorkingHour = store["_source"]['timeZoneWorkingHour']
                                            next_open_time = store["_source"]['nextOpenTime']
                                            next_open_time = time_zone_converter(timezone, next_open_time,
                                                                                 timeZoneWorkingHour)
                                            local_time = datetime.datetime.fromtimestamp(next_open_time)
                                            next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                                            next_day_midnight = next_day.replace(hour=0, minute=0, second=0)
                                            next_day_midnight_timestamp = int(next_day_midnight.timestamp())
                                            store_tag = ""
                                            if next_day_midnight_timestamp > next_open_time:
                                                open_time = local_time.strftime("%I:%M %p")
                                                store_tag = "Opens Next At " + open_time
                                            else:
                                                open_time = local_time.strftime("%I:%M %p")
                                                store_tag = "Opens Tomorrow At " + open_time
                                            close_store.append({
                                                "id": str(store['_id']),
                                                "safetyStandardsDynamicContent": store["_source"][
                                                    'safetyStandardsDynamicContent'],
                                                "storeName": store["_source"]['storeName'][language] if language in
                                                                                                        store[
                                                                                                            "_source"][
                                                                                                            'storeName'] else
                                                store["_source"]['storeName']["en"],
                                                "safetyStandards": store["_source"]['safetyStandards'],
                                                "avgRating": round(store["_source"]['avgRating'], 2) if "avgRating" in
                                                                                                        store[
                                                                                                            '_source'] else 0,
                                                "averageCostForMealForTwo": store["_source"][
                                                    'averageCostForMealForTwo'] if 'averageCostForMealForTwo' in store[
                                                    "_source"] else "",
                                                "shopifyId": store['_source']['shopifyId'] if 'shopifyId' in store[
                                                    '_source'] else "",
                                                "currencyCode": store["_source"]['currencyCode'] if "currencyCode" in
                                                                                                    store[
                                                                                                        "_source"] else "INR",
                                                "logoImages": store["_source"]['logoImages'],
                                                "freeDeliveryAbove": store["_source"][
                                                    'freeDeliveryAbove'] if "freeDeliveryAbove" in store[
                                                    "_source"] else 0,
                                                "averageDeliveryTime": str(store["_source"][
                                                                               'averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in
                                                                                                                               store[
                                                                                                                                   "_source"] else "",
                                                "currencySymbol": store["_source"][
                                                    'currencySymbol'] if "currencySymbol" in store[
                                                    "_source"] else "INR",
                                                "currency": store["_source"]['currencyCode'] if "currencyCode" in store[
                                                    "_source"] else "INR",
                                                "isExpressDelivery": store["_source"][
                                                    'isExpressDelivery'] if "isExpressDelivery" in store else 0,
                                                "specialities": spe_name,
                                                "businessLocationAddress": store["_source"][
                                                    'businessLocationAddress'] if "businessLocationAddress" in store else {},
                                                "billingAddress": store["_source"][
                                                    'billingAddress'] if 'billingAddress' in store else {},
                                                "headOffice": store["_source"][
                                                    'headOffice'] if 'headOffice' in store else {},
                                                "paymentMethods": {
                                                    "cashOnPickUp": store["_source"][
                                                        'cashOnPickUp'] if "cashOnPickUp" in store[
                                                        "_source"] else False,
                                                    "pickUpPrePaymentCard": store["_source"][
                                                        'pickUpPrePaymentCard'] if "pickUpPrePaymentCard" in store[
                                                        "_source"] else False,
                                                    "cashOnDelivery": store["_source"][
                                                        'cashOnDelivery'] if "cashOnDelivery" in store[
                                                        "_source"] else False,
                                                    "deliveryPrePaymentCard": store["_source"][
                                                        'deliveryPrePaymentCard'] if "deliveryPrePaymentCard" in store[
                                                        "_source"] else False,
                                                    "cardOnDelivery": store["_source"][
                                                        'cardOnDelivery'] if "cardOnDelivery" in store[
                                                        "_source"] else False,
                                                    "acceptsCashOnDelivery": store["_source"][
                                                        'acceptsCashOnDelivery'] if "acceptsCashOnDelivery" in store[
                                                        "_source"] else False,
                                                    "acceptsCard": store["_source"]['acceptsCard'] if "acceptsCard" in
                                                                                                      store[
                                                                                                          "_source"] else False,
                                                    "acceptsWallet": store["_source"][
                                                        'acceptsWallet'] if "acceptsWallet" in store[
                                                        "_source"] else False,
                                                },
                                                "galleryImages": store["_source"]['galleryImages'],
                                                "minimumOrderValue": str(store["_source"]['minimumOrder']) if
                                                store["_source"]['minimumOrder'] != 0 else "No Minimum",
                                                "cityId": store["_source"]['cityId'],
                                                "citiesOfOperation": store["_source"]['citiesOfOperation'],
                                                "isExpressDelivery": int(
                                                    store["_source"]['isExpressDelivery']) if "isExpressDelivery" in
                                                                                              store["_source"] else 0,
                                                "parentstoreIdOrSupplierId": store["_source"][
                                                    'parentstoreIdOrSupplierId'] if "parentstoreIdOrSupplierId" in
                                                                                    store['_source'] else "",
                                                "storeName": store["_source"]['storeName'][language] if language in
                                                                                                        store[
                                                                                                            "_source"][
                                                                                                            'storeName'] else
                                                store["_source"]['storeName']['en'],
                                                "address": store["_source"]['headOffice'][
                                                    'headOfficeAddress'] if "headOfficeAddress" in store["_source"][
                                                    'headOffice'] else "",
                                                "storeTypeId": store["_source"]['storeTypeId'],
                                                "supportedOrderTypes": store["_source"][
                                                    'supportedOrderTypes'] if "supportedOrderTypes" in store[
                                                    '_source'] else 3,
                                                "storeType": store["_source"]['storeType'],
                                                "averageDeliveryTimeInMins": store["_source"][
                                                    'averageDeliveryTimeInMins'] if "averageDeliveryTimeInMins" in
                                                                                    store['_source'] else 0,
                                                "storeFrontTypeId": store["_source"]['storeFrontTypeId'],
                                                "storeFrontType": store["_source"]['storeFrontType'],
                                                "driverTypeId": store["_source"]['driverTypeId'] if "driverTypeId" in
                                                                                                    store[
                                                                                                        "_source"] else 0,
                                                "driverType": store["_source"]['driverType'] if "driverType" in store[
                                                    "_source"] else 0,
                                                "phoneNumber": contact_phone,
                                                "cuisines": cusine_name,
                                                "distance": distance_km,
                                                "distanceMiles": distance_miles,
                                                "offerName": offer_name,
                                                "percentageText": percentage_text,
                                                "nextCloseTime": store["_source"]['nextCloseTime'] if "nextCloseTime" in
                                                                                                      store[
                                                                                                          '_source'] else "",
                                                "nextOpenTime": store["_source"]['nextOpenTime'] if "nextOpenTime" in
                                                                                                    store[
                                                                                                        '_source'] else "",
                                                "status": store["_source"]['status'],
                                                "shopifyEnable": shopify_enable,
                                                "address": store["_source"]['businessLocationAddress'][
                                                    'address'] if "address" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "locality": store["_source"]['businessLocationAddress'][
                                                    'locality'] if "locality" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "postCode": store["_source"]['businessLocationAddress'][
                                                    'postCode'] if "postCode" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "addressArea": store["_source"]['businessLocationAddress'][
                                                    'addressArea'] if "addressArea" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "state": store["_source"]['businessLocationAddress'][
                                                    'state'] if "state" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "country": store["_source"]['businessLocationAddress'][
                                                    'country'] if "country" in store["_source"][
                                                    'businessLocationAddress'] else "",
                                                "city": store["_source"]['businessLocationAddress']['city'] if "city" in store["_source"]['businessLocationAddress'] else ""
                                            })
                                        else:
                                            pass
                                except Exception as e:
                                    print(e)
                                    pass
                        fav_store = {
                            "openStore": fav_open,
                            "closeStore": fav_close,
                            "temporaryClose": fav_tem_close
                        }
                        final = {
                            "favirouteStore": fav_store,
                            "openStore": open_store,
                            "closeStore": close_store,
                            "temporaryClose": temporary_close,
                        }
                        final_response['data'] = final
                        return JsonResponse(final_response, status=200)
                    else:
                        response_data = {'message': 'no store link wth user'}
                        return JsonResponse(response_data, status=404)
