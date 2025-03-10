# Create your views here.
from re import T
from urllib.parse import ParseResultBytes
import redis
from epic_api.utils import UtilsObj
from search.views import HyperLocalStores, search_read_stores
from validations.calculate_avg_rating import product_avg_rating
from validations.combo_special_validation import combo_special_type_validation
from validations.product_validation import product_modification
from validations.get_stores_zone_wise import store_validation_function
from search_api.settings import (
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
    rj_brand,
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
from validations.store_validate_data import store_function
from validations.product_unit_validation import validate_units_data
from validations.product_data_validation import get_linked_unit_attribute
from validations.store_category_validation import validate_store_category
from bson.objectid import ObjectId
from validations.driver_roaster import (
    next_availbale_driver_roaster,
    next_availbale_driver_shift_in_stock,
    next_availbale_driver_shift_out_stock,
    next_availbale_driver_out_stock_shift,
)
import os
import sys
import asyncio
from bs4 import BeautifulSoup

character_data = [
    # {"name": "TOP"},
    {"name": "*"},
    {"name": "a"},
    {"name": "b"},
    {"name": "c"},
    {"name": "d"},
    {"name": "e"},
    {"name": "f"},
    {"name": "g"},
    {"name": "h"},
    {"name": "i"},
    {"name": "j"},
    {"name": "k"},
    {"name": "l"},
    {"name": "m"},
    {"name": "n"},
    {"name": "o"},
    {"name": "p"},
    {"name": "q"},
    {"name": "r"},
    {"name": "s"},
    {"name": "t"},
    {"name": "u"},
    {"name": "v"},
    {"name": "w"},
    {"name": "x"},
    {"name": "y"},
    {"name": "z"},
]

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
timezonename = TIME_ZONE


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


class PythonBrand(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Mega Menu"],
        operation_description="api for get brand for the show in mega menu",
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
            openapi.Parameter(
                name="categoryId",
                default="611f420cc1dce2d8d68c4faf",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while clicking on particular brand category",
            ),
            openapi.Parameter(
                name="search",
                default="mochi",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while searching brand from the front end, send searched keywords in "
                            "this key ",
            ),
            openapi.Parameter(
                name="from",
                default="0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="pagination from which page data need",
            ),
            openapi.Parameter(
                name="to",
                default="10",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="pagination for how much data need",
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
            store_category_id = request.GET.get("storeCategoryId", "")
            category_id = request.GET.get("categoryId", "")
            search_text = request.GET.get("search", "")
            from_data = int(request.GET.get("from", "0"))
            to_data = from_data + 20
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
                brand_query = {"status": 1, "storeCategoryId": store_category_id}
                if search_text != "":
                    brand_query["name.en"] = {"$regex": search_text, "$options": "i"}

                # =============brand data============================
                all_brands_details = []
                brand_details = (
                    db.brands.find(brand_query, {"name": 1})
                        .collation({'locale': 'en'})
                        .sort([("name.en", 1)])
                    # .skip(from_data)
                    # .limit(20)
                )
                brand_total_count = db.brands.find(brand_query).count()
                if brand_details.count() > 0:
                    for b_d in brand_details:
                        all_brands_details.append(
                            {
                                "id": str(b_d["_id"]),
                                "name": b_d["name"][language]
                                if language in b_d["name"]
                                else b_d["name"]["en"],
                            }
                        )
                else:
                    pass

                brand_json = {
                    "title": "All brands",
                    "penCount": brand_total_count,
                    "data": all_brands_details,
                    "charcterData": character_data,
                }

                if category_id == "":
                    if store_id != "0":
                        key_name = store_category_id + store_id
                    else:
                        key_name = store_category_id
                    try:
                        redis_response_data = rj_brand.get(key_name)
                        print("data getting from redis json")
                    except:
                        redis_response_data = []
                    if redis_response_data is not None:
                        if len(redis_response_data) > 0:
                            last_response = {
                                "title": "Brands",
                                "brandData": brand_json,
                                "message": "data found",
                                "data": json.loads(redis_response_data),
                            }
                            print(
                                "############end time for brand api##########",
                                time.time() - start_time,
                            )
                            return JsonResponse(last_response, safe=False, status=200)
                        else:
                            last_response = {
                                "title": "Brands",
                                "brandData": brand_json,
                                "message": "data found",
                                "data": [],
                            }
                            return JsonResponse(last_response, safe=False, status=404)
                    else:
                        last_response = {
                            "title": "Brands",
                            "brandData": brand_json,
                            "message": "data found",
                            "data": [],
                        }
                        return JsonResponse(last_response, safe=False, status=404)
                else:
                    key_name = category_id
                    print("key name")
                    try:
                        redis_response_data = rj_brand.get(key_name)
                        print("data getting from redis json")
                    except:
                        redis_response_data = {}
                    if len(redis_response_data) > 0:
                        last_response = {
                            "title": "Brands",
                            "message": "data found",
                            "brandData": brand_json,
                            "data": [json.loads(redis_response_data)],
                        }
                        print(
                            "############end time for brand api##########", time.time() - start_time
                        )
                        return JsonResponse(last_response, safe=False, status=200)
                    else:
                        last_response = {
                            "title": "Brands",
                            "brandData": brand_json,
                            "message": "data found",
                            "data": [],
                        }
                        return JsonResponse(last_response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class PythonStores(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Mega Menu"],
        operation_description="api for get seller for the show in mega menu",
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
                name="search",
                default="mochi",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while searching brand from the front end, send searched keywords in "
                            "this key ",
            ),
            openapi.Parameter(
                name="from",
                default="0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="pagination from which page data need",
            ),
            openapi.Parameter(
                name="to",
                default="10",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="pagination for how much data need",
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
            search_text = request.GET.get("search", "")
            from_data = int(request.GET.get("from", "0"))
            to_data = int(request.GET.get("to", "20"))
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
                seller_query = {"status": 1, "categoryId": store_category_id}
                if search_text != "":
                    seller_query["storeName.en"] = {"$regex": search_text, "$options": "i"}

                # =============brand data============================
                all_brands_details = []
                seller_list = []
                store_with_products = db.childProducts.aggregate([
                    {"$match": {"storeId": {"$ne": "0"}, "status": 1}},
                    {"$group": {"_id": "$storeId", "total": {"$sum": 1}}}
                ])
                store_id_with_prod = [x["_id"] for x in store_with_products]
                seller_query["_id"] = {"$in": store_id_with_prod}
                seller_details = (
                    db.stores.find(seller_query, {"specialities": 1, "storeName": 1})
                        .sort([("storeName.en", 1)])
                    # .skip(from_data)
                    # .limit(to_data)
                )
                seller_total_count = db.stores.find(seller_query).count()
                if seller_details.count() > 0:
                    for b_d in seller_details:
                        store_name = b_d["storeName"][language] if language in b_d["storeName"] else b_d["storeName"][
                            "en"]
                        name = '. '.join(list(map(lambda x: x.strip().capitalize(), store_name.split('.'))))
                        all_brands_details.append(
                            {
                                "id": str(b_d["_id"]),
                                "name": name,
                            }
                        )
                else:
                    pass
                seller_query["$where"] = "this.specialities.length>0"
                all_seller_details = (db.stores.find(seller_query, {"specialities": 1, "bannerImages": 1, "storeName": 1, "logoImages": 1, }).sort([("_id", -1)]).skip(0).limit(100))
                specialities_list = []
                if all_seller_details.count() > 0:
                    for store in all_seller_details:
                        for specialities in store["specialities"]:
                            specialities_list.append(ObjectId(specialities))
                            if len(store["bannerImages"]) > 0:
                                store_name = store["storeName"][language] if language in store["storeName"] else \
                                    store["storeName"]["en"]

                                seller_list.append(
                                    {
                                        "id": str(store["_id"]),
                                        "name": store_name,
                                        "websiteBannerImage": store["bannerImages"][
                                            "bannerImageweb"
                                        ],
                                        "logoImage": store["logoImages"]["logoImageweb"],
                                        "bannerImage": store["bannerImages"]["bannerImageweb"],
                                        "specialities": specialities,
                                    }
                                )
                else:
                    pass

                spec_dict = {}
                if len(specialities_list) > 0:
                    specialities_data = db.specialities.find(
                        {"_id": {"$in": list(set(specialities_list))}}, {"specialityName": 1}
                    )
                    for special in specialities_data:
                        spec_dict[str(special["_id"])] = special["specialityName"]["en"]
                else:
                    pass

                d = {}
                if len(seller_list) > 0:
                    for i in seller_list:
                        d.setdefault(i["specialities"], []).append(i)

                new_seller_list = []
                for s_p in list(set(specialities_list)):
                    if str(s_p) in d:
                        new_seller_list.append(
                            {
                                "keyName": spec_dict[str(s_p)].upper(),
                                "id": str(s_p),
                                "seqId": 1,
                                "data": d[str(s_p)],
                            }
                        )

                brand_json = {
                    "title": "Top sellers",
                    "penCount": seller_total_count,
                    "data": all_brands_details,
                    "charcterData": character_data,
                }
                last_response = {
                    "title": "Sellers",
                    "brandData": brand_json,
                    "message": "data found",
                    "data": new_seller_list,
                }
                return JsonResponse(last_response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class PythonProductVariant(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Search Filter"],
        operation_description="api for get variant details for the products",
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
                name="childProductId",
                default="60f8fa25c8697174dd8e8026",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="child product id from which child we need to get the products",
            ),
            openapi.Parameter(
                name="storeId",
                default="5e20914ac348027af2f9028e",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store id from which store we need to get the details",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5fcf541871d7bd51e6008c94",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="zone id from which zone we need to get the details",
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
            child_product_id = request.GET.get("childProductId", "")
            zone_id = request.GET.get("zoneId", "")
            token = request.META["HTTP_AUTHORIZATION"]
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif child_product_id == "":
                response_data = {
                    "message": "child product id is missing",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # ==================================product variant get api======================================
                product_json = []
                product_query = {"_id": ObjectId(child_product_id)}
                primary_product_details = db.childProducts.find_one(product_query)
                if primary_product_details is not None:
                    main_units_query = {
                        "parentProductId": primary_product_details["parentProductId"]
                    }
                    if primary_product_details["storeId"] == "0":
                        main_units_query["storeId"] = "0"
                    else:
                        main_units_query["storeId"] = ObjectId(primary_product_details["storeId"])
                    if zone_id != "":
                        store_query = {
                            "categoryId": str(primary_product_details["storeCategoryId"]),
                            "serviceZones.zoneId": zone_id,
                        }
                        store_data = db.stores.find(store_query)
                        store_data_details = ["0"]
                        if store_data.count() > 0:
                            for store in store_data:
                                store_data_details.append(ObjectId(store["_id"]))
                        main_units_query["storeId"] = {"$in": store_data_details}
                    else:
                        pass
                    product_details = db.childProducts.find(main_units_query)
                    if product_details.count() > 0:
                        for product in product_details:
                            offers_details = []
                            if "offer" in product:
                                for offer in product["offer"]:
                                    if offer["offerFor"] == 1 or offer["offerFor"] == 0:
                                        offer_terms = db.offers.find_one(
                                            {"_id": ObjectId(offer["offerId"])}
                                        )
                                        if offer_terms is not None:
                                            if offer_terms["startDateTime"] <= int(time.time()):
                                                if offer["status"] == 1:
                                                    offers_details.append(offer)
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

                            if len(offers_details) > 0:
                                best_offer = max(offers_details, key=lambda x: x["discountValue"])
                                offer_details = db.offers.find(
                                    {
                                        "_id": ObjectId(best_offer["offerId"]),
                                        "status": 1,
                                        "storeId": str(product["storeId"]),
                                    }
                                ).count()
                                if offer_details != 0:
                                    best_offer = best_offer
                                else:
                                    best_offer = {}
                            else:
                                best_offer = {}

                            try:
                                final_price = product["units"][0]["discountPrice"]
                            except:
                                final_price = product["units"][0]["floatValue"]

                            currencySymbol = product["currencySymbol"]
                            currency = product["currency"]
                            base_price = final_price
                            try:
                                size = product["units"][0]["unitSizeGroupValue"][language]
                            except:
                                try:
                                    size = product["units"][0]["unitSizeGroupValue"]["en"]
                                except:
                                    try:
                                        size = product["units"][0]["unitSizeGroupValue"]["en"]
                                    except:
                                        size = ""

                            available_qty = product["units"][0]["availableQuantity"]
                            if available_qty > 0:
                                out_of_stock = False
                            else:
                                out_of_stock = True
                            if size != "":
                                product_json.append(
                                    {
                                        "parentProductId": product["parentProductId"],
                                        "childProductId": str(product["_id"]),
                                        "storeId": str(product["storeId"]),
                                        "unitId": str(product["units"][0]["unitId"]),
                                        "offers": best_offer,
                                        "outOfStock": out_of_stock,
                                        "size": (size.strip()).upper(),
                                        "availableQuantity": available_qty,
                                        "currencySymbol": currencySymbol,
                                        "currency": currency,
                                        "price": base_price,
                                    }
                                )
                            else:
                                pass
                        product_details_new = db.childProducts.find(main_units_query)
                        if len(product_json) == 0:
                            for product in product_details_new:
                                offers_details = []
                                if "offer" in product:
                                    for offer in product["offer"]:
                                        if offer["offerFor"] == 1 or offer["offerFor"] == 0:
                                            offer_terms = db.offers.find_one(
                                                {"_id": ObjectId(offer["offerId"])}
                                            )
                                            if offer_terms is not None:
                                                if offer_terms["startDateTime"] <= int(time.time()):
                                                    if offer["status"] == 1:
                                                        offers_details.append(offer)
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

                                if len(offers_details) > 0:
                                    best_offer = max(
                                        offers_details, key=lambda x: x["discountValue"]
                                    )
                                    offer_details = db.offers.find(
                                        {
                                            "_id": ObjectId(best_offer["offerId"]),
                                            "status": 1,
                                            "storeId": str(product["storeId"]),
                                        }
                                    ).count()
                                    if offer_details != 0:
                                        best_offer = best_offer
                                    else:
                                        best_offer = {}
                                else:
                                    best_offer = {}

                                try:
                                    final_price = product["units"][0]["discountPrice"]
                                except:
                                    final_price = product["units"][0]["floatValue"]

                                currencySymbol = product["currencySymbol"]
                                currency = product["currency"]
                                base_price = final_price
                                size = ""
                                try:
                                    for attr in product["units"][0]["attributes"]:
                                        for attrlist in attr["attrlist"]:
                                            if attrlist["linkedtounit"] == 1:
                                                try:
                                                    measurement_unit = attrlist["measurementUnit"]
                                                except:
                                                    measurement_unit = ""
                                                try:
                                                    size = (
                                                            attrlist["value"][language]
                                                            + " "
                                                            + measurement_unit
                                                    )
                                                except:
                                                    try:
                                                        size = (
                                                                attrlist["value"][0][language]
                                                                + " "
                                                                + measurement_unit
                                                        )
                                                    except:
                                                        try:
                                                            size = (
                                                                    attrlist["value"]
                                                                    + " "
                                                                    + measurement_unit
                                                            )
                                                        except:
                                                            size = ""
                                except:
                                    size = ""
                                available_qty = product["units"][0]["availableQuantity"]
                                if available_qty > 0:
                                    out_of_stock = False
                                else:
                                    out_of_stock = True
                                if size != "":
                                    product_json.append(
                                        {
                                            "parentProductId": product["parentProductId"],
                                            "childProductId": str(product["_id"]),
                                            "storeId": str(product["storeId"]),
                                            "unitId": str(product["units"][0]["unitId"]),
                                            "offers": best_offer,
                                            "outOfStock": out_of_stock,
                                            "size": (size.strip()).upper(),
                                            "availableQuantity": available_qty,
                                            "currencySymbol": currencySymbol,
                                            "currency": currency,
                                            "price": base_price,
                                        }
                                    )
                                else:
                                    pass
                        if len(product_json) > 0:
                            newlist = sorted(
                                product_json,
                                key=lambda k: (k["price"], k["outOfStock"]),
                                reverse=False,
                            )
                            dataframe_product = pd.DataFrame(newlist)
                            dataframe_product = dataframe_product.drop_duplicates(
                                "size", keep="last"
                            )
                            dataframe_product = dataframe_product.to_json(orient="records")
                            category_path = json.loads(dataframe_product)
                            final_response = {"data": category_path, "message": "data found"}
                            return JsonResponse(final_response, safe=False, status=200)
                        else:
                            final_response = {"data": [], "message": "data not found"}
                            return JsonResponse(final_response, safe=False, status=404)
                    else:
                        final_response = {"data": [], "message": "data not found"}
                        return JsonResponse(final_response, safe=False, status=404)
                else:
                    final_response = {"data": [], "message": "data not found"}
                    return JsonResponse(final_response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class PythonCategorySeo(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Products"],
        operation_description="api for get seo details for category base on list",
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
                name="fname",
                default="Men",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="parent category name need to send here",
            ),
            openapi.Parameter(
                name="sname",
                default="Top wear",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="child category name need to send here",
            ),
            openapi.Parameter(
                name="tname",
                default="T-Shirt",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="child category name need to send here",
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
            fname = request.GET.get("fname", "")  # category-name
            fname = fname.replace("%20", " ")
            fname = fname.replace("+", " ")
            fname = fname.replace("%26", "&")
            sname = request.GET.get("sname", "")  # sub-category-name
            sname = sname.replace("%20", " ")
            sname = sname.replace("+", " ")
            sname = sname.replace("%26", "&")
            tname = request.GET.get("tname", "")  # sub-sub-category-name
            tname = tname.replace("%20", " ")
            tname = tname.replace("+", " ")
            category_query = {}
            if tname != "":
                tname = tname.replace("%2C", ",")
                category_query["categoryName.en"] = tname.replace("%20", " ")
            elif sname != "":
                sname = sname.replace("%2C", ",")
                category_query["categoryName.en"] = sname.replace("%20", " ")
            elif fname != "":
                fname = fname.replace("%2C", ",")
                category_query["categoryName.en"] = fname.replace("%20", " ")
            else:
                category_query = {}
            if len(category_query) > 0:
                category_details = db.category.find_one(category_query)
                if category_details is not None:
                    if "categorySeo" in category_details:
                        soup = BeautifulSoup(category_details["categorySeo"]["description"]["en"])
                        seo_details = {
                            "title": category_details["categorySeo"]["title"]["en"],
                            "description": soup.get_text(),
                            "mobileImage": category_details["mobileImage"],
                        }
                    else:
                        soup = BeautifulSoup(category_details["categoryDesc"]["en"])
                        seo_details = {
                            "title": category_details["categoryName"]["en"],
                            "description": soup.get_text(),
                            "mobileImage": category_details["mobileImage"],
                        }
                    final_response = {"data": seo_details, "message": "data found"}
                    return JsonResponse(final_response, safe=False, status=200)
                else:
                    final_response = {"data": {}, "message": "data not found"}
                    return JsonResponse(final_response, safe=False, status=404)
            else:
                final_response = {"data": {}, "message": "data not found"}
                return JsonResponse(final_response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class PythonAllSales(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Home Page"],
        operation_description="api for get all the details for sales",
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
                description="store category id from which store category need to fetch the details",
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
            store_category_id = request.GET.get("storeCategoryId", "")  # category-name
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            category_query = {"contentType": 4, "status": 1, "testMode": "false"}
            if store_category_id == "":
                response = {"data": [], "message": "store category id is missing"}
                return JsonResponse(response, safe=False, status=422)
            else:
                category_query["storeCategoryId"] = store_category_id
                home_page_details = db.ecomHomePage.find(category_query)
                home_page_list = list()
                if home_page_details.count() > 0:
                    for home in home_page_details:
                        entity_list = list()
                        for entity in home["entity"]:
                            entity_list.append(
                                {
                                    "name": entity["name"],
                                    "offerId": entity["offerId"],
                                    "buttonText": entity["buttonText"],
                                    "linkedWith": entity["linkedWith"],
                                    "value": entity["value"],
                                    "images": entity["images"],
                                    "id": entity["id"],
                                    "seqId": entity["seqId"],
                                }
                            )
                        if len(entity_list) > 0:
                            newlist = sorted(entity_list, key=lambda k: k["seqId"], reverse=True)
                            if home["testMode"] == "false":
                                test_mode = False
                            else:
                                test_mode = True

                            if home["visible"] == "true":
                                visible = True
                            else:
                                visible = False
                            home_page_list.append(
                                {
                                    "title": home["title"][language]
                                    if language in home["title"]
                                    else home["title"]["en"],
                                    "description": home["description"][language]
                                    if language in home["description"]
                                    else home["description"]["en"],
                                    "linkedWith": home["linkedWith"][0],
                                    "sectionType": home["sectionType"],
                                    "type": home["type"],
                                    "testMode": test_mode,
                                    "visible": visible,
                                    "entity": newlist,
                                    "cellType": home["cellType"],
                                }
                            )
                        else:
                            pass
                    if len(home_page_list) > 0:
                        final_response = {"data": home_page_list, "message": "data found"}
                        return JsonResponse(final_response, safe=False, status=200)
                    else:
                        final_response = {"data": {}, "message": "data not found"}
                        return JsonResponse(final_response, safe=False, status=404)
                else:
                    final_response = {"data": {}, "message": "data not found"}
                    return JsonResponse(final_response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class BlogGet(APIView):
    def post(self, request):
        try:
            data = request.data
            ans_data = []
            if "blog" not in data:
                response = {"messsage": "data not found"}
                return JsonResponse(response, status=422)
            blog = data["blog"]
            if blog == "":
                response = {"messsage": "key not found"}
                return JsonResponse(response, status=422)
            else:
                if len(blog) < 1:
                    response_ans = {"message": "data empty"}
                    return JsonResponse(response_ans, status=422)
                final_blog = list(set(blog))
                for data in final_blog:
                    check_data = db.childProducts.find(
                        {"linkedBlogs": {"$in": [data]}, "status": 1}
                    ).count()
                    if int(check_data) > 0:
                        ans_dict = {"logId": data, "count": int(check_data)}
                        ans_data.append(ans_dict)
                    else:
                        pass
                response_ans = {
                    "message": f"Data{' not' if not len(ans_data) else ''} found",
                    "data": ans_data,
                }
                return JsonResponse(response_ans, status=200 if len(ans_data) != 0 else 404)

        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)
