# -*- coding: utf-8 -*-
import traceback

from search_api.settings import (
    db,
    es,
    POPULAR_API_URL,
    CASSANDRA_KEYSPACE,
    CASSANDRA_SERVER,
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
    PYTHON_BASE_URL,
    REDIS_IP,
    STORE_CREATE_TIME,
    TIME_ZONE,
    PHARMACY_STORE_CATEGORY_ID,
    ECOMMERCE_STORE_CATEGORY_ID,
    WEBSITE_URL,
    conv_fac,
    EARTH_REDIS,
    MEAT_STORE_CATEGORY_ID,
    DINE_STORE_CATEGORY_ID,
    GROCERY_STORE_CATEGORY_ID,
    CANNABIS_STORE_CATEGORY_ID,
    LIQUOR_STORE_CATEGORY_ID,
    REBBITMQ_USERNAME,
    REBBITMQ_PASSWORD,
    REBBITMQ_IP,
    GRPC_URL,
    CASSANDRA_PASSWORD,
    CASSANDRA_USERNAME,
    session,
    APP_NAME,
    referral_db,
    currency_exchange_rate,
    MEAT_STORE_TYPE,
    DINE_STORE_TYPE,
    YUMMY_STORE_TYPE, IS_B2B_ENABLE
)
from json import dumps
from notification import notification_pb2, notification_pb2_grpc
import json
from rest_framework.views import APIView
from rest_framework.decorators import action
from pytz import timezone
from math import sin, cos, sqrt, atan2, radians
from kafka import KafkaProducer
from googletrans import Translator
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from django.http import JsonResponse
from dateutil import tz
from cassandra.cluster import Cluster
from bson.objectid import ObjectId
import time
import threading
import requests
import re
import queue
import pytz
import pika
import pandas as pd
import grpc
import datetime
import asyncio
import ast
from validations.product_best_offer_redis import (
    product_get_best_offer_data,
    product_best_offer_data,
)
from validations.product_best_supplier_redis import update_best_suppliers_redis
from validations.search_products_function import food_search_data
from mongo_query_module.query_module import zone_find
from validations.product_dc_validation import validate_dc_product_data
from validations.product_unit_validation import validate_units_data
from validations.product_variant_validation import validate_variant
from validations.supplier_validation import best_supplier_function
from validations.store_validate_data import store_function
from validations.driver_roaster import (
    next_availbale_driver_roaster,
    next_availbale_driver_shift_out_stock,
)
from validations.product_data_validation import validate_product_data
from validations.store_category_validation import validate_store_category
from validations.driver_intensive_api import driver_intensive
from validations.meat_availbility_validation import meat_availability_check
from search.views import get_linked_unit_attribute
from validations.product_city_pricing import cal_product_city_pricing
import os
import sys
import html2text
from validations.combo_special_validation import combo_special_type_validation
from validations.calculate_avg_rating import product_avg_rating
from search.views import product_reviews, category_search_logs

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")

meal_timing = {
    "latenightdinner": 0,
    "breakfast": 5,
    "brunch": 10,
    "lunch": 11,
    "tea": 15,
    "dinner": 19,
}

# =====================import all the files which need to call from api============================

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

timezonename = TIME_ZONE

# Create your views here.

'''
    Function for get the product details and in return need to send the product details
'''


def product_details_data(
        productId,
        parent_productId,
        lan,
        user_id,
        login_type,
        zone_id,
        store_id,
        is_search,
        static_json_data,
        currency_code,
        city_id,
        out_res19,
):
    try:
        unit_package_type = "Box" # package type of the unit, box, pack, etc
        unit_moq_type = "Box" # moq type of the product
        shopping_list_id = "" # id of the shopping list in which shopping list the product is added
        last_color_json = [] # list of dict for the colour details
        quantity_json = [] # list of dict for quantity of all the variants
        sizeChart = [] # size chart details of the product
        attribute_rating_data = [] # list of dict for the product attribute rating
        link_to_unit_list = [] # list of dict, all the linked to unit variants list
        link_to_unit = [] # list of dict, all the linked to unit attributes list
        customizable_attributes = [] # list of dict, all the attributes details which are customizable
        offers_details = [] # list of dict, all the offer details which are created on products
        attr_json = [] # list of dict, all the attributes list
        attr_html_json = [] # list of dict, all the attributes list which are html type
        hightlight_data = [] # list of product highlight
        static_data = []
        minimum_purchase_unit = ""
        isFavourite = False
        is_dc_linked = False
        dc_data = []
        'login type we are using for the check which type user is currectly logged in app or web app' \
        '0 for guest user, 1 for normal user and 2 for b2b(institute) buyers'
        number_for = [0, int(login_type)]
        central_query = {"parentProductId": str(parent_productId), "status": 1}

        ### find the store details, to check store is closed or open
        ### this part we are using storelisting true and hyperlocal true
        try:
            if store_id != "0" and store_id != "":
                store_details = db.stores.find_one({"_id": ObjectId(store_id)})
                try:
                    if "nextCloseTime" in store_details:
                        next_close_time = store_details["nextCloseTime"]
                    else:
                        next_close_time = ""
                except:
                    next_close_time = ""

                try:
                    if "nextOpenTime" in store_details:
                        next_open_time = store_details["nextOpenTime"]
                    else:
                        next_open_time = ""
                except:
                    next_open_time = ""

                if next_close_time == "" and next_open_time == "":
                    store_tag = True
                else:
                    store_tag = False
            else:
                store_tag = False
        except:
            store_tag = False

        ### fetch the product details from mongo
        primary_child_product = db.childProducts.find_one(
            {
                "_id": ObjectId(productId)
            },
            {
                "storeCategoryId": 1,
                "containsMeat":1,
                "storeId": 1,
                "images" : 1,
                "parentProductId": 1,
                "units": 1,
                "offer": 1,
                "serversFor": 1,
                "numberOfPcs": 1,
                "Weight": 1,
                "productSeo": 1,
                "brandTitle": 1,
                "manufactureName": 1,
                "b2cunitPackageType": 1,
                "currency": 1,
                "currencySymbol": 1,
                "linkedBlogs": 1,
                "categoryList": 1,
                "allowOrderOutOfStock": 1,
                "maxQuantity": 1,
                "prescriptionRequired": 1,
                "needsIdProof": 1,
                "saleOnline": 1,
                "uploadProductDetails": 1,
                "b2cbulkPackingEnabled": 1,
                "b2cpackingNoofUnits": 1,
                "b2cpackingPackageUnits": 1,
                "b2cpackingPackageType": 1,
                "detailDescription": 1,
                "replacementPolicy": 1,
                "warranty": 1,
                "term & condition": 1,
                "exchangePolicy": 1,
                "returnPolicy": 1,
                "substitute": 1,
                "comboProducts": 1,
                "tax": 1
            }
        )

        ### find the central product details
        central_product_details = db.products.find_one(
            {
                "_id": ObjectId(parent_productId)
            },
            {
                "units": 1,
                "b2bCityPricing": 1
            }
        )
        if primary_child_product is not None:
            store_category_id = primary_child_product["storeCategoryId"]
        else:
            store_category_id = ""
        category_query = {"storeCategory.storeCategoryId": store_category_id}
        dc_seller_id = ""

        ### this section we are using for find the store category details, we need to find the store category settings
        ### hyperlocal, storelisting
        ### from here we need to set that we need to remove the central store id or not,
        # if hyperlocal true and storelisting true or hyperlocal true and storelisting false
        # then we don't need to show central store

        if zone_id != "" and zone_id is not None and zone_id != "None" and zone_id != "null":
            zone_details = zone_find({"_id": ObjectId(zone_id)})
            dc_seller_id = zone_details["DCStoreId"] if "DCStoreId" in zone_details else ""
            category_query["_id"] = ObjectId(zone_details["city_ID"])
        elif store_id != "" and store_id != "undefined" and store_id != "0":
            store_details = db.stores.find_one({"_id": ObjectId(store_id)}, {"cityId": 1})
            category_query["_id"] = ObjectId(store_details["cityId"])
        else:
            pass

        categoty_details = db.cities.find_one(category_query, {"storeCategory": 1})
        hyperlocal = False  # to check store category is hyper local or not, which means store category is setup for city or for global
        storelisting = False  # to check store category type is storelisting or not, if storelisting true means need to show store listing in app and web app
        if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
            hyperlocal = False
            storelisting = False
        else:
            if categoty_details is not None:
                if "storeCategory" in categoty_details:
                    for cat in categoty_details["storeCategory"]:
                        if cat["storeCategoryId"] == store_category_id:
                            if cat["hyperlocal"] == True and cat["storeListing"] == 1:
                                hyperlocal = True
                                storelisting = True
                                store_id = store_id
                            elif cat["hyperlocal"] == True and cat["storeListing"] == 0:
                                hyperlocal = True
                                storelisting = False
                            else:
                                hyperlocal = False
                                storelisting = False
                        else:
                            pass
                else:
                    hyperlocal = False
                    storelisting = False
            else:
                hyperlocal = False
                storelisting = False

        store_list = []
        more_seller_list = []

        ### need to find the store details, to show the store details for hyperlocal true and storelisting true
        if zone_id != "" and hyperlocal == True and storelisting == False:
            if store_category_id == MEAT_STORE_CATEGORY_ID:
                store_data = db.stores.find(
                    {"categoryId": {"$in": ["0", str(store_category_id)]}, "serviceZones.zoneId": zone_id, "status": 1},
                    {"serviceZones": 1}
                )
            else:
                store_data = db.stores.find(
                    {"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id, "status": 1},
                    {"serviceZones": 1}
                )
            for s_data in store_data:
                store_list.append(ObjectId(s_data["_id"]))
                more_seller_list.append(ObjectId(s_data["_id"]))

        if hyperlocal == True and storelisting == True:
            try:
                central_query["storeId"] = ObjectId(store_id)
            except:
                central_query["storeId"] = "0"
        elif hyperlocal == False and storelisting == False:
            if primary_child_product["storeId"] == "0":
                central_query["storeId"] = primary_child_product["storeId"]
            else:
                central_query["storeId"] = ObjectId(primary_child_product["storeId"])
        elif hyperlocal == True and storelisting == False:
            if store_category_id == MEAT_STORE_CATEGORY_ID:
                store_list.append("0")
            else:
                pass
            central_query["storeId"] = {"$in": store_list}

        main_sellers = []
        if zone_id != "":
            main_store_query = {
                "categoryId": str(store_category_id),
                "storeFrontTypeId": {"$ne": 5},
                "status": 1,
                "serviceZones.zoneId": str(zone_id),
            }
            main_store_data = db.stores.find(main_store_query, {"serviceZones": 1})
            for seller in main_store_data:
                main_sellers.append(str(seller["_id"]))

        ### find all the products which are in same store with same parent product id, fetch all variant of the product
        child_products_details = db.childProducts.find(central_query, {
            "units": 1,
            "storeId": 1,
            "linkedAttributeCategory": 1,
            "parentProductId": 1
        })
        central_product_data = db.childProducts.find(
            {
                "parentProductId": primary_child_product["parentProductId"],
                "units.unitId": primary_child_product["units"][0]["unitId"],
                "status": 1,
            },
            {
                "storeId": 1,
                "units": 1,
            }
        )
        more_seller_query = {
            "parentProductId": primary_child_product["parentProductId"],
            "storeId": {"$ne": "0"},
            "units.unitId": primary_child_product["units"][0]["unitId"],
            "status": 1,
        }
        if hyperlocal == False and storelisting == False:
            pass
        elif hyperlocal == True and storelisting == True:
            if store_id != "":
                more_seller_query["storeId"] = ObjectId(store_id)
        elif hyperlocal == True and storelisting == False:
            more_seller_query["storeId"] = {"$in": more_seller_list}
        more_seller_count = 0 # we are using for store the seller count
        more_seller_details = db.childProducts.find(more_seller_query, {"storeId": 1})

        ### find all sellers who have the same product
        for m_s in more_seller_details:
            if str(m_s['storeId']) != "0":
                store_count = db.stores.find(
                    {
                        "_id": ObjectId(m_s['storeId']),
                        "storeFrontTypeId": {"$ne": 5},
                        "status": 1,
                    }
                ).count()
                if store_count > 0:
                    more_seller_count = more_seller_count + 1
                else:
                    pass
            else:
                pass

        if dc_seller_id != "":
            central_meat_product_query = {
                "parentProductId": primary_child_product["parentProductId"],
                "storeId": ObjectId(dc_seller_id),
                "units.unitId": primary_child_product["units"][0]["unitId"],
                "status": 1,
            }
        else:
            central_meat_product_query = {
                "parentProductId": primary_child_product["parentProductId"],
                "storeId": "0",
                "units.unitId": primary_child_product["units"][0]["unitId"],
                "status": 1,
            }

        ### query for find the all meat products, including central products
        central_meat_product_data = db.childProducts.find_one(central_meat_product_query)
        if central_meat_product_data is None:
            central_meat_product_query = {
                "parentProductId": primary_child_product["parentProductId"],
                "storeId": "0",
                "units.unitId": primary_child_product["units"][0]["unitId"],
                "status": 1,
            }
            central_meat_product_data = db.childProducts.find_one(central_meat_product_query, {"offer": 1, "seller": 1})

        ### seller count to show the more selled for hyperlocal false and storelisting false or hyperlocal true and storelisting false
        seller_count = more_seller_count - 1
        if hyperlocal == True and storelisting == True:
            seller_count = 0
        else:
            seller_count = seller_count

        ### find the offer for the product, for meat type need to fetch offer from central products
        if store_category_id == MEAT_STORE_CATEGORY_ID:
            if "offer" in central_meat_product_data:
                for offer in central_meat_product_data["offer"]:
                    offer_details = db.offers.find_one(
                        {"_id": ObjectId(offer["offerId"]), "status": 1}
                    )
                    if offer_details is not None:
                        if offer["status"] == 1:
                            if offer_details["startDateTime"] <= int(time.time()):
                                if offer_details != None:
                                    offer["termscond"] = offer_details["termscond"]
                                else:
                                    offer["termscond"] = ""
                                offer["name"] = offer_details["name"]["en"]
                                offer["discountValue"] = offer["discountValue"]
                                offer["discountType"] = offer["discountType"]
                                offers_details.append(offer)
                        else:
                            pass
                    else:
                        pass
        else:
            if "offer" in primary_child_product:
                for offer in primary_child_product["offer"]:
                    offer_query = {"_id": ObjectId(offer["offerId"]), "status": 1}
                    offer_details = db.offers.find_one(offer_query)
                    if offer_details is not None:
                        if offer["status"] == 1:
                            if offer_details["startDateTime"] <= int(time.time()):
                                if offer_details != None:
                                    offer["termscond"] = offer_details["termscond"]
                                else:
                                    offer["termscond"] = ""
                                offer["name"] = offer_details["name"]["en"]
                                offer["discountValue"] = offer["discountValue"]
                                offer["discountType"] = offer["discountType"]
                                offers_details.append(offer)
                        else:
                            pass
                    else:
                        pass
        if len(offers_details) > 0:
            best_offer = max(offers_details, key=lambda x: x["discountValue"])
        else:
            best_offer = {}

        ### get teh percentage and discount type from the best offer
        # discountType 1- for flat discount, 2 for percentage and 3 for BUY X GET Y
        if len(best_offer) == 0:
            percentage = 0
        else:
            if "discountType" in best_offer:
                if best_offer["discountType"] == 0:
                    percentage = 0
                else:
                    percentage = int(best_offer["discountValue"])
            else:
                percentage = 0

        # if len(best_offer) > 0:
        #     discount_type = int(best_offer["discountType"]) if "discountType" in best_offer else 0
        #     discount_value = best_offer["discountValue"]
        # else:
        #     discount_value = 0
        #     discount_type = 0

        if len(best_offer) == 0:
            discount_value = 0
            discount_type = 0
        else:
            if "discountType" in best_offer:
                discount_value = int(
                    best_offer["discountValue"]
                )
                discount_type = best_offer[
                    "discountType"
                ]
            else:
                discount_value = 0
                discount_type = 0

        ### size chart details need to send in response
        is_size_chart = False
        if "sizeChartId" in primary_child_product["units"][0]:
            if primary_child_product["units"][0]["sizeChartId"] != "" and primary_child_product["units"][0][
                "sizeChartId"] is not None:
                size_query = {"_id": ObjectId(primary_child_product["units"][0]["sizeChartId"])}
            else:
                size_query = None
        else:
            size_query = None

        if size_query is not None:
            size_group = db.sizeGroup.find_one(size_query, {"sizeGroup": 1, "description": 1})
        else:
            size_group = None
        if size_group is not None:
            is_size_chart = True
        size_query = None
        if size_group is None:
            if central_product_details is not None:
                try:
                    if "sizeChartId" in central_product_details["units"][0]:
                        if central_product_details["units"][0]["sizeChartId"] != "":
                            size_query = {
                                "_id": ObjectId(central_product_details["units"][0]["sizeChartId"])
                            }
                        else:
                            size_query = None
                    else:
                        size_query = None
                except:
                    size_query = None
            else:
                pass
        else:
            pass

        if size_query is not None and size_group is None:
            size_group = db.sizeGroup.find_one(size_query, {"sizeGroup": 1, "description": 1})
        else:
            pass
        size_chart_seq_id = {}
        if size_group is not None:
            if "sizeGroup" in size_group:
                for size in size_group['sizeGroup']:
                    if size['keyId'] == 1:
                        for value in size['value']:
                            size_chart_seq_id[value['en']] = value['seqId'] if "seqId" in value else 1
                    else:
                        pass
            else:
                pass
        else:
            pass
        # ==========================unit data================================================================

        # ! tax we are reading from units array for meola because they have tax variant wise
        tax_price = 0
        tax_value = []
        if store_category_id != DINE_STORE_CATEGORY_ID:
            '''
                inside if loop we are reading the tax from units array for meola products
                and from else we are reading the tax from outside the units array
            '''
            if "tax" in primary_child_product['units'][0]:
                if type(primary_child_product['units'][0]["tax"]) == list:
                    for tax in primary_child_product['units'][0]["tax"]:
                        if "taxValue" in tax:
                            tax_value.append({"value": tax["taxValue"]})
                else:
                    if primary_child_product['units'][0]["tax"] != None:
                        if "taxValue" in primary_child_product['units'][0]["tax"]:
                            tax_value.append({"value": primary_child_product['units'][0]["tax"]["taxValue"]})
                        else:
                            tax_value.append({"value": primary_child_product['units'][0]["tax"]})
                    else:
                        pass
            else:
                if type(primary_child_product["tax"]) == list:
                    for tax in primary_child_product["tax"]:
                        if "taxValue" in tax:
                            tax_value.append({"value": tax["taxValue"]})
                else:
                    if primary_child_product["tax"] != None:
                        if "taxValue" in primary_child_product["tax"]:
                            tax_value.append({"value": primary_child_product["tax"]["taxValue"]})
                        else:
                            tax_value.append({"value": primary_child_product["tax"]})
                    else:
                        pass
            print("store_category_id",store_category_id)
            print("DINE_STORE_CATEGORY_ID",DINE_STORE_CATEGORY_ID)
            if store_category_id == DINE_STORE_CATEGORY_ID:
                pass
            else:
                try:
                    if len(tax_value) == 0:
                        tax_price = 0
                    else:
                        for amount in tax_value:
                            tax_price = tax_price + (int(amount["value"]))
                except:
                    pass
        else:
            tax_price = 0

        ### find the dc details for the DC type stores
        if central_product_data.count() > 0:
            for child in central_product_data:
                if str(child["storeId"]) != "0":
                    store_dc_data = db.stores.find(
                        {
                            "_id": ObjectId(child["storeId"]),
                            "storeFrontTypeId": 5,
                            "serviceZones.zoneId": zone_id,
                            "status": 1,
                        }
                    ).count()
                    if store_dc_data > 0:
                        child_product_count = db.childProducts.find_one(
                            {"_id": ObjectId(child["_id"])}, {"seller": 1}
                        )
                        if child_product_count is not None:
                            if "seller" in child_product_count:
                                if len(child_product_count["seller"]) > 0:
                                    is_dc_linked = True
                                    dc_data.append(
                                        {
                                            "id": str(child["storeId"]),
                                            "productId": str(child["_id"]),
                                            "retailerPrice": child["units"][0]["b2cPricing"][0][
                                                "b2cproductSellingPrice"
                                            ],
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
        else:
            pass

        ### section for the get the all variant details with variant is inStock or not,
        # also get which are primary and which are visible
        linked_to_attribute_variant = []
        for child in child_products_details:
            if str(child["_id"]) == productId:  # and str(child['storeId']) == str(primary_child_product['storeId']):
                is_primary = True
            else:
                is_primary = False
            if "availableQuantity" in child["units"][0]:
                try:
                    if (
                            child["units"][0]["availableQuantity"] > 0
                            and child["units"][0]["availableQuantity"] != ""
                    ):
                        outOfStock = False
                        availableStock = child["units"][0]["availableQuantity"]
                    else:
                        outOfStock = True
                        availableStock = 0
                except:
                    outOfStock = True
                    availableStock = 0
            else:
                outOfStock = True
                availableStock = 0
            if "modelImage" in child["units"][0]:
                if child["units"][0]["modelImage"] is not None:
                    if len(child["units"][0]["modelImage"]) > 0:
                        small_img = child["units"][0]["modelImage"][0]["small"]
                        extra_large_img = child["units"][0]["modelImage"][0]["extraLarge"]
                    else:
                        try:
                            small_img = child["units"][0]["image"][0]["small"]
                            extra_large_img = child["units"][0]["image"][0]["extraLarge"]
                        except:
                            small_img = ""
                            extra_large_img = ""
                else:
                    small_img = child["units"][0]["image"][0]["small"]
                    extra_large_img = child["units"][0]["image"][0]["extraLarge"]
            else:
                small_img = child["units"][0]["image"][0]["small"]
                extra_large_img = child["units"][0]["image"][0]["extraLarge"]

            primary_child_product_size = -1
            child_size = 0

            ### for the colour variant (used in ecommerce)
            try:
                if (
                        child["units"][0]["colorName"] is not None
                        and child["units"][0]["colorName"] != ""
                ):
                    link_to_unit_list.append("Colors")
                    if is_primary:
                        linked_to_attribute_variant.append(
                            {"name": child["units"][0]["colorName"], "keyName": "Colors"}
                        )
                    if "en" in child["units"][0]["unitSizeGroupValue"]:
                        child_size = child["units"][0]["unitSizeGroupValue"]["en"]
                    else:
                        child_size = 0

                    if child_size == "":
                        child_size = 0

                    if "en" in primary_child_product["units"][0]["unitSizeGroupValue"]:
                        primary_child_product_size = primary_child_product["units"][0][
                            "unitSizeGroupValue"
                        ]["en"]
                    else:
                        primary_child_product_size = -1
                    if primary_child_product_size == "":
                        primary_child_product_size = -1

                    if (
                            child["units"][0]["colorName"]
                            == primary_child_product["units"][0]["colorName"]
                            or child_size == primary_child_product_size
                    ):
                        size_visible = True
                    elif (
                            child["units"][0]["colorName"]
                            == primary_child_product["units"][0]["colorName"]
                            or child_size == 0
                    ):
                        size_visible = True
                    else:
                        size_visible = False

                    if outOfStock:
                        size_visible = False
                    link_to_unit.append(
                        {
                            "name": child["units"][0]["colorName"],
                            "keyName": "Colors",
                            "unitData": child["units"][0]["colorName"],
                            "rgb": str("rgb(") + child["units"][0]["color"] + ")"
                            if "color" in child["units"][0]
                            else "",
                            "childProductId": str(child["_id"]),
                            "isPrimary": is_primary,
                            "finalPriceList": {},
                            "unitId": child["units"][0]["unitId"],
                            "visible": size_visible,  # visible,
                            "outOfStock": outOfStock,
                            "availableStock": availableStock,
                            "image": small_img,
                            "seqId": 1,
                            "extraLarge": extra_large_img,
                            "size": child["units"][0]["colorName"],
                        }
                    )
            except:
                pass

            ### for the size variant (used in ecommerce)
            if "unitSizeGroupValue" in child["units"][0]:
                if "en" in child["units"][0]["unitSizeGroupValue"]:
                    if (
                            child["units"][0]["unitSizeGroupValue"]["en"] is not None
                            and child["units"][0]["unitSizeGroupValue"]["en"] != ""
                    ):
                        link_to_unit_list.append("Sizes")

                        if child["units"][0]["colorName"] == primary_child_product["units"][0][
                            "colorName"] or child_size == primary_child_product_size:
                            size_visible = True
                        else:
                            size_visible = False

                        if outOfStock:
                            size_visible = False

                        if is_primary:
                            linked_to_attribute_variant.append(
                                {
                                    "name": child["units"][0]["unitSizeGroupValue"]["en"],
                                    "keyName": "Sizes",
                                }
                            )
                        link_to_unit.append(
                            {
                                "name": child["units"][0]["unitSizeGroupValue"]["en"],
                                "unitData": child["units"][0]["unitSizeGroupValue"]["en"],
                                "keyName": "Sizes",
                                "rgb": "",
                                "finalPriceList": {},
                                "childProductId": str(child["_id"]),
                                "isPrimary": is_primary,
                                "unitId": child["units"][0]["unitId"],
                                "visible": size_visible,
                                "outOfStock": outOfStock,
                                # "seqId": 1,
                                "seqId": size_chart_seq_id[child["units"][0]["unitSizeGroupValue"]["en"]] if
                                child["units"][0]["unitSizeGroupValue"]["en"] in size_chart_seq_id else 1,
                                "availableStock": availableStock,
                                "image": small_img,
                                "extraLarge": extra_large_img,
                                "size": child["units"][0]["unitSizeGroupValue"]["en"],
                            }
                        )

            ### for the linked to unit attributes(dynamic attributes) (used in ecommerce)
            try:
                for attr in child["units"][0]["attributes"]:
                    inner_html_attributes = []
                    inner_attributes = []
                    for attrlist in attr["attrlist"]:
                        attr_value_list_data = ""
                        # ===========================for linked to unit attriute====================
                        attribute_data_query = {
                            "_id": ObjectId(attrlist["attributeId"]),
                            "linkedtounit": 1,
                            "status": 1,
                        }
                        attr_count = db.productAttribute.find(attribute_data_query).count()
                        if attr_count > 0:
                            if int(attrlist["linkedtounit"]) == 0:
                                pass
                            else:
                                link_to_unit_list.append(
                                    attrlist["attrname"][lan]
                                    if lan in attrlist["attrname"]
                                    else attrlist["attrname"]["en"]
                                )
                                attribute_query = {
                                    "_id": ObjectId(child["_id"]),
                                    "units.attributes.attrlist.attrname.en": attrlist["attrname"][lan] if lan in
                                                                                                          attrlist[
                                                                                                              "attrname"] else
                                    attrlist["attrname"]["en"],
                                }
                                child_product_count = db.childProducts.find(attribute_query).count()
                                if child_product_count > 0:
                                    if type(attrlist["value"]) == list:
                                        if len(attrlist["value"]) > 0:
                                            units_data = attrlist["value"][0][lan] if lan in attrlist["value"][0] else \
                                                attrlist["value"][0]["en"]
                                        else:
                                            units_data = ""
                                    else:
                                        units_data = attrlist["value"][lan] if lan in attrlist["value"] else \
                                            attrlist["value"]["en"]

                                    if attrlist["measurementUnit"] != "":
                                        units_data = (
                                                str(units_data) + " " + attrlist["measurementUnit"]
                                        )
                                    else:
                                        units_data = str(units_data)

                                    if (
                                            child["units"][0]["colorName"]
                                            == primary_child_product["units"][0]["colorName"]
                                            or child_size == primary_child_product_size
                                    ):
                                        size_visible = True
                                    else:
                                        size_visible = False

                                    attr_product_query = {
                                        "parentProductId": child["parentProductId"],
                                        "units.unitId": child["units"][0]["unitId"],
                                    }

                                    if dc_seller_id != "":
                                        attr_product_query["storeId"] = ObjectId(dc_seller_id)
                                    else:
                                        attr_product_query["storeId"] = "0"
                                    attribute_central_product = db.childProducts.find_one(
                                        attr_product_query
                                    )
                                    if attribute_central_product is None:
                                        attribute_central_product = db.childProducts.find_one(
                                            {
                                                "parentProductId": child["parentProductId"],
                                                "units.unitId": child["units"][0]["unitId"],
                                                "storeId": "0",
                                            }
                                        )
                                    # price calculation
                                    if store_category_id == MEAT_STORE_CATEGORY_ID:
                                        try:
                                            if central_meat_product_data != None:
                                                varint_price = attribute_central_product["units"][
                                                    0
                                                ]["b2cPricing"][0]["b2cproductSellingPrice"]
                                                all_dc_list_data = []
                                                if "seller" in central_meat_product_data:
                                                    for seller in central_meat_product_data[
                                                        "seller"
                                                    ]:
                                                        if seller["storeId"] in main_sellers:
                                                            if seller["preOrder"]:
                                                                all_dc_list_data.append(seller)
                                                            else:
                                                                pass
                                                        else:
                                                            pass
                                                else:
                                                    pass
                                                if len(all_dc_list_data) == 0:
                                                    if "seller" in central_meat_product_data:
                                                        for new_seller in central_meat_product_data[
                                                            "seller"
                                                        ]:
                                                            if (
                                                                    new_seller["storeId"]
                                                                    in main_sellers
                                                            ):
                                                                all_dc_list_data.append(new_seller)
                                                            else:
                                                                pass
                                                    else:
                                                        pass
                                                try:
                                                    available_stock = (
                                                        central_meat_product_data["units"][0][
                                                            "availableQuantity"
                                                        ]
                                                        if "availableQuantity"
                                                           in central_meat_product_data["units"][0]
                                                        else 0
                                                    )
                                                except:
                                                    available_stock = 0

                                                if is_dc_linked:
                                                    if len(all_dc_list_data) > 0:
                                                        best_seller_product = min(
                                                            all_dc_list_data,
                                                            key=lambda x: x["procurementTime"],
                                                        )
                                                    else:
                                                        best_seller_product = {}
                                                else:
                                                    best_seller_product = {}

                                                if len(best_seller_product) > 0:
                                                    pre_order = best_seller_product["preOrder"]
                                                else:
                                                    pre_order = False
                                                if available_stock <= 0 and pre_order == True:
                                                    outOfStock = True
                                            else:
                                                varint_price = attribute_central_product["units"][
                                                    0
                                                ]["floatValue"]
                                        except:
                                            varint_price = attribute_central_product["units"][0][
                                                "floatValue"
                                            ]

                                        child_products_data = db.childProducts.find(central_query)
                                        for j in child_products_data:
                                            try:
                                                if child_products_data is not None:
                                                    varint_price = j["units"][0][
                                                        "b2cPricing"
                                                    ][0]["b2cproductSellingPrice"]
                                                else:
                                                    varint_price = j["units"][0][
                                                        "floatValue"
                                                    ]
                                            except:
                                                varint_price = j["units"][0][
                                                    "floatValue"
                                                ]
                                            # ==================================get currecny rate============================
                                            if str(child["storeId"]) == "0":
                                                if varint_price == 0 or varint_price == "":
                                                    discount_price = 0
                                                    varint_final_price = 0
                                                else:
                                                    varint_price = varint_price + (
                                                            (varint_price * tax_price) / 100
                                                    )
                                                    if int(discount_type) == 0:
                                                        discount_price = float(discount_value)
                                                    elif int(discount_type) == 1:
                                                        discount_price = (
                                                                                 float(varint_price) * float(
                                                                             discount_value)
                                                                         ) / 100
                                                    else:
                                                        discount_price = 0
                                                    varint_final_price = varint_price - discount_price
                                            else:
                                                varint_price = varint_price + (
                                                        (varint_price * tax_price) / 100
                                                )
                                                if discount_type == 0:
                                                    discount_price = float(discount_value)
                                                elif discount_type == 1:
                                                    discount_price = (
                                                                             float(varint_price) * float(discount_value)
                                                                     ) / 100
                                                else:
                                                    discount_price = 0
                                                varint_final_price = varint_price - discount_price

                                            if is_primary:
                                                linked_to_attribute_variant.append(
                                                    {
                                                        "name": units_data,
                                                        "keyName": attrlist["attrname"][lan]
                                                        if lan in attrlist["attrname"]
                                                        else attrlist["attrname"]["en"],
                                                    }
                                                )

                                            if outOfStock:
                                                size_visible = False
                                            if type(attrlist["value"]) == list:
                                                if len(attrlist["value"]) > 0:
                                                    units_data = attrlist["value"][0][lan] if lan in attrlist["value"][
                                                        0] else \
                                                        attrlist["value"][0]["en"]
                                                else:
                                                    units_data = ""
                                            else:
                                                units_data = attrlist["value"][lan] if lan in attrlist["value"] else \
                                                    attrlist["value"]["en"]

                                            if "measurementUnit" in attrlist:
                                                measurement_unit = attrlist['measurementUnit']
                                            else:
                                                measurement_unit = ""

                                            if measurement_unit != "":
                                                units_data = str(units_data) + " " + measurement_unit
                                            link_to_unit.append(
                                                {
                                                    "name": str(units_data),
                                                    "unitData": units_data,
                                                    "keyName": attrlist["attrname"][lan]
                                                    if lan in attrlist["attrname"]
                                                    else attrlist["attrname"]["en"],
                                                    "childProductId": str(child["_id"]),
                                                    "finalPriceList": {
                                                        "basePrice": varint_price,
                                                        "discountPrice": discount_price,
                                                        "finalPrice": varint_final_price,
                                                        "discountPercentage": discount_value,
                                                    },
                                                    "isPrimary": is_primary,
                                                    "visible": size_visible,
                                                    "unitId": child["units"][0]["unitId"]
                                                    if "unitId" in child["units"][0]
                                                    else "",
                                                    "rgb": str("rgb(") + child["units"][0]["color"] + ")"
                                                    if "rgb" in child["units"][0]
                                                    else "",
                                                    "outOfStock": outOfStock,
                                                    "seqId": 1,
                                                    "availableStock": availableStock,
                                                    "image": small_img,
                                                    "extraLarge": extra_large_img,
                                                    "size": units_data,
                                                }
                                            )
                                    else:
                                        child_products_data = db.childProducts.find(central_query)
                                        for j in child_products_data:
                                            try:
                                                if child_products_data is not None:
                                                    varint_price = j["units"][0][
                                                        "b2cPricing"
                                                    ][0]["b2cproductSellingPrice"]
                                                else:
                                                    varint_price = j["units"][0][
                                                        "floatValue"
                                                    ]
                                            except:
                                                varint_price = j["units"][0][
                                                    "floatValue"
                                                ]
                                            # ==================================get currecny rate============================
                                            if str(child["storeId"]) == "0":
                                                if varint_price == 0 or varint_price == "":
                                                    discount_price = 0
                                                    varint_final_price = 0
                                                else:
                                                    varint_price = varint_price + (
                                                            (varint_price * tax_price) / 100
                                                    )
                                                    if int(discount_type) == 0:
                                                        discount_price = float(discount_value)
                                                    elif int(discount_type) == 1:
                                                        discount_price = (
                                                                                 float(varint_price) * float(discount_value)
                                                                         ) / 100
                                                    else:
                                                        discount_price = 0
                                                    varint_final_price = varint_price - discount_price
                                            else:
                                                varint_price = varint_price + (
                                                        (varint_price * tax_price) / 100
                                                )
                                                if discount_type == 0:
                                                    discount_price = float(discount_value)
                                                elif discount_type == 1:
                                                    discount_price = (
                                                                             float(varint_price) * float(discount_value)
                                                                     ) / 100
                                                else:
                                                    discount_price = 0
                                                varint_final_price = varint_price - discount_price

                                            if is_primary:
                                                linked_to_attribute_variant.append(
                                                    {
                                                        "name": units_data,
                                                        "keyName": attrlist["attrname"][lan]
                                                        if lan in attrlist["attrname"]
                                                        else attrlist["attrname"]["en"],
                                                    }
                                                )

                                            if outOfStock:
                                                size_visible = False
                                            if type(attrlist["value"]) == list:
                                                if len(attrlist["value"]) > 0:
                                                    units_data = attrlist["value"][0][lan] if lan in attrlist["value"][0] else \
                                                        attrlist["value"][0]["en"]
                                                else:
                                                    units_data = ""
                                            else:
                                                units_data = attrlist["value"][lan] if lan in attrlist["value"] else \
                                                    attrlist["value"]["en"]

                                            if "measurementUnit" in attrlist:
                                                measurement_unit = attrlist['measurementUnit']
                                            else:
                                                measurement_unit = ""

                                            if measurement_unit != "":
                                                units_data = str(units_data) + " " + measurement_unit
                                            link_to_unit.append(
                                                {
                                                    "name": str(units_data),
                                                    "unitData": units_data,
                                                    "keyName": attrlist["attrname"][lan]
                                                    if lan in attrlist["attrname"]
                                                    else attrlist["attrname"]["en"],
                                                    "childProductId": str(child["_id"]),
                                                    "finalPriceList": {
                                                        "basePrice": varint_price,
                                                        "discountPrice": discount_price,
                                                        "finalPrice": varint_final_price,
                                                        "discountPercentage": discount_value,
                                                    },
                                                    "isPrimary": is_primary,
                                                    "visible": size_visible,
                                                    "unitId": child["units"][0]["unitId"]
                                                    if "unitId" in child["units"][0]
                                                    else "",
                                                    "rgb": str("rgb(") + child["units"][0]["color"] + ")"
                                                    if "rgb" in child["units"][0]
                                                    else "",
                                                    "outOfStock": outOfStock,
                                                    "seqId": 1,
                                                    "availableStock": availableStock,
                                                    "image": small_img,
                                                    "extraLarge": extra_large_img,
                                                    "size": units_data,
                                                }
                                            )
                        else:
                            pass
                        # ====================================done==================================
                        if is_primary:
                            attr_value_list = attrlist["value"]
                            if type(attr_value_list) == list:
                                for v in attr_value_list:
                                    if attrlist["measurementUnit"] != "":
                                        v = v["name"]["en"] + " " + attrlist["measurementUnit"]
                                    else:
                                        v = v["name"]["en"]
                                    if attr_value_list_data == "":
                                        attr_value_list_data = v
                                    else:
                                        attr_value_list_data = attr_value_list_data + ", " + v
                            elif type(attr_value_list["en"]) == list:
                                for v in attr_value_list["en"]:
                                    if attrlist["measurementUnit"] != "":
                                        v = str(v) + " " + attrlist["measurementUnit"]
                                    if attr_value_list_data == "":
                                        attr_value_list_data = str(v)
                                    else:
                                        attr_value_list_data = attr_value_list_data + ", " + v
                            else:
                                if attr_value_list["en"] != "":
                                    if attr_value_list["en"] != "0":
                                        if attrlist["measurementUnit"] != "":
                                            attr_value_list_data = (
                                                    str(attr_value_list["en"])
                                                    + " "
                                                    + attrlist["measurementUnit"]
                                            )
                                        else:
                                            attr_value_list_data = str(attr_value_list["en"])
                                    else:
                                        attr_value_list_data = ""
                                else:
                                    attr_value_list_data = ""

                            ## check the attribute is customizale type or not
                            # if customizable then need to send as a customizable
                            if "customizable" in attrlist:
                                if attrlist["customizable"] != 1:
                                    if attrlist["attriubteType"] == 5:
                                        is_html = True
                                    else:
                                        is_html = False
                                    if "None" in attr_value_list_data:
                                        attr_value_list_data = "0 %"
                                    if is_html:
                                        if attr_value_list_data != "":
                                            inner_html_attributes.append(
                                                {
                                                    "name": attrlist["attrname"][lan]
                                                    if lan in attrlist["attrname"]
                                                    else attrlist["attrname"]["en"],
                                                    "value": str(attr_value_list_data),
                                                    "attriubteType": attrlist["attriubteType"]
                                                    if "attriubteType" in attrlist
                                                    else 0,
                                                    "attributeImage": attrlist["attributeImage"]
                                                    if "attributeImage" in attrlist
                                                    else "",
                                                    "customizable": attrlist["customizable"]
                                                    if "customizable" in attrlist
                                                    else 0,
                                                    "isHtml": is_html,
                                                }
                                            )
                                    else:
                                        if attr_value_list_data != "":
                                            if len(child["linkedAttributeCategory"]) > 0:
                                                attribute_details = db.category.find(
                                                    {
                                                        "attributeGroupData.AttributeList.attributeId": str(
                                                            attrlist["attributeId"]
                                                        ),
                                                        "_id": ObjectId(
                                                            child["linkedAttributeCategory"][
                                                                "categoryId"
                                                            ]
                                                        ),
                                                        "status": 1,
                                                    }
                                                ).count()
                                                if attribute_details > 0:
                                                    inner_attributes.append(
                                                        {
                                                            "name": attrlist["attrname"][lan]
                                                            if lan in attrlist["attrname"]
                                                            else attrlist["attrname"]["en"],
                                                            "value": (
                                                                str(attr_value_list_data)
                                                            ).replace("% %", "%"),
                                                            "attributeImage": attrlist[
                                                                "attributeImage"
                                                            ]
                                                            if "attributeImage" in attrlist
                                                            else "",
                                                            "attriubteType": attrlist[
                                                                "attriubteType"
                                                            ]
                                                            if "attriubteType" in attrlist
                                                            else 0,
                                                            "customizable": attrlist["customizable"]
                                                            if "customizable" in attrlist
                                                            else 0,
                                                            "isHtml": is_html,
                                                        }
                                                    )
                                if attrlist["customizable"] == 1:
                                    for inner_custom in attrlist["value"]:
                                        customizable_attributes.append(
                                            {
                                                "name": attrlist["attrname"][lan]
                                                if lan in attrlist["attrname"]
                                                else attrlist["attrname"]["en"],
                                                "value": inner_custom["name"]["en"],
                                                "attriubteType": attrlist["attriubteType"]
                                                if "attriubteType" in attrlist
                                                else 0,
                                                "customizable": attrlist["customizable"]
                                                if "customizable" in attrlist
                                                else 0,
                                                "attributeId": attrlist["attributeId"],
                                                "image": inner_custom["image"],
                                                "bannerImage": inner_custom["bannerImage"],
                                                "logoImage": inner_custom["logoImage"],
                                            }
                                        )
                                else:
                                    pass
                            else:
                                pass
                            if "attrgrpname" in attr:
                                if len(inner_html_attributes) > 0:
                                    attr_html_json.append(
                                        {
                                            "innerAttributes": inner_html_attributes,
                                            "name": attr["attrgrpname"][lan]
                                            if lan in attr["attrgrpname"]
                                            else attr["attrgrpname"]["en"],
                                        }
                                    )
                                if len(inner_attributes) > 0:
                                    attr_grp_details = db.productAttributeGroup.find_one(
                                        {"_id": ObjectId(attr["attributeGrpId"])}
                                    )
                                    if attr_grp_details is not None:
                                        attr_json.append(
                                            {
                                                "innerAttributes": inner_attributes,
                                                "seqId": attr_grp_details["seqId"],
                                                "name": attr["attrgrpname"][lan]
                                                if lan in attr["attrgrpname"]
                                                else attr["attrgrpname"]["en"],
                                            }
                                        )
            except Exception as ex:
                print(
                    "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                    type(ex).__name__,
                    ex,
                )
                pass

        if primary_child_product is not None:
            '''
                here we need to set all the static data which we need to send in api respose.
                this static data we are using for meat type category
            '''
            # ==============================for static data only for meat==================================
            if "serversFor" in primary_child_product["units"][0]:
                if (
                        str(primary_child_product["units"][0]["serversFor"]) != "0"
                        and str(primary_child_product["units"][0]["serversFor"]) != ""
                ):
                    servers_for = str(primary_child_product["units"][0]["serversFor"]) + " Person"
                else:
                    servers_for = ""
            else:
                servers_for = ""

            if servers_for != "":
                static_data.append(
                    {
                        "name": "Serves",
                        "value": servers_for,
                        "image": "https://dvidm5lncff50.cloudfront.net/eyJidWNrZXQiOiAicm9hZHlvIiwgImtleSI6ICJQcm9kdWN0Q2F0ZWdvcnkvMC8wL2xhcmdlL3NlcnZlXzE2NDg3MTU5OTEucG5nIiwgImVkaXRzIjogeyJyZXNpemUiOiB7IndpZHRoIjogMjIsICJoZWlnaHQiOiAxNiwgImZpdCI6ICJjb250YWluIn0sICJqcGVnIjogeyJxdWFsaXR5IjogMTAwfX19",
                    }
                )

            if "numberOfPcs" in primary_child_product["units"][0]:
                if (
                        str(primary_child_product["units"][0]["numberOfPcs"]) != "0"
                        and str(primary_child_product["units"][0]["numberOfPcs"]) != ""
                ):
                    number_of_pcs = str(primary_child_product["units"][0]["numberOfPcs"]) + " Pcs"
                else:
                    number_of_pcs = ""
            else:
                number_of_pcs = ""

            if number_of_pcs != "":
                static_data.append(
                    {
                        "name": "No. of Pcs",
                        "value": number_of_pcs,
                        "image": "https://dvidm5lncff50.cloudfront.net/eyJidWNrZXQiOiAicm9hZHlvIiwgImtleSI6ICJQcm9kdWN0Q2F0ZWdvcnkvMC8wL2xhcmdlL25vXzE2NDg3MTYwNDIucG5nIiwgImVkaXRzIjogeyJyZXNpemUiOiB7IndpZHRoIjogMjksICJoZWlnaHQiOiAyMiwgImZpdCI6ICJjb250YWluIn0sICJqcGVnIjogeyJxdWFsaXR5IjogMTAwfX19",
                    }
                )

            if "Weight" in primary_child_product["units"][0]:
                if (
                        str(primary_child_product["units"][0]["Weight"]) != "0"
                        and str(primary_child_product["units"][0]["Weight"]) != ""
                ):
                    Weight = str(primary_child_product["units"][0]["Weight"]) + " gms"
                else:
                    Weight = ""
            else:
                Weight = ""

            if Weight != "":
                static_data.append(
                    {
                        "name": "Weight",
                        "value": Weight,
                        "image": "https://dvidm5lncff50.cloudfront.net/eyJidWNrZXQiOiAicm9hZHlvIiwgImtleSI6ICJQcm9kdWN0Q2F0ZWdvcnkvMC8wL2xhcmdlL3dlaWdodF8xNjQ4NzE2MjMyLnBuZyIsICJlZGl0cyI6IHsicmVzaXplIjogeyJ3aWR0aCI6IDI1LCAiaGVpZ2h0IjogMTgsICJmaXQiOiAiY29udGFpbiJ9LCAianBlZyI6IHsicXVhbGl0eSI6IDEwMH19fQ==",
                    }
                )

            try:
                product_name = primary_child_product["units"][0]["unitName"][lan]
            except:
                product_name = primary_child_product["units"][0]["unitName"]["en"]
            category_path = []

            ### section we are using for get the seo details of the product, for set the seo for webapp
            if "productSeo" in primary_child_product:
                try:
                    if len(primary_child_product["productSeo"]["title"]) > 0:
                        title = (
                            primary_child_product["productSeo"]["title"][lan]
                            if lan in primary_child_product["productSeo"]["title"]
                            else primary_child_product["productSeo"]["title"]["en"]
                        )
                    else:
                        title = ""
                except:
                    title = ""

                try:
                    if len(primary_child_product["productSeo"]["description"]) > 0:
                        description = (
                            primary_child_product["productSeo"]["description"][lan]
                            if lan in primary_child_product["productSeo"]["description"]
                            else primary_child_product["productSeo"]["description"]["en"]
                        )
                    else:
                        description = ""
                except:
                    description = ""

                try:
                    if len(primary_child_product["productSeo"]["metatags"]) > 0:
                        metatags = (
                            primary_child_product["productSeo"]["metatags"][lan]
                            if lan in primary_child_product["productSeo"]["metatags"]
                            else primary_child_product["productSeo"]["metatags"]["en"]
                        )
                    else:
                        metatags = ""
                except:
                    metatags = ""

                try:
                    if len(primary_child_product["productSeo"]["slug"]) > 0:
                        slug = (
                            primary_child_product["productSeo"]["slug"][lan]
                            if lan in primary_child_product["productSeo"]["slug"]
                            else primary_child_product["productSeo"]["slug"]["en"]
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
                product_seo = {"title": "", "description": "", "metatags": "", "slug": ""}

            if len(primary_child_product["brandTitle"]) > 0:
                brand_name = (
                    primary_child_product["brandTitle"][lan]
                    if lan in primary_child_product["brandTitle"]
                    else primary_child_product["brandTitle"]["en"]
                )
            else:
                brand_name = ""

            if len(primary_child_product["manufactureName"]) > 0:
                manufacture_name = (
                    primary_child_product["manufactureName"][lan]
                    if lan in primary_child_product["manufactureName"]
                    else ""
                )
            else:
                manufacture_name = ""
            store_category_id = primary_child_product["storeCategoryId"]

            ### function we are using for the get the details for storelisting, remove central, category is ecommerce type or not
            is_ecommerce, remove_central, hide_recent_view, store_listing = validate_store_category(
                store_category_id, ECOMMERCE_STORE_CATEGORY_ID
            )
            if is_ecommerce == True and store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                zone_id = ""
            else:
                pass

            ### section for fetch the MOQ details of the product
            if "b2cunitPackageType" in primary_child_product:
                if lan in primary_child_product["b2cunitPackageType"]:
                    b2c_unit_package_type = (
                        primary_child_product["b2cunitPackageType"][lan]
                        if lan in primary_child_product["b2cunitPackageType"]
                        else primary_child_product["b2cunitPackageType"]["en"]
                    )
                else:
                    b2c_unit_package_type = "Quantity"
            else:
                b2c_unit_package_type = "Quantity"

            ### set the best suppilier details in supplier object, to show the supplier details on pdp page
            best_supplier = {
                "productId": str(productId),
                "id": str(primary_child_product["storeId"]),
            }

            product_id = best_supplier["productId"]
            try:
                availableStock = int(primary_child_product["units"][0]["availableQuantity"])
            except:
                availableStock = 0
            best_supplier["retailerQty"] = availableStock
            best_supplier["distributorPrice"] = 0
            best_supplier["distributorQty"] = 0
            if login_type == 1:
                try:
                    best_supplier["retailerPrice"] = primary_child_product["units"][0][
                        "b2cPricing"
                    ][0]["b2cproductSellingPrice"]
                except:
                    best_supplier["retailerPrice"] = primary_child_product["units"][0]["floatValue"]
            else:
                try:
                    best_supplier["retailerPrice"] = primary_child_product["units"][0][
                        "b2bPricing"
                    ][0]["b2bproductSellingPrice"]
                except:
                    best_supplier["retailerPrice"] = primary_child_product["units"][0]["floatValue"]
            # =========================================margin part==========================================
            try:
                currency_rate = currency_exchange_rate[
                    str(primary_child_product["currency"]) + "_" + str(currency_code)
                    ]
            except:
                currency_rate = 0
            currency_details = db.currencies.find_one({"currencyCode": currency_code})
            if currency_details is not None:
                currency_symbol = currency_details["currencySymbol"]
                currency = currency_details["currencyCode"]
            else:
                currency_symbol = primary_child_product["currencySymbol"]
                currency = primary_child_product["currency"]

            if "linkedBlogs" in primary_child_product:
                if store_category_id == MEAT_STORE_CATEGORY_ID:
                    if type(primary_child_product["linkedBlogs"]) == list:
                        linked_blogs = primary_child_product["linkedBlogs"]
                    else:
                        linked_blogs = [primary_child_product["linkedBlogs"]]
                else:
                    if type(primary_child_product["linkedBlogs"]) == list:
                        linked_blogs = primary_child_product["linkedBlogs"]
                    else:
                        linked_blogs = [primary_child_product["linkedBlogs"]]
            else:
                linked_blogs = []
            try:
                if linked_blogs[0] == "":
                    linked_blogs = []
            except:
                linked_blogs = []

            ### section for get the category path, which we need to show on pdp page as bradcrums
            if "categoryList" in primary_child_product:
                if len(primary_child_product["categoryList"]) > 0:
                    if (
                            len(
                                primary_child_product["categoryList"][0]["parentCategory"][
                                    "childCategory"
                                ]
                            )
                            > 0
                    ):
                        for cat in primary_child_product["categoryList"][0]["parentCategory"][
                            "childCategory"
                        ]:
                            try:
                                parent_details = db.category.find_one(
                                    {"_id": ObjectId(cat["categoryId"])},
                                    {"parentId": 1, "categoryName": 1},
                                )
                                if parent_details is not None:
                                    parent_details_1 = db.category.find_one(
                                        {"_id": ObjectId(parent_details["parentId"])},
                                        {"parentId": 1, "categoryName": 1},
                                    )
                                    if parent_details_1 is not None:
                                        if "parentId" in parent_details_1:
                                            parent_details_2 = db.category.find_one(
                                                {"_id": ObjectId(parent_details_1["parentId"])},
                                                {"parentId": 1, "categoryName": 1},
                                            )
                                            if parent_details_2 is not None:
                                                if "parentId" not in parent_details_2:
                                                    cat_name = parent_details_2["categoryName"][
                                                        "en"
                                                    ]
                                                    path_cat_name = cat_name.replace("&", "%26")
                                                    sub_cat_name = parent_details_1["categoryName"][
                                                        "en"
                                                    ]
                                                    path_sub_cat_name = sub_cat_name.replace(
                                                        "&", "%26"
                                                    )
                                                    sub_sub_cat_name = parent_details[
                                                        "categoryName"
                                                    ]["en"]
                                                    path_sub_sub_cat_name = (
                                                        sub_sub_cat_name.replace("&", "%26")
                                                    )
                                                    category_path.append(
                                                        {
                                                            "level": 1,
                                                            "name": parent_details_2[
                                                                "categoryName"
                                                            ]["en"],
                                                            "path": "product-list/categories/"
                                                                    + (path_cat_name).replace(
                                                                " ", "%20"
                                                            )  # "?fname=" + (path_cat_name).replace(" ",
                                                            #                                   "%20"),
                                                        }
                                                    )
                                                    category_path.append(
                                                        {
                                                            "level": 2,
                                                            "name": parent_details_1[
                                                                "categoryName"
                                                            ]["en"],
                                                            "path": "product-list/categories/"
                                                                    + (path_cat_name).replace(" ", "%20")
                                                                    + "/"
                                                                    + (path_sub_cat_name).replace(
                                                                " ", "%20"
                                                            )
                                                            #     "?fname=" + (path_cat_name).replace(
                                                            # " ", "%20") + "&sname=" +
                                                            #     (path_sub_cat_name).replace(" ",
                                                            #                                 "%20"),
                                                        }
                                                    )
                                                    category_path.append(
                                                        {
                                                            "level": 3,
                                                            "isSelected": True,
                                                            "path": "product-list/categories/"
                                                                    + (path_cat_name).replace(" ", "%20")
                                                                    + "/"
                                                                    + (path_sub_cat_name).replace(
                                                                " ", "%20"
                                                            )
                                                                    + "/"
                                                                    + (path_sub_sub_cat_name).replace(
                                                                " ", "%20"
                                                            ),
                                                            "name": parent_details["categoryName"][
                                                                "en"
                                                            ].replace("&", "%26"),
                                                        }
                                                    )
                                                else:
                                                    parent_details_3 = db.category.find_one(
                                                        {
                                                            "_id": ObjectId(
                                                                parent_details_2["parentId"]
                                                            )
                                                        },
                                                        {"parentId": 1, "categoryName": 1},
                                                    )
                                                    if "parentId" not in parent_details_3:
                                                        cat_name = parent_details_3["categoryName"][
                                                            "en"
                                                        ]
                                                        path_cat_name = cat_name.replace("&", "%26")
                                                        sub_cat_name = parent_details_2[
                                                            "categoryName"
                                                        ]["en"]
                                                        path_sub_cat_name = sub_cat_name.replace(
                                                            "&", "%26"
                                                        )
                                                        sub_sub_cat_name = parent_details_1[
                                                            "categoryName"
                                                        ]["en"]
                                                        path_sub_sub_cat_name = (
                                                            sub_sub_cat_name.replace("&", "%26")
                                                        )
                                                        category_path.append(
                                                            {
                                                                "level": 1,
                                                                "name": (
                                                                    parent_details_3[
                                                                        "categoryName"
                                                                    ]["en"]
                                                                ).replace("&", "%26"),
                                                                "path": "product-list/categories/"
                                                                        + (path_sub_cat_name).replace(
                                                                    " ", "%20"
                                                                ),
                                                            }
                                                        )
                                                        category_path.append(
                                                            {
                                                                "level": 2,
                                                                "name": (
                                                                    parent_details_2[
                                                                        "categoryName"
                                                                    ]["en"]
                                                                ).replace("&", "%26"),
                                                                "path": "product-list/categories/"
                                                                        + (path_sub_cat_name).replace(
                                                                    " ", "%20"
                                                                )
                                                                        + "/"
                                                                        + (path_sub_sub_cat_name).replace(
                                                                    " ", "%20"
                                                                ),
                                                            }
                                                        )
                                                        category_path.append(
                                                            {
                                                                "level": 3,
                                                                "name": path_sub_sub_cat_name.replace(
                                                                    "&", "%26"
                                                                ),
                                                                "path": "product-list/categories/"
                                                                        + (path_sub_cat_name).replace(
                                                                    " ", "%20"
                                                                )
                                                                        + "/"
                                                                        + (path_sub_sub_cat_name).replace(
                                                                    " ", "%20"
                                                                )
                                                                        + "/"
                                                                        + (
                                                                            parent_details["categoryName"][
                                                                                "en"
                                                                            ]
                                                                        ).replace(" ", "%20"),
                                                            }
                                                        )
                                                        category_path.append(
                                                            {
                                                                "level": 4,
                                                                "isSelected": True,
                                                                "name": (
                                                                    parent_details["categoryName"][
                                                                        "en"
                                                                    ]
                                                                ).replace("&", "%26"),
                                                                "path": "product-list/categories/"
                                                                        + path_sub_cat_name
                                                                        + "/ "
                                                                        + path_sub_sub_cat_name
                                                                        + "/"
                                                                        + (
                                                                            parent_details["categoryName"][
                                                                                "en"
                                                                            ]
                                                                        ).replace("&", "%26"),
                                                            }
                                                        )
                                                    else:
                                                        pass
                                            else:
                                                cat_name = (
                                                    parent_details_2["categoryName"]["en"]
                                                ).replace(" ", "%20")
                                                sub_cat_name = (
                                                    parent_details_1["categoryName"]["en"]
                                                ).replace(" ", "%20")
                                                sub_sub_cat_name = (
                                                    parent_details["categoryName"]["en"]
                                                ).replace(" ", "%20")
                                                category_path.append(
                                                    {
                                                        "level": 1,
                                                        "path": "product-list/categories/"
                                                                + cat_name.replace("&", "%26"),
                                                        "name": (
                                                            parent_details_2["categoryName"]["en"]
                                                        ).replace("&", "%26"),
                                                    }
                                                )
                                                category_path.append(
                                                    {
                                                        "level": 2,
                                                        "path": "product-list/categories/"
                                                                + cat_name.replace("&", "%26")
                                                                + "/"
                                                                + sub_cat_name.replace("&", "%26"),
                                                        "name": (
                                                            parent_details_1["categoryName"]["en"]
                                                        ).replace("&", "%26"),
                                                    }
                                                )
                                                category_path.append(
                                                    {
                                                        "level": 3,
                                                        "isSelected": True,
                                                        "path": "product-list/categories/"
                                                                + cat_name.replace("&", "%26")
                                                                + "/"
                                                                + sub_cat_name.replace("&", "%26")
                                                                + "/"
                                                                + sub_sub_cat_name.replace("&", "%26"),
                                                    }
                                                )
                                        else:
                                            cat_name = (
                                                parent_details_1["categoryName"]["en"]
                                            ).replace(" ", "%20")
                                            sub_cat_name = (
                                                parent_details["categoryName"]["en"]
                                            ).replace(" ", "%20")
                                            category_path.append(
                                                {
                                                    "level": 1,
                                                    "path": "product-list/categories/"
                                                            + cat_name.replace("&", "%26"),
                                                    "name": parent_details_1["categoryName"]["en"],
                                                }
                                            )
                                            category_path.append(
                                                {
                                                    "level": 2,
                                                    "isSelected": True,
                                                    "path": "product-list/categories/"
                                                            + cat_name.replace("&", "%26")
                                                            + "/"
                                                            + sub_cat_name.replace("&", "%26"),
                                                    "name": parent_details["categoryName"]["en"],
                                                }
                                            )
                                    else:
                                        cat_name = (parent_details_1["categoryName"]["en"]).replace(
                                            " ", "%20"
                                        )
                                        sub_cat_name = (
                                            parent_details["categoryName"]["en"]
                                        ).replace(" ", "%20")
                                        category_path.append(
                                            {
                                                "level": 1,
                                                "path": "product-list/categories/"
                                                        + cat_name.replace("&", "%26"),
                                                "name": parent_details_1["categoryName"]["en"],
                                            }
                                        )
                                        category_path.append(
                                            {
                                                "level": 2,
                                                "isSelected": True,
                                                "path": "product-list/categories/"
                                                        + cat_name.replace("&", "%26")
                                                        + "/"
                                                        + sub_cat_name.replace("&", "%26"),
                                                "name": parent_details["categoryName"]["en"],
                                            }
                                        )
                                else:
                                    cat_name = (parent_details["categoryName"]["en"]).replace(
                                        " ", "%20"
                                    )
                                    category_path.append(
                                        {
                                            "level": 1,
                                            "path": "product-list/categories/"
                                                    + cat_name.replace("&", "%26"),
                                            "name": parent_details["categoryName"]["en"],
                                        }
                                    )
                                    category_path.append(
                                        {
                                            "level": 2,
                                            "isSelected": True,
                                            "name": (product_name).replace("&", "%26"),
                                            "path": "",
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
                                pass
                    else:
                        cat_name = (
                            primary_child_product["categoryList"][0]["parentCategory"][
                                "categoryName"
                            ]["en"]
                        ).replace(" ", "%20")
                        category_path.append(
                            {
                                "level": 1,
                                "path": "product-list/categories/"
                                        + cat_name.replace("&", "%26")
                                        + "?fname="
                                        + cat_name.replace("&", "%26"),
                                "name": primary_child_product["categoryList"][0]["parentCategory"][
                                    "categoryName"
                                ]["en"],
                            }
                        )
                        category_path.append(
                            {
                                "level": 2,
                                "isSelected": True,
                                "name": (product_name).replace("&", "%26"),
                                "path": "",
                            }
                        )
                if len(category_path) > 0:
                    dataframe_offers = pd.DataFrame(category_path)
                    dataframe_offers = dataframe_offers.drop_duplicates("name", keep="last")
                    dataframe_offers = dataframe_offers.to_json(orient="records")
                    category_path = json.loads(dataframe_offers)
                else:
                    category_path = []
            if "allowOrderOutOfStock" in primary_child_product:
                allow_out_of_stock = primary_child_product["allowOrderOutOfStock"]
            else:
                allow_out_of_stock = False

            # =========================for max quantity=================================================
            if "maxQuantity" in primary_child_product:
                if primary_child_product["maxQuantity"] != "":
                    max_quantity = int(primary_child_product["maxQuantity"])
                else:
                    max_quantity = 30
            else:
                max_quantity = 30

            for qty in range(1, max_quantity + 1):
                quantity_json.append(qty)

            try:
                iosARModel = primary_child_product["units"][0]["iosARModel"][0]["small"]
            except:
                iosARModel = ""

            try:
                androidARModel = primary_child_product["units"][0]["uploadAndroidModel"][0]["small"]
            except:
                androidARModel = ""

            # =========================================pharmacy details=========================================
            if "prescriptionRequired" in primary_child_product:
                try:
                    if int(primary_child_product["prescriptionRequired"]) == 0:
                        prescription_required = False
                    else:
                        prescription_required = True
                except:
                    prescription_required = False
            else:
                prescription_required = False

            if "needsIdProof" in primary_child_product:
                if not primary_child_product["needsIdProof"]:
                    needsIdProof = False
                else:
                    needsIdProof = True
            else:
                needsIdProof = False

            if "saleOnline" in primary_child_product:
                if primary_child_product["saleOnline"] == 0:
                    sales_online = False
                else:
                    sales_online = True
            else:
                sales_online = False

            if "uploadProductDetails" in primary_child_product:
                upload_details = primary_child_product["uploadProductDetails"]
            else:
                upload_details = ""

            ### section for the find the MOQ details of the product which we need to show for B2B products
            if "b2cbulkPackingEnabled" in primary_child_product["units"][0]:
                try:
                    b2c_bulk_packing = int(primary_child_product["units"][0]["b2cbulkPackingEnabled"])
                except:
                    b2c_bulk_packing = 0
            else:
                try:
                    b2c_bulk_packing = int(primary_child_product["b2cbulkPackingEnabled"])
                except:
                    b2c_bulk_packing = 0

            if "b2cpackingNoofUnits" in primary_child_product["units"][0]:
                if type(primary_child_product["units"][0]["b2cpackingNoofUnits"]) == int:
                    b2c_packing_no_units = primary_child_product["units"][0]["b2cpackingNoofUnits"]
                    if b2c_packing_no_units == 0:
                        b2c_packing_no_units = 1
                else:
                    b2c_packing_no_units = 1
            else:
                try:
                    b2c_packing_no_units = primary_child_product["b2cpackingNoofUnits"]
                except:
                    b2c_packing_no_units = 1

            try:
                if int(primary_child_product["units"][0]["b2cbulkPackingEnabled"]) == 1:
                    b2c_packing_package_units = primary_child_product["units"][0]["b2cpackingPackageUnits"]["en"]
                else:
                    b2c_packing_package_units = primary_child_product["units"][0]["b2cunitPackageType"]["en"]
            except:
                try:
                    if int(primary_child_product["b2cbulkPackingEnabled"]) == 1:
                        b2c_packing_package_units = primary_child_product["b2cpackingPackageUnits"]["en"]
                    else:
                        b2c_packing_package_units = primary_child_product["b2cunitPackageType"]["en"]
                except:
                    b2c_packing_package_units = ""

            try:
                if int(primary_child_product["units"][0]["b2cbulkPackingEnabled"]) == 1:
                    b2c_packing_units = primary_child_product["units"][0]["b2cpackingPackageType"]["en"]
                else:
                    b2c_packing_units = primary_child_product["units"][0]["b2cunitPackageTypes"]["en"]
            except:
                try:
                    if int(primary_child_product["b2cbulkPackingEnabled"]) == 1:
                        b2c_packing_units = primary_child_product["b2cpackingPackageType"]["en"]
                    else:
                        b2c_packing_units = primary_child_product["b2cunitPackageTypes"]["en"]
                except:
                    b2c_packing_units = ""

            if b2c_bulk_packing != 0:
                if b2c_packing_package_units != "" and b2c_packing_units != "":
                    try:
                        mouDataUnit = (
                                str(b2c_packing_no_units)
                                + " "
                                + b2c_packing_package_units
                                + " per "
                                + b2c_packing_units
                        )
                    except:
                        mouDataUnit = ""
                elif b2c_packing_package_units != "" and b2c_packing_units == "":
                    mouDataUnit = str(b2c_packing_no_units) + " " + b2c_packing_package_units
                elif b2c_packing_package_units == "" and b2c_packing_units != "":
                    mouDataUnit = str(b2c_packing_no_units) + " " + b2c_packing_units
                else:
                    mouDataUnit = ""
            else:
                try:
                    mouDataUnit = str(b2c_packing_no_units) + " " + b2c_packing_units
                except:
                    mouDataUnit = ""
            minimum_order_qty = 1
            moq_data = ""
            if login_type == 2:
                if "b2bunitPackageType" in primary_child_product["units"][0]:
                    try:
                        if primary_child_product["units"][0]["b2bbulkPackingEnabled"] == 0:
                            minimum_order_qty = primary_child_product["units"][0]["b2bminimumOrderQty"]
                            if type(primary_child_product["units"][0]["b2bunitPackageType"]["en"]) == str:
                                unit_package_type = primary_child_product["units"][0]["b2bunitPackageType"]["en"]
                                unit_moq_type = primary_child_product["units"][0]["b2bunitPackageType"]["en"]
                            else:
                                unit_package_type = primary_child_product["units"][0]["b2bunitPackageType"]["en"]["en"]
                                unit_moq_type = primary_child_product["units"][0]["b2bunitPackageType"]["en"]["en"]
                            moq_data = str(minimum_order_qty) + " " + unit_package_type
                        else:
                            minimum_order_qty = primary_child_product["units"][0]["b2bpackingNoofUnits"]
                            unit_package_type = primary_child_product["units"][0]["b2bpackingPackageUnits"]["en"]
                            unit_moq_type = primary_child_product["units"][0]["b2bpackingPackageType"]["en"]
                            moq_data = str(minimum_order_qty) + " " + unit_package_type + " Per " + unit_moq_type
                    except:
                        unit_package_type = "Box"
                else:
                    pass

            ### section for the fetch the rating of the products which are given by users
            seller_data = db.ratingParams.find({"status": 1, "storeId": store_category_id})
            for seller_attr in seller_data:
                if len(best_supplier) > 0:
                    seller_rating = db.sellerReviewRatings.find(
                        {"sellerId": best_supplier["id"], "attributeId": str(seller_attr["_id"])}
                    )
                else:
                    seller_rating = db.sellerReviewRatings.find(
                        {"sellerId": "0", "attributeId": str(seller_attr["_id"])}
                    )
                if seller_rating.count() > 0:
                    for seller in seller_rating:
                        attribute_rating_data.append(
                            {
                                "attributeId": str(seller_attr["_id"]),
                                "name": seller_attr["name"][lan] if lan in seller_attr["name"] else seller_attr["name"][
                                    "en"],
                                "rating": seller["rating"],
                            }
                        )
                else:
                    pass

            if len(attribute_rating_data) > 0:
                try:
                    dataframe_attribute = pd.DataFrame(attribute_rating_data)
                    dataframe_attribute["TotalStarRating"] = dataframe_attribute.groupby(
                        dataframe_attribute["attributeId"]
                    ).transform("mean")
                    dataframe_attribute["rating"] = dataframe_attribute.groupby(
                        dataframe_attribute["attributeId"]
                    ).transform("mean")
                    dataframe_attribute = dataframe_attribute.drop_duplicates(
                        "attributeId", keep="last"
                    )
                    dataframe_attribute = dataframe_attribute.to_json(orient="records")
                    dataframe_attribute = json.loads(dataframe_attribute)
                except:
                    dataframe_attribute = []
            else:
                dataframe_attribute = []

            ### find the seller rating
            avg_rating_value = 0
            seller_rating = db.sellerReviewRatings.aggregate(
                [
                    {
                        "$match": {
                            "sellerId": str(best_supplier["id"]),
                            "rating": {"$ne": 0},
                            "status": 1,
                        }
                    },
                    {"$group": {"_id": "$sellerId", "avgRating": {"$avg": "$rating"}}},
                ]
            )
            for avg_rating in seller_rating:
                avg_rating_value = avg_rating["avgRating"]

            if avg_rating_value is None:
                avg_rating_value = 0
            addons_count = False
            ### supplier details need to set on supplier object
            if best_supplier["id"] == "0":
                dt_object = datetime.datetime.fromtimestamp(central_zero_store_creation_ts)
                day_s = datetime.datetime.now() - dt_object
                if day_s.days == 0:
                    if int(day_s.seconds) > 59:
                        sec = datetime.timedelta(seconds=day_s.seconds)
                        if int(sec.seconds / 60) > 59:
                            time_create = str(int(sec.seconds / 3600)) + " hours"
                        else:
                            time_create = str(int(sec.seconds / 60)) + " minutes"
                    else:
                        time_create = str(day_s.seconds) + " seconds"

                else:
                    if day_s.days > 30:
                        total_month = round((day_s.days) / 30)
                        time_create = str(total_month) + " months"
                    else:
                        time_create = str(day_s.days) + " days"
                if hyperlocal == True and storelisting == False:
                    best_supplier["supplierName"] = "No seller available"
                    best_supplier["storeAliasName"] = "No seller available"
                else:
                    best_supplier["supplierName"] = central_store
                    best_supplier["storeAliasName"] = central_store
                if hyperlocal == True and storelisting == False:
                    best_supplier["rating"] = 0
                else:
                    best_supplier["rating"] = round(avg_rating_value, 2)  # TotalStoreRating
                if hyperlocal == True and storelisting == False:
                    best_supplier["totalRating"] = len(attribute_rating_data)
                else:
                    best_supplier["totalRating"] = len(attribute_rating_data)
                best_supplier["reviewParameter"] = dataframe_attribute
                best_supplier["sellerSince"] = time_create
                best_supplier["storeFrontTypeId"] = 0
                best_supplier["storeFrontType"] = "Central"
                best_supplier["storeTypeId"] = 0
                best_supplier["postCode"] = ""
                best_supplier["storeType"] = "Central"
                best_supplier["cityName"] = "Banglore"
                best_supplier["areaName"] = "Hebbal"
                best_supplier["latitude"] = "-34.6576031"
                best_supplier["longitude"] = "135.6763443"
                best_supplier["sellerType"] = "Central"
                best_supplier["sellerTypeId"] = 1
                best_supplier["street1"] = ""
                best_supplier["city"] = ""
                best_supplier["state"] = ""
                best_supplier["country"] = ""
                best_supplier["email"] = ""
                best_supplier["designation"] = ""
                best_supplier["profilePic"] = ""
            else:
                ''' find addons avilable in product '''
                product_count = db.childProducts.find({"storeId":ObjectId(best_supplier["id"]),"parentProductId": parent_productId,"status": 1}).count()
                if product_count > 1:
                    addons_count = True
                seller_details = db.stores.find_one(
                    {"_id": ObjectId(best_supplier["id"])},
                    {
                        "storeName": 1,
                        "registrationDateTimeStamp": 1,
                        "cityName": 1,
                        "logoImages": 1,
                        "bannerImages": 1,
                        "storeTypeId": 1,
                        "contactEmail": 1,
                        "storeAliasName": 1,
                        "storeType": 1,
                        "storeFrontTypeId": 1,
                        "storeFrontType": 1,
                        "sellerType": 1,
                        "sellerTypeId": 1,
                        "billingAddress": 1,
                        "contactPerson": 1,
                        "businessLocationAddress": 1,
                    },
                )
                if seller_details is not None:
                    if "storeName" in seller_details:
                        dt_object = datetime.datetime.fromtimestamp(
                            seller_details["registrationDateTimeStamp"]
                        )
                        day_s = datetime.datetime.now() - dt_object
                        if day_s.days == 0:
                            if int(day_s.seconds) > 59:
                                sec = datetime.timedelta(seconds=day_s.seconds)
                                if int(sec.seconds / 60) > 59:
                                    time_create = str(int(sec.seconds / 3600)) + " hours"
                                else:
                                    time_create = str(int(sec.seconds / 60)) + " minutes"
                            else:
                                time_create = str(day_s.seconds) + " seconds"

                        else:
                            if day_s.days > 30:
                                total_month = round((day_s.days) / 30)
                                time_create = str(total_month) + " months"
                            else:
                                time_create = str(day_s.days) + " days"

                        if "contactPerson" in seller_details:
                            if "designation" in seller_details["contactPerson"]:
                                contact_person = seller_details["contactPerson"]["designation"]
                            else:
                                contact_person = ""
                            if "profilePic" in seller_details["contactPerson"]:
                                profile_pic = seller_details["contactPerson"]["profilePic"]
                            else:
                                profile_pic = ""
                        else:
                            contact_person = ""
                            profile_pic = ""

                        best_supplier["supplierName"] = (
                            seller_details["storeName"][lan]
                            if lan in seller_details["storeName"]
                            else seller_details["storeName"]["en"]
                        )
                        best_supplier["cityName"] = seller_details["cityName"]
                        best_supplier["logoImages"] = seller_details["logoImages"]
                        best_supplier["bannerImages"] = seller_details["bannerImages"]
                        try:
                            best_supplier["postCode"] = seller_details["businessLocationAddress"]["postCode"]
                        except:
                            best_supplier["postCode"] = ""

                        try:
                            best_supplier["latitude"] = seller_details["businessLocationAddress"]["lat"]
                        except:
                            best_supplier["latitude"] = ""
                        try:
                            best_supplier["longitude"] = seller_details["businessLocationAddress"]["long"]
                        except:
                            best_supplier["longitude"] = ""
                        best_supplier["rating"] = round(avg_rating_value, 2)  # TotalStoreRating
                        best_supplier["totalRating"] = len(attribute_rating_data)
                        best_supplier["reviewParameter"] = dataframe_attribute
                        best_supplier["sellerSince"] = time_create
                        best_supplier["storeAliasName"] = seller_details[
                            'storeAliasName'] if "storeAliasName" in seller_details else best_supplier["supplierName"]
                        best_supplier["storeFrontTypeId"] = seller_details[
                            "storeFrontTypeId"] if "storeFrontTypeId" in seller_details else 0
                        best_supplier["storeFrontType"] = seller_details[
                            "storeFrontType"] if "storeFrontType" in seller_details else "Central"
                        best_supplier["storeTypeId"] = seller_details[
                            "storeTypeId"] if "storeTypeId" in seller_details else 0
                        best_supplier["storeType"] = seller_details[
                            "storeType"] if "storeType" in seller_details else "Central"
                        best_supplier["sellerTypeId"] = seller_details[
                            "sellerTypeId"] if "sellerTypeId" in seller_details else 0
                        best_supplier["sellerType"] = seller_details[
                            "sellerType"] if "sellerType" in seller_details else "Central"
                        best_supplier["areaName"] = seller_details["billingAddress"][
                            "areaOrDistrict"] if "areaOrDistrict" in seller_details["billingAddress"] else "Hebbal"
                        best_supplier["street1"] = seller_details["billingAddress"]["addressLine1"] if "addressLine1" in \
                                                                                                       seller_details[
                                                                                                           "billingAddress"] else ""
                        best_supplier["city"] = seller_details["billingAddress"]["city"] if "city" in seller_details[
                            "billingAddress"] else ""
                        best_supplier["state"] = seller_details["billingAddress"]["state"] if "state" in seller_details[
                            "billingAddress"] else ""
                        best_supplier["country"] = seller_details["billingAddress"]["country"] if "country" in \
                                                                                                  seller_details[
                                                                                                      "billingAddress"] else ""
                        best_supplier["email"] = seller_details[
                            "contactEmail"] if "contactEmail" in seller_details else ""
                        best_supplier["designation"] = contact_person
                        best_supplier["profilePic"] = profile_pic
                    else:
                        pass
                else:
                    pass
            best_supplier_product = best_supplier

            ### check the product is in favourite section or not for the user
            try:
                response_casandra = session.execute(
                    """SELECT * FROM favouriteproductsuserwise where userid=%(userid)s AND childproductid=%(productid)s ALLOW FILTERING""",
                    {"userid": user_id, "productid": str(primary_child_product["_id"])},
                )

                if not response_casandra:
                    isFavourite = False
                else:
                    for fav in response_casandra:
                        isFavourite = True
            except Exception as e:
                print(e)
                isFavourite = False
                response_casandra = ""
            # ==========================================================================================================
            liked_users_cursor = db.likesProducts.find({"childproductid": str(best_supplier_product["productId"]), "isInfluencer": False}, {"userid": 1})
            liked_users_count = liked_users_cursor.count()
            user_ids = [ObjectId(like["userid"]) for like in liked_users_cursor]
            # Step 2: Find the users with the highest follower count, limit to 3, and with non-empty profile pictures
            max_follower_users = db.customer.aggregate([
                {"$match": {"_id": {"$in": user_ids}}},
                {"$project": {
                    "_id": 1,
                    "profilePic": 1
                }},
                {"$match": {"profilePic": {"$exists": True, "$ne": ""}}},  # Match only non-empty profile pics
                {"$sort": {"count.followerCount": -1}},
                {"$limit": 3}
            ])

            profile_pics = []
            for user in max_follower_users:
                profile_pic_url = user.get("profilePic")
                if profile_pic_url:
                    profile_pics.append(profile_pic_url)

             ### check the total like count for influencer and normal user
            likesByUsers=0
            likesByInfluencers=0
            if APP_NAME == "GetFudo":
                try:
                    likesByUsers=liked_users_count
                    likesByInfluencers=db.likesProducts.find({"childproductid": str(best_supplier_product["productId"]),"isInfluencer":True}).count()
                except:
                    likesByUsers=0
                    likesByInfluencers=0


            if "highlights" in primary_child_product["units"][0]:
                if primary_child_product["units"][0]["highlights"] is not None:
                    for highlight in primary_child_product["units"][0]["highlights"]:
                        try:
                            hightlight_data.append(
                                highlight[lan] if lan in highlight else highlight["en"]
                            )
                        except:
                            pass
                else:
                    pass

            hightlight_data = list(set(hightlight_data))
            hightlight_data = [x for x in hightlight_data if x]
            detail_description = (
                primary_child_product["detailDescription"][lan]
                if lan in primary_child_product["detailDescription"]
                else primary_child_product["detailDescription"]["en"]
            )

            ### check the product is in shopping list for user or not
            shoppinglist_product = db.userShoppingList.find(
                {
                    "userId": user_id,
                    "products.centralProductId": parent_productId,
                    "products.childProductId": best_supplier["productId"],
                }
            )
            if shoppinglist_product.count() == 0:
                isShoppingList = False
            else:
                for s_id in shoppinglist_product:
                    shopping_list_id = str(s_id["_id"])
                isShoppingList = True
            hard_limit = 0
            pre_order = False
            procurementTime = 0
            all_dc_list = []

            ### section for the fetch the all dc list for the products
            if store_category_id == MEAT_STORE_CATEGORY_ID:
                if is_dc_linked:
                    if len(dc_data) > 0:
                        best_dc = min(dc_data, key=lambda x: x["retailerPrice"])
                    else:
                        best_dc = {}
                    if len(best_dc) > 0:
                        dc_product_details = db.childProducts.find_one(
                            {"_id": ObjectId(best_dc["productId"])}
                        )
                        if "seller" in dc_product_details:
                            for seller in dc_product_details["seller"]:
                                if seller["storeId"] in main_sellers:  # == best_supplier['id']:
                                    if seller["preOrder"]:
                                        all_dc_list.append(seller)
                                    else:
                                        pass
                                else:
                                    pass
                        else:
                            pass
                        if len(all_dc_list) == 0:
                            if "seller" in dc_product_details:
                                for new_seller in dc_product_details["seller"]:
                                    if (
                                            new_seller["storeId"] in main_sellers
                                    ):  # == best_supplier['id']:
                                        all_dc_list.append(new_seller)
                                    else:
                                        pass
                            else:
                                pass
                        try:
                            available_stock = (
                                dc_product_details["units"][0]["availableQuantity"]
                                if "availableQuantity" in dc_product_details["units"][0]
                                else 0
                            )
                        except:
                            available_stock = 0
                    else:
                        available_stock = 0
                        pre_order = False

                    if is_dc_linked:
                        if len(all_dc_list) > 0:
                            best_seller_product = min(
                                all_dc_list, key=lambda x: x["procurementTime"]
                            )
                        else:
                            best_seller_product = {}
                    else:
                        best_seller_product = {}

                    if len(best_seller_product) > 0:
                        hard_limit = best_seller_product["hardLimit"]
                        pre_order = best_seller_product["preOrder"]
                        procurementTime = best_seller_product["procurementTime"]
                    else:
                        pass

                    # =========================================for the check delivery time=============
                    driver_roaster = next_availbale_driver_roaster(zone_id)

                    ### function for check the avaibility of the meat products
                    # check the product is in stock or not
                    # check the driver avability
                    out_of_stock, next_availbale_driver_time = meat_availability_check(
                        primary_child_product,
                        available_stock,
                        is_dc_linked,
                        driver_roaster["productText"],
                        hard_limit,
                        pre_order,
                        driver_roaster,
                        dc_product_details,
                        zone_id,
                        procurementTime,
                    )
                else:
                    available_stock = 0
                    out_of_stock = True
                    next_availbale_driver_time = ""
            else:
                if str(primary_child_product["storeId"]) == "0":
                    out_of_stock = True
                    available_stock = 0
                else:
                    try:
                        available_stock = (
                            primary_child_product["units"][0]["availableQuantity"]
                            if "availableQuantity" in primary_child_product["units"][0]
                            else 0
                        )
                    except:
                        available_stock = 0
                    if zone_id != "":
                        store_count = db.stores.find(
                            {
                                "_id": ObjectId(primary_child_product["storeId"]),
                                "serviceZones.zoneId": zone_id,
                            }
                        ).count()
                    else:
                        store_count = 1
                    if store_count > 0:
                        if available_stock > 0:
                            out_of_stock = False
                        else:
                            out_of_stock = True
                    else:
                        out_of_stock = True
                next_availbale_driver_time = ""

            # ====================for the bulk pricing for the data======================
            b2b_pricing_list = []
            best_store_data_price = best_supplier["retailerPrice"]

            ### function for the get the price of the product
            response_price = cal_product_city_pricing(login_type, city_id, primary_child_product)
            price = response_price[0]
            # check on currecny converter, currency rate is non zero then need to multiply that with price
            allPrice=primary_child_product["units"][0]["b2cPricing"]
            if float(currency_rate) > 0:
                price = price * float(currency_rate)
            base_price = price + ((price * tax_price) / 100)
            if best_supplier["id"] == "0":
                if price == 0 or price == "":
                    final_price = 0
                    discount_price = 0
                    isPrimary = True
                else:
                    if int(discount_type) == 0:
                        discount_price = float(discount_value)
                    elif int(discount_type) == 1:
                        discount_price = (float(price) * float(discount_value)) / 100
                    else:
                        discount_price = 0
                    # if float(currency_rate) > 0 and discount_type == 0:
                    #     discount_price = discount_price * float(currency_rate)

                    final_price = base_price - discount_price
            else:
                if discount_type == 0:
                    discount_price = float(discount_value)
                elif discount_type == 1:
                    discount_price = (float(price) * float(discount_value)) / 100
                else:
                    discount_price = 0

                # if float(currency_rate) > 0 and discount_type == 0:
                #     discount_price = discount_price * float(currency_rate)
                final_price = base_price - discount_price
                isPrimary = True

            model_images = (
                primary_child_product["units"][0]["modelImage"]
                if "modelImage" in primary_child_product["units"][0]
                else []
            )
            images = []
            try:
                images_list = (
                    primary_child_product["units"][0]["image"]
                    if "image" in primary_child_product["units"][0]
                    else primary_child_product["units"][0]["images"]
                )
                for image in images_list:
                    image['seqId'] = 2
                    images.append(image)
            except:
                images = []

            mobile_images = []
            try:
                mobile_images_list = (
                    primary_child_product["units"][0]["mobileImage"]
                    if "mobileImage" in primary_child_product["units"][0]
                    else primary_child_product["units"][0]["images"]
                )
                for mob_image in mobile_images_list:
                    mob_image['seqId'] = 2
                    mobile_images.append(mob_image)
            except:
                mobile_images = []

            for mob_img in model_images:
                try:
                    mob_img['seqId'] = 1
                    images.append(mob_img)
                except:
                    pass
                try:
                    mob_img['seqId'] = 1
                    mobile_images.append(mob_img)
                except:
                    pass

            if len(mobile_images) > 0:
                mobile_images = sorted(mobile_images, key=lambda k: k["seqId"], reverse=False)
            else:
                pass

            if len(images) > 0:
                images = sorted(images, key=lambda k: k["seqId"], reverse=False)
            else:
                pass
            try:
                uploadARModel = primary_child_product["units"][0]["uploadIosModel"][0]["small"]
            except:
                uploadARModel = ""

            # ====================================product details==================================================
            try:
                avgvolume = primary_child_product["units"][0]["avgvolume"]
            except:
                avgvolume = ""

            try:
                avgvolumeunit = primary_child_product["units"][0]["avgvolumeunit"]
            except:
                avgvolumeunit = ""

            product_volumn_unit = avgvolume + " " + avgvolumeunit
            # ===================================end of the unit===================================================
            # ===================================start the mou=====================================================
            minimum_purchase = 1
            package_units = ""
            package_type = ""

            if package_units != "" and package_type != "":
                minimum_purchase_qty = int(minimum_purchase)
                minimum_purchase_details = minimum_purchase + " " + package_units
            else:
                minimum_purchase_details = str(minimum_purchase)
                minimum_purchase_qty = int(minimum_purchase)

            mou_unit = None

            if int(login_type) == 1 or int(login_type) != 2:
                if b2c_packing_units == "":
                    mou_data = {"mesurmentQuantity": b2c_packing_package_units}

                elif b2c_packing_units != "":
                    mou_data = {"mesurmentQuantity": b2c_packing_units}
                else:
                    mou_data = package_type
            else:
                mou_data = {
                    "mou": minimum_purchase_details,
                    "mouUnit": mou_unit,
                    "mesurmentQuantity": b2c_unit_package_type,
                    "mouQty": minimum_purchase_qty,
                    "minimumPurchaseUnit": minimum_purchase_unit,
                }

            res = {}
            for d in link_to_unit:
                res.setdefault(d["keyName"], []).append(
                    {
                        "childProductId": d["childProductId"],
                        "size": d["size"],
                        "keyName": d["keyName"],
                        "unitData": d["unitData"],
                        "rgb": d["rgb"],
                        "isPrimary": d["isPrimary"],
                        "finalPriceList": d["finalPriceList"] if "finalPriceList" in d else {},
                        "unitId": d["unitId"],
                        "name": d["name"],
                        "visible": d["visible"],
                        "seqId": d["seqId"] if "seqId" in d else 1,
                        "image": d["image"],
                        "extraLarge": d["extraLarge"],
                        "outOfStock": d["outOfStock"],
                        "availableStock": d["availableStock"],
                    }
                )

            ### need to remove duplicate variant base on primaty and visible value
            for color in list(set(link_to_unit_list)):
                try:
                    newlist = sorted(res[color], key=lambda k: (k["isPrimary"], k["visible"]))
                    try:
                        newlist = sorted(res[color], key=lambda k: (k["finalPriceList"]))
                    except:
                        pass
                    dataframe_details = pd.DataFrame(newlist)
                    try:
                        dataframe_details = dataframe_details.drop_duplicates("size", keep="last")
                    except:
                        dataframe_details = dataframe_details.drop_duplicates("name", keep="last")
                    color_data = dataframe_details.to_json(orient="records")
                    color_data = json.loads(color_data)
                    try:
                        if color == "Sizes":
                            color_data = sorted(color_data, key=lambda k: k["seqId"], reverse=False)
                        else:
                            color_data = sorted(color_data, key=lambda k: k["name"], reverse=False)
                    except Exception as ex:
                        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
                        color_data = color_data

                    if not any(d["isPrimary"] == True for d in res[color]):
                        isPrimary = False
                    else:
                        isPrimary = True

                    for res_color in res[color]:
                        if res_color["size"] != "":
                            last_color_json.append(
                                {
                                    "name": color,
                                    "keyName": res_color["keyName"],
                                    "rgb": res_color["rgb"],
                                    "unitData": res_color["unitData"],
                                    "childProductId": res_color["childProductId"],
                                    "sizeData": color_data,
                                    "image": res_color["image"],
                                    "unitId": res_color["unitId"],
                                    "extraLarge": res_color["extraLarge"],
                                    "isPrimary": isPrimary,
                                }
                            )
                except:
                    pass

            if len(last_color_json) > 0:
                dataframe_details = pd.DataFrame(last_color_json)
                try:
                    dataframe_details = dataframe_details.drop_duplicates("keyName", keep="last")
                except:
                    dataframe_details = dataframe_details.drop_duplicates("name", keep="last")
                name_data = dataframe_details.to_json(orient="records")
                new_name_data = json.loads(name_data)
            else:
                new_name_data = []
            if size_group is not None:
                for value in size_group["sizeGroup"]:
                    size_value = []
                    for s in value["value"]:
                        size_value.append(s[lan] if lan in s else s["en"])
                    key_name = (
                        value["keyName"][0][lan]
                        if lan in value["keyName"][0]
                        else value["keyName"][0]["en"]
                    )
                    sizeChart.append({"name": key_name, "size": size_value})
                if "description" in size_group:
                    if size_group["description"] is not None:
                        if "en" in size_group["description"]:
                            size_description = size_group["description"]["en"]
                        else:
                            size_description = ""
                    else:
                        size_description = ""
                else:
                    size_description = ""
            else:
                size_description = ""

            isSizeChart = is_size_chart
            try:
                share_link = (
                        WEBSITE_URL
                        + "product/"
                        + product_name.replace(
                    " ",
                    "-",
                )
                        + "?pid="
                        + product_id
                        + "&cpid="
                        + parent_productId
                )
            except:
                share_link = ""

            replacement_policy = (
                primary_child_product["replacementPolicy"]
                if "replacementPolicy" in primary_child_product
                else {"isReplacement": False}
            )
            warranty = (
                primary_child_product["warranty"] if "warranty" in primary_child_product else {}
            )
            term_condition = (
                primary_child_product["term&condition"]
                if "term&condition" in primary_child_product
                else {}
            )
            exchange_policy = (
                primary_child_product["exchangePolicy"]
                if "exchangePolicy" in primary_child_product
                else {"isExchange": False}
            )
            return_policy = (
                primary_child_product["returnPolicy"]
                if "returnPolicy" in primary_child_product
                else {"isReturn": False}
            )
            try:
                percentage_price = final_price - best_store_data_price
                per_value = round((100 * percentage_price) / final_price, 2)
            except:
                per_value = 0

            try:
                product_name = product_name
            except:
                product_name = ""

            try:
                new_category_path = sorted(category_path, key=lambda k: k["level"], reverse=False)
            except:
                new_category_path = []

            # ===================== MOQ data for show the details on pdp page=====================
            whole_sale_price = []
            if int(login_type) == 2:
                end_price = 1
                if city_id != "":
                    central_product_details = db.childProducts.find_one(
                        {
                            "units.unitId": primary_child_product['units'][0]['unitId'],
                            "storeId": "0"
                        },
                        {
                            "b2bCityPricing": 1
                        }
                    )
                    if central_product_details is not None:
                        if "b2bCityPricing" in central_product_details:
                            city_price = {}
                            for x in central_product_details['b2bCityPricing']:
                                if x['cityId'] == city_id:
                                    city_price = x
                                else:
                                    pass
                            if "afterTax" in city_price:
                                if city_price['afterTax'] is not None:
                                    for b2b_price in city_price['afterTax']:
                                        if float(currency_rate) > 0:
                                            priceBeforeTax = b2b_price["priceAfterTax"] * float(currency_rate)
                                        else:
                                            priceBeforeTax = b2b_price["priceAfterTax"]
                                        whole_sale_price.append(
                                            {
                                                "price": round(priceBeforeTax, 2),
                                                "b2buptoQty": str(end_price)
                                                              + "-"
                                                              + str(b2b_price["toQuantity"])
                                                              + " "
                                                              + unit_moq_type,
                                                "unitPackageType": unit_moq_type,
                                                "currency": currency,
                                                "currencySymbol": currency_symbol,
                                            }
                                        )
                                        end_price = b2b_price["toQuantity"] + 1
                                else:
                                    if "b2bPricing" in primary_child_product["units"][0]:
                                        for b2b_price in primary_child_product["units"][0]["b2bPricing"]:
                                            if float(currency_rate) > 0:
                                                b2bproductSellingPrice = b2b_price["b2bproductSellingPrice"] * float(
                                                    currency_rate)
                                            else:
                                                b2bproductSellingPrice = b2b_price["b2bproductSellingPrice"]
                                            whole_sale_price.append(
                                                {
                                                    "price": round(b2bproductSellingPrice, 2),
                                                    "b2buptoQty": str(end_price)
                                                                  + "-"
                                                                  + str(b2b_price["b2buptoQty"])
                                                                  + " "
                                                                  + unit_moq_type,
                                                    "unitPackageType": unit_moq_type,
                                                    "currency": currency,
                                                    "currencySymbol": currency_symbol,
                                                }
                                            )
                                            end_price = b2b_price["b2buptoQty"] + 1
                                    else:
                                        pass
                            else:
                                if "b2bPricing" in primary_child_product["units"][0]:
                                    for b2b_price in primary_child_product["units"][0]["b2bPricing"]:
                                        if float(currency_rate) > 0:
                                            b2bproductSellingPrice = b2b_price["b2bproductSellingPrice"] * float(
                                                currency_rate)
                                        else:
                                            b2bproductSellingPrice = b2b_price["b2bproductSellingPrice"]
                                        whole_sale_price.append(
                                            {
                                                "price": round(b2bproductSellingPrice, 2),
                                                "b2buptoQty": str(end_price)
                                                              + "-"
                                                              + str(b2b_price["b2buptoQty"])
                                                              + " "
                                                              + unit_moq_type,
                                                "unitPackageType": unit_moq_type,
                                                "currency": currency,
                                                "currencySymbol": currency_symbol,
                                            }
                                        )
                                        end_price = b2b_price["b2buptoQty"] + 1
                                else:
                                    pass
                        else:
                            if "b2bPricing" in primary_child_product["units"][0]:
                                for b2b_price in primary_child_product["units"][0]["b2bPricing"]:
                                    if float(currency_rate) > 0:
                                        b2bproductSellingPrice = b2b_price["b2bproductSellingPrice"] * float(
                                            currency_rate)
                                    else:
                                        b2bproductSellingPrice = b2b_price["b2bproductSellingPrice"]
                                    whole_sale_price.append(
                                        {
                                            "price": round(b2bproductSellingPrice, 2),
                                            "b2buptoQty": str(end_price)
                                                          + "-"
                                                          + str(b2b_price["b2buptoQty"])
                                                          + " "
                                                          + unit_moq_type,
                                            "unitPackageType": unit_moq_type,
                                            "currency": currency,
                                            "currencySymbol": currency_symbol,
                                        }
                                    )
                                    end_price = b2b_price["b2buptoQty"] + 1
                            else:
                                pass
                    else:
                        if "b2bPricing" in primary_child_product["units"][0]:
                            for b2b_price in primary_child_product["units"][0]["b2bPricing"]:
                                if float(currency_rate) > 0:
                                    b2bproductSellingPrice = b2b_price["b2bproductSellingPrice"] * float(
                                        currency_rate)
                                else:
                                    b2bproductSellingPrice = b2b_price["b2bproductSellingPrice"]
                                whole_sale_price.append(
                                    {
                                        "price": round(b2bproductSellingPrice, 2),
                                        "b2buptoQty": str(end_price)
                                                      + "-"
                                                      + str(b2b_price["b2buptoQty"])
                                                      + " "
                                                      + unit_moq_type,
                                        "unitPackageType": unit_moq_type,
                                        "currency": currency,
                                        "currencySymbol": currency_symbol,
                                    }
                                )
                                end_price = b2b_price["b2buptoQty"] + 1
                        else:
                            pass
                else:
                    if "b2bPricing" in primary_child_product["units"][0]:
                        for b2b_price in primary_child_product["units"][0]["b2bPricing"]:
                            if float(currency_rate) > 0:
                                b2bproductSellingPrice = b2b_price["b2bproductSellingPrice"] * float(
                                    currency_rate)
                            else:
                                b2bproductSellingPrice = b2b_price["b2bproductSellingPrice"]
                            whole_sale_price.append(
                                {
                                    "price": round(b2bproductSellingPrice, 2),
                                    "b2buptoQty": str(end_price)
                                                  + "-"
                                                  + str(b2b_price["b2buptoQty"])
                                                  + " "
                                                  + unit_moq_type,
                                    "unitPackageType": unit_moq_type,
                                    "currency": currency,
                                    "currencySymbol": currency_symbol,
                                }
                            )
                            end_price = b2b_price["b2buptoQty"] + 1
                    else:
                        pass
            else:
                pass

            if "substitute" in primary_child_product:
                substitute_length = len(primary_child_product["substitute"])
            else:
                substitute_length = 0

            if substitute_length > 0:
                is_substitute_available = True
            else:
                is_substitute_available = False

            unit_product_name = product_name
            if len(attr_json) > 0:
                dataframe_attribute = pd.DataFrame(attr_json)
                dataframe_attribute = dataframe_attribute.drop_duplicates("name", keep="first")
                dataframe_attribute = dataframe_attribute.to_json(orient="records")
                attr_json_data = json.loads(dataframe_attribute)
            else:
                attr_json_data = []

            if len(attr_html_json) > 0:
                dataframe_attribute = pd.DataFrame(attr_html_json)
                dataframe_attribute = dataframe_attribute.drop_duplicates("name", keep="first")
                dataframe_attribute = dataframe_attribute.to_json(orient="records")
                attr_html_json_data = json.loads(dataframe_attribute)
            else:
                attr_html_json_data = []

            # =====================================for prefrence data=======================================
            prefrence_data = db.cart.find_one(
                {
                    "products._id": best_supplier_product["productId"],
                    "user.userId": user_id,
                    "cartLogs.status": 0,
                    "products.substituteProduct": {"$exists": True},
                }
            )
            substitute_product = {}
            if prefrence_data is not None:
                for inner_product in prefrence_data["products"]:
                    if inner_product["_id"] == best_supplier_product["productId"]:
                        if "substituteProduct" in inner_product:
                            substitute_product = inner_product["substituteProduct"]
                        else:
                            pass
                    else:
                        pass
            else:
                pass

            if "substitute" in primary_child_product:
                if len(primary_child_product["substitute"]) > 0:
                    is_substitute = True
                else:
                    is_substitute = False
            else:
                is_substitute = False

            # ======================add recent search=============================================
            if int(is_search) != 1:
                thread_logs = threading.Thread(
                    target=category_search_logs,
                    args=(
                        "",
                        "",
                        "",
                        "4",
                        user_id,
                        static_json_data["seachPlatform"],
                        static_json_data["ipAddress"],
                        0,
                        0,
                        static_json_data["cityName"],
                        static_json_data["countryName"],
                        unit_product_name,
                        store_category_id,
                        "",
                        "",
                        best_supplier_product["id"],
                        True,
                        parent_productId,
                        best_supplier_product["productId"],
                    ),
                )
                thread_logs.start()
            else:
                pass

            if best_supplier_product["id"] == "0" and store_category_id == MEAT_STORE_CATEGORY_ID:
                out_of_stock = True
                available_stock = 0
            elif (
                    hyperlocal == True and storelisting == False and best_supplier_product["id"] == "0"
            ):
                out_of_stock = True
                allow_out_of_stock = False
                available_stock = 0

            additional_info = []
            if "THC" in primary_child_product["units"][0]:
                additional_info.append(
                    {
                        "seqId": 2,
                        "attrname": "THC",
                        "value": str(primary_child_product["units"][0]["THC"]) + " %",
                    }
                )
            else:
                pass
            if "CBD" in primary_child_product["units"][0]:
                additional_info.append(
                    {
                        "seqId": 1,
                        "attrname": "CBD",
                        "value": str(primary_child_product["units"][0]["CBD"]) + " %",
                    }
                )
            else:
                pass

            # =================================================canniber product type========================
            if primary_child_product is not None:
                if "cannabisProductType" in primary_child_product["units"][0]:
                    if primary_child_product["units"][0]["cannabisProductType"] != "":
                        cannabis_type_details = db.cannabisProductType.find_one(
                            {
                                "_id": ObjectId(
                                    primary_child_product["units"][0]["cannabisProductType"]
                                ),
                                "status": 1,
                            }
                        )
                        if cannabis_type_details is not None:
                            additional_info.append(
                                {
                                    "seqId": 3,
                                    "attrname": "Type",
                                    "value": cannabis_type_details["productType"]["en"],
                                    "id": primary_child_product["units"][0]["cannabisProductType"],
                                }
                            )
                        else:
                            pass
                else:
                    pass
            else:
                pass

            product_type = combo_special_type_validation(
                best_supplier_product["productId"]
            )  # get product type, is normal or combo or special product
            combo_product_details = []
            ### section for the get the combo product details for the product
            ## this part will execute only for product_type 2 which means combo products
            if product_type == 2:
                for combo in primary_child_product["comboProducts"]:
                    combo_product_offers_details = []
                    if combo["productId"] != "":
                        combo_product = db.childProducts.find_one(
                            {"_id": ObjectId(combo["productId"]), "status": 1}
                        )
                        if combo_product is None:
                            combo_product = db.childProducts.find_one(
                                {
                                    "parentProductId": combo["productId"],
                                    "status": 1,
                                }
                            )
                        if combo_product is not None:
                            try:
                                if combo_product is not None:
                                    combo_product_price = combo_product["units"][0]["b2cPricing"][0][
                                        "b2cproductSellingPrice"
                                    ]
                                else:
                                    combo_product_price = combo_product["units"][0]["floatValue"]
                            except:
                                combo_product_price = combo_product["units"][0]["floatValue"]
                            # ==================================get currecny rate============================
                            if float(currency_rate) > 0:
                                combo_product_price = combo_product_price * float(currency_rate)
                            else:
                                pass
                            if "offer" in combo_product:
                                for combo_product_offer in combo_product["offer"]:
                                    combo_offer_query = {"_id": ObjectId(combo_product_offer["offerId"]), "status": 1}
                                    if str(combo_product["storeId"]) != "":
                                        combo_offer_query["storeId"] = combo_product["storeId"]
                                    combo_product_offer_details = db.offers.find_one(combo_offer_query)
                                    if combo_product_offer_details is not None:
                                        if combo_product_offer["status"] == 1:
                                            if combo_product_offer_details["startDateTime"] <= int(
                                                    time.time()
                                            ):
                                                if combo_product_offer_details is not None:
                                                    combo_product_offer[
                                                        "termscond"
                                                    ] = combo_product_offer_details["termscond"]
                                                else:
                                                    combo_product_offer["termscond"] = ""
                                                combo_product_offer[
                                                    "name"
                                                ] = combo_product_offer_details["name"]["en"]
                                                combo_product_offer[
                                                    "discountValue"
                                                ] = combo_product_offer["discountValue"]
                                                combo_product_offer[
                                                    "discountType"
                                                ] = combo_product_offer["discountType"]
                                                combo_product_offers_details.append(combo_product_offer)
                                        else:
                                            pass
                                    else:
                                        pass
                            if len(combo_product_offers_details) > 0:
                                combo_product_best_offer = max(
                                    combo_product_offers_details, key=lambda x: x["discountValue"]
                                )
                            else:
                                combo_product_best_offer = {}

                            if len(combo_product_best_offer) == 0:
                                combo_percentage = 0
                            else:
                                if "discountType" in combo_product_best_offer:
                                    if combo_product_best_offer["discountType"] == 0:
                                        combo_percentage = 0
                                    else:
                                        combo_percentage = int(combo_product_best_offer["discountValue"])
                                else:
                                    combo_percentage = 0

                            if len(combo_product_best_offer) > 0:
                                combo_discount_type = (
                                    int(combo_product_best_offer["discountType"])
                                    if "discountType" in combo_product_best_offer
                                    else 0
                                )
                            else:
                                combo_discount_type = 2

                            # ==========================unit data================================================================
                            combo_tax_price = 0
                            combo_tax_value = []
                            if store_category_id != DINE_STORE_CATEGORY_ID:
                                if type(combo_product["tax"]) == list:
                                    for combo_tax in combo_product["tax"]:
                                        combo_tax_value.append({"value": combo_tax["taxValue"]})
                                else:
                                    if combo_product["tax"] is not None:
                                        if "taxValue" in combo_product["tax"]:
                                            combo_tax_value.append(
                                                {"value": combo_product["tax"]["taxValue"]}
                                            )
                                        else:
                                            combo_tax_value.append({"value": combo_product["tax"]})
                                    else:
                                        pass

                                if store_category_id != DINE_STORE_CATEGORY_ID:
                                    if len(combo_tax_value) == 0:
                                        combo_tax_price = 0
                                    else:
                                        for amount in combo_tax_value:
                                            combo_tax_price = combo_tax_price + (int(amount["value"]))
                                else:
                                    combo_tax_price = 0
                            else:
                                combo_tax_price = 0

                            combo_product_images = (
                                combo_product["units"][0]["image"]
                                if "image" in combo_product["units"][0]
                                else combo_product["units"][0]["images"]
                            )
                            try:
                                combo_product_mobile_images = (
                                    combo_product["units"][0]["mobileImage"]
                                    if "mobileImage" in combo_product["units"][0]
                                    else combo_product["units"][0]["images"]
                                )
                            except:
                                combo_product_mobile_images = []
                            combo_product_model_images = (
                                combo_product["units"][0]["modelImage"]
                                if "modelImage" in combo_product["units"][0]
                                else []
                            )
                            combo_product_price = combo_product_price + (
                                    (combo_product_price * combo_tax_price) / 100
                            )
                            combo_discount_price = 0
                            combo_final_price = combo_product_price - combo_discount_price
                            combo_available_stock = combo_product["units"][0]["availableQuantity"]
                            if combo_available_stock == 0:
                                combo_out_of_stock = True
                            else:
                                combo_out_of_stock = False

                            try:
                                product_name = combo_product["units"][0]["unitName"][lan]
                            except:
                                product_name = combo_product["units"][0]["unitName"]["en"]

                            combo_product_details.append(
                                {
                                    "productName": product_name,
                                    "finalPriceList": {
                                        "basePrice": round(combo_product_price, 2),
                                        "finalPrice": round(combo_final_price, 2),
                                        "discountPrice": combo_discount_price,
                                        "discountType": combo_discount_type,
                                        "discountPercentage": combo_percentage,
                                    },
                                    "childProductId": str(combo_product["_id"]),
                                    "parentProductId": str(combo_product["parentProductId"]),
                                    "images": combo_product_images,
                                    "mobileImage": combo_product_mobile_images,
                                    "modelImage": combo_product_model_images,
                                    "currency": combo_product["currency"],
                                    "currencySymbol": combo_product["currencySymbol"],
                                    "offers": combo_product_best_offer,
                                    "offer": combo_product_best_offer,
                                    "outOfStock": combo_out_of_stock,
                                    "availableQuantity": combo_available_stock,
                                    "unitId": combo_product["units"][0]["unitId"],
                                    "maxQuantityCombo": combo["minimumQuantity"],
                                }
                            )
                        else:
                            pass
                    else:
                        pass
            else:
                pass

            ### calculate offer and offer price for combo type products
            buy_x_get_x_product_details = []
            if len(best_offer) > 0:
                if "discountType" in best_offer:
                    if int(best_offer['discountType']) == 2:
                        best_offer_details = db.offers.find_one(
                            {
                                "_id": ObjectId(best_offer['offerId'])
                            }
                        )
                        if best_offer_details is not None:
                            for combo in best_offer_details["comboProducts"]:
                                combo_product_offers_details = []
                                combo_product = db.childProducts.find_one(
                                    {"_id": ObjectId(combo["productId"]), "status": 1}
                                )
                                if combo_product is None:
                                    combo_product = db.childProducts.find_one(
                                        {
                                            "parentProductId": combo["productId"],
                                            # "storeId": ObjectId(store_id),
                                            "status": 1,
                                        }
                                    )
                                if combo_product is not None:
                                    try:
                                        if combo_product is not None:
                                            combo_product_price = combo_product["units"][0]["b2cPricing"][0][
                                                "b2cproductSellingPrice"
                                            ]
                                        else:
                                            combo_product_price = combo_product["units"][0]["floatValue"]
                                    except:
                                        combo_product_price = combo_product["units"][0]["floatValue"]
                                    # ==================================get currecny rate============================
                                    if float(currency_rate) > 0:
                                        combo_product_price = combo_product_price * float(currency_rate)
                                    else:
                                        pass
                                    if "offer" in combo_product:
                                        for combo_product_offer in combo_product["offer"]:
                                            combo_product_offer_details = db.offers.find_one(
                                                {"_id": ObjectId(combo_product_offer["offerId"]), "status": 1}
                                            )
                                            if combo_product_offer_details is not None:
                                                if combo_product_offer["status"] == 1:
                                                    if combo_product_offer_details["startDateTime"] <= int(
                                                            time.time()
                                                    ):
                                                        if combo_product_offer_details is not None:
                                                            combo_product_offer[
                                                                "termscond"
                                                            ] = combo_product_offer_details["termscond"]
                                                        else:
                                                            combo_product_offer["termscond"] = ""
                                                        combo_product_offer[
                                                            "name"
                                                        ] = combo_product_offer_details["name"]["en"]
                                                        combo_product_offer[
                                                            "discountValue"
                                                        ] = combo_product_offer["discountValue"]
                                                        combo_product_offer[
                                                            "discountType"
                                                        ] = combo_product_offer["discountType"]
                                                        combo_product_offers_details.append(combo_product_offer)
                                                else:
                                                    pass
                                            else:
                                                pass
                                    if len(combo_product_offers_details) > 0:
                                        combo_product_best_offer = max(
                                            combo_product_offers_details, key=lambda x: x["discountValue"]
                                        )
                                        com_offer_details = db.offers.find(
                                            {"_id": ObjectId(combo_product_best_offer["offerId"]), "status": 1}
                                        ).count()
                                        if com_offer_details != 0:
                                            combo_product_best_offer = combo_product_best_offer
                                        else:
                                            combo_product_best_offer = {}
                                    else:
                                        combo_product_best_offer = {}

                                    if len(combo_product_best_offer) == 0:
                                        combo_percentage = 0
                                    else:
                                        if "discountType" in combo_product_best_offer:
                                            if combo_product_best_offer["discountType"] == 0:
                                                combo_percentage = 0
                                            else:
                                                combo_percentage = int(
                                                    combo_product_best_offer["discountValue"]
                                                )
                                        else:
                                            combo_percentage = 0

                                    if len(combo_product_best_offer) > 0:
                                        combo_discount_type = (
                                            int(combo_product_best_offer["discountType"])
                                            if "discountType" in combo_product_best_offer
                                            else 0
                                        )
                                        combo_discount_value = combo_product_best_offer["discountValue"]
                                    else:
                                        combo_discount_value = 0
                                        combo_discount_type = 2

                                    # ==========================unit data================================================================
                                    combo_tax_price = 0
                                    combo_tax_value = []
                                    if store_category_id != DINE_STORE_CATEGORY_ID:
                                        if type(combo_product["tax"]) == list:
                                            for combo_tax in combo_product["tax"]:
                                                combo_tax_value.append({"value": combo_tax["taxValue"]})
                                        else:
                                            if combo_product["tax"] is not None:
                                                if "taxValue" in combo_product["tax"]:
                                                    combo_tax_value.append(
                                                        {"value": combo_product["tax"]["taxValue"]}
                                                    )
                                                else:
                                                    combo_tax_value.append({"value": combo_product["tax"]})
                                            else:
                                                pass

                                        if store_category_id != DINE_STORE_CATEGORY_ID:
                                            if len(combo_tax_value) == 0:
                                                combo_tax_price = 0
                                            else:
                                                for amount in combo_tax_value:
                                                    combo_tax_price = combo_tax_price + (int(amount["value"]))
                                        else:
                                            combo_tax_price = 0
                                    else:
                                        combo_tax_price = 0

                                    combo_product_images = (
                                        combo_product["units"][0]["image"]
                                        if "image" in combo_product["units"][0]
                                        else combo_product["units"][0]["images"]
                                    )
                                    try:
                                        combo_product_mobile_images = (
                                            combo_product["units"][0]["mobileImage"]
                                            if "mobileImage" in combo_product["units"][0]
                                            else combo_product["units"][0]["images"]
                                        )
                                    except:
                                        combo_product_mobile_images = []
                                    combo_product_model_images = (
                                        combo_product["units"][0]["modelImage"]
                                        if "modelImage" in combo_product["units"][0]
                                        else []
                                    )
                                    combo_product_price = combo_product_price + (
                                            (combo_product_price * combo_tax_price) / 100
                                    )
                                    if combo_discount_type == 0:
                                        combo_discount_price = float(combo_discount_value)
                                    elif combo_discount_type == 1:
                                        combo_discount_price = (
                                                                       float(combo_product_price) * float(
                                                                   combo_discount_value)
                                                               ) / 100
                                    else:
                                        combo_discount_price = 0
                                    combo_final_price = combo_product_price - combo_discount_price
                                    combo_available_stock = combo_product["units"][0]["availableQuantity"]
                                    if combo_available_stock == 0:
                                        combo_out_of_stock = True
                                    else:
                                        combo_out_of_stock = False

                                    try:
                                        product_name = combo_product["units"][0]["unitName"][lan]
                                    except:
                                        product_name = combo_product["units"][0]["unitName"]["en"]

                                    buy_x_get_x_product_details.append(
                                        {
                                            "productName": product_name,
                                            "finalPriceList": {
                                                "basePrice": round(combo_product_price, 2),
                                                "finalPrice": round(combo_final_price, 2),
                                                "discountPrice": combo_discount_price,
                                                "discountType": combo_discount_type,
                                                "discountPercentage": combo_percentage,
                                            },
                                            "childProductId": str(combo_product["_id"]),
                                            "parentProductId": str(combo_product["parentProductId"]),
                                            "images": combo_product_images,
                                            "mobileImage": combo_product_mobile_images,
                                            "modelImage": combo_product_model_images,
                                            "currency": combo_product["currency"],
                                            "currencySymbol": combo_product["currencySymbol"],
                                            "offers": combo_product_best_offer,
                                            "offer": combo_product_best_offer,
                                            "outOfStock": combo_out_of_stock,
                                            "availableQuantity": combo_available_stock,
                                            "unitId": combo_product["units"][0]["unitId"],
                                            "maxQuantityCombo": combo["minimumQuantity"],
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

            if len(additional_info) > 0:
                additional_info = sorted(additional_info, key=lambda k: k["seqId"], reverse=True)
            else:
                additional_info = []

            if len(attr_json_data) > 0:
                new_attr_json_data = sorted(attr_json_data, key=lambda k: k["seqId"], reverse=False)
            else:
                new_attr_json_data = []

            try:
                reseller_commission = primary_child_product['units'][0]['b2cPricing'][0]['b2cresellerCommission']
            except:
                reseller_commission = 0

            reseller_commission_type = 0
            if "containsMeat" in primary_child_product:
                contains_Meat = primary_child_product["containsMeat"]
            else:
                contains_Meat = False
            if "productTag" in primary_child_product["units"][0]:
                productTag = primary_child_product["units"][0]["productTag"]
            else:
                productTag ={}
            ### make the response for the pdp page
            ## add all keys which we need to send in api
            response_data = [
                {
                    "parentProductId": parent_productId,
                    "containsMeat": contains_Meat,
                    "productTag":productTag,
                    "childProductId": best_supplier_product["productId"],
                    "comboProducts": combo_product_details,
                    "productType": product_type,
                    "offerProductDetails": buy_x_get_x_product_details,
                    "unitId": primary_child_product["units"][0]["unitId"],
                    "storeCategoryId": primary_child_product["storeCategoryId"],
                    "cashOnDelivery": False,
                    "shareLink": share_link,
                    "replacementPolicy": replacement_policy,
                    "maxQuantity": quantity_json,
                    "maxQuantityPerUser": max_quantity,
                    "warranty": warranty,
                    "resellerCommission": reseller_commission,
                    "resellerCommissionType": reseller_commission_type,
                    "term&condition": term_condition,
                    "sizeChartDescription": size_description,
                    "isSubstituteAvailable": is_substitute_available,
                    "substituteProduct": substitute_product,
                    "exchangePolicy": exchange_policy,
                    "extraAttributeDetails": additional_info,
                    "returnPolicy": return_policy,
                    "productName": unit_product_name,
                    "brandName": brand_name,
                    "categoryPath": new_category_path,
                    "percentageValue": per_value,
                    "isShoppingList": isShoppingList,
                    "shoppingListId": shopping_list_id,
                    "blogId": linked_blogs,
                    "highlight": hightlight_data,
                    "isSubstitute": is_substitute,
                    "catName": "",
                    "subCatName": "",
                    "subSubCatName": "",
                    "isStoreClose": store_tag,
                    "uploadARModel": uploadARModel,
                    "productVolumn": product_volumn_unit,
                    "iosARModel": iosARModel,
                    "androidARModel": androidARModel,
                    "manufactureName": manufacture_name,
                    "currency": currency,
                    "currencySymbol": currency_symbol,
                    "b2bBulkPricing": b2b_pricing_list,
                    "allOffers": [],  # offer_data,
                    "attributes": new_attr_json_data,
                    "htmlAttributes": attr_html_json_data,
                    "customizable": customizable_attributes,
                    "images": primary_child_product['images'] if "images" in primary_child_product else [] ,
                    "mobileImage": mobile_images,
                    "modelImage": model_images,
                    "overviewData": static_data,
                    "offers": best_offer,
                    "offer": best_offer,
                    "mouData": mou_data,
                    "mouDataUnit": mouDataUnit,
                    "linkedVariant": linked_to_attribute_variant,
                    "isFavourite": isFavourite,
                    "LikesByUsers":likesByUsers,
                    "needsFreezer": primary_child_product['needsFreezer'] if "needsFreezer" in primary_child_product else False,
                    "LikesByInfluencers":likesByInfluencers,
                    "variants": new_name_data,
                    "sellerCount": seller_count,
                    "MOQData": {
                        "minimumOrderQty": minimum_order_qty,
                        "unitPackageType": unit_package_type,
                        "unitMoqType": unit_moq_type,
                        "MOQ": moq_data
                    },
                    "allowOrderOutOfStock": allow_out_of_stock,
                    "finalPriceList": {
                        "basePrice": round(price, 2),
                        "finalPrice": round(final_price, 2),
                        "discountPrice": round(discount_price, 2),
                        "discountType": discount_type,
                        "discountPercentage": percentage,
                    },
                    "allPrice":allPrice,
                    "availableQuantity": available_stock,
                    "wholeSalePrice": whole_sale_price,
                    "supplier": best_supplier_product,
                    "suppliers": best_supplier_product,
                    "productSeo": product_seo,
                    "sizeChart": sizeChart,
                    "isSizeChart": isSizeChart,
                    "detailDesc": detail_description,
                    "outOfStock": out_of_stock,
                    "prescriptionRequired": prescription_required,
                    "needsIdProof": needsIdProof,
                    "saleOnline": sales_online,
                    "nextSlotTime": next_availbale_driver_time,
                    "uploadProductDetails": upload_details,
                    "isAddOns": addons_count,
                    "likeUserProfile" :profile_pics
                }
            ]
            response = {"data": response_data, "message": "Products Data Found"}
            return response
    except Exception as ex:
        traceback.print_exc()
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
        response = {"data": [], "message": "Products Data Not Found"}
        return response


def substitute_products(productId, parent_productId, lan, login_type, currency_code, res28):
    header = {"language": lan, "authorization": "something", "currencycode": currency_code}
    substitue_response = requests.get(
        PYTHON_BASE_URL
        + "python/substitute/products?parentProductId="
        + str(parent_productId)
        + "&productId="
        + str(productId),
        headers=header,
    )
    if substitue_response.status_code == 200:
        response = {
            "data": substitue_response.json()["data"],
            "totalCount": substitue_response.json()["totalCount"],
        }
        return response
    else:
        response = {"data": [], "totalCount": 0}
        return response


"""
    Function for the add the recent view product in the cassandra
"""


def recent_view_add(
        parent_productId,
        productId,
        user_id,
        ip_address,
        latitude,
        longitude,
        city_name,
        country_name,
        seach_platform,
        session_id,
):
    timestamp_data = int(datetime.datetime.now().timestamp())
    print('productId---',productId)
    print('user_id---',user_id)
    store_category = db.childProducts.find_one(
        {"_id": ObjectId(productId)}, {"storeCategoryId": 1, "storeId": 1}
    )
    if store_category != None:
        data = {
            "productId": productId,
            "userid": user_id,
            "storeid": str(store_category["storeId"]),
            "store_category_id": store_category["storeCategoryId"],
            "createdtimestamp": int(timestamp_data * 1000),
            "ipAddress": ip_address,
            "latitute": latitude,
            "longitude": longitude,
            "cityName": city_name,
            "countryName": country_name,
            "seach_platform": seach_platform,
            "session_id": session_id,
        }
        product_count = db.userRecentView.find(
            {"productId": productId, "userid": user_id, "storeid": str(store_category["storeId"])}
        ).count()
        if product_count == 0:
            db.userRecentView.insert(data)
        else:
            db.userRecentView.update(
                {
                    "productId": productId,
                    "userid": user_id,
                    "storeid": str(store_category["storeId"]),
                },
                {
                    "$set": {
                        "createdtimestamp": int(timestamp_data * 1000),
                    }
                },
                upsert=False,
            )
    response = {"message": "data inserted successfully...!!!"}
    return response


"""
    Function for the add the referral logs in the cassandra
"""


def referral_view_logs(
        parent_productId,
        productId,
        user_id,
        ip_address,
        latitude,
        longitude,
        city_name,
        country_name,
        seach_platform,
        session_id,
        referral_id,
):
    timestamp_data = int(datetime.datetime.now().timestamp())
    store_category = db.childProducts.find_one({"_id": ObjectId(productId)})
    referral_user_details = referral_db.referralCodes.find_one({"referralCode": referral_id})
    if referral_user_details is not None:
        if store_category != None and user_id != referral_user_details["userId"]:
            main_user_details = db.customers.find_one({"_id": ObjectId(user_id)})
            refer_user_details = db.customers.find_one(
                {"_id": ObjectId(referral_user_details["userId"])}
            )
            if main_user_details is not None and refer_user_details is not None:
                db.referrallogs.insert(
                    {
                        "parentproductid": parent_productId,
                        "childproductid": str(productId),
                        "productName": store_category["units"][0]["unitName"],
                        "userid": user_id,
                        "shareuserid": referral_user_details["userId"],
                        "userName": main_user_details["firstName"] + main_user_details["lastName"],
                        "referUserName": refer_user_details["firstName"]
                                         + refer_user_details["lastName"],
                        "referralid": referral_id,
                        "store_category_id": store_category["storeCategoryId"],
                        "platform": int(seach_platform),
                        "ipaddress": str(ip_address),
                        "latitude": str(latitude),
                        "longitude": str(longitude),
                        "cityname": city_name,
                        "countryname": country_name,
                        "session_id": session_id,
                        "storeid": str(store_category["storeId"]),
                        "createdtimestamp": timestamp_data * 1000,
                    }
                )
            else:
                pass
        else:
            pass
    else:
        pass
    response = {"message": "data inserted successfully...!!!"}
    return response


"""
    API for the get the product details
    :parameter ---> productId (get the details of this products)
                    parentProductId (parent product id of the product)
    @discountType:
        offerType == 0: Flat Discount
        offerType == 1: Percentage Discount
        offerType == 2: Normal
"""


class ProductDetails(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Products"],
        operation_description="API for getting product details for the PDP page",
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
            openapi.Parameter(
                name="loginType",
                default="1",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="login type of the user. value should be 1 for retailer and 2 for distributor",
            ),
            openapi.Parameter(
                name="ipAddress",
                default="124.40.244.94",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="ip address of the network",
            ),
            openapi.Parameter(
                name="latitude",
                default="12.9716",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="latitude of the user where website or application opened",
            ),
            openapi.Parameter(
                name="longitude",
                default="77.5946",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="longitude of the user where website or application opened",
            ),
            openapi.Parameter(
                name="platform",
                default="0",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="from which platform requested for data.values will be 0 for website, 1 for iOS and 2 for android",
            ),
            openapi.Parameter(
                name="city",
                default="Mumbai",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="city id of the user where browser or app opened if not there value should be empty string",
            ),
            openapi.Parameter(
                name="country",
                default="India",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="country id of the user where browser or app opened if not there value should be empty string",
            ),
            openapi.Parameter(
                name="productId",
                required=True,
                default="5dfa190dbd309205c3ccde98",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="child product id of the product(variant id of the product)",
            ),
            openapi.Parameter(
                name="referralId",
                required=False,
                default="PRJC4",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="referral id of the by whom the product is shared",
            ),
            openapi.Parameter(
                name="cityId",
                default="5df7b7218798dc2c1114e6bf",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="city id from which city we need to show price, for city pricing, mainly we are using for meat flow",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5df8b7628798dc19d926bd29",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular zone",
            ),
            openapi.Parameter(
                name="storeId",
                default="5ed0c7861db3c601bdbe23b6",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular zone",
            ),
            openapi.Parameter(
                name="parentProductId",
                default="5df85105e80e605065d3cdfe",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="central product id of the product",
            ),
            openapi.Parameter(
                name="isSearch",
                default="1",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="if product opening from product suggestion that time need to send value as 0 and if click on product from any other pages value should be 1",
            ),
        ],
        responses={
            200: "successfully. product data found",
            401: "Unauthorized. token expired",
            404: "data not found. it might be product not found or product details not found",
            422: "required fields are not found. it might be child product id not found or central product id not found",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            start_time = time.time()
            token = request.META["HTTP_AUTHORIZATION"]
            'login type we are using for the check which type user is currectly logged in app or web app' \
            '0 for guest user, 1 for normal user and 2 for b2b(institute) buyers'
            try:
                login_type = json.loads(token)["metaData"]["institutionType"]
            except:
                login_type = 1

            currency_code = request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            productId = request.GET["productId"]  # main product id, using this id need to fetch the product details
            parent_productId = request.GET["parentProductId"]  # parent product id of the product
            referral_id = request.GET.get("referralId",
                                          "")  # referral id of the user, if someone reffered the product to another user
            store_id = request.GET.get("storeId", "")

            # from where the product is opening
            # product opening from product suggestion value is 0
            # product from any other pages value is 1
            is_search = request.GET.get("isSearch", "1")
            # ==========================================headers which send by client side================================================
            ip_address = request.META["HTTP_IPADDRESS"] if "HTTP_IPADDRESS" in request.META else ""

            # from which platform requested for data.
            # values will be 0 for website, 1 for iOS and 2 for android
            seach_platform = request.META["HTTP_PLATFORM"] if "HTTP_PLATFORM" in request.META else "0"
            city_name = request.META["HTTP_CITY"] if "HTTP_CITY" in request.META else ""
            city_id = request.GET.get("cityId", "5df7b7218798dc2c1114e6bf")
            zone_id = request.META["HTTP_ZONEID"] if "HTTP_ZONEID" in request.META else ""
            country_name = request.META["HTTP_COUNTRY"] if "HTTP_COUNTRY" in request.META else ""

            ### making json for static data which we need to use in product view (user's recent view)
            static_json_data = {
                "ipAddress": ip_address,
                "seachPlatform": seach_platform,
                "cityName": city_name,
                "countryName": country_name,
            }
            try:
                latitude = (float(request.META["HTTP_LATITUDE"]) if "HTTP_LATITUDE" in request.META else 0)
            except:
                latitude = 0
            try:
                longitude = (
                    float(request.META["HTTP_LONGITUDE"]) if "HTTP_LONGITUDE" in request.META else 0
                )
            except:
                longitude = 0

            ### need to check token is empty or not
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)

            ### need to check product id is empty or not
            elif productId == "" or productId == "undefined":
                response_data = {
                    "message": "child product id blank",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)

            ### need to check central product id is empty or not
            elif parent_productId == "" or parent_productId == "undefined":
                response_data = {
                    "message": "central product id blank",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                try:
                    session_id = json.loads(token)["sessionId"]
                except:
                    session_id = ""
                user_id = json.loads(token)["userId"]
                lan = request.META["HTTP_LANGUAGE"]

                ### call get product details function for get the all details regarding the product
                # which needs to show in pdp page
                ### return pdp page response, rating review of the products, substitute product details for the product
                product_response, review_response, substitute_response = self.get_prod_details(login_type,
                                                                                               currency_code, productId,
                                                                                               parent_productId,
                                                                                               store_id, is_search,
                                                                                               city_id, zone_id,
                                                                                               static_json_data,
                                                                                               user_id, lan)

                ### call add recent view logs in database
                threading.Thread(target=self.start_product_access_log, args=(
                    productId, parent_productId, referral_id, ip_address, seach_platform, city_name, country_name,
                    latitude,
                    longitude, session_id, user_id,)).start()

                if len(product_response["data"]) == 0:
                    response = {
                        "data": {
                            "productData": {},
                            "review": review_response,
                            "substituteData": substitute_response,
                        }
                    }
                    return JsonResponse(response, safe=False, status=404)
                else:
                    response = {
                        "data": {
                            "productData": product_response,
                            "review": review_response,
                            "substituteData": substitute_response,
                        }
                    }
                    print("total time for pdp page response.....!!!!", time.time() - start_time)
                    return JsonResponse(response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)

    def start_product_access_log(self, productId, parent_productId, referral_id, ip_address, seach_platform, city_name,
                                 country_name, latitude, longitude, session_id, user_id):
        thread1 = threading.Thread(
            target=recent_view_add,
            args=(
                parent_productId,
                productId,
                user_id,
                ip_address,
                latitude,
                longitude,
                city_name,
                country_name,
                seach_platform,
                session_id,
            ),
        )
        thread1.start()
        if referral_id != "":
            thread2 = threading.Thread(
                target=referral_view_logs,
                args=(
                    parent_productId,
                    productId,
                    user_id,
                    ip_address,
                    latitude,
                    longitude,
                    city_name,
                    country_name,
                    seach_platform,
                    session_id,
                    referral_id,
                ),
            )
            thread2.start()
        else:
            pass

    def get_prod_details(self, login_type, currency_code, productId, parent_productId, store_id, is_search, city_id,
                         zone_id, static_json_data, user_id, lan):
        ## function for the get the product details
        product_response = product_details_data(
            productId,
            parent_productId,
            lan,
            user_id,
            login_type,
            zone_id,
            store_id,
            is_search,
            static_json_data,
            currency_code, city_id,
            res19,
        )
        ## function for the get the product review and rating
        review_response = product_reviews(parent_productId, lan, 0, 3, user_id, res20)

        ## function for the get the substitute product details
        substitute_response = substitute_products(
            productId, parent_productId, lan, login_type, currency_code, res28
        )
        return product_response, review_response, substitute_response


"""
API for the get the size chart for the product based on product id
"""


class SizeChart(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Products"],
        operation_description="API for getting the size chart of the product",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="language",
                default="en",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="language",
            ),
            openapi.Parameter(
                name="centralProductId",
                default="5df88294e80e605065d3ce1e",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="central product id of the product",
            ),
        ],
        responses={
            200: "successfully. size chart found for the product",
            404: "data not found. it might be size chart not found for the product",
            422: "required fields are not found. it might be central product id not found",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            sizeChart = []
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            product_id = request.GET.get("centralProductId", "")
            if product_id == "":
                response = {"message": "Product not found", "data": sizeChart}
                return JsonResponse(response, safe=False, status=422)
            else:
                cat_name = ""
                sub_cat_name = ""
                sub_sub_cat_name = ""
                product_details = db.products.find_one(
                    {"_id": ObjectId(product_id)}, {"categoryList": 1}
                )
                if "categoryList" in product_details:
                    if len(product_details["categoryList"]) > 0:
                        for cat in product_details["categoryList"][0]["parentCategory"][
                            "childCategory"
                        ]:
                            try:
                                parent_details = db.category.find_one(
                                    {"_id": ObjectId(cat["categoryId"])},
                                    {"parentId": 1, "categoryName": 1},
                                )
                                if parent_details != None:
                                    parent_details_1 = db.category.find_one(
                                        {"_id": ObjectId(parent_details["parentId"])},
                                        {"parentId": 1, "categoryName": 1},
                                    )
                                    if parent_details_1 != None:
                                        if "parentId" in parent_details_1:
                                            parent_details_2 = db.category.find_one(
                                                {"_id": ObjectId(parent_details_1["parentId"])},
                                                {"parentId": 1, "categoryName": 1},
                                            )
                                            if parent_details_2 != None:
                                                if "parentId" not in parent_details_2:
                                                    cat_name = parent_details_2["categoryName"][
                                                        "en"
                                                    ]
                                                    sub_cat_name = parent_details_1["categoryName"][
                                                        "en"
                                                    ]
                                                    sub_sub_cat_name = parent_details[
                                                        "categoryName"
                                                    ]["en"]
                                                else:
                                                    parent_details_3 = db.category.find_one(
                                                        {
                                                            "_id": ObjectId(
                                                                parent_details_2["parentId"]
                                                            )
                                                        },
                                                        {"parentId": 1, "categoryName": 1},
                                                    )
                                                    if "parentId" not in parent_details_3:
                                                        cat_name = parent_details_3["categoryName"][
                                                            "en"
                                                        ]
                                                        sub_cat_name = parent_details_2[
                                                            "categoryName"
                                                        ]["en"]
                                                        sub_sub_cat_name = parent_details_1[
                                                            "categoryName"
                                                        ]["en"]
                                                    else:
                                                        pass
                                            else:
                                                cat_name = (
                                                    parent_details_2["categoryName"]["en"]
                                                ).replace(" ", "%20")
                                                sub_cat_name = (
                                                    parent_details_1["categoryName"]["en"]
                                                ).replace(" ", "%20")
                                                sub_sub_cat_name = (
                                                    parent_details["categoryName"]["en"]
                                                ).replace(" ", "%20")
                                        else:
                                            cat_name = (
                                                parent_details_1["categoryName"]["en"]
                                            ).replace(" ", "%20")
                                            sub_cat_name = (
                                                parent_details["categoryName"]["en"]
                                            ).replace(" ", "%20")
                                    else:
                                        cat_name = (parent_details_1["categoryName"]["en"]).replace(
                                            " ", "%20"
                                        )
                                        sub_cat_name = (
                                            parent_details["categoryName"]["en"]
                                        ).replace(" ", "%20")
                                else:
                                    cat_name = (parent_details["categoryName"]["en"]).replace(
                                        " ", "%20"
                                    )
                            except Exception as ex:
                                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                                message = template.format(type(ex).__name__, ex.args)
                                print(
                                    "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                                    type(ex).__name__,
                                    ex,
                                )
                                pass

                    if sub_sub_cat_name != "" and sub_cat_name != "" and cat_name != "":
                        size_group = db.sizeGroup.find_one(
                            {
                                "categories.categoryName.en": cat_name,
                                "categories.categoryName.en": sub_cat_name,
                                "categories.categoryName.en": sub_sub_cat_name,
                                "status": 1,
                            },
                            {"sizeGroup": 1},
                        )
                    elif sub_sub_cat_name == "" and sub_cat_name != "" and cat_name != "":
                        size_group = db.sizeGroup.find_one(
                            {
                                "categories.categoryName.en": cat_name,
                                "categories.categoryName.en": sub_cat_name,
                                "status": 1,
                            },
                            {"sizeGroup": 1},
                        )
                    else:
                        size_group = db.sizeGroup.find_one(
                            {"categories.categoryName.en": cat_name, "status": 1}, {"sizeGroup": 1}
                        )
                else:
                    size_group = None

                if size_group != None:
                    for value in size_group["sizeGroup"]:
                        size_value = []
                        for s in value["value"]:
                            size_value.append(s[language] if language in s else s["en"])
                        key_name = (
                            value["keyName"][0][language]
                            if language in value["keyName"][0]
                            else value["keyName"][0]["en"]
                        )
                        sizeChart.append({"name": key_name, "size": size_value})
                    response = {"message": "Size chart found", "data": sizeChart}
                    return JsonResponse(response, safe=False, status=200)
                else:
                    response = {"message": "Size chart not found", "data": sizeChart}
                    return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error_message = {"error": "Invalid request", "message": message, "total_count": 0}
            return JsonResponse(error_message, status=500)


"""
API for the get the size chart for the product based on product id
"""


class ProductSizeChart(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Products"],
        operation_description="API for getting the size chart of the product",
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
                name="childProductId",
                default="5efa08a2568601c20549cd67",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="child product id of the product",
            ),
        ],
        responses={
            200: "successfully. size chart found for the product",
            404: "data not found. it might be size chart not found for the product",
            422: "required fields are not found. it might be central product id not found",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            sizeChart = []
            token = (
                request.META["HTTP_AUTHORIZATION"] if "HTTP_AUTHORIZATION" in request.META else ""
            )
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            product_id = request.GET.get("childProductId", "")
            if product_id == "":
                response = {"message": "Product not found", "data": sizeChart}
                return JsonResponse(response, safe=False, status=422)
            else:
                user_id = json.loads(token)["userId"]
                cat_name = ""
                sub_cat_name = ""
                sub_sub_cat_name = ""
                size_group = None
                product_details = db.childProducts.find_one(
                    {"_id": ObjectId(product_id)},
                    {"categoryList": 1, "units": 1, "parentProductId": 1, "storeId": 1},
                )
                if "sizeChartId" in product_details["units"][0]:
                    if (
                            product_details["units"][0]["sizeChartId"] != ""
                            and product_details["units"][0]["sizeChartId"] is not None
                    ):
                        size_query = {"_id": ObjectId(product_details["units"][0]["sizeChartId"])}
                        size_group = db.sizeGroup.find_one(size_query)
                    else:
                        central_product_details = db.products.find_one(
                            {"_id": ObjectId(product_details["parentProductId"])}
                        )
                        if central_product_details is not None:
                            if "sizeChartId" in central_product_details["units"][0]:
                                if (
                                        central_product_details["units"][0]["sizeChartId"] != ""
                                        or central_product_details["units"][0]["sizeChartId"]
                                        is not None
                                ):
                                    size_query = {
                                        "_id": ObjectId(
                                            central_product_details["units"][0]["sizeChartId"]
                                        )
                                    }
                                    size_group = db.sizeGroup.find_one(size_query)
                else:
                    if "categoryList" in product_details:
                        if len(product_details["categoryList"]) > 0:
                            for cat in product_details["categoryList"][0]["parentCategory"][
                                "childCategory"
                            ]:
                                try:
                                    parent_details = db.category.find_one(
                                        {"_id": ObjectId(cat["categoryId"])},
                                        {"parentId": 1, "categoryName": 1},
                                    )
                                    if parent_details != None:
                                        parent_details_1 = db.category.find_one(
                                            {"_id": ObjectId(parent_details["parentId"])},
                                            {"parentId": 1, "categoryName": 1},
                                        )
                                        if parent_details_1 != None:
                                            if "parentId" in parent_details_1:
                                                parent_details_2 = db.category.find_one(
                                                    {"_id": ObjectId(parent_details_1["parentId"])},
                                                    {"parentId": 1, "categoryName": 1},
                                                )
                                                if parent_details_2 != None:
                                                    if "parentId" not in parent_details_2:
                                                        cat_name = parent_details_2["categoryName"][
                                                            "en"
                                                        ]
                                                        sub_cat_name = parent_details_1[
                                                            "categoryName"
                                                        ]["en"]
                                                        sub_sub_cat_name = parent_details[
                                                            "categoryName"
                                                        ]["en"]
                                                    else:
                                                        parent_details_3 = db.category.find_one(
                                                            {
                                                                "_id": ObjectId(
                                                                    parent_details_2["parentId"]
                                                                )
                                                            },
                                                            {"parentId": 1, "categoryName": 1},
                                                        )
                                                        if "parentId" not in parent_details_3:
                                                            cat_name = parent_details_3[
                                                                "categoryName"
                                                            ]["en"]
                                                            sub_cat_name = parent_details_2[
                                                                "categoryName"
                                                            ]["en"]
                                                            sub_sub_cat_name = parent_details_1[
                                                                "categoryName"
                                                            ]["en"]
                                                        else:
                                                            pass
                                                else:
                                                    cat_name = (
                                                        parent_details_2["categoryName"]["en"]
                                                    ).replace(" ", "%20")
                                                    sub_cat_name = (
                                                        parent_details_1["categoryName"]["en"]
                                                    ).replace(" ", "%20")
                                                    sub_sub_cat_name = (
                                                        parent_details["categoryName"]["en"]
                                                    ).replace(" ", "%20")
                                            else:
                                                cat_name = (
                                                    parent_details_1["categoryName"]["en"]
                                                ).replace(" ", "%20")
                                                sub_cat_name = (
                                                    parent_details["categoryName"]["en"]
                                                ).replace(" ", "%20")
                                        else:
                                            cat_name = (
                                                parent_details_1["categoryName"]["en"]
                                            ).replace(" ", "%20")
                                            sub_cat_name = (
                                                parent_details["categoryName"]["en"]
                                            ).replace(" ", "%20")
                                    else:
                                        cat_name = (parent_details["categoryName"]["en"]).replace(
                                            " ", "%20"
                                        )
                                except Exception as ex:
                                    template = (
                                        "An exception of type {0} occurred. Arguments:\n{1!r}"
                                    )
                                    message = template.format(type(ex).__name__, ex.args)
                                    print(
                                        "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                                        type(ex).__name__,
                                        ex,
                                    )
                                    pass

                        if sub_sub_cat_name != "" and sub_cat_name != "" and cat_name != "":
                            size_group = db.sizeGroup.find_one(
                                {
                                    "categories.categoryName.en": cat_name,
                                    "categories.categoryName.en": sub_cat_name,
                                    "categories.categoryName.en": sub_sub_cat_name,
                                    "status": 1,
                                }
                            )
                        elif sub_sub_cat_name == "" and sub_cat_name != "" and cat_name != "":
                            size_group = db.sizeGroup.find_one(
                                {
                                    "categories.categoryName.en": cat_name,
                                    "categories.categoryName.en": sub_cat_name,
                                    "status": 1,
                                }
                            )
                        else:
                            size_group = db.sizeGroup.find_one(
                                {"categories.categoryName.en": cat_name, "status": 1}
                            )
                    else:
                        size_group = None

                if size_group != None:
                    existing_sizes = []
                    selected_sizes = []
                    product_name_details = []
                    fav_product = []
                    child_product_ids = []
                    parent_product_ids = []
                    unit_ids = []
                    store_ids = []
                    out_of_stock = []
                    store_category_ids = []
                    best_offer_data = []
                    final_price_data = []
                    base_price_data = []
                    discount_value_data = []
                    discount_price_data = []
                    description = ""
                    try:
                        description = size_group["description"][language]
                    except:
                        try:
                            description = size_group["description"]["en"]
                        except:
                            description = ""
                    for value in range(0, len(size_group["sizeGroup"])):
                        size_value = []
                        if value == 0:
                            for main_size in range(0, len(size_group["sizeGroup"][value]["value"])):
                                child_product_query = {
                                    "parentProductId": product_details["parentProductId"],
                                    "units.unitSizeGroupValue.en": size_group["sizeGroup"][value][
                                        "value"
                                    ][main_size][language]
                                    if language
                                       in size_group["sizeGroup"][value]["value"][main_size]
                                    else size_group["sizeGroup"][value]["value"][main_size]["en"],
                                    "status": 1,
                                }
                                if product_details["units"][0]["colorName"] != "":
                                    child_product_query["units.colorName"] = product_details[
                                        "units"
                                    ][0]["colorName"]
                                if str(product_details["storeId"]) == "0":
                                    child_product_query["storeId"] = "0"
                                else:
                                    child_product_query["storeId"] = ObjectId(
                                        product_details["storeId"]
                                    )
                                child_product_count = db.childProducts.find_one(child_product_query)
                                if child_product_count is not None:
                                    if str(child_product_count["_id"]) == product_id:
                                        selected_sizes.append(True)
                                    else:
                                        selected_sizes.append(False)
                                    if child_product_count["units"][0]["availableQuantity"] == 0:
                                        out_of_stock.append(True)
                                    else:
                                        out_of_stock.append(False)
                                    existing_sizes.append(main_size)
                                    child_product_ids.append(str(child_product_count["_id"]))
                                    parent_product_ids.append(
                                        str(child_product_count["parentProductId"])
                                    )
                                    unit_ids.append(str(child_product_count["units"][0]["unitId"]))
                                    store_ids.append(str(child_product_count["storeId"]))
                                    store_category_ids.append(
                                        str(child_product_count["storeCategoryId"])
                                    )
                                    size_value.append(
                                        size_group["sizeGroup"][value]["value"][main_size][language]
                                        if language
                                           in size_group["sizeGroup"][value]["value"][main_size]
                                        else size_group["sizeGroup"][value]["value"][main_size][
                                            "en"
                                        ]
                                    )
                                    # =====================================offer data===================================
                                    best_offer = product_best_offer_data(
                                        str(child_product_count["_id"])
                                    )
                                    if "b2cPricing" in child_product_count["units"][0]:
                                        price = child_product_count["units"][0]["b2cPricing"][0][
                                            "b2cproductSellingPrice"
                                        ]
                                    else:
                                        price = child_product_count["units"][0]["floatValue"]

                                    if len(best_offer) > 0:
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
                                        discount_type = 2
                                        discount_value = 0

                                    # ==================================get currecny rate============================
                                    product_name = (
                                        child_product_count["units"][0]["unitName"][language]
                                        if language in child_product_count["units"][0]["unitName"]
                                        else child_product_count["units"][0]["unitName"]["en"]
                                    )
                                    product_name_details.append(product_name)
                                    # ===============================faviourite products========================================================
                                    isFavourite = False
                                    response_casandra = session.execute(
                                        """SELECT * FROM favouriteproductsuserwise where userid=%(userid)s AND childproductid=%(productid)s ALLOW FILTERING""",
                                        {
                                            "userid": user_id,
                                            "productid": str(child_product_count["_id"]),
                                        },
                                    )

                                    if not response_casandra:
                                        isFavourite = False
                                    else:
                                        for fav in response_casandra:
                                            isFavourite = True
                                    fav_product.append(isFavourite)
                                    currency_symbol = child_product_count["currencySymbol"]
                                    currency = child_product_count["currencySymbol"]
                                    if price == 0 or price == "":
                                        final_price = 0
                                        discount_price = 0
                                    else:
                                        if discount_type == 0:
                                            discount_price = float(discount_value)
                                        elif discount_type == 1:
                                            discount_price = (
                                                                     float(price) * float(discount_value)
                                                             ) / 100
                                        else:
                                            discount_price = 0

                                    tax_value = []
                                    if type(child_product_count["tax"]) == list:
                                        for tax in child_product_count["tax"]:
                                            if "taxValue" in tax:
                                                tax_value.append({"value": tax["taxValue"]})
                                    else:
                                        if child_product_count["tax"] != None:
                                            if "taxValue" in child_product_count["tax"]:
                                                tax_value.append(
                                                    {
                                                        "value": child_product_count["tax"][
                                                            "taxValue"
                                                        ]
                                                    }
                                                )
                                            else:
                                                tax_value.append(
                                                    {"value": child_product_count["tax"]}
                                                )
                                        else:
                                            pass
                                    tax_price = 0
                                    if len(tax_value) == 0:
                                        tax_price = 0
                                    else:
                                        for amount in tax_value:
                                            tax_price = tax_price + (int(amount["value"]))

                                    final_price = price - discount_price
                                    final_price = final_price + ((final_price * tax_price) / 100)
                                    final_price_data.append(round(final_price, 2))
                                    base_price_data.append(round(price, 2))
                                    discount_price_data.append(round(discount_price, 2))
                                    discount_value_data.append(round(discount_value, 2))
                                    best_offer_data.append(best_offer)
                        else:
                            for s in range(0, len(size_group["sizeGroup"][value]["value"])):
                                if s in existing_sizes:
                                    size_value.append(
                                        size_group["sizeGroup"][value]["value"][s][language]
                                        if language in size_group["sizeGroup"][value]["value"][s]
                                        else size_group["sizeGroup"][value]["value"][s]["en"]
                                    )

                        key_name = (
                            size_group["sizeGroup"][value]["keyName"][0][language]
                            if language in size_group["sizeGroup"][value]["keyName"][0]
                            else size_group["sizeGroup"][value]["keyName"][0]["en"]
                        )
                        sizeChart.append({"name": key_name, "size": size_value})

                    sizeChart.append({"name": "parentProductIds", "size": parent_product_ids})
                    sizeChart.append({"name": "childProductIds", "size": child_product_ids})
                    sizeChart.append({"name": "unitIds", "size": unit_ids})
                    sizeChart.append({"name": "storeIds", "size": store_ids})
                    sizeChart.append({"name": "storeCategoryIds", "size": store_category_ids})
                    sizeChart.append({"name": "outOfStock", "size": out_of_stock})
                    sizeChart.append({"name": "isFavourite", "size": fav_product})
                    sizeChart.append({"name": "selectedSizes", "size": selected_sizes})
                    sizeChart.append({"name": "finalPrice", "size": final_price_data})
                    sizeChart.append({"name": "basePrice", "size": base_price_data})
                    sizeChart.append({"name": "discountPrice", "size": discount_price_data})
                    sizeChart.append({"name": "discountValue", "size": discount_value_data})
                    sizeChart.append({"name": "productName", "size": product_name_details})
                    # sizeChart.append({
                    #     "name": "offerData",
                    #     "size": best_offer_data
                    # })
                    response = {
                        "description": description,
                        "message": "Size chart found",
                        "data": sizeChart,
                    }
                    return JsonResponse(response, safe=False, status=200)
                else:
                    response = {
                        "message": "Size chart not found",
                        "description": "",
                        "data": sizeChart,
                    }
                    return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error_message = {"error": "Invalid request", "message": message, "total_count": 0}
            return JsonResponse(error_message, status=500)


class SellerList(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Products"],
        operation_description="API for suggestion the product name based on user search",
        required=["AUTHORIZATION"],
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
                default="1",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="login type of the user. value should be 1 for retailer and 2 for distributor",
            ),
            openapi.Parameter(
                name="productId",
                required=True,
                default="5dfa190dbd309205c3ccde98",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="child product id of the product(variant id of the product)",
            ),
            openapi.Parameter(
                name="parentProductId",
                default="5df85105e80e605065d3cdfe",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="central product id of the product",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5df8b6ea8798dc19d926bd28",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="zone id if need to get the zone seller only",
            ),
            openapi.Parameter(
                name="cityId",
                default="5df7b7218798dc2c1114e6bf",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular city",
            ),
        ],
        responses={
            200: "successfully. seller data found",
            401: "Unauthorized. token expired",
            404: "data not found. it might be seller not found for the products",
            422: "required fields are not found. it might be child product id not found or central product id not found",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        global hide_more_seller
        try:
            start_time = time.time()
            supplier_list = []
            variant_product_details = {}
            tax_price = 0
            final_supplier_list = []
            token = (
                request.META["HTTP_AUTHORIZATION"] if "HTTP_AUTHORIZATION" in request.META else ""
            )
            lan = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            login_type = request.META["HTTP_LOGINTYPE"] if "HTTP_LOGINTYPE" in request.META else 1
            city_id = (
                request.META["HTTP_CITYID"]
                if "HTTP_CITYID" in request.META
                else "5df7b7218798dc2c1114e6bf"
            )
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            central_productId = request.GET.get("parentProductId", "")
            varient_productId = request.GET.get("productId", "")
            zone_id = request.GET.get("zoneId", "")
            if central_productId == "":
                response_data = {
                    "message": "parent product id empty",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            elif varient_productId == "":
                response_data = {
                    "message": "product id empty",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                child_product_data = db.childProducts.find_one({"_id": ObjectId(varient_productId)})
                if child_product_data is not None:
                    try:
                        main_unit_id = str(child_product_data["units"][0]["unitId"])
                    except:
                        main_unit_id = ''
                        print('no unit.unitId found')
                else:
                    main_unit_id = ''
                    print('no unit.unitId found')
                central_product = db.products.find_one(
                    {"_id": ObjectId(central_productId)},
                    {
                        "replacementPolicy": 1,
                        "warranty": 1,
                        "images": 1,
                        "storeCategoryId": 1,
                        "term&condition": 1,
                        "productSeo": 1,
                        "cashOnDelivery": 1,
                        "exchangePolicy": 1,
                        "returnPolicy": 1,
                        "offer": 1,
                        "currency": 1,
                        "currencySymbol": 1,
                        "avgRating": 1,
                        "units": 1,
                        "childProducts": 1,
                    },
                )
                offers_details = []
                tax_value = []
                variant_product_details = db.childProducts.find_one(
                    {"_id": ObjectId(varient_productId)}
                )
                if variant_product_details is not None:
                    if "offer" in variant_product_details:
                        for offer in variant_product_details["offer"]:
                            if login_type == 1:
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

                    if len(offers_details) > 0:
                        best_offer = max(offers_details, key=lambda x: x["discountValue"])
                        offer_details = db.offers.find(
                            {"_id": ObjectId(best_offer["offerId"]), "status": 1}
                        ).count()
                        if offer_details != 0:
                            best_offer = best_offer
                        else:
                            best_offer = {}
                    else:
                        best_offer = {}

                    if len(best_offer) == 0:
                        percentage = 0
                        discount_type = 0
                    else:
                        if "discountType" in best_offer:
                            if best_offer["discountType"] == 0:
                                percentage = 0
                                discount_type = 0
                            else:
                                percentage = (
                                    best_offer["discountValue"]
                                    if "discountValue" in best_offer
                                    else 0
                                )
                                discount_type = best_offer["discountType"]
                        else:
                            percentage = 0
                            discount_type = 0

                    if len(best_offer) > 0:
                        discount_type = (
                            int(best_offer["discountType"]) if "discountType" in best_offer else 0
                        )
                        discount_value = (
                            best_offer["discountValue"] if "discountValue" in best_offer else 0
                        )
                    else:
                        discount_type = 2

                    # ==========================unit data================================================================
                    if int(login_type) == 1 or int(login_type) != 2:
                        if "b2cPricing" in variant_product_details["units"][0]:
                            price = variant_product_details["units"][0]["b2cPricing"][0][
                                "b2cproductSellingPrice"
                            ]
                        else:
                            price = variant_product_details["units"][0]["floatValue"]
                    else:
                        if "b2bPricing" in supplier:
                            price = variant_product_details["units"][0]["b2bPricing"][0][
                                "b2bproductSellingPrice"
                            ]
                            if price == "":
                                price = 0
                        else:
                            price = 0

                    # ==================================get currecny rate============================
                    try:
                        currency_rate = currency_exchange_rate[
                            str(variant_product_details["currency"]) + "_" + str(currency_code)
                            ]
                    except:
                        currency_rate = 0
                    currency_details = db.currencies.find_one({"currencyCode": currency_code})
                    if currency_details is not None:
                        currency_symbol = currency_details["currencySymbol"]
                        currency = currency_details["currencySymbol"]
                    else:
                        currency_symbol = variant_product_details["currencySymbol"]
                        currency = variant_product_details["currencySymbol"]

                    if currency is None:
                        currency = variant_product_details["currencySymbol"]

                    if currency_symbol is None:
                        currency_symbol = variant_product_details["currencySymbol"]
                    if float(currency_rate) > 0:
                        price = price * float(currency_rate)
                    else:
                        pass
                    if price == 0 or price == "":
                        final_price = 0
                        discount_price = 0
                        isPrimary = True
                    else:
                        if discount_type == 0:
                            discount_price = float(discount_value)
                        elif discount_type == 1:
                            discount_price = (float(price) * float(discount_value)) / 100
                        else:
                            discount_price = 0

                    if variant_product_details != None:
                        if type(variant_product_details["tax"]) == list:
                            for tax in variant_product_details["tax"]:
                                tax_value.append({"value": tax["taxValue"]})
                        else:
                            if variant_product_details["tax"] != None:
                                if "taxValue" in variant_product_details["tax"]:
                                    tax_value.append(
                                        {"value": variant_product_details["tax"]["taxValue"]}
                                    )
                                else:
                                    tax_value.append({"value": variant_product_details["tax"]})
                            else:
                                pass
                    else:
                        tax_value = []

                    if variant_product_details["storeCategoryId"] != DINE_STORE_CATEGORY_ID:
                        if len(tax_value) == 0:
                            tax_price = 0
                        else:
                            for amount in tax_value:
                                tax_price = tax_price + (int(amount["value"]))
                    else:
                        tax_price = 0

                    final_price = price - discount_price
                    final_price = final_price + ((final_price * tax_price) / 100)
                    out_of_stock = (
                        False
                        if int(variant_product_details["units"][0]["availableQuantity"]) > 0
                        else True
                    )
                    # ===============product rating=================
                    avg_product_rating_value = 0
                    product_rating = db.reviewRatings.aggregate(
                        [
                            {
                                "$match": {
                                    "productId": str(variant_product_details["parentProductId"]),
                                    "rating": {"$ne": 0},
                                    "status": 1,
                                }
                            },
                            {"$group": {"_id": "$orderId", "avgRating": {"$avg": "$rating"}}},
                        ]
                    )
                    for avg_product_rating in product_rating:
                        avg_product_rating_value = avg_product_rating["avgRating"]
                    try:
                        avg_product_rating_value_new = round(avg_product_rating_value, 2)
                    except:
                        avg_product_rating_value_new = 0
                    variant_product_details = {
                        "parentProductId": variant_product_details["parentProductId"],
                        "currencySymbol": currency_symbol,
                        "currency": currency,
                        "avgRating": avg_product_rating_value_new,
                        "TotalStarRating": avg_product_rating_value_new,
                        "images": variant_product_details["units"][0]["image"]
                        if "image" in variant_product_details["units"][0]
                        else [],
                        "childProductId": str(variant_product_details["_id"]),
                        "availableQuantity": variant_product_details["units"][0][
                            "availableQuantity"
                        ],
                        "outOfStock": out_of_stock,
                        "productName": variant_product_details["units"][0]["unitName"][lan],
                        "finalPrice": round(final_price, 2),
                        "finalPriceList": {
                            "basePrice": round(price, 2),
                            "finalPrice": round(final_price, 2),
                            "discountPrice": round(discount_price, 2),
                            "discountPercentage": percentage,
                        },
                    }

                if central_product is not None:
                    if central_product["storeCategoryId"] != MEAT_STORE_CATEGORY_ID:
                        # if len(res_varient_parameters) > 0:
                        store_category_id = central_product["storeCategoryId"]
                        store_category_details = db.storeCategory.find_one(
                            {"_id": ObjectId(store_category_id)}
                        )
                        store_type = 0
                        if store_category_details is not None:
                            store_type = store_category_details["type"]

                        product_tag = ""
                        store_list_json = []
                        if zone_id != "":
                            stores_list = db.stores.find(
                                {"status": 1, "serviceZones.zoneId": zone_id}
                            )
                            for s in stores_list:
                                store_list_json.append(str(s["_id"]))
                        else:
                            pass

                        if (
                                zone_id != ""
                                and central_product["storeCategoryId"] != ECOMMERCE_STORE_CATEGORY_ID
                        ):
                            city_details = db.zones.find_one(
                                {"_id": ObjectId(zone_id)}, {"city_ID": 1}
                            )
                            categoty_details = db.cities.find_one(
                                {
                                    "storeCategory.storeCategoryId": store_category_id,
                                    "_id": ObjectId(city_details["city_ID"]),
                                },
                                {"storeCategory": 1},
                            )
                        else:
                            categoty_details = db.cities.find_one(
                                {"storeCategory.storeCategoryId": store_category_id},
                                {"storeCategory": 1},
                            )
                        remove_central = False
                        if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                            is_ecommerce = True
                            remove_central = False
                            hide_recent_view = False
                            hide_more_seller = False
                        else:
                            if "storeCategory" in categoty_details:
                                for cat in categoty_details["storeCategory"]:
                                    if cat["storeCategoryId"] == store_category_id:
                                        if cat["hyperlocal"] == True and cat["storeListing"] == 1:
                                            remove_central = True
                                            hide_more_seller = True
                                        elif cat["hyperlocal"] == True and cat["storeListing"] == 0:
                                            remove_central = True
                                            hide_more_seller = False
                                        else:
                                            remove_central = False
                                            hide_more_seller = False
                                    else:
                                        pass
                            else:
                                remove_central = False
                                hide_more_seller = False

                        if hide_more_seller == False:
                            # ==========================================product seo======================================================
                            if "productSeo" in central_product:
                                if "title" in central_product["productSeo"]:
                                    if len(central_product["productSeo"]["title"]) > 0:
                                        title = (
                                            central_product["productSeo"]["title"][lan]
                                            if lan in central_product["productSeo"]["title"]
                                            else central_product["productSeo"]["title"]["en"]
                                        )
                                    else:
                                        title = ""
                                else:
                                    title = ""

                                if "description" in central_product["productSeo"]:
                                    if len(central_product["productSeo"]["description"]) > 0:
                                        description = (
                                            central_product["productSeo"]["description"][lan]
                                            if lan in central_product["productSeo"]["description"]
                                            else central_product["productSeo"]["description"]["en"]
                                        )
                                    else:
                                        description = ""
                                else:
                                    description = ""

                                if "metatags" in central_product["productSeo"]:
                                    if len(central_product["productSeo"]["metatags"]) > 0:
                                        metatags = (
                                            central_product["productSeo"]["metatags"][lan]
                                            if lan in central_product["productSeo"]["metatags"]
                                            else central_product["productSeo"]["metatags"]["en"]
                                        )
                                    else:
                                        metatags = ""
                                else:
                                    metatags = ""

                                if "slug" in central_product["productSeo"]:
                                    if len(central_product["productSeo"]["slug"]) > 0:
                                        slug = (
                                            central_product["productSeo"]["slug"][lan]
                                            if lan in central_product["productSeo"]["slug"]
                                            else central_product["productSeo"]["slug"]["en"]
                                        )
                                    else:
                                        slug = ""
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
                            # =========================================offers ========================================================
                            for child in central_product["childProducts"]:
                                if "suppliers" in child:
                                    for d in child["suppliers"]:
                                        if d["productId"] == varient_productId:
                                            supplier_json = []
                                            if "suppliers" in child:
                                                if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                                                    for s in child["suppliers"]:
                                                        if s["id"] == "0":
                                                            pass
                                                        else:
                                                            if main_unit_id != "":
                                                                find_varient_data = db.childProducts.find(
                                                                    {"parentProductId": central_productId, 'status': 1,
                                                                     "units.unitId": main_unit_id,
                                                                     "_id": ObjectId(s['productId']),
                                                                     'storeId': {'$ne': '0'}}).count()
                                                                if find_varient_data != 0:
                                                                    supplier_json.append(s)
                                                                else:
                                                                    pass
                                                            else:
                                                                supplier_json.append(s)
                                                elif store_category_id == GROCERY_STORE_CATEGORY_ID:
                                                    for s in child["suppliers"]:
                                                        if s["id"] == "0":
                                                            pass
                                                        else:
                                                            parent_store_count = db.stores.find(
                                                                {
                                                                    "status": 1,
                                                                    "_id": ObjectId(s["id"]),
                                                                    "storeFrontTypeId": {"$nin": [2]},
                                                                }
                                                            ).count()
                                                            if parent_store_count == 0:
                                                                pass
                                                            else:
                                                                supplier_json.append(s)
                                                elif zone_id != "" and store_type in [6, 12]:
                                                    if any(
                                                            d["id"] in store_list_json
                                                            for d in child["suppliers"]
                                                    ):
                                                        product_tag = ""
                                                    else:
                                                        product_tag = (
                                                            "We Can not delivering in your area"
                                                        )
                                                    for sp in child["suppliers"]:
                                                        if sp["id"] == "0":
                                                            pass
                                                        else:
                                                            store_count_more = db.stores.find(
                                                                {
                                                                    "status": 1,
                                                                    "serviceZones.zoneId": zone_id,
                                                                    "_id": ObjectId(sp["id"]),
                                                                }
                                                            ).count()
                                                            if store_count_more != 0:
                                                                child_product_count = (
                                                                    db.childProducts.find(
                                                                        {
                                                                            "_id": ObjectId(
                                                                                sp["productId"]
                                                                            ),
                                                                            "status": 1,
                                                                        }
                                                                    ).count()
                                                                )
                                                                if child_product_count > 0:
                                                                    supplier_json.append(sp)
                                                                else:
                                                                    pass
                                                else:
                                                    if remove_central == False and zone_id == "":
                                                        # supplier_json = child['suppliers']
                                                        for sp in child["suppliers"]:
                                                            if sp["id"] == "0":
                                                                pass
                                                            else:
                                                                store_count_more = db.stores.find(
                                                                    {
                                                                        "status": 1,
                                                                        "_id": ObjectId(sp["id"]),
                                                                    }
                                                                ).count()
                                                                if store_count_more != 0:
                                                                    child_product_count = (
                                                                        db.childProducts.find(
                                                                            {
                                                                                "_id": ObjectId(
                                                                                    sp["productId"]
                                                                                ),
                                                                                "status": 1,
                                                                            }
                                                                        ).count()
                                                                    )
                                                                    if child_product_count > 0:
                                                                        supplier_json.append(sp)
                                                                    else:
                                                                        pass
                                                                else:
                                                                    pass
                                                    else:
                                                        for sp in child["suppliers"]:
                                                            if sp["id"] == "0":
                                                                pass
                                                            else:
                                                                store_count_more = db.stores.find(
                                                                    {
                                                                        "status": 1,
                                                                        "_id": ObjectId(sp["id"]),
                                                                    }
                                                                ).count()
                                                                if store_count_more != 0:
                                                                    child_product_count = (
                                                                        db.childProducts.find(
                                                                            {
                                                                                "_id": ObjectId(
                                                                                    sp["productId"]
                                                                                ),
                                                                                "status": 1,
                                                                            }
                                                                        ).count()
                                                                    )
                                                                    if child_product_count > 0:
                                                                        supplier_json.append(sp)
                                                                    else:
                                                                        pass
                                                                else:
                                                                    pass
                                                if len(supplier_json) > 0:
                                                    best_supplier_per = min(
                                                        supplier_json, key=lambda x: x["retailerPrice"]
                                                    )
                                                    best_store_data_price = best_supplier_per[
                                                        "retailerPrice"
                                                    ]
                                                    best_store = [
                                                        store
                                                        for store in supplier_json
                                                        if store["productId"] != varient_productId
                                                    ]

                                                    if len(best_store) > 0:
                                                        best_store = best_store
                                                    else:
                                                        best_store = supplier_list

                                                    if len(best_store) > 0:
                                                        best_supplier = best_store
                                                    else:
                                                        best_supplier = []
                                                    for supplier in best_supplier:
                                                        if supplier["id"] != "0":
                                                            store_count = db.stores.find(
                                                                {
                                                                    "_id": ObjectId(supplier["id"]),
                                                                    "storeFrontTypeId": {"$ne": 5},
                                                                    "status": 1,
                                                                }
                                                            ).count()
                                                        else:
                                                            store_count = 1
                                                        if store_count > 0:
                                                            offers_details = []
                                                            attribute_rating_data = []
                                                            tax_value = []
                                                            tax_price = 0
                                                            # =================================tax price=====================================================
                                                            tax_details = db.childProducts.find_one(
                                                                {"_id": ObjectId(supplier["productId"])}
                                                            )
                                                            if tax_details is not None:
                                                                if "offer" in tax_details:
                                                                    for offer in tax_details["offer"]:
                                                                        if login_type == 1:
                                                                            if offer["status"] == 1:
                                                                                offer_terms = db.offers.find_one(
                                                                                    {
                                                                                        "_id": ObjectId(
                                                                                            offer[
                                                                                                "offerId"
                                                                                            ]
                                                                                        )
                                                                                    }
                                                                                )
                                                                                if (
                                                                                        offer_terms
                                                                                        is not None
                                                                                ):
                                                                                    if offer_terms[
                                                                                        "startDateTime"
                                                                                    ] <= int(
                                                                                        time.time()
                                                                                    ):
                                                                                        offers_details.append(
                                                                                            offer
                                                                                        )
                                                                                    else:
                                                                                        pass
                                                                                else:
                                                                                    pass
                                                                            else:
                                                                                pass
                                                                        else:
                                                                            if offer["status"] == 1:
                                                                                offer_terms = db.offers.find_one(
                                                                                    {
                                                                                        "_id": ObjectId(
                                                                                            offer[
                                                                                                "offerId"
                                                                                            ]
                                                                                        )
                                                                                    }
                                                                                )
                                                                                if (
                                                                                        offer_terms
                                                                                        is not None
                                                                                ):
                                                                                    if offer_terms[
                                                                                        "startDateTime"
                                                                                    ] <= int(
                                                                                        time.time()
                                                                                    ):
                                                                                        offers_details.append(
                                                                                            offer
                                                                                        )
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
                                                                        offers_details,
                                                                        key=lambda x: x[
                                                                            "discountValue"
                                                                        ],
                                                                    )
                                                                    offer_details = db.offers.find(
                                                                        {
                                                                            "_id": ObjectId(
                                                                                best_offer["offerId"]
                                                                            ),
                                                                            "status": 1,
                                                                        }
                                                                    ).count()
                                                                    if offer_details != 0:
                                                                        best_offer = best_offer
                                                                    else:
                                                                        best_offer = {}
                                                                else:
                                                                    best_offer = {}

                                                                if len(best_offer) == 0:
                                                                    percentage = 0
                                                                    discount_type = 0
                                                                else:
                                                                    if "discountType" in best_offer:
                                                                        if (
                                                                                best_offer["discountType"]
                                                                                == 0
                                                                        ):
                                                                            percentage = 0
                                                                            discount_type = 0
                                                                        else:
                                                                            percentage = (
                                                                                best_offer[
                                                                                    "discountValue"
                                                                                ]
                                                                                if "discountValue"
                                                                                   in best_offer
                                                                                else 0
                                                                            )
                                                                            discount_type = best_offer[
                                                                                "discountType"
                                                                            ]
                                                                    else:
                                                                        percentage = 0
                                                                        discount_type = 0

                                                                if len(best_offer) > 0:
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
                                                                    discount_type = 2

                                                                # ==========================unit data================================================================
                                                                if (
                                                                        int(login_type) == 1
                                                                        or int(login_type) != 2
                                                                ):
                                                                    if (
                                                                            "b2cPricing"
                                                                            in tax_details["units"][0]
                                                                    ):
                                                                        price = tax_details["units"][0][
                                                                            "b2cPricing"
                                                                        ][0]["b2cproductSellingPrice"]
                                                                    else:
                                                                        price = tax_details["units"][0][
                                                                            "floatValue"
                                                                        ]
                                                                else:
                                                                    if "b2bPricing" in supplier:
                                                                        price = tax_details["units"][0][
                                                                            "b2bPricing"
                                                                        ][0]["b2bproductSellingPrice"]
                                                                        if price == "":
                                                                            price = 0
                                                                    else:
                                                                        price = 0

                                                                # ==================================get currecny rate============================
                                                                try:
                                                                    currency_rate = (
                                                                        currency_exchange_rate[
                                                                            str(tax_details["currency"])
                                                                            + "_"
                                                                            + str(currency_code)
                                                                            ]
                                                                    )
                                                                except:
                                                                    currency_rate = 0
                                                                currency_details = (
                                                                    db.currencies.find_one(
                                                                        {"currencyCode": currency_code}
                                                                    )
                                                                )
                                                                if currency_details is not None:
                                                                    currency_symbol = currency_details[
                                                                        "currencySymbol"
                                                                    ]
                                                                    currency = currency_details[
                                                                        "currencySymbol"
                                                                    ]
                                                                else:
                                                                    currency_symbol = tax_details[
                                                                        "currencySymbol"
                                                                    ]
                                                                    currency = tax_details[
                                                                        "currencySymbol"
                                                                    ]

                                                                if (
                                                                        currency is None
                                                                        or currency == "None"
                                                                ):
                                                                    currency = "₹"

                                                                if (
                                                                        currency_symbol is None
                                                                        or currency == "None"
                                                                ):
                                                                    currency_symbol = "₹"

                                                                if float(currency_rate) > 0:
                                                                    price = price * float(currency_rate)
                                                                else:
                                                                    pass
                                                                if price == 0 or price == "":
                                                                    final_price = 0
                                                                    discount_price = 0
                                                                    isPrimary = True
                                                                else:
                                                                    if discount_type == 0:
                                                                        discount_price = float(
                                                                            discount_value
                                                                        )
                                                                    elif discount_type == 1:
                                                                        discount_price = (
                                                                                                 float(price)
                                                                                                 * float(discount_value)
                                                                                         ) / 100
                                                                    else:
                                                                        discount_price = 0

                                                                if tax_details != None:
                                                                    if type(tax_details["tax"]) == list:
                                                                        for tax in tax_details["tax"]:
                                                                            tax_value.append(
                                                                                {
                                                                                    "value": tax[
                                                                                        "taxValue"
                                                                                    ]
                                                                                }
                                                                            )
                                                                    else:
                                                                        if tax_details["tax"] != None:
                                                                            if (
                                                                                    "taxValue"
                                                                                    in tax_details["tax"]
                                                                            ):
                                                                                tax_value.append(
                                                                                    {
                                                                                        "value": tax_details[
                                                                                            "tax"
                                                                                        ][
                                                                                            "taxValue"
                                                                                        ]
                                                                                    }
                                                                                )
                                                                            else:
                                                                                tax_value.append(
                                                                                    {
                                                                                        "value": tax_details[
                                                                                            "tax"
                                                                                        ]
                                                                                    }
                                                                                )
                                                                        else:
                                                                            pass
                                                                else:
                                                                    tax_value = []

                                                                if (
                                                                        store_category_id
                                                                        != DINE_STORE_CATEGORY_ID
                                                                ):
                                                                    if len(tax_value) == 0:
                                                                        tax_price = 0
                                                                    else:
                                                                        for amount in tax_value:
                                                                            tax_price = tax_price + (
                                                                                int(amount["value"])
                                                                            )
                                                                else:
                                                                    tax_price = 0

                                                                final_best_price = (
                                                                        best_store_data_price
                                                                        - discount_price
                                                                )
                                                                final_best_price_text = (
                                                                        final_best_price
                                                                        + (
                                                                                (final_best_price * tax_price)
                                                                                / 100
                                                                        )
                                                                )

                                                                final_price = price - discount_price
                                                                final_price = final_price + (
                                                                        (final_price * tax_price) / 100
                                                                )
                                                                percentage_price = (
                                                                        final_price - final_best_price_text
                                                                )
                                                                per_value = round(
                                                                    (100 * percentage_price)
                                                                    / final_price,
                                                                    2,
                                                                )

                                                                seller_data = db.ratingParams.find(
                                                                    {"status": 1}
                                                                )
                                                                for seller_attr in seller_data:
                                                                    if len(best_supplier) > 0:
                                                                        seller_rating = db.reviewRatings.find(
                                                                            {
                                                                                "productId": central_productId,
                                                                                "sellerId": supplier[
                                                                                    "id"
                                                                                ],
                                                                                "attributeId": str(
                                                                                    seller_attr["_id"]
                                                                                ),
                                                                            }
                                                                        )
                                                                    else:
                                                                        seller_rating = db.reviewRatings.find(
                                                                            {
                                                                                "productId": central_productId,
                                                                                "sellerId": "0",
                                                                                "attributeId": str(
                                                                                    seller_attr["_id"]
                                                                                ),
                                                                            }
                                                                        )
                                                                    if seller_rating.count() > 0:
                                                                        for seller in seller_rating:
                                                                            attribute_rating_data.append(
                                                                                {
                                                                                    "attributeId": str(
                                                                                        seller_attr[
                                                                                            "_id"
                                                                                        ]
                                                                                    ),
                                                                                    "name": seller_attr[
                                                                                        "name"
                                                                                    ][lan],
                                                                                    "rating": seller[
                                                                                        "rating"
                                                                                    ],
                                                                                }
                                                                            )
                                                                    else:
                                                                        pass

                                                                if len(attribute_rating_data) > 0:
                                                                    dataframe_attribute = pd.DataFrame(
                                                                        attribute_rating_data
                                                                    )
                                                                    dataframe_attribute[
                                                                        "TotalStarRating"
                                                                    ] = dataframe_attribute.groupby(
                                                                        dataframe_attribute[
                                                                            "attributeId"
                                                                        ]
                                                                    ).transform(
                                                                        "mean"
                                                                    )
                                                                    dataframe_attribute = dataframe_attribute.drop_duplicates(
                                                                        "attributeId", keep="last"
                                                                    )
                                                                    dataframe_attribute = (
                                                                        dataframe_attribute.to_json(
                                                                            orient="records"
                                                                        )
                                                                    )
                                                                    TotalStoreRating = (
                                                                        json.loads(dataframe_attribute)
                                                                    )[0]["TotalStarRating"]
                                                                    dataframe_attribute = json.loads(
                                                                        dataframe_attribute
                                                                    )
                                                                else:
                                                                    TotalStoreRating = 0

                                                                if supplier["id"] == "0":
                                                                    supplier_name = central_store
                                                                    supplier[
                                                                        "supplierName"
                                                                    ] = central_store
                                                                    supplier[
                                                                        "rating"
                                                                    ] = TotalStoreRating
                                                                    supplier["totalRating"] = 400
                                                                else:
                                                                    seller_details = db.stores.find_one(
                                                                        {
                                                                            "_id": ObjectId(
                                                                                supplier["id"]
                                                                            )
                                                                        },
                                                                        {"storeName": 1},
                                                                    )
                                                                    if seller_details != None:
                                                                        if (
                                                                                "storeName"
                                                                                in seller_details
                                                                        ):
                                                                            supplier_name = (
                                                                                seller_details[
                                                                                    "storeName"
                                                                                ][lan]
                                                                            )
                                                                            supplier[
                                                                                "supplierName"
                                                                            ] = supplier_name
                                                                            supplier[
                                                                                "rating"
                                                                            ] = TotalStoreRating
                                                                            supplier[
                                                                                "totalRating"
                                                                            ] = 400
                                                                        else:
                                                                            supplier_name = (
                                                                                central_store
                                                                            )
                                                                            supplier[
                                                                                "supplierName"
                                                                            ] = central_store
                                                                            supplier[
                                                                                "rating"
                                                                            ] = TotalStoreRating
                                                                            supplier[
                                                                                "totalRating"
                                                                            ] = 400
                                                                    else:
                                                                        supplier_name = central_store
                                                                        supplier[
                                                                            "supplierName"
                                                                        ] = central_store
                                                                        supplier[
                                                                            "rating"
                                                                        ] = TotalStoreRating
                                                                        supplier["totalRating"] = 400

                                                                # ================================currency=================================================
                                                                if product_tag != "":
                                                                    out_of_stock = True
                                                                else:
                                                                    out_of_stock = (
                                                                        False
                                                                        if int(
                                                                            tax_details["units"][0][
                                                                                "availableQuantity"
                                                                            ]
                                                                        )
                                                                           > 0
                                                                        else True
                                                                    )

                                                                supplier[
                                                                    "currencySymbol"
                                                                ] = currency_symbol
                                                                supplier["currency"] = currency

                                                                # ===========seller rating======================
                                                                avg_rating_value = 0
                                                                seller_rating = db.sellerReviewRatings.aggregate(
                                                                    [
                                                                        {
                                                                            "$match": {
                                                                                "sellerId": str(
                                                                                    supplier["id"]
                                                                                ),
                                                                                "rating": {"$ne": 0},
                                                                                "status": 1,
                                                                            }
                                                                        },
                                                                        {
                                                                            "$group": {
                                                                                "_id": "$sellerId",
                                                                                "avgRating": {
                                                                                    "$avg": "$rating"
                                                                                },
                                                                            }
                                                                        },
                                                                    ]
                                                                )
                                                                for avg_rating in seller_rating:
                                                                    avg_rating_value = avg_rating[
                                                                        "avgRating"
                                                                    ]

                                                                try:
                                                                    avg_seller_rating_value = round(
                                                                        avg_rating_value, 2
                                                                    )
                                                                except:
                                                                    avg_seller_rating_value = 0

                                                                try:
                                                                    supplier["storeName"] = supplier[
                                                                        "storeName"
                                                                    ][lan]
                                                                except:
                                                                    try:
                                                                        supplier[
                                                                            "storeName"
                                                                        ] = supplier["storeName"]["en"]
                                                                    except:
                                                                        pass

                                                                supplier_query = {"_id": ObjectId(supplier["id"]),
                                                                                  "status": 1}
                                                                if zone_id != "":
                                                                    supplier_query['serviceZones.zoneId'] = zone_id

                                                                seller_count = db.stores.find(supplier_query).count()
                                                                if seller_count > 0:
                                                                    supplier_list.append(
                                                                        {
                                                                            "parentProductId": central_productId,
                                                                            "currencySymbol": currency_symbol,
                                                                            "currency": currency,
                                                                            "childProductId": supplier[
                                                                                "productId"
                                                                            ],
                                                                            "offers": best_offer,
                                                                            "availableQuantity": supplier[
                                                                                "retailerQty"
                                                                            ],
                                                                            "supplier": supplier,
                                                                            "supplierName": supplier_name,
                                                                            "supplierId": supplier["id"],
                                                                            "TotalStarRating": avg_seller_rating_value,
                                                                            "percentageValue": per_value,
                                                                            # False if int(supplier['retailerQty']) > 0 else True,
                                                                            "outOfStock": out_of_stock,
                                                                            "finalPrice": round(
                                                                                final_price, 2
                                                                            ),
                                                                            "finalPriceList": {
                                                                                "basePrice": round(
                                                                                    price, 2
                                                                                ),
                                                                                "finalPrice": round(
                                                                                    final_price, 2
                                                                                ),
                                                                                "discountPrice": round(
                                                                                    discount_price, 2
                                                                                ),
                                                                                "discountPercentage": percentage,
                                                                            },
                                                                        }
                                                                    )
                                                                else:
                                                                    pass
                                                            else:
                                                                pass
                                                        else:
                                                            pass
                                                    if len(supplier_list) > 0:
                                                        df = pd.DataFrame(supplier_list)
                                                        df = df.drop_duplicates(
                                                            subset="supplierId", keep="last"
                                                        )
                                                        store_list = df.to_dict(orient="records")
                                                        store_list = sorted(
                                                            store_list,
                                                            key=lambda k: k["finalPrice"],
                                                            reverse=False,
                                                        )
                                                    else:
                                                        store_list = []

                                                    if len(store_list) > 0:
                                                        final_supplier_list.append(
                                                            {
                                                                "productName": child["unitName"][lan],
                                                                "currency": currency,
                                                                "TotalStarRating": central_product[
                                                                    "avgRating"
                                                                ]
                                                                if "avgRating" in central_product
                                                                else 0,
                                                                "currencySymbol": currency_symbol,
                                                                "images": central_product["images"],
                                                                "supplier": store_list,
                                                                "productSeo": product_seo,
                                                                "replacementPolicy": central_product[
                                                                    "replacementPolicy"
                                                                ]
                                                                if "replacementPolicy"
                                                                   in central_product
                                                                else {"isReplacement": False},
                                                                "warranty": central_product["warranty"]
                                                                if "warranty" in central_product
                                                                else {},
                                                                "term&condition": central_product[
                                                                    "term&condition"
                                                                ]
                                                                if "term&condition" in central_product
                                                                else {},
                                                                "exchangePolicy": central_product[
                                                                    "exchangePolicy"
                                                                ]
                                                                if "exchangePolicy" in central_product
                                                                else {"isExchange": False},
                                                                "returnPolicy": central_product[
                                                                    "returnPolicy"
                                                                ]
                                                                if "returnPolicy" in central_product
                                                                else {"isReturn": False},
                                                            }
                                                        )
                                                else:
                                                    pass
                                            else:
                                                pass
                                else:
                                    pass
                            # finalSuggestions = {
                            #     "data": df.to_dict(orient="records"),
                            if len(final_supplier_list) > 0:
                                response = {
                                    "data": final_supplier_list,
                                    "productData": variant_product_details,
                                    "message": "data found",
                                }
                                return JsonResponse(response, safe=False, status=200)
                            else:
                                response = {
                                    "data": [],
                                    "productData": {},
                                    "message": "data not found",
                                }
                                return JsonResponse(response, safe=False, status=404)
                        else:
                            response = {"data": [], "productData": {}, "message": "data not found"}
                            return JsonResponse(response, safe=False, status=404)
                    else:
                        response = {"data": [], "productData": {}, "message": "data not found"}
                        return JsonResponse(response, safe=False, status=404)
                else:
                    response = {"data": [], "productData": {}, "message": "data not found"}
                    return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "productData": {}, "message": message}
            return JsonResponse(error, safe=False, status=500)


"""
    api for the get the substitute of the products list
"""


class SubstituteProduct(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Products"],
        operation_description="API for getting substitute products of the given product",
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
                name="parentProductId",
                default="5e00da6e14fd8715730fa4c6",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="central product id of the product",
            ),
            openapi.Parameter(
                name="productId",
                default="5e00da6e14fd8715730fa4c6",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="child product id of the product",
            ),
            openapi.Parameter(
                name="page",
                default="1",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="page number for the pagination",
            ),
            openapi.Parameter(
                name="entity",
                default="0",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for the check from where request is coming. for customer app value is 0 and for store app value is 1",
            ),
            openapi.Parameter(
                name="zoneId",
                default="5df8b6ea8798dc19d926bd28",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="zone id if need to get the zone seller only",
            ),
            openapi.Parameter(
                name="cityId",
                default="5df7b7218798dc2c1114e6bf",
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="fetching the data in particular city",
            ),
        ],
        responses={
            200: "successfully. substitute products found",
            401: "Unauthorized. token expired",
            404: "data not found. it might be product substitute not found",
            422: "required fields are not found. it might be central product id not found",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            start_time = time.time()
            variant_data = []
            token = (
                request.META["HTTP_AUTHORIZATION"] if "HTTP_AUTHORIZATION" in request.META else ""
            )
            lan = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)

            central_productId = request.GET.get("parentProductId", "")
            child_product_ids = request.GET.get("childProductId","")
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            try:
                user_id = json.loads(token)["userId"]
            except:
                user_id = "5df88ea3db72a50c970a99bc"

            manager_data = db.managers.find_one({"_id": ObjectId(user_id), "linkedWith": 2})
            if manager_data != None:
                entity = 1
            else:
                entity = 0
            product_id = request.GET.get("productId", "")
            zone_id = request.GET.get("zoneId", "")
            page = int(request.GET.get("page", 0))
            from_data = page * 3
            to_data = (page * 3) + 3
            if central_productId == "":
                response_data = {
                    "message": "parent product id empty",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                if product_id != "":
                    store_details = db.childProducts.find_one(
                        {"_id": ObjectId(product_id)}, {"storeId": 1}
                    )

                    try:
                        if str(store_details["storeId"]) == "0":
                            central_product = db.products.find_one(
                                {"_id": ObjectId(central_productId)}, {"substitute": 1}
                            )
                        else:
                            central_product = db.childProducts.find_one(
                                {"_id": ObjectId(product_id)}, {"substitute": 1}
                            )
                    except:
                        central_product = db.products.find_one(
                            {"_id": ObjectId(central_productId)}, {"substitute": 1}
                        )
                    if central_product != None and "substitute" in central_product:
                        total_count = len(list(set(central_product["substitute"])))
                        for substitue in list(set(central_product["substitute"])):
                            child_product_data = db.childProducts.find_one(
                                {"_id": ObjectId(substitue)}
                            )
                            if child_product_data != None:
                                substitue = str(child_product_data["_id"])
                            else:
                                substitue = substitue

                            central_product_query = {"_id": ObjectId(substitue)}

                            res_central_parameters = db.products.find_one(central_product_query)
                            if res_central_parameters is not None:
                                prescription_required = False
                                image_data = {}
                                offers_details = []
                                supplier_list = []
                                try:
                                    if "suppliers" in res_central_parameters["units"][0]:
                                        for s in res_central_parameters["units"][0]["suppliers"]:
                                            if s["id"] == str(store_details["storeId"]):
                                                supplier_list.append(s)
                                            else:
                                                pass
                                        best_supplier = min(
                                            supplier_list, key=lambda x: x["retailerPrice"]
                                        )
                                        if best_supplier["retailerQty"] == 0:
                                            best_supplier = max(
                                                supplier_list, key=lambda x: x["retailerQty"]
                                            )
                                        else:
                                            best_supplier = best_supplier
                                    else:
                                        best_supplier = {}
                                except:
                                    best_supplier = {}

                                if len(best_supplier) > 0:
                                    query={"_id": ObjectId(best_supplier["productId"]), "status": 1}
                                    if child_product_ids:
                                        query["_id"] = {"$ne": ObjectId(child_product_ids)}
                                    child_product_details = db.childProducts.find_one(query)
                                    if child_product_details != None:
                                        if (
                                                res_central_parameters["storeCategoryId"]
                                                == PHARMACY_STORE_CATEGORY_ID
                                        ):
                                            # ==============================pres===================================
                                            if "prescriptionRequired" in child_product_details:
                                                try:
                                                    if (
                                                            int(child_product_details["prescriptionRequired"])
                                                            == 0
                                                    ):
                                                        prescription_required = False
                                                    else:
                                                        prescription_required = True
                                                except:
                                                    prescription_required = False

                                            else:
                                                prescription_required = False
                                        else:
                                            prescription_required = False
                                        if "needsIdProof" in child_product_details:
                                            if child_product_details["needsIdProof"] == False:
                                                needsIdProof = False
                                            else:
                                                needsIdProof = True
                                        else:
                                            needsIdProof = False

                                        currency_symbol = (
                                            child_product_details["currencySymbol"]
                                            if "currencySymbol" in child_product_details
                                            else "$"
                                        )
                                        currency = (
                                            child_product_details["currency"]
                                            if "currency" in child_product_details
                                            else "INR"
                                        )
                                        try:
                                            brand_title = variant["_source"]["brandTitle"]["en"]
                                        except:
                                            brand_title = ""
                                        try:
                                            manufacture_title = child_product_details[
                                                "manufactureName"
                                            ]["en"]
                                        except:
                                            manufacture_title = ""

                                        image_data = res_central_parameters["images"][0]
                                        if "offer" in child_product_details:
                                            for offer in child_product_details["offer"]:
                                                if offer["status"] == 1:
                                                    offer_terms = db.offers.find_one(
                                                        {"_id": ObjectId(offer["offerId"])}
                                                    )
                                                    if offer_terms is not None:
                                                        if offer_terms["startDateTime"] <= int(
                                                                time.time()
                                                        ):
                                                            offers_details.append(offer)
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
                                                    "storeId": best_supplier["id"],
                                                }
                                            ).count()
                                            if offer_details != 0:
                                                best_offer = best_offer
                                            else:
                                                best_offer = {}
                                        else:
                                            best_offer = {}

                                        if len(best_offer) == 0:
                                            percentage = 0
                                            discount_type = 0
                                            discount_value = 0
                                        else:
                                            if "discountType" in best_offer:
                                                if best_offer["discountType"] == 0:
                                                    percentage = 0
                                                    discount_type = 0
                                                    discount_value = 0
                                                else:
                                                    percentage = (
                                                        best_offer["discountValue"]
                                                        if "discountValue" in best_offer
                                                        else 0
                                                    )
                                                    discount_value = (
                                                        best_offer["discountValue"]
                                                        if "discountValue" in best_offer
                                                        else 0
                                                    )
                                                    discount_type = best_offer["discountType"]
                                            else:
                                                percentage = 0
                                                discount_type = 0
                                                discount_value = 0

                                            if len(best_offer) > 0:
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
                                                discount_type = 2
                                                discount_value = 0

                                        # ==========================unit data================================================================
                                        try:
                                            if entity == 0:
                                                price = (
                                                    child_product_details["units"][0]["b2cPricing"][
                                                        0
                                                    ]["b2cproductSellingPrice"]
                                                    if "b2cproductSellingPrice"
                                                       in child_product_details["units"][0][
                                                           "b2cPricing"
                                                       ][0]
                                                    else child_product_details["units"][0][
                                                        "floatValue"
                                                    ]
                                                )
                                            else:
                                                price = (
                                                    child_product_details["units"][0]["b2cPricing"][
                                                        0
                                                    ]["b2cpriceWithTax"]
                                                    if "b2cpriceWithTax"
                                                       in child_product_details["units"][0][
                                                           "b2cPricing"
                                                       ][0]
                                                    else child_product_details["units"][0][
                                                        "floatValue"
                                                    ]
                                                )
                                        except:
                                            price = child_product_details["units"][0]["floatValue"]

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
                                            currency_symbol = child_product_details[
                                                "currencySymbol"
                                            ]
                                            currency = child_product_details["currency"]

                                        if float(currency_rate) > 0:
                                            price = price * float(currency_rate)

                                        # =====================================tax calculation============
                                        tax_value = []
                                        tax_price = 0
                                        if type(child_product_details["tax"]) == list:
                                            for tax in child_product_details["tax"]:
                                                tax_value.append({"value": tax["taxValue"]})
                                        else:
                                            if child_product_details["tax"] != None:
                                                if "taxValue" in child_product_details["tax"]:
                                                    tax_value.append(
                                                        {
                                                            "value": child_product_details["tax"][
                                                                "taxValue"
                                                            ]
                                                        }
                                                    )
                                                else:
                                                    tax_value.append(
                                                        {"value": child_product_details["tax"]}
                                                    )
                                            else:
                                                tax_value = []

                                        if (
                                                child_product_details["storeCategoryId"]
                                                != DINE_STORE_CATEGORY_ID
                                        ):
                                            try:
                                                if len(child_product_details["tax"]) == 0:
                                                    tax_price = 0
                                                else:
                                                    for amount in child_product_details["tax"]:
                                                        if "value" in amount:
                                                            tax_price = tax_price + (
                                                                int(amount["value"])
                                                            )
                                                        elif "taxValue" in amount:
                                                            tax_price = tax_price + (
                                                                int(amount["taxValue"])
                                                            )
                                                        else:
                                                            tax_price = tax_price + 0
                                            except:
                                                tax_price = 0
                                        else:
                                            tax_price = 0

                                        price = price + ((float(price) * tax_price) / 100)

                                        if price == 0 or price == "":
                                            final_price = 0
                                            discount_price = 0
                                        else:
                                            if discount_type == 0:
                                                discount_price = float(discount_value)
                                            elif discount_type == 1:
                                                discount_price = (
                                                                         float(price) * float(discount_value)
                                                                 ) / 100
                                            else:
                                                discount_price = 0
                                            final_price = price - discount_price
                                        final_price_list = {
                                            "basePrice": round(price, 2),
                                            "finalPrice": round(final_price, 2),
                                            "discountPrice": round(discount_price, 2),
                                            "discountPercentage": percentage,
                                        }

                                        if len(best_supplier) > 0:
                                            if "productId" in best_supplier:
                                                product_id = best_supplier["productId"]
                                            else:
                                                product_id = variant["_id"]
                                        else:
                                            product_id = variant["_id"]

                                        if (
                                                child_product_details["units"][0]["availableQuantity"]
                                                > 0
                                        ):
                                            outOfStock = False
                                        else:
                                            outOfStock = True

                                        linked_attribute = get_linked_unit_attribute(
                                            child_product_details["units"]
                                        )
                                        # ===================rating and review data===============================
                                        avg_rating_value = 0
                                        product_rating = db.reviewRatings.aggregate(
                                            [
                                                {
                                                    "$match": {
                                                        "productId": str(
                                                            res_central_parameters["_id"]
                                                        )
                                                    }
                                                },
                                                {
                                                    "$group": {
                                                        "_id": "$productId",
                                                        "avgRating": {"$avg": "$rating"},
                                                    }
                                                },
                                            ]
                                        )
                                        try:
                                            try:
                                                without_tax_price = child_product_details["units"][0][
                                                                    "b2cPricing"
                                                                ][0]["b2cpriceWithTax"]
                                            except:

                                                without_tax_price = child_product_details["units"][0][
                                                                    "floatValue"
                                                                ]
                                            try:
                                                # price = child_product_details["units"][0]["b2cPricing"][0][
                                                #     "b2cproductSellingPrice"
                                                # ]
                                                with_out_margin_base_price = child_product_details["units"][0]["b2cPricing"][
                                                    0
                                                ]["b2cpriceWithTax"]
                                            except:
                                                # price = child_product_details["units"][0]["floatValue"]
                                                with_out_margin_base_price = child_product_details["units"][0]["floatValue"]
                                            try:
                                                # final_price = product_details['units'][0]['discountPrice']
                                                # final_price = base_price - discount_price
                                                final_without_margin_price = float(with_out_margin_base_price) - float(discount_price)
                                            except:
                                                # final_price = float(base_price) - discount_price
                                                final_without_margin_price = float(with_out_margin_base_price) - float(discount_price)

                                            final_price_list = {
                                                "withOutTaxPrice": round(float(without_tax_price), 2),
                                                "basePrice": round(price, 2),
                                                "withOutMarginPrice": round(
                                                    with_out_margin_base_price, 2
                                                ),
                                                "finalWithOutMarginPrice": final_without_margin_price,
                                                "finalPrice": round(final_price, 2),
                                                "discountPrice": round(
                                                    discount_price, 2
                                                ),
                                                "discountPercentage": percentage,
                                            }
                                        except Exception as e:
                                            print('12-------',e)
                                            final_price_list
                                        for avg_rating in product_rating:
                                            avg_rating_value = avg_rating["avgRating"]
                                        # ====================for variant specs========================
                                        attribute_data = []
                                        for attr in child_product_details["units"][0]["attributes"]:
                                            if "attrlist" in attr:
                                                for attr_data in attr["attrlist"]:
                                                    try:
                                                        if attr_data["linkedtounit"] == 1:
                                                            attr_value = ""
                                                            if (
                                                                    type(attr_data["value"]["en"])
                                                                    == list
                                                            ):
                                                                for li in attr_data["value"]["en"]:
                                                                    if attr_value == "":
                                                                        attr_value = str(li)
                                                                    else:
                                                                        attr_value = (
                                                                                str(attr_value)
                                                                                + ", "
                                                                                + str(li)
                                                                        )
                                                            else:
                                                                attr_value = (
                                                                    attr_data["value"][lan]
                                                                    if lan in attr_data["value"]
                                                                    else attr_data["value"]["en"]
                                                                )
                                                            attribute_data.append(
                                                                {
                                                                    "attrname": attr_data[
                                                                        "attrname"
                                                                    ][lan]
                                                                    if lan in attr_data["attrname"]
                                                                    else attr_data["attrname"][
                                                                        "en"
                                                                    ],
                                                                    "value": attr_value,
                                                                    "name": attr_data["attrname"][
                                                                        lan
                                                                    ]
                                                                    if lan in attr_data["attrname"]
                                                                    else attr_data["attrname"][
                                                                        "en"
                                                                    ],
                                                                }
                                                            )
                                                        else:
                                                            pass
                                                    except:
                                                        pass
                                        product_type = combo_special_type_validation(
                                            product_id
                                        )  # get product type, is normal or combo or special product
                                        # ================================currency=================================================
                                        variant_data.append(
                                            {
                                                "childProductId": product_id,
                                                "brandTitle": brand_title,
                                                "productType": product_type,
                                                "manufactureName": manufacture_title,
                                                "linkedAttribute": linked_attribute,
                                                "prescriptionRequired": prescription_required,
                                                "needsIdProof": needsIdProof,
                                                "currency": currency,
                                                "variantData": attribute_data,
                                                "currencySymbol": currency_symbol,
                                                "unitId": child_product_details["units"][0][
                                                    "unitId"
                                                ],
                                                "needsWeighed": child_product_details[
                                                    "needsWeighed"
                                                ]
                                                if "needsWeighed" in child_product_details
                                                else False,
                                                "productName": child_product_details["units"][0][
                                                    "unitName"
                                                ][lan]
                                                if lan
                                                   in child_product_details["units"][0]["unitName"]
                                                else child_product_details["units"][0]["unitName"][
                                                    "en"
                                                ],
                                                "parentProductId": str(
                                                    res_central_parameters["_id"]
                                                ),  # ['parentProductId'],
                                                "images": child_product_details["units"][0][
                                                    "image"
                                                ][0]
                                                if "image" in child_product_details["units"][0]
                                                else image_data,
                                                "finalPriceList": final_price_list,
                                                "offers": best_offer,
                                                "outOfStock": outOfStock,
                                                "TotalStarRating": round(avg_rating_value, 2),
                                                "supplier": best_supplier,
                                            }
                                        )
                                    else:
                                        pass
                                else:
                                    pass
                            else:
                                pass

                if len(variant_data) > 0:
                    response = {
                        "message": "data found",
                        "totalCount": total_count,
                        "data": variant_data,
                    }
                    return JsonResponse(response, safe=False, status=200)
                else:
                    response = {"message": "data not found", "totalCount": 0, "data": []}
                    return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class ComboProductList(APIView):
    @swagger_auto_schema(
        method="post",
        tags=["Inventory"],
        operation_description="API for get the details of the products",
        required=["language"],
        manual_parameters=[
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
            required=["data"],
            properties={
                "data": openapi.Schema(
                    type=openapi.TYPE_ARRAY,
                    description="array of the data or object(combo products)",
                    items=openapi.Items(
                        type=openapi.TYPE_OBJECT,
                        required=["productId", "minimumQuantity"],
                        properties={
                            "productId": openapi.Schema(
                                type=openapi.TYPE_STRING,
                                default="6154748fffef9fa22a2c32ce",
                                description="product id from combo product key",
                            ),
                            "minimumQuantity": openapi.Schema(
                                type=openapi.TYPE_STRING,
                                default=1,
                                description="minimum order qty from combo product details",
                            ),
                        },
                    ),
                )
            },
        ),
        responses={
            200: "Successfully. data found",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["post"])
    def post(self, request):
        try:
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            data = request.data
            if data == "":
                response = {"message": "request data is missing", "data": []}
                return JsonResponse(response, safe=False, status=422)
            elif len(data) == 0:
                response = {"message": "request data is missing", "data": []}
                return JsonResponse(response, safe=False, status=422)
            elif "data" not in data:
                response = {"message": "request data is missing", "data": []}
                return JsonResponse(response, safe=False, status=422)
            elif len(data["data"]) == 0:
                response = {"message": "request data is missing", "data": []}
                return JsonResponse(response, safe=False, status=422)
            else:
                last_product_response = []
                for product in data["data"]:
                    product_details = db.childProducts.find_one(
                        {"_id": ObjectId(product["productId"])}
                    )
                    if product_details != None:
                        # =====================================offer data===================================
                        best_offer = product_best_offer_data(str(product_details["_id"]))
                        if "b2cPricing" in product_details["units"][0]:
                            price = product_details["units"][0]["b2cPricing"][0][
                                "b2cproductSellingPrice"
                            ]
                        else:
                            price = product_details["units"][0]["floatValue"]

                        if len(best_offer) > 0:
                            discount_type = (
                                int(best_offer["discountType"])
                                if "discountType" in best_offer
                                else 0
                            )
                            discount_value = (
                                best_offer["discountValue"] if "discountValue" in best_offer else 0
                            )
                        else:
                            discount_type = 2
                            discount_value = 0

                        # ==================================get currecny rate============================
                        currency_symbol = product_details["currencySymbol"]
                        currency = product_details["currencySymbol"]
                        if price == 0 or price == "":
                            final_price = 0
                            discount_price = 0
                        else:
                            if discount_type == 0:
                                discount_price = float(discount_value)
                            elif discount_type == 1:
                                discount_price = (float(price) * float(discount_value)) / 100
                            else:
                                discount_price = 0

                        tax_value = []
                        if type(product_details["tax"]) == list:
                            for tax in product_details["tax"]:
                                tax_value.append({"value": tax["taxValue"]})
                        else:
                            if product_details["tax"] != None:
                                if "taxValue" in product_details["tax"]:
                                    tax_value.append({"value": product_details["tax"]["taxValue"]})
                                else:
                                    tax_value.append({"value": product_details["tax"]})
                            else:
                                pass

                        tax_price = 0
                        if len(tax_value) == 0:
                            tax_price = 0
                        else:
                            for amount in tax_value:
                                tax_price = tax_price + (int(amount["value"]))

                        final_price = price - discount_price
                        final_price = final_price + ((final_price * tax_price) / 100)
                        product_name = product_details["units"][0]["unitName"]["en"]
                        available_qty = product_details["units"][0]["availableQuantity"]
                        if available_qty > 0:
                            outOfStock = False
                        else:
                            outOfStock = True

                        if len(product_details["units"][0]["image"]) > 0:
                            image_data = product_details["units"][0]["image"]
                        else:
                            image_data = [
                                {
                                    "extraLarge": "",
                                    "medium": "",
                                    "altText": "",
                                    "large": "",
                                    "small": "",
                                }
                            ]

                        last_product_response.append(
                            {
                                "childProductId": str(product_details["_id"]),
                                "productName": product_name,
                                "parentProductId": product_details["parentProductId"],
                                "availableQty": available_qty,
                                "storeCategoryId": product_details["storeCategoryId"],
                                "currencySymbol": currency_symbol,
                                "currency": currency,
                                "finalPriceList": {
                                    "basePrice": round(price, 2),
                                    "finalPrice": round(final_price, 2),
                                    "discountPrice": round(discount_price, 2),
                                },
                                "minimumQuantity": product["minimumQuantity"],
                                "mobileImage": image_data,
                                "unitId": product_details["units"][0]["unitId"],
                                "outOfStock": outOfStock,
                            }
                        )
                    else:
                        pass
                if len(last_product_response) > 0:
                    response = {"message": "data found", "data": last_product_response}
                    return JsonResponse(response, safe=False, status=200)
                else:
                    response = {"message": "data not found", "data": []}
                    return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error_message = {"error": "Invalid request", "message": message, "total_count": 0}
            return JsonResponse(error_message, status=500)


class ProductPortionData(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Product AddOns"],
        operation_description="API for get the portiuon and addons details for a product",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization", in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True
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
                name="centralProductId",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="central product id of the product",
                default="5ed52d3d29bc4d7d6e2d89f3",
            ),
            openapi.Parameter(
                name="childProductId",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                default="5ed52d3e29bc4d7d6e2d89f5",
                description="child product of the product",
            ),
            openapi.Parameter(
                name="storeId",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                default="5ed0d2a187606b01c31fb9bb",
                description="store id from which store need to fetch the details",
            ),
        ],
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            start_time = time.time()
            central_product_id = request.GET.get("centralProductId", "")
            store_id = request.GET.get("storeId", "")
            child_product_id = request.GET.get("childProductId", "")
            token = request.META["HTTP_AUTHORIZATION"]
            language = request.META["HTTP_LANGUAGE"]
            product_add_ons = []
            last_response_data = []
            if token == "":
                response_data = {"message": "unauthorized", "data": []}
                return JsonResponse(response_data, safe=False, status=401)
            elif central_product_id == "":
                response_data = {"message": "central product id is blank or missing", "data": []}
                return JsonResponse(response_data, safe=False, status=422)
            elif child_product_id == "":
                response_data = {"message": "child product id is blank or missing", "data": []}
                return JsonResponse(response_data, safe=False, status=422)
            elif store_id == "" or store_id == "0":
                response_data = {"message": "store id is blank or missing", "data": []}
                return JsonResponse(response_data, safe=False, status=422)
            else:
                start_time = time.time()
                add_ons_query = {
                    "parentProductId": central_product_id,
                    "storeId": ObjectId(store_id),
                    "status":1
                }
                add_ons_response = db.childProducts.find(add_ons_query)
                selected_price = 0
                portion_details = []
                if add_ons_response.count() > 0:
                    for add_ons in add_ons_response:
                        product_addons = []
                        if "addOns" in add_ons["units"][0]:
                            if add_ons["units"][0]["addOns"] is not None:
                                for p_a in add_ons["units"][0]["addOns"]:
                                    inner_addons_details = []
                                    for inner_addons in p_a["addOns"]:
                                        try:
                                            inner_addons_details.append(
                                                {
                                                    "id": inner_addons["id"],
                                                    "name": inner_addons["name"][language]
                                                    if language in inner_addons["name"]
                                                    else inner_addons["name"]["en"],
                                                    "currencySymbol": add_ons["currencySymbol"],
                                                    "currency": add_ons["currency"],
                                                    "price": inner_addons["price"]
                                                    if "price" in inner_addons
                                                    else 0,
                                                }
                                            )
                                        except:pass
                                    product_addons.append(
                                        {
                                            "name": p_a["name"][language]
                                            if language in p_a["name"]
                                            else p_a["name"]["en"],
                                            "addOns": inner_addons_details,
                                            "currencySymbol": add_ons["currencySymbol"],
                                            "currency": add_ons["currencySymbol"],
                                            "storeId": p_a["storeId"],
                                            "description": p_a["description"][language]
                                            if language in p_a["description"]
                                            else p_a["description"]["en"],
                                            "minimumLimit": p_a["minimumLimit"],
                                            "maximumLimit": p_a["maximumLimit"],
                                            "mandatory": p_a["mandatory"],
                                            "multiple": p_a["multiple"],
                                            "addOnLimit": p_a["addOnLimit"]
                                            if "addOnLimit" in p_a
                                            else 0,
                                            "unitAddOnId": p_a["unitAddOnId"],
                                            "seqId": p_a["seqId"],
                                        }
                                    )
                        if str(add_ons["_id"]) == child_product_id:
                            is_primary = True
                        else:
                            is_primary = False
                        try:
                            final_price = add_ons["units"][0]["b2cPricing"][0]["b2cproductSellingPrice"]
                        except:
                            final_price = add_ons["units"][0]["floatValue"]
                        portion_details.append(
                            {
                                "id": str(add_ons["_id"]),
                                "isPrimary": is_primary,
                                "childProductId": str(add_ons["_id"]),
                                "name": add_ons["units"][0]["unitName"][language]
                                if language in add_ons["units"][0]["unitName"]
                                else add_ons["units"][0]["unitName"]["en"],
                                "price": final_price,
                                "addOns": product_addons,
                                "currencySymbol": add_ons["currencySymbol"],
                                "currency": add_ons["currency"],
                                "unitId": add_ons["units"][0]["unitId"],
                                "parentProductId": add_ons["parentProductId"],
                            }
                        )
                    if len(portion_details) > 0:
                        response_data = {"message": "data found", "data": portion_details}
                        return JsonResponse(response_data, safe=False, status=200)
                    else:
                        response_data = {"message": "data not found", "data": []}
                        return JsonResponse(response_data, safe=False, status=404)
                else:
                    response_data = {"message": "data not found", "data": []}
                    return JsonResponse(response_data, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)
