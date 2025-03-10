import asyncio
import datetime
import json
from json import decoder
from json import encoder
import os
from cassandra import query
import pandas as pd
import queue
import re
import sys
import threading
import time
from bson.objectid import ObjectId
from django.http import JsonResponse
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from googletrans import Translator
from json import dumps
from kafka import KafkaProducer
from operator import itemgetter
from pytz import timezone
from rest_framework.decorators import action
from rest_framework.views import APIView
from rejson import Client, Path

from search.views import store_search_data_new
from search_api.settings import (
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
    ECOMMERCE_STORE_CATEGORY_ID,
    conv_fac,
    EARTH_REDIS,
    DINE_STORE_CATEGORY_ID,
    session,
    currency_exchange_rate,
    REDIS_IP,
    REDIS_PASSWORD,
    db,
    es,
    REDIS_JSON_PORT,
    REDIS_JSON_IP
)
from .response_handler import ResponseHandlerObj as RespObj
from .response_handler import JSONEncoder as CustomEncoder
from .api_doc import APIDocObj
from .process_handler import ProcessHandlerObj as ProObj
from search.views import category_search_logs

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
# import json


# approximate radius of earth in km
R = EARTH_REDIS
conv_fac = conv_fac

# redis json python client
rj = Client(host=REDIS_JSON_IP, port=6379, decode_responses=True, encoder=CustomEncoder())

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
central_zero_store_creation_ts = STORE_CREATE_TIME


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


def home_units_data(row, lan, sort, status, logintype, store_category_id, margin_price, city_id):
    try:
        currency_rate = row["currencyRate"]
    except:
        currency_rate = 0
    try:
        tax_price = 0
        unitdata = []
        best_supplier = {}
        best_offer = row["offer"]
        if store_category_id != DINE_STORE_CATEGORY_ID:
            if len(row["tax"]) == 0:
                tax_price = 0
            else:
                for amount in row["tax"]:
                    tax_price = tax_price + (int(amount["value"]))
        else:
            tax_price = 0

        if len(best_offer) > 0:
            if len(best_offer) > 0:
                discount_type = (
                    int(best_offer["discountType"]) if "discountType" in best_offer else 1
                )
                discount_value = best_offer["discountValue"] if "discountValue" in best_offer else 0
            else:
                discount_type = 2
                discount_value = 0

            try:
                if "availableQuantity" in row["units"][0]:
                    if row["units"][0]["availableQuantity"] > 0:
                        outOfStock = False
                        availableQuantity = row["units"][0]["availableQuantity"]
                    else:
                        outOfStock = True
                        availableQuantity = 0
                else:
                    outOfStock = True
                    availableQuantity = 0
            except:
                outOfStock = True
                availableQuantity = 0

            try:
                if margin_price == True:
                    base_price = float(row["units"][0]["b2cPricing"][0]["b2cproductSellingPrice"])
                else:
                    base_price = float(row["units"][0]["b2cPricing"][0]["b2cpriceWithTax"])
                base_price = base_price + ((float(base_price) * tax_price) / 100)
            except:
                base_price = float(row["units"][0]["floatValue"])
                base_price = base_price + ((float(base_price) * tax_price) / 100)

            if float(currency_rate) > 0:
                base_price = base_price * float(currency_rate)

            # ==============calculate discount price =============================
            if discount_type == 0:
                discount_price = float(discount_value)
            elif discount_type == 1:
                try:
                    discount_price = (float(base_price) * float(discount_value)) / 100
                except:
                    discount_price = (
                        float(row["units"][0]["price"]["en"]) * float(discount_value)
                    ) / 100
            else:
                discount_price = 0
            final_price = base_price - discount_price

            if final_price == 0 or base_price == 0:
                discount_price = 0
            else:
                discount_price = discount_price

            minimum_purchase = 1
            package_units = ""
            package_type = ""

            minimum_purchase_qty = int(minimum_purchase)
            if package_units != "" and package_type != "":
                minimum_purchase_details = (minimum_purchase) + " " + minimum_purchase_unit
                if no_unit != "0":
                    try:
                        mou_unit = (
                            str(int(float(no_unit))) + " " + package_units + "/" + package_type
                        )
                    except:
                        mou_unit = no_unit + " " + package_units + "/" + package_type
                else:
                    mou_unit = None
            else:
                minimum_purchase_details = None
                mou_unit = None
                minimum_purchase_unit = ""
                # minimum_purchase_qty = 0

            pricedata = {
                "outOfStock": outOfStock,
                "availableQuantity": availableQuantity,
                "unitName": row["units"][0]["unitName"][lan]
                if lan in row["units"][0]["unitName"]
                else row["units"][0]["unitName"]["en"],
                "unitId": row["units"][0]["unitId"],
                "basePrice": round(base_price, 2),
                "finalPrice": round(final_price, 2),
                "minimumPurchaseUnit": minimum_purchase_unit,
                "minimumPurchaseQty": minimum_purchase_qty,
                "discountPrice": discount_price,
                "mou": minimum_purchase_details,
                "mouUnit": mou_unit,
            }
            result = pricedata
            return result
        else:
            if "availableQuantity" in row["units"][0]:
                availableQuantity = (
                    row["units"][0]["availableQuantity"]
                    if "availableQuantity" in row["units"][0]
                    else 0
                )
            else:
                availableQuantity = 0

            if availableQuantity == "":
                availableQuantity = 0

            if availableQuantity > 0:
                outOfStock = False
            else:
                outOfStock = True
            for unit in row["units"]:
                minimum_purchase = 1
                package_units = ""
                package_type = ""

                minimum_purchase_qty = int(minimum_purchase)
                if package_units != "" and package_type != "":
                    minimum_purchase_details = (minimum_purchase) + " " + minimum_purchase_unit
                    if no_unit != "0":
                        try:
                            mou_unit = (
                                str(int(float(no_unit))) + " " + package_units + "/" + package_type
                            )
                        except:
                            mou_unit = no_unit + " " + package_units + "/" + package_type
                    else:
                        mou_unit = None
                else:
                    minimum_purchase_details = None
                    mou_unit = None
                    minimum_purchase_unit = ""

                try:
                    if margin_price == True:
                        base_price_tax = float(
                            row["units"][0]["b2cPricing"][0]["b2cproductSellingPrice"]
                        )
                    else:
                        base_price_tax = float(row["units"][0]["b2cPricing"][0]["b2cpriceWithTax"])
                except:
                    base_price_tax = row["suppliers"]["retailerPrice"]
                if base_price_tax == "":
                    base_price_tax = 0

                if float(currency_rate) > 0:
                    base_price_tax = base_price_tax * float(currency_rate)

                base_price = base_price_tax + ((base_price_tax) * tax_price) / 100

                pricedata = {
                    "availableQuantity": availableQuantity,
                    "outOfStock": outOfStock,
                    "unitName": unit["unitName"][lan]
                    if lan in unit["unitName"]
                    else unit["unitName"]["en"],
                    "unitId": unit["unitId"],
                    "basePrice": round(base_price, 2),
                    "finalPrice": round(base_price, 2),
                    "discountPrice": 0,
                    "minimumPurchaseQty": minimum_purchase_qty,
                    "minimumPurchaseUnit": minimum_purchase_unit,
                    "mou": minimum_purchase_details,
                    "mouUnit": mou_unit,
                }
                result = pricedata
                return result

    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
        pricedata = {
            "availableQuantity": 0,
            "outOfStock": True,
            "unitName": "",
            "mouUnit": "",
            "unitId": "",
            "minimumPurchaseUnit": "",
            "minimumPurchaseQty": 0,
            "basePrice": 0,
            "finalPrice": 0,
            "discountPrice": 0,
            "mou": "",
        }
        result = pricedata
        return result


async def search_read_new(
    res,
    start_time,
    language,
    filter_responseJson,
    finalfilter_responseJson_products,
    popularstatus,
    sort,
    login_type,
    store_id,
    sort_type,
    store_category_id,
    currency_code,
    from_data,
    to_data,
    user_id,
    remove_central,
    zone_id,
    min_price,
    max_price,
    margin_price,
    search_query,
    token,
    city_id,
):
    try:
        if len(res) <= 0:
            error = {"data": [], "message": "No Products Found"}
            return error
        else:
            last_data = store_search_data_new(
                res,
                start_time,
                language,
                filter_responseJson,
                finalfilter_responseJson_products,
                popularstatus,
                sort,
                login_type,
                store_id,
                sort_type,
                store_category_id,
                from_data,
                to_data,
                user_id,
                remove_central,
                zone_id,
                min_price,
                max_price,
                margin_price,
                currency_code,
                search_query,
                token,
                True,
                False,
                "",
            )
            finalSearchResults = {
                "data": last_data,
                "message": "Got the details",
            }
            return finalSearchResults
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
        finalResponse = {"message": message, "data": []}
        return finalResponse


class Search(APIView):
    """
    for popular status
            0 for normal item search
            1 for trending
            2 for popular search
    filter type
        1 for website and app search(central)
        0 for supplier search
    seach type
        0 for category
        1 for sub-category
        2 for sub-sub-category
    seach platform
        0 for website
        1 for ios
        2 for android
    click type
        1 for category
        2 for subcategory
        3 for subsubcategory"""

    @swagger_auto_schema(
        method="get",
        tags=["Metamuss Search & Filter"],
        operation_description="API for getting the category, sub-category, sub-sub-category and search and filter",
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
                name="ipAddress",
                default="124.40.244.94",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="ip address of the network",
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
                default="5df7b7218798dc2c1114e6bf",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="city id of the user where browser or app opened if not there value should be empty string",
            ),
            openapi.Parameter(
                name="country",
                default="5df7b7218798dc2c1114e6c0",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="country id of the user where browser or app opened if not there value should be empty string",
            ),
            openapi.Parameter(
                name="searchType",
                default="1",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="search type based on click on category, subcategory, subcategory or click on searched result. values should be 1 for category, 2 for subcategory, 3 for subsubcstegory, 4 for searched result click..Note if filter apply that time value should be 100",
            ),
            openapi.Parameter(
                name="searchIn",
                default="",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="search In for the data which on clicked.example In AppleMobile, In Mens",
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
                name="storeCategoryId",
                default="5cc0846e087d924456427975",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="q",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for the search the item in search bar ex. ni, nik, addi",
            ),
            openapi.Parameter(
                name="page",
                default="1",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="page number. which page number data want to display",
            ),
            openapi.Parameter(
                name="sort",
                default="price_asc",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for the sorting like price high to low value's should be...for low to high price:price_asc, high to low price:price_desc,popularity:recency_desc",
            ),
            openapi.Parameter(
                name="fname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="category name of the product..ex. Men, Women",
            ),
            openapi.Parameter(
                name="sname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="sub category name of the product while getting data through subcategory that time category name mandatory..ex. Footware",
            ),
            openapi.Parameter(
                name="tname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="sub sub category name of the product while getting data through subsubcategory that time category name and subcategory mandatory..ex. Footware",
            ),
            openapi.Parameter(
                name="colour",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="colour name for the which colour product want. ex. White, Green",
            ),
            openapi.Parameter(
                name="bname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="brand name for the product which want to display. ex. Nike, H&M",
            ),
            openapi.Parameter(
                name="customizable",
                default="0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="apply the filter for get only customizable products. NOTE if need data send value 1",
            ),
            openapi.Parameter(
                name="size",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="size for the product which want to display. ex. 8,9,S,M",
            ),
            openapi.Parameter(
                name="maxprice",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="maximum price for the product while applying price filter",
            ),
            openapi.Parameter(
                name="minprice",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="minimum price for the product while applying price filter",
            ),
            openapi.Parameter(
                name="o_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to displaying particular offers product. ex.5df89d3edd77d6ca2752bd10",
            ),
            openapi.Parameter(
                name="s_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to display particular stores product. ex.5df89d3edd77d6ca2752bd10",
            ),
            openapi.Parameter(
                name="z_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to display particular zone product. ex.5df8b7ad8798dc19da1a4b0e",
            ),
            openapi.Parameter(
                name="facet_",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while applying filter on attribute it will creating dynamic link. ex. facet_OCCASION, facet_IDEAL FOR",
            ),
        ],
        responses={
            200: "successfully. data found",
            404: "data not found. it might be product not found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request, *args, **kwargs):
        try:
            start_time = time.time()
            token = request.META["HTTP_AUTHORIZATION"]
            query = []
            should_query = []
            facet_value = ""
            facet_key = ""
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            user_id = json.loads(token)["userId"]
            try:
                session_id = json.loads(token)["sessionId"]
            except:
                session_id = ""
            # user_id = "5d92f959fc2045620ce36c92"
            start_time = time.time()
            finalfilter_responseJson_products = []
            filter_responseJson = []
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            try:
                login_type = json.loads(token)["metaData"]["institutionType"]
            except:
                login_type = 1

            if login_type == 0:
                login_type = 1
            print("login_type", login_type)
            ip_address = request.META["HTTP_IPADDRESS"] if "HTTP_IPADDRESS" in request.META else ""
            seach_platform = (
                request.META["HTTP_PLATFORM"] if "HTTP_PLATFORM" in request.META else "0"
            )
            city_name = request.META["HTTP_CITY"] if "HTTP_CITY" in request.META else ""
            country_name = request.META["HTTP_COUNTRY"] if "HTTP_COUNTRY" in request.META else ""
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            latitude = (
                float(request.META["HTTP_LATITUDE"]) if "HTTP_LATITUDE" in request.META else 0
            )
            longitude = (
                float(request.META["HTTP_LONGITUDE"]) if "HTTP_LONGITUDE" in request.META else 0
            )
            popular_status = (
                int(request.META["HTTP_POPULARSTATUS"])
                if "HTTP_POPULARSTATUS" in request.META
                else 0
            )
            search_type = (
                int(request.META["HTTP_SEARCHTYPE"]) if "HTTP_SEARCHTYPE" in request.META else 100
            )
            search_in = request.META["HTTP_SEARCHIN"] if "HTTP_SEARCHIN" in request.META else ""
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])

            # ========================================query parameter====================================================
            # for the search the item in search bar
            search_query = request.GET.get("q", "")
            page = int(request.GET.get("page", 1))  # for the pagination

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
            tname = tname.replace("%26", "&")

            colour = request.GET.get("colour", "")  # colour name
            bname = request.GET.get("bname", "")  # brand name for the search
            size = request.GET.get("size", "")  # size name
            max_price = request.GET.get("maxprice", "")  # maximum price
            min_price = request.GET.get("minprice", "")  # minimum price
            # sorting based on lowest price, newest first
            sort_data = request.GET.get("sort", "")
            # get the list of all products which have offers
            best_deals = request.GET.get("best_deals", "")
            customizable = request.GET.get("customizable", "0")
            offer_id = request.GET.get("o_id", "")  # get particular offer data
            store_id = request.GET.get("s_id", "")  # get particular offer data
            QUERY_STRING = request.META["QUERY_STRING"]
            if store_id != "":
                store_id = store_id.replace("Sid%3D", "")
                store_id = store_id.replace("%3F", "")
                store_id = store_id.replace("Sid=", "")
                store_id = store_id.replace("?", "")
            else:
                pass
            zone_id = request.GET.get("z_id", "")  # get particular offer data
            to_data = 30  # page*30
            from_data = int(page * 30) - 30
            facet_attribute = request.META["QUERY_STRING"]

            for facet in facet_attribute.split("&"):
                if "facet_" in facet:
                    if facet_value == "":
                        facet_value = facet_value + ((facet.split("_")[1]).split("=")[1]).replace(
                            "%20", " "
                        )
                    else:
                        facet_value = (
                            facet_value
                            + ", "
                            + ((facet.split("_")[1]).split("=")[1]).replace("%20", " ")
                        )

                    if facet_key == "":
                        facet_key = facet_key + ((facet.split("_")[1]).split("=")[0]).replace(
                            "%20", " "
                        )
                    else:
                        facet_key = (
                            facet_key
                            + ", "
                            + ((facet.split("_")[1]).split("=")[0]).replace("%20", " ")
                        )

            facet_value = facet_value.replace("+", " ")
            facet_value = facet_value.replace("%2F", "/")
            facet_value = facet_value.replace("%2C", ", ")
            facet_value = facet_value.strip()
            facet_value = facet_value.replace("%26", "")
            facet_key = facet_key.replace("+", " ")

            # started thread for inserting the data for search and category click data stored
            print("search type", search_type)
            if int(search_type) != 100:
                thread_logs = threading.Thread(
                    target=category_search_logs,
                    args=(
                        fname,
                        sname,
                        tname,
                        str(search_type),
                        user_id,
                        seach_platform,
                        ip_address,
                        latitude,
                        longitude,
                        city_name,
                        country_name,
                        search_query,
                        store_category_id,
                        search_in,
                        session_id,
                        store_id,
                        False,
                        "",
                        "",
                    ),
                )
                thread_logs.start()
            else:
                pass
            query.append({"match": {"storeCategoryId": store_category_id}})

            if offer_id != "":
                query.append({"match": {"offer.offerId": offer_id}})
                query.append({"match": {"offer.status": 1}})

            if store_id != "":
                query.append({"match": {"storeId": store_id.replace("=", "")}})
            else:
                pass

            if best_deals != "":
                query.append({"match": {"offer.status": 1}})

            if facet_value != "":
                number_facet_value = ""
                number_facet = re.findall(r"\d+(?:\.\d+)?", facet_value)
                my_number_facet = ",".join(number_facet)
                if len(number_facet) > 0:
                    if "," in facet_value or "%2C" in facet_value:
                        query.append(
                            {"match": {"units.attributes.attrlist.attrname.en": facet_key}}
                        )
                        query.append(
                            {"match": {"units.attributes.attrlist.attrname.en": facet_key}}
                        )
                        should_query.append(
                            {
                                "match": {
                                    "units.attributes.attrlist.value.en": {
                                        "analyzer": "standard",
                                        "query": facet_value.replace("%2C", ","),
                                        "boost": 6,
                                    }
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match": {
                                    "units.attributes.attrlist.value.name.en": {
                                        "analyzer": "standard",
                                        "query": my_number_facet,
                                        "boost": 6,
                                    }
                                }
                            }
                        )
                    else:
                        query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.en": facet_key
                                }
                            }
                        )
                        query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.en": facet_key
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.value.en": {
                                        "analyzer": "standard",
                                        "query": facet_value.replace("%2C", ","),
                                        "boost": 6,
                                    }
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.value.name.en": {
                                        "analyzer": "standard",
                                        "query": my_number_facet,
                                        "boost": 5,
                                    }
                                }
                            }
                        )

                else:
                    if "," in facet_value or "%2C" in facet_value:
                        query.append(
                            {"match": {"units.attributes.attrlist.attrname.en": facet_key}}
                        )
                        should_query.append(
                            {
                                "match": {
                                    "units.attributes.attrlist.value.en": {
                                        "analyzer": "standard",
                                        "query": facet_value.replace("%2C", ","),
                                        "boost": 6,
                                    }
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match": {
                                    "units.attributes.attrlist.value.name.en": {
                                        "query": facet_value.replace("%2C", ","),
                                        "boost": 5,
                                    }
                                }
                            }
                        )

                    else:
                        query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.en": facet_key
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.value.en": {
                                        "analyzer": "standard",
                                        "query": facet_value.replace("%2C", ","),
                                        "boost": 6,
                                    }
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.value.name.en": {
                                        "query": facet_value.replace("%2C", ","),
                                        "boost": 5,
                                    }
                                }
                            }
                        )

            if search_query != "":
                search_item = {
                    "searched_item": search_query,
                    "ip_addess": ip_address,
                    "latitude": float(latitude),
                    "longitude": float(longitude),
                    "seach_platform": seach_platform,
                    "city_name": city_name,
                    "country_name": country_name,
                    "user_id": user_id,
                    "timestamp": int(datetime.datetime.now().timestamp()) * 1000,
                }
                # for the search need to set the priority for the match highest product, medium for category and lowest for barnd or seller
                # here we are using boost for the set the priority. highest number have highest prioriy(Individual fields can be boosted automatically — count more towards the relevance score — at query time, with the boost parameter)
                # here we are using fuzziness for the set the match string data. highest number have highest fuzziness(Maximum edit distance allowed for matching. See Fuzziness for valid values and more information.)
                space_count = 0
                for a in search_query.replace("%20", " "):
                    if (a.isspace()) == True:
                        space_count += 1

                # ===========================product name========================================
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "pName.en": {
                                "analyzer": "standard",
                                "query": search_query.replace("%20", " "),
                                "boost": 6,
                            }
                        }
                    }
                )
                should_query.append(
                    {"match": {"pName.en": {"query": search_query.replace("%20", " "), "boost": 6}}}
                )

            if max_price != "" and min_price != "":
                query.append(
                    {
                        "range": {
                            "units.floatValue": {"gte": float(min_price), "lte": float(max_price)}
                        }
                    }
                )

            if size != "":
                size = size.replace("%2C", ",")
                size = size.replace(",", ", ")
                query.append({"match": {"units.unitSizeGroupValue.en": size.replace("%20", " ")}})
            else:
                pass

            if colour != "":
                if "," in colour or "%2C" in colour:
                    query.append({"match": {"units.colorName": colour.replace("%20", " ")}})
                else:
                    query.append({"term": {"units.colorName.keyword": colour.replace("%20", " ")}})

            if bname != "":
                if "," in bname or "%2C" in bname:
                    query.append({"match": {"brandTitle." + language: bname.replace("%20", " ")}})
                else:
                    query.append(
                        {
                            "match_phrase_prefix": {
                                "brandTitle." + language: bname.replace("%20", " ")
                            }
                        }
                    )

            if fname != "":
                if "," in fname or "%2C" in fname:
                    fname = fname.replace("%20", " ")
                    query.append(
                        {
                            "terms": {
                                "categoryList.parentCategory.categoryName."
                                + language
                                + ".keyword": fname.split(",")
                            }
                        }
                    )
                else:
                    if fname != "":
                        category_query = {"categoryName.en": fname.replace("%20", " ")}
                        if store_id != "":
                            category_query["$or"] = [
                                {"storeid": {"$in": [store_id]}, "storeId": {"$in": [store_id]}}
                            ]
                        category_details = db.category.find_one(category_query)
                        if (
                            category_details != None
                            and store_category_id != ECOMMERCE_STORE_CATEGORY_ID
                        ):
                            cat_name = str(category_details["_id"])
                            query.append(
                                {
                                    "match_phrase_prefix": {
                                        "categoryList.parentCategory.categoryName."
                                        + language: fname.replace("%20", " ")
                                    }
                                }
                            )
                        else:
                            query.append(
                                {
                                    "match_phrase_prefix": {
                                        "categoryList.parentCategory.categoryName."
                                        + language: fname.replace("%20", " ")
                                    }
                                }
                            )

            if sname != "":
                if "," in sname or "%2C" in sname:
                    query.append(
                        {
                            "match": {
                                "categoryList.parentCategory.childCategory.categoryName."
                                + language: sname.replace("%20", " ")
                            }
                        }
                    )
                else:
                    if sname != "":
                        query.append(
                            {
                                "match": {
                                    "categoryList.parentCategory.childCategory.categoryName."
                                    + language: sname.replace("%20", " ")
                                }
                            }
                        )
            if tname != "":
                if "," in tname or "%2C" in tname:
                    query.append(
                        {
                            "match": {
                                "categoryList.parentCategory.childCategory.categoryName."
                                + language: tname.replace("%20", " ")
                            }
                        }
                    )
                else:
                    query.append(
                        {
                            "match_phrase_prefix": {
                                "categoryList.parentCategory.childCategory.categoryName."
                                + language: tname.replace("%20", " ")
                            }
                        }
                    )

            if int(customizable) == 1:
                query.append({"match": {"units.attributes.attrlist.customizable": 1}})

            if "_" in sort_data:
                if "price" in sort_data.split("_")[0]:
                    if sort_data.split("_")[1] == "desc":
                        sort_type = 1
                    else:
                        sort_type = 0

                    sort_query = [
                        {"isInStock": {"order": "desc"}},
                        {"units.discountPrice": {"order": sort_data.split("_")[1]}},
                    ]
                elif "recency" in sort_data.split("_")[0]:
                    sort_type = 2
                    sort_query = [
                        {"isInStock": {"order": "desc"}},
                        {"units.discountPrice": {"order": "asc"}},
                    ]
                else:
                    sort_type = 2
                    sort_query = [
                        {"isInStock": {"order": "desc"}},
                        {"units.discountPrice": {"order": "asc"}},
                    ]
            else:
                sort_type = 2
                sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"units.discountPrice": {"order": "asc"}},
                ]

            must_not = []
            if int(login_type) != 2:
                must_not.append({"match": {"units.b2cPricing.b2cproductSellingPrice": 0}})
            else:
                must_not.append({"match": {"units.b2bPricing.b2bproductSellingPrice": 0}})
            must_not.append({"match": {"storeId": "0"}})
            query.append({"match": {"status": 1}})
            number_for = [0]
            number_for.append(login_type)
            query.append({"terms": {"productFor": number_for}})
            store_json = ["0"]
            store_details = db.stores.find({"storeFrontTypeId": 5})
            for s in store_details:
                store_json.append(str(s["_id"]))

            if len(should_query) > 0:
                search_item_query = {
                    "query": {
                        "bool": {
                            "must": query,
                            "should": should_query,
                            "must_not": must_not,
                            "minimum_should_match": 1,
                            "boost": 1.0,
                        }
                    },
                    "track_total_hits": True,
                    "sort": sort_query,
                    "aggs": {
                        "group_by_sub_category": {
                            "terms": {
                                "field": "parentProductId.keyword",
                                "order": {"avg_score": "desc"},
                                "size": int(to_data),
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
                                                "parentProductId",
                                                "currencySymbol",
                                                "prescriptionRequired",
                                                "needsIdProof",
                                                "saleOnline",
                                                "detailDescription",
                                                "offer",
                                                "uploadProductDetails",
                                                "currency",
                                                "pPName",
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
                                },
                            },
                        }
                    },
                }
            else:
                search_item_query = {
                    "query": {"bool": {"must": query, "must_not": must_not}},
                    "track_total_hits": True,
                    "sort": sort_query,
                    "aggs": {
                        "group_by_sub_category": {
                            "terms": {
                                "field": "parentProductId.keyword",
                                "order": {"avg_score": "desc"},
                                "size": int(to_data),
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
                                                "detailDescription",
                                                "offer",
                                                "parentProductId",
                                                "currencySymbol",
                                                "currency",
                                                "pPName",
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
                                },
                            },
                        }
                    },
                }
            res = es.search(index=index_products, body=search_item_query)
            print("after query execute time", time.time() - start_time)
            try:
                if "value" in res["hits"]["total"]:
                    if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                        final_json = {"data": [], "message": "No Data Found"}
                        return JsonResponse(final_json, safe=False, status=404)
                else:
                    if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                        final_json = {"data": [], "message": "No Data Found"}
                        return JsonResponse(final_json, safe=False, status=404)
            except:
                if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                    final_json = {"data": [], "message": "No Data Found"}
                    return JsonResponse(final_json, safe=False, status=404)

            loop = asyncio.new_event_loop()
            event_loop = asyncio.set_event_loop(loop)
            if "sort" in search_item_query:
                if "units.floatValue.keyword" in search_item_query["sort"]:
                    if search_item_query["sort"]["units.floatValue.keyword"]["order"] == "asc":
                        sort = 0
                    elif search_item_query["sort"]["units.floatValue.keyword"]["order"] == "desc":
                        sort = 1
                else:
                    sort = 2
            else:
                sort = 3

            data = loop.run_until_complete(
                asyncio.gather(
                    search_read_new(
                        res,
                        start_time,
                        language,
                        filter_responseJson,
                        finalfilter_responseJson_products,
                        popular_status,
                        sort,
                        login_type,
                        store_id,
                        sort_type,
                        store_category_id,
                        currency_code,
                        from_data,
                        to_data,
                        user_id,
                        False,
                        zone_id,
                        min_price,
                        max_price,
                        True,
                        search_query,
                        token,
                        QUERY_STRING,
                    )
                )
            )
            # =============================================for brand need to fetch the footer details===================
            footer_details = {}
            if bname != "":
                brand_name = bname.replace("%20", " ")
                brand_name_title = brand_name.title()
                brand_name_upper = brand_name.upper()
                brand_name_lower = brand_name.lower()
                or_query = [
                    {"name.en": brand_name_title},
                    {"name.en": brand_name_upper},
                    {"name.en": brand_name_lower},
                ]
                brand_details = db.brands.find_one(
                    {"$or": or_query, "storeCategoryId": store_category_id}
                )
                if brand_details is not None:
                    footer_details["description"] = (
                        brand_details["description"][language]
                        if language in brand_details["description"]
                        else brand_details["description"]["en"]
                    )
                    footer_details["title"] = (
                        brand_details["name"][language]
                        if language in brand_details["name"]
                        else brand_details["name"]["en"]
                    )
                else:
                    pass
            elif tname != "":
                brand_details = db.category.find_one({"categoryName.en": tname.replace("%20", " ")})
                if brand_details is not None:
                    footer_details["description"] = (
                        brand_details["categoryDesc"][language]
                        if language in brand_details["categoryDesc"]
                        else brand_details["categoryDesc"]["en"]
                    )
                    footer_details["title"] = (
                        brand_details["categoryName"][language]
                        if language in brand_details["categoryName"]
                        else brand_details["categoryName"]["en"]
                    )
                else:
                    pass
            elif sname != "":
                brand_details = db.category.find_one({"categoryName.en": sname.replace("%20", " ")})
                if brand_details is not None:
                    footer_details["description"] = (
                        brand_details["categoryDesc"][language]
                        if language in brand_details["categoryDesc"]
                        else brand_details["categoryDesc"]["en"]
                    )
                    footer_details["title"] = (
                        brand_details["categoryName"][language]
                        if language in brand_details["categoryName"]
                        else brand_details["categoryName"]["en"]
                    )
                else:
                    pass
            elif fname != "":
                brand_details = db.category.find_one(
                    {
                        "categoryName.en": fname.replace("%20", " "),
                        "storeCategory.storeCategoryId": store_category_id,
                    }
                )
                if brand_details is not None:
                    if brand_details["categoryDesc"] is not None:
                        footer_details["description"] = (
                            brand_details["categoryDesc"][language]
                            if language in brand_details["categoryDesc"]
                            else brand_details["categoryDesc"]["en"]
                        )
                        footer_details["title"] = (
                            brand_details["categoryName"][language]
                            if language in brand_details["categoryName"]
                            else brand_details["categoryName"]["en"]
                        )
                    else:
                        pass
                else:
                    pass
            else:
                pass
            if len(data[0]) == 0:
                data[0]["data"]["footer"] = footer_details
                return JsonResponse(data[0], safe=False, status=404)
            else:
                try:
                    data[0]["data"]["footer"] = footer_details
                except:
                    try:
                        data[0]["data"]["footer"] = ""
                    except:
                        pass
                return JsonResponse(data[0], safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)

class MettamuseSearch(APIView):
    """
    for popular status
            0 for normal item search
            1 for trending
            2 for popular search
    filter type
        1 for website and app search(central)
        0 for supplier search
    seach type
        0 for category
        1 for sub-category
        2 for sub-sub-category
    seach platform
        0 for website
        1 for ios
        2 for android
    click type
        1 for category
        2 for subcategory
        3 for subsubcategory"""

    @swagger_auto_schema(
        method="get",
        tags=["Metamuss Search & Filter"],
        operation_description="API for getting the category, sub-category, sub-sub-category and search and filter",
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
                name="ipAddress",
                default="124.40.244.94",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="ip address of the network",
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
                default="5df7b7218798dc2c1114e6bf",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="city id of the user where browser or app opened if not there value should be empty string",
            ),
            openapi.Parameter(
                name="country",
                default="5df7b7218798dc2c1114e6c0",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="country id of the user where browser or app opened if not there value should be empty string",
            ),
            openapi.Parameter(
                name="searchType",
                default="1",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="search type based on click on category, subcategory, subcategory or click on searched result. values should be 1 for category, 2 for subcategory, 3 for subsubcstegory, 4 for searched result click..Note if filter apply that time value should be 100",
            ),
            openapi.Parameter(
                name="searchIn",
                default="",
                required=False,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="search In for the data which on clicked.example In AppleMobile, In Mens",
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
                name="storeCategoryId",
                default="5cc0846e087d924456427975",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="q",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for the search the item in search bar ex. ni, nik, addi",
            ),
            openapi.Parameter(
                name="page",
                default="1",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="page number. which page number data want to display",
            ),
            openapi.Parameter(
                name="sort",
                default="price_asc",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for the sorting like price high to low value's should be...\n\
                for low to high price: price_asc, high to low price: price_desc,\n\
                popularity: recency_desc, popular_score, name: name_asc, name_desc\n\
                newest: newest_first",
            ),
            openapi.Parameter(
                name="fname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="first category id",
            ),
            openapi.Parameter(
                name="mainCategoryId",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="category id",
            ),
            openapi.Parameter(
                name="sname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="sub category id",
            ),
            openapi.Parameter(
                name="tname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="sub sub category id",
            ),
            openapi.Parameter(
                name="colour",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="colour name for the which colour product want. ex. White, Green",
            ),
            openapi.Parameter(
                name="bid",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="brand id",
            ),
            openapi.Parameter(
                name="customizable",
                default="0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="apply the filter for get only customizable products. NOTE if need data send value 1",
            ),
            openapi.Parameter(
                name="size",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="size for the product which want to display. ex. 8,9,S,M",
            ),
            openapi.Parameter(
                name="maxprice",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="maximum price for the product while applying price filter",
            ),
            openapi.Parameter(
                name="minprice",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="minimum price for the product while applying price filter",
            ),
            openapi.Parameter(
                name="o_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to displaying particular offers product. ex.5df89d3edd77d6ca2752bd10",
            ),
            openapi.Parameter(
                name="s_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to display particular stores product. ex.5df89d3edd77d6ca2752bd10",
            ),
            openapi.Parameter(
                name="z_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to display particular zone product. ex.5df8b7ad8798dc19da1a4b0e",
            ),
            openapi.Parameter(
                name="facet_",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while applying filter on attribute it will creating dynamic link. ex. facet_OCCASION, facet_IDEAL FOR",
            ),
        ],
        responses={
            200: "successfully. data found",
            404: "data not found. it might be product not found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request, *args, **kwargs):
        try:
            start_time = time.time()
            token = request.META["HTTP_AUTHORIZATION"]
            query = []
            should_query = []
            facet_value = ""
            facet_key = ""
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            user_id = json.loads(token)["userId"]
            try:
                session_id = json.loads(token)["sessionId"]
            except:
                session_id = ""
            # user_id = "5d92f959fc2045620ce36c92"
            start_time = time.time()
            finalfilter_responseJson_products = []
            filter_responseJson = []
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            try:
                login_type = json.loads(token)["metaData"]["institutionType"]
            except:
                login_type = 1

            if login_type == 0:
                login_type = 1
            ip_address = request.META["HTTP_IPADDRESS"] if "HTTP_IPADDRESS" in request.META else ""
            seach_platform = (
                request.META["HTTP_PLATFORM"] if "HTTP_PLATFORM" in request.META else "0"
            )
            city_name = request.META["HTTP_CITY"] if "HTTP_CITY" in request.META else ""
            country_name = request.META["HTTP_COUNTRY"] if "HTTP_COUNTRY" in request.META else ""
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            latitude = (
                float(request.META["HTTP_LATITUDE"]) if "HTTP_LATITUDE" in request.META else 0
            )
            longitude = (
                float(request.META["HTTP_LONGITUDE"]) if "HTTP_LONGITUDE" in request.META else 0
            )
            popular_status = (
                int(request.META["HTTP_POPULARSTATUS"])
                if "HTTP_POPULARSTATUS" in request.META
                else 0
            )
            search_type = (
                int(request.META["HTTP_SEARCHTYPE"]) if "HTTP_SEARCHTYPE" in request.META else 100
            )
            search_in = request.META["HTTP_SEARCHIN"] if "HTTP_SEARCHIN" in request.META else ""
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])

            # ========================================query parameter====================================================
            # for the search the item in search bar
            search_query = request.GET.get("q", "")
            page = int(request.GET.get("page", 1))  # for the pagination

            fname = request.GET.get("fname", "")  # category-name
            fname = fname.replace("%20", " ")
            fname = fname.replace("+", " ")

            sname = request.GET.get("sname", "")  # sub-category-name
            sname = sname.replace("%20", " ")
            sname = sname.replace("+", " ")

            tname = request.GET.get("tname", "")  # sub-sub-category-name
            tname = tname.replace("%20", " ")
            tname = tname.replace("+", " ")

            f_id = request.GET.get("mainCategoryId", "")  # category id
            colour = request.GET.get("colour", "")  # colour name
            b_id = request.GET.get("bid", "")  # brand id
            size = request.GET.get("size", "")  # size name
            max_price = request.GET.get("maxprice", "")  # maximum price
            min_price = request.GET.get("minprice", "")  # minimum price
            # sorting based on lowest price, newest first
            sort_data = request.GET.get("sort", "")
            # get the list of all products which have offers
            best_deals = request.GET.get("best_deals", "")
            customizable = request.GET.get("customizable", "0")
            offer_id = request.GET.get("o_id", "")  # get particular offer data
            store_id = request.GET.get("s_id", "")  # get particular offer data
            QUERY_STRING = request.META["QUERY_STRING"]
            if store_id != "":
                store_id = store_id.replace("Sid%3D", "")
                store_id = store_id.replace("%3F", "")
                store_id = store_id.replace("Sid=", "")
                store_id = store_id.replace("?", "")
            else:
                pass
            zone_id = request.GET.get("z_id", "")  # get particular offer data
            to_data = 30  # page*30
            from_data = int(page * 30) - 30
            facet_attribute = request.META["QUERY_STRING"]

            for facet in facet_attribute.split("&"):
                if "facet_" in facet:
                    if facet_value == "":
                        facet_value = facet_value + ((facet.split("_")[1]).split("=")[1]).replace(
                            "%20", " "
                        )
                    else:
                        facet_value = (
                            facet_value
                            + ", "
                            + ((facet.split("_")[1]).split("=")[1]).replace("%20", " ")
                        )

                    if facet_key == "":
                        facet_key = facet_key + ((facet.split("_")[1]).split("=")[0]).replace(
                            "%20", " "
                        )
                    else:
                        facet_key = (
                            facet_key
                            + ", "
                            + ((facet.split("_")[1]).split("=")[0]).replace("%20", " ")
                        )

            facet_value = facet_value.replace("+", " ")
            facet_value = facet_value.replace("%2F", "/")
            facet_value = facet_value.replace("%2C", ", ")
            facet_value = facet_value.strip()
            facet_value = facet_value.replace("%26", "")
            facet_key = facet_key.replace("+", " ")

            # started thread for inserting the data for search and category click data stored
            print("search type", search_type)
            if int(search_type) != 100:
                thread_logs = threading.Thread(
                    target=category_search_logs,
                    args=(
                        fname,
                        sname,
                        tname,
                        str(search_type),
                        user_id,
                        seach_platform,
                        ip_address,
                        latitude,
                        longitude,
                        city_name,
                        country_name,
                        search_query,
                        store_category_id,
                        search_in,
                        session_id,
                        store_id,
                        False,
                        "",
                        "",
                    ),
                )
                thread_logs.start()
            else:
                pass
            query.append({"match": {"storeCategoryId": store_category_id}})

            if offer_id != "":
                query.append({"match": {"offer.offerId": offer_id}})
                query.append({"match": {"offer.status": 1}})

            if store_id != "":
                query.append({"match": {"storeId": store_id.replace("=", "")}})
            else:
                pass

            if best_deals != "":
                query.append({"match": {"offer.status": 1}})

            if facet_value != "":
                number_facet_value = ""
                number_facet = re.findall(r"\d+(?:\.\d+)?", facet_value)
                my_number_facet = ",".join(number_facet)
                if len(number_facet) > 0:
                    if "," in facet_value or "%2C" in facet_value:
                        query.append(
                            {"match": {"units.attributes.attrlist.attrname.en": facet_key}}
                        )
                        query.append(
                            {"match": {"units.attributes.attrlist.attrname.en": facet_key}}
                        )
                        should_query.append(
                            {
                                "match": {
                                    "units.attributes.attrlist.value.en": {
                                        "analyzer": "standard",
                                        "query": facet_value.replace("%2C", ","),
                                        "boost": 6,
                                    }
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match": {
                                    "units.attributes.attrlist.value.name.en": {
                                        "analyzer": "standard",
                                        "query": my_number_facet,
                                        "boost": 6,
                                    }
                                }
                            }
                        )
                    else:
                        query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.en": facet_key
                                }
                            }
                        )
                        query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.en": facet_key
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.value.en": {
                                        "analyzer": "standard",
                                        "query": facet_value.replace("%2C", ","),
                                        "boost": 6,
                                    }
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.value.name.en": {
                                        "analyzer": "standard",
                                        "query": my_number_facet,
                                        "boost": 5,
                                    }
                                }
                            }
                        )

                else:
                    if "," in facet_value or "%2C" in facet_value:
                        query.append(
                            {"match": {"units.attributes.attrlist.attrname.en": facet_key}}
                        )
                        should_query.append(
                            {
                                "match": {
                                    "units.attributes.attrlist.value.en": {
                                        "analyzer": "standard",
                                        "query": facet_value.replace("%2C", ","),
                                        "boost": 6,
                                    }
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match": {
                                    "units.attributes.attrlist.value.name.en": {
                                        "query": facet_value.replace("%2C", ","),
                                        "boost": 5,
                                    }
                                }
                            }
                        )

                    else:
                        query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.en": facet_key
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.value.en": {
                                        "analyzer": "standard",
                                        "query": facet_value.replace("%2C", ","),
                                        "boost": 6,
                                    }
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.value.name.en": {
                                        "query": facet_value.replace("%2C", ","),
                                        "boost": 5,
                                    }
                                }
                            }
                        )

            if search_query != "":
                search_item = {
                    "searched_item": search_query,
                    "ip_addess": ip_address,
                    "latitude": float(latitude),
                    "longitude": float(longitude),
                    "seach_platform": seach_platform,
                    "city_name": city_name,
                    "country_name": country_name,
                    "user_id": user_id,
                    "timestamp": int(datetime.datetime.now().timestamp()) * 1000,
                }
                # for the search need to set the priority for the match highest product, medium for category and lowest for barnd or seller
                # here we are using boost for the set the priority. highest number have highest prioriy(Individual fields can be boosted automatically — count more towards the relevance score — at query time, with the boost parameter)
                # here we are using fuzziness for the set the match string data. highest number have highest fuzziness(Maximum edit distance allowed for matching. See Fuzziness for valid values and more information.)
                space_count = 0
                for a in search_query.replace("%20", " "):
                    if (a.isspace()) == True:
                        space_count += 1

                is_redis_data = False
                search_query = search_query.replace("%20", " ")
                should_query.extend([
                    # {
                    #     "term": {
                    #         "pName.en": {
                    #             "value": search_query,
                    #             "boost": 5
                    #         }
                    #     }
                    # },
                    {
                        "term": {
                            "brandTitle.en.keyword": {
                                "value": search_query,
                                "boost": 4
                            }
                        }
                    } if not " " in search_query else {
                        "term": {
                            "brandTitle.en": {
                                "value": search_query,
                                "boost": 4
                            }
                        }
                    },
                    {
                        "term": {
                            "categoryList.parentCategory.categoryName.en": {
                                "value": search_query,
                                "boost": 4
                            }
                        }
                    },
                    {
                        "term": {
                            "categoryList.parentCategory.childCategory.categoryName.en": {
                                "value": search_query,
                                "boost": 4
                            }
                        }
                    },
                    {
                        "span_first": {
                            "match": {
                                "span_term": {
                                    "pName.en": search_query
                                }
                            },
                            "end": len(search_query)
                        }
                    },
                    {
                        "match_phrase_prefix": {
                            "pName.en": {
                                "analyzer": "standard",
                                "query": search_query,
                                "boost": 5
                            }
                        }
                    },
                    {
                        "match": {
                            "pName.en": {
                                "analyzer": "standard",
                                "query": search_query,
                                "boost": 4
                            }
                        }
                    },
                    {
                        "match": {
                            "pName.en": {
                                "analyzer": "edgengram_analyzer",
                                "query": search_query,
                                "boost": 0.1,
                                # "cutoff_frequency": 1,
                                "operator": "and"
                            }
                        }
                    },
                    # ======================================brand name=======================================
                    {
                        "match": {
                            "brandTitle." + language:
                                {
                                    "analyzer": "standard",
                                    "query": search_query,
                                    "boost": 2
                                }
                        }
                    },
                    # ====================================child category=======================================
                    {
                        "match": {
                            "categoryList.parentCategory.categoryName." + language:{
                                "analyzer": "standard",
                                "query": search_query,
                                "boost": 1
                            }
                        }
                    },
                    {
                        "match": {
                            "categoryList.parentCategory.childCategory.categoryName." + language: {
                                "analyzer": "standard",
                                "query": search_query,
                                "boost": 1
                            }
                        }
                    },
                    # ===========================================detail description============================
                    {
                        "match_phrase_prefix": {
                            "detailDescription." + language: {
                                "analyzer": "whitespace",
                                "query": search_query,
                                "boost": 0
                            }
                        }
                    },
                    # ===========================unit name========================================
                    {
                        "match_phrase_prefix": {
                            "units.unitName.en": {
                                "analyzer": "whitespace",
                                "query": search_query,
                                "boost": 0,
                            }
                        }
                    }
                ])

            if max_price != "" and min_price != "":
                query.append(
                    {
                        "range": {
                            "units.floatValue": {"gte": float(min_price), "lte": float(max_price)}
                        }
                    }
                )

            if size != "":
                size = size.replace("%2C", ",")
                size = size.replace(",", ", ")
                query.append({"match": {"units.unitSizeGroupValue.en": size.replace("%20", " ")}})
            else:
                pass

            if colour != "":
                if "," in colour or "%2C" in colour:
                    query.append({"match": {"units.colorName": colour.replace("%20", " ")}})
                else:
                    query.append({"term": {"units.colorName.keyword": colour.replace("%20", " ")}})

            ### need to search base on category id
            if f_id != "":
                f_id = f_id.replace("%2C", ",")
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "categoryList.parentCategory.categoryId": f_id
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "categoryList.parentCategory.childCategory.categoryId": f_id
                        }
                    }
                )
            else:
                pass

            if b_id != "":
                query.append(
                    {
                        "match": {
                            "brand": b_id.replace(",", ", ")
                        }
                    }
                )

            if fname != "":
                query.append(
                    {
                        "match": {
                            "categoryList.parentCategory.categoryId": fname
                        }
                    }
                )

            if sname != "":
                query.append(
                    {
                        "match": {
                            "categoryList.parentCategory.childCategory.categoryId": sname
                        }
                    }
                )
            if tname != "":
                query.append(
                    {
                        "match": {
                            "categoryList.parentCategory.childCategory.categoryId": tname
                        }
                    }
                )

            if int(customizable) == 1:
                query.append({"match": {"units.attributes.attrlist.customizable": 1}})
            if "price" in sort_data.split("_")[0]:
                if sort_data.split("_")[1] == "desc":
                    sort_type = 1
                else:
                    sort_type = 0

                sort_query = [
                    {"units.discountPrice": {"order": sort_data.split("_")[1]}},
                ]
                bucket_sort_query = [
                    {"units.discountPrice": {"order": sort_data.split("_")[1]}},
                ]
            elif "name" in sort_data.split("_")[0]:
                print("in::: 2")
                if sort_data.split("_")[1] == "desc":
                    sort_type = 3  # name sorted descending
                else:
                    sort_type = 4  # name sorted ascending

                sort_query = [
                    {"pName.en.keyword": {"order": sort_data.split("_")[1]}},
                    {"isInStock": {"order": "desc"}},
                ]
                bucket_sort_query = [
                    {"pName.en.keyword": {"order": sort_data.split("_")[1]}},
                    {"isInStock": {"order": "desc"}},
                ]
            elif "recency" in sort_data.split("_")[0] or "newest" in sort_data.split("_")[0]:
                print("in::: 3")
                sort_type = 6 # newest first
                sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"createdTimestamp": {"order": "desc"}},
                ]
                bucket_sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"createdTimestamp": {"order": "desc"}},
                ]
            elif "popular_score" == sort_data.lower() and store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                print("in::: 4")
                sort_type = 5 # popularScore descending
                sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"popularScore": {"order": "desc"}},
                ]
                bucket_sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"popularScore": {"order": "desc"}},
                ]
            else:
                print("in::: 1")
                sort_type = 7
                sort_query = [
                    {"isInStock": {"order": "desc"}},
                ]
                bucket_sort_query = [
                    {"isInStock": {"order": "desc"}},
                ]
            print("sort_data:::", sort_data)
            print("sort_type:::", sort_type)
            must_not = []
            if int(login_type) != 2:
                must_not.append({"match": {"units.b2cPricing.b2cproductSellingPrice": 0}})
            else:
                must_not.append({"match": {"units.b2bPricing.b2bproductSellingPrice": 0}})
            must_not.append({"match": {"storeId": "0"}})
            query.append({"match": {"status": 1}})
            number_for = [0]
            number_for.append(login_type)
            query.append({"terms": {"productFor": number_for}})
            store_json = ["0"]
            store_details = db.stores.find({"storeFrontTypeId": 5})
            for s in store_details:
                store_json.append(str(s["_id"]))

            # this we added for the sorting the bucket base on request
            '''
                sort type 0 means price sorting in asc
                sort type 1 means price sorting in desc
            '''
            if sort_type == 0:
                avg_score = {"max": {"script": "_score"}}
                order_score = {"avg_score": "desc"}
            elif sort_type == 1:
                avg_score = {"max": {"field": "units.discountPrice"}}
                order_score = {"avg_score": "desc"}
            elif sort_type == 0:
                avg_score = {"min": {"field": "units.discountPrice"}}
                order_score = {"avg_score": "asc"}
            elif sort_type == 5:
                avg_score = {"max": {"field": "popularScore"}}
                order_score = {"avg_score": "desc"}
            elif sort_type == 6:
                avg_score = {"max": {"field": "createdTimestamp"}}
                order_score = {"avg_score": "desc"}
            elif sort_type == 7:
                avg_score = {"avg": {"script": "_score"}}
                order_score = {"avg_score": "desc"}

            if len(should_query) > 0:
                search_item_query = {
                    "query": {
                        "bool": {
                            "must": query,
                            "should": should_query,
                            "must_not": must_not,
                            "minimum_should_match": 1,
                            "boost": 1.0,
                        }
                    },
                    "track_total_hits": True,
                    "sort": bucket_sort_query,
                    "aggs": {
                        "group_by_sub_category": {
                            "terms": {
                                "field": "parentProductId.keyword",
                                "order": order_score,
                                "size": int(to_data),
                            },
                            "aggs": {
                                "avg_score": avg_score,
                                "top_sales_hits": {
                                    "top_hits": {
                                        "sort": bucket_sort_query,
                                        "_source": {
                                            "includes": [
                                                "_id",
                                                "_score",
                                                "pName",
                                                "storeId",
                                                "parentProductId",
                                                "currencySymbol",
                                                "prescriptionRequired",
                                                "needsIdProof",
                                                "saleOnline",
                                                "detailDescription",
                                                "offer",
                                                "uploadProductDetails",
                                                "currency",
                                                "pPName",
                                                "tax",
                                                "brandTitle",
                                                "categoryList",
                                                "images",
                                                "avgRating",
                                                "units",
                                                "storeCategoryId",
                                                "manufactureName",
                                                "maxQuantity",
                                                "popularScore"
                                            ]
                                        },
                                        "size": 1,
                                    }
                                },
                            },
                        }
                    },
                }
            else:
                search_item_query = {
                    "query": {"bool": {"must": query, "must_not": must_not}},
                    "track_total_hits": True,
                    "sort": bucket_sort_query,
                    "aggs": {
                        "group_by_sub_category": {
                            "terms": {
                                "field": "parentProductId.keyword",
                                "order": order_score,
                                "size": int(to_data),
                            },
                            "aggs": {
                                "avg_score": avg_score,
                                "top_sales_hits": {
                                    "top_hits": {
                                        "sort": bucket_sort_query,
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
                                                "detailDescription",
                                                "offer",
                                                "parentProductId",
                                                "currencySymbol",
                                                "currency",
                                                "pPName",
                                                "tax",
                                                "brandTitle",
                                                "categoryList",
                                                "images",
                                                "avgRating",
                                                "units",
                                                "storeCategoryId",
                                                "manufactureName",
                                                "maxQuantity",
                                                "popularScore"
                                            ]
                                        },
                                        "size": 1,
                                    }
                                },
                            },
                        }
                    },
                }
            print("search_item_query", search_item_query)
            res = es.search(index=index_products, body=search_item_query)
            try:
                if "value" in res["hits"]["total"]:
                    if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                        final_json = {"data": [], "message": "No Data Found"}
                        return JsonResponse(final_json, safe=False, status=404)
                else:
                    if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                        final_json = {"data": [], "message": "No Data Found"}
                        return JsonResponse(final_json, safe=False, status=404)
            except:
                if res["hits"]["total"] == 0 or "hits" not in res["hits"]:
                    final_json = {"data": [], "message": "No Data Found"}
                    return JsonResponse(final_json, safe=False, status=404)

            loop = asyncio.new_event_loop()
            event_loop = asyncio.set_event_loop(loop)
            if "sort" in search_item_query:
                if "units.floatValue.keyword" in search_item_query["sort"]:
                    if search_item_query["sort"]["units.floatValue.keyword"]["order"] == "asc":
                        sort = 0
                    elif search_item_query["sort"]["units.floatValue.keyword"]["order"] == "desc":
                        sort = 1
                else:
                    sort = 2
            else:
                sort = 3

            data = loop.run_until_complete(
                asyncio.gather(
                    search_read_new(
                        res,
                        start_time,
                        language,
                        filter_responseJson,
                        finalfilter_responseJson_products,
                        popular_status,
                        sort,
                        login_type,
                        store_id,
                        sort_type,
                        store_category_id,
                        currency_code,
                        from_data,
                        to_data,
                        user_id,
                        False,
                        zone_id,
                        min_price,
                        max_price,
                        True,
                        search_query,
                        token,
                        QUERY_STRING,
                    )
                )
            )
            # =============================================for brand need to fetch the footer details===================
            footer_details = {}
            if b_id != "":
                brand_details = db.brands.find_one(
                    {"_id": ObjectId(b_id), "storeCategoryId": store_category_id}
                )
                if brand_details is not None:
                    footer_details["description"] = (
                        brand_details["description"][language]
                        if language in brand_details["description"]
                        else brand_details["description"]["en"]
                    )
                    footer_details["title"] = (
                        brand_details["name"][language]
                        if language in brand_details["name"]
                        else brand_details["name"]["en"]
                    )
                else:
                    pass
            elif tname != "":
                brand_details = db.category.find_one({"_id": ObjectId(tname)})
                if brand_details is not None:
                    footer_details["description"] = (
                        brand_details["categoryDesc"][language]
                        if language in brand_details["categoryDesc"]
                        else brand_details["categoryDesc"]["en"]
                    )
                    footer_details["title"] = (
                        brand_details["categoryName"][language]
                        if language in brand_details["categoryName"]
                        else brand_details["categoryName"]["en"]
                    )
                else:
                    pass
            elif sname != "":
                brand_details = db.category.find_one({"_id": ObjectId(sname)})
                if brand_details is not None:
                    footer_details["description"] = (
                        brand_details["categoryDesc"][language]
                        if language in brand_details["categoryDesc"]
                        else brand_details["categoryDesc"]["en"]
                    )
                    footer_details["title"] = (
                        brand_details["categoryName"][language]
                        if language in brand_details["categoryName"]
                        else brand_details["categoryName"]["en"]
                    )
                else:
                    pass
            elif fname != "":
                brand_details = db.category.find_one(
                    {
                        "_id": ObjectId(fname),
                        "storeCategory.storeCategoryId": store_category_id,
                    }
                )
                if brand_details is not None:
                    footer_details["description"] = (
                        brand_details["categoryDesc"][language]
                        if language in brand_details["categoryDesc"]
                        else brand_details["categoryDesc"]["en"]
                    )
                    footer_details["title"] = (
                        brand_details["categoryName"][language]
                        if language in brand_details["categoryName"]
                        else brand_details["categoryName"]["en"]
                    )
                else:
                    pass
            else:
                pass
            if len(data[0]) == 0:
                data[0]["data"]["footer"] = footer_details
                return JsonResponse(data[0], safe=False, status=404)
            else:
                try:
                    data[0]["data"]["footer"] = footer_details
                except:
                    try:
                        data[0]["data"]["footer"] = ""
                    except:
                        pass
                return JsonResponse(data[0], safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class FilterList(APIView):
    """
    for popular status
            0 for normal item search
            1 for trending
            2 for popular search
    filter type
        1 for website and app search(central)
        0 for supplier search
    seach type
        0 for category
        1 for sub-category
        2 for sub-sub-category
    seach platform
        0 for website
        1 for ios
        2 for android
    filter type
        category 1
        subcategory 2
        subsubcategory 3
        brand 4
        colour 5
        size 6
        price 7
        attribute 8
        symptoms 9
    """

    @swagger_auto_schema(
        method="get",
        tags=["Metamuss Search & Filter"],
        operation_description="API for getting the category, sub-category, sub-sub-category",
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
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="level",
                default="1",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="level for the category list value is 1, level 2 for get the subcategory filter, level 3 for sub sub category filter ",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default="5cc0846e087d924456427975",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="q",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for the search the item in search bar ex. ni, nik, addi",
            ),
            openapi.Parameter(
                name="page",
                default="1",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="page number. which page number data want to display",
            ),
            openapi.Parameter(
                name="sort",
                default="price_asc",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for the sorting like price high to low value's should be...for low to high price:price_asc, high to low price:price_desc,popularity:recency_desc",
            ),
            openapi.Parameter(
                name="fname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="category name of the product..ex. Men, Women",
            ),
            openapi.Parameter(
                name="sname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="sub category name of the product while getting data through subcategory that time category name mandatory..ex. Footware",
            ),
            openapi.Parameter(
                name="tname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="sub sub category name of the product while getting data through subsubcategory that time category name and subcategory mandatory..ex. Footware",
            ),
            openapi.Parameter(
                name="symptoms",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="symptom name for the product which want to display. ex. Nike, H&M",
            ),
            openapi.Parameter(
                name="colour",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="colour name for the which colour product want. ex. White, Green",
            ),
            openapi.Parameter(
                name="bname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="brand name for the product which want to display. ex. Nike, H&M",
            ),
            openapi.Parameter(
                name="size",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="size for the product which want to display. ex. 8,9,S,M",
            ),
            openapi.Parameter(
                name="maxprice",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="maximum price for the product while applying price filter",
            ),
            openapi.Parameter(
                name="minprice",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="minimum price for the product while applying price filter",
            ),
            openapi.Parameter(
                name="customizable",
                default="0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="apply the filter for get only customizable products. NOTE if need data send value 1",
            ),
            openapi.Parameter(
                name="o_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to displaying particular offers product. ex.5df89d3edd77d6ca2752bd10",
            ),
            openapi.Parameter(
                name="s_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to display particular stores product. ex.5df89d3edd77d6ca2752bd10",
            ),
            openapi.Parameter(
                name="z_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to display particular zones product. ex.5df89d3edd77d6ca2752bd10",
            ),
            openapi.Parameter(
                name="facet_",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while applying filter on attribute it will creating dynamic link. ex. facet_OCCASION, facet_IDEAL FOR",
            ),
        ],
        responses={
            200: "successfully. data found",
            404: "data not found. it might be user not found, product not found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request, *args, **kwargs):
        try:
            start_time = time.time()
            token = request.META["HTTP_AUTHORIZATION"]
            query = []
            should_query = []
            facet_value = ""
            facet_key = ""
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            start_time = time.time()
            currencySymbol = "$"
            color_list = []
            brand_list = []
            sym_list = []
            category_list = []
            sub_categpry_list = []
            sub_sub_category_list = []
            finalfilter_responseJson_products = []
            filter_responseJson = []
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            filter_level = request.META["HTTP_LEVEL"] if "HTTP_LEVEL" in request.META else 1
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            login_type = 1
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            # ========================================query parameter====================================================
            # for the search the item in search bar
            search_query = request.GET.get("q", "")
            page = int(request.GET.get("page", 1))  # for the pagination
            # for the sorting like price high to low
            sort = request.GET.get("sort", "")

            fname = request.GET.get("fname", "")  # category-name
            fname = fname.replace("%20", " ")
            fname = fname.replace("+", " ")

            sname = request.GET.get("sname", "")  # sub-category-name
            sname = sname.replace("%20", " ")
            sname = sname.replace("+", " ")

            tname = request.GET.get("tname", "")  # sub-sub-category-name
            tname = tname.replace("%20", " ")
            tname = tname.replace("+", " ")

            colour = request.GET.get("colour", "")  # colour name
            bname = request.GET.get("bname", "")  # brand name for the search
            symptoms_name = request.GET.get("symptoms", "")  # brand name for the search
            size = request.GET.get("size", "")  # size name
            max_price = request.GET.get("maxprice", "")  # maximum price
            min_price = request.GET.get("minprice", "")  # minimum price
            customizable = request.GET.get("customizable", "0")  # customizable
            best_deals = request.GET.get("best_deals", "")  # get the best deals
            # get the data for particular offer data
            offer_id = request.GET.get("o_id", "")
            # get the data for particular store data
            store_id = request.GET.get("s_id", "")
            if store_id != "":
                store_id = store_id.replace("Sid%3D", "")
                store_id = store_id.replace("%3F", "")
                store_id = store_id.replace("Sid=", "")
                store_id = store_id.replace("?", "")
            else:
                pass
            zone_id = request.GET.get("z_id", "")
            to_data = 300
            from_data = int(page * 300) - 300

            facet_attribute = request.META["QUERY_STRING"] if "QUERY_STRING" in request.META else ""
            sname = sname.replace("+", " ")

            # ==============================for customizable attributes==================================
            if int(customizable) == 1:
                query.append({"match": {"units.attributes.attrlist.customizable": 1}})

            for facet in facet_attribute.split("&"):
                if "facet_" in facet:
                    if facet_value == "":
                        facet_value = facet_value + ((facet.split("_")[1]).split("=")[1]).replace(
                            "%20", " "
                        )
                    else:
                        facet_value = (
                            facet_value
                            + ", "
                            + ((facet.split("_")[1]).split("=")[1]).replace("%20", " ")
                        )

                    if facet_key == "":
                        facet_key = facet_key + ((facet.split("_")[1]).split("=")[0]).replace(
                            "%20", " "
                        )
                    else:
                        facet_key = (
                            facet_key
                            + ", "
                            + ((facet.split("_")[1]).split("=")[0]).replace("%20", " ")
                        )

            facet_value = facet_value.replace("+", " ")
            facet_value = facet_value.replace("%2F", "/")
            facet_value = facet_value.replace("%2C", ", ")
            facet_value = facet_value.strip()
            facet_value = facet_value.replace("%26", "")
            facet_key = facet_key.replace("+", " ")

            query.append({"match": {"storeCategoryId": store_category_id}})

            if offer_id != "":
                query.append({"match": {"offer.offerId": offer_id}})
                query.append(
                    {"match": {"offer.status": 1}}
                    # {"terms": {"status": ["0", "1"]}}
                )

            if best_deals != "":
                query.append(
                    {"match": {"offer.status": 1}}
                    # {"terms": {"status": ["1"]}}
                )

            if store_id != "":
                query.append(
                    {"match": {"storeId": store_id.replace("=", "")}}
                    # {"terms": {"status": ["1"]}}
                )

            if facet_value != "":
                number_facet_value = ""
                number_facet = re.findall(r"\d+(?:\.\d+)?", facet_value)
                if len(number_facet) > 0:
                    if "," in facet_value or "%2C" in facet_value:
                        should_query.append(
                            {"match": {"units.attributes.attrlist.attrname.en": facet_key}}
                        )
                        should_query.append(
                            {
                                "match": {
                                    "units.attributes.attrlist.attrname.value.en": facet_value.replace(
                                        "%2C", ","
                                    )
                                }
                            }
                        )
                        should_query.append(
                            {"match": {"units.attributes.attrlist.attrname.en": facet_key}}
                        )
                        should_query.append(
                            {"terms": {"units.attributes.attrlist.attrname.value.en": number_facet}}
                        )
                    else:
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.en": facet_key
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.value.en": facet_value.replace(
                                        "%2C", ","
                                    )
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.en": facet_key
                                }
                            }
                        )
                        should_query.append(
                            {"terms": {"units.attributes.attrlist.attrname.value.en": number_facet}}
                        )

                else:
                    if "," in facet_value or "%2C" in facet_value:
                        should_query.append(
                            {"match": {"units.attributes.attrlist.attrname.en": facet_key}}
                        )
                        should_query.append(
                            {
                                "match": {
                                    "units.attributes.attrlist.attrname.value.en": facet_value.replace(
                                        "%2C", ","
                                    )
                                }
                            }
                        )

                    else:
                        query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.en": facet_key
                                }
                            }
                        )
                        query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.value.en": facet_value.replace(
                                        "%2C", ","
                                    )
                                }
                            }
                        )

            if max_price != "" and min_price != "":
                query.append(
                    {
                        "range": {
                            "units.floatValue": {"gte": float(min_price), "lte": float(max_price)}
                        }
                    }
                )

            if size != "":
                size = size.replace("%2C", ",")
                size = size.replace(",", ", ")
                query.append({"match": {"units.unitSizeGroupValue.en": size.replace("%20", " ")}})
            else:
                pass

            if colour != "":
                if "," in colour or "%2C" in colour:
                    query.append({"match": {"units.colorName": colour.replace("%20", " ")}})
                else:
                    query.append({"term": {"units.colorName.keyword": colour.replace("%20", " ")}})

            if bname != "":
                if "," in bname or "%2C" in bname:
                    query.append({"match": {"brandTitle." + language: bname.replace("%20", " ")}})
                else:
                    query.append(
                        {
                            "match_phrase_prefix": {
                                "brandTitle." + language: bname.replace("%20", " ")
                            }
                        }
                    )

            if fname != "":
                if "," in fname or "%2C" in fname:
                    fname = fname.replace("%20", " ")
                    query.append(
                        {
                            "terms": {
                                "categoryList.parentCategory.categoryName."
                                + language
                                + ".keyword": fname.split(",")
                            }
                        }
                    )
                else:
                    if fname != "":
                        category_query = {"categoryName.en": fname.replace("%20", " ")}
                        if store_id != "":
                            category_query["$or"] = [
                                {"storeid": {"$in": [store_id]}, "storeId": {"$in": [store_id]}}
                            ]
                        category_details = db.category.find_one(category_query)
                        if (
                            category_details != None
                            and store_category_id != ECOMMERCE_STORE_CATEGORY_ID
                        ):
                            cat_name = str(category_details["_id"])
                            query.append(
                                {
                                    "match_phrase_prefix": {
                                        "categoryList.parentCategory.categoryName."
                                        + language: fname.replace("%20", " ")
                                    }
                                }
                            )
                        else:
                            query.append(
                                {
                                    "match_phrase_prefix": {
                                        "categoryList.parentCategory.categoryName."
                                        + language: fname.replace("%20", " ")
                                    }
                                }
                            )

            if sname != "":
                if "," in sname or "%2C" in sname:
                    query.append(
                        {
                            "match": {
                                "categoryList.parentCategory.childCategory.categoryName."
                                + language: sname.replace("%20", " ")
                            }
                        }
                    )
                else:
                    if sname != "":
                        query.append(
                            {
                                "term": {
                                    "categoryList.parentCategory.childCategory.categoryName."
                                    + language
                                    + ".keyword": sname.replace("%20", " ")
                                }
                            }
                        )
            if tname != "":
                if "," in tname or "%2C" in tname:
                    query.append(
                        {
                            "match": {
                                "categoryList.parentCategory.childCategory.categoryName."
                                + language: tname.replace("%20", " ")
                            }
                        }
                    )
                else:
                    query.append(
                        {
                            "match_phrase_prefix": {
                                "categoryList.parentCategory.childCategory.categoryName."
                                + language: tname.replace("%20", " ")
                            }
                        }
                    )

            query.append({"match": {"status": 1}})
            number_for = [0]
            number_for.append(int(login_type))
            query.append({"terms": {"productFor": number_for}})

            if search_query != "":
                if len(should_query) > 0:
                    filter_parameters_query = {
                        "query": {
                            "bool": {
                                "must": query,
                                "should": should_query,
                                "minimum_should_match": 1,
                                "must_not": [{"match": {"storeId": "0"}}],
                                "boost": 1.0,
                            }
                        },
                        "size": to_data,
                        "from": from_data,
                    }
                else:
                    filter_parameters_query = {
                        "query": {
                            "bool": {"must": query, "must_not": [{"match": {"storeId": "0"}}]}
                        },
                        "size": to_data,
                        "from": from_data,
                    }
            else:
                if len(should_query) > 0:
                    filter_parameters_query = {
                        "query": {
                            "bool": {
                                "must": query,
                                "should": should_query,
                                "minimum_should_match": 1,
                                "must_not": [{"match": {"storeId": "0"}}],
                                "boost": 1.0,
                            }
                        },
                        "size": to_data,
                        "from": from_data,
                    }
                else:
                    filter_parameters_query = {
                        "query": {
                            "bool": {"must": query, "must_not": [{"match": {"storeId": "0"}}]}
                        },
                        "size": to_data,
                        "from": from_data,
                    }

            units_data = []
            size_data = []
            res = {}
            # Es Query to get all the filter parameters
            res_filter_parameters = es.search(
                index=index_products,
                body=filter_parameters_query,
                filter_path=[
                    "hits.hits._id",
                    "hits.hits._source.units",
                    "hits.hits._source.currencySymbol",
                    "hits.hits._source.brandTitle",
                    "hits.hits._source.catName",
                    "hits.hits._source.subCatName",
                    "hits.hits._source.subSubCatName",
                    "hits.hits._source.currency",
                    "hits.hits._source.units",
                    "hits.hits._source.colour",
                    "hits.hits._source.symptoms",
                    "hits.hits._source.sizes",
                    "hits.hits._source.categoryList",
                ],
            )
            if int(filter_level) == 1:
                if len(res_filter_parameters) == 0:
                    response = {"data": [], "message": "No Data Found"}
                    return JsonResponse(response, safe=False, status=404)

            # gets all the required data for a particular filter type
            if int(filter_level) == 1:
                currencySymbol = "€"
                currency = "EUR"
                attr_data = []
                attr_name = []
                number_attr_name = []
                for i in res_filter_parameters["hits"]["hits"]:
                    facet_data = db.childProducts.find_one({"_id": ObjectId(i["_id"])})
                    tax_value = []
                    if facet_data != None:
                        if type(facet_data["tax"]) == list:
                            for tax in facet_data["tax"]:
                                tax_value.append({"value": tax["taxValue"]})
                        else:
                            if facet_data["tax"] != None:
                                if "taxValue" in facet_data["tax"]:
                                    tax_value.append({"value": facet_data["tax"]["taxValue"]})
                                else:
                                    tax_value.append({"value": facet_data["tax"]})
                            else:
                                pass
                    else:
                        tax_value = []

                    for facet in facet_data["units"]:
                        try:
                            # ==============================price ========================================
                            try:
                                main_price = facet["b2cPricing"][0]["b2cproductSellingPrice"]
                            except:
                                main_price = facet["b2cPricing"][0]["floatValue"]
                            # ==================================get currecny rate============================
                            try:
                                currency_rate = float(
                                    currency_exchange_rate[
                                        str(facet_data["currency"]) + "_" + str(currency_code)
                                    ]
                                )
                            except:
                                currency_rate = 0
                            currency_details = db.currencies.find_one(
                                {"currencyCode": currency_code}
                            )
                            if currency_details is not None:
                                currencySymbol = currency_details["currencySymbol"]
                                currency = currency_details["currencyCode"]

                            if currency_rate > 0:
                                main_price = main_price * currency_rate
                            else:
                                pass

                            try:
                                tax_price = 0
                                if len(tax_value) == 0:
                                    tax_price = 0
                                else:
                                    for amount in tax_value:
                                        tax_price = tax_price + (int(amount["value"]))
                                base_price = main_price + ((main_price * tax_price) / 100)
                                units_data.append(base_price)
                            except:
                                pass

                            # ======================= colour and size=====================================
                            if "unitSizeGroupValue" in facet:
                                if facet["unitSizeGroupValue"] != "":
                                    try:
                                        if facet["unitSizeGroupValue"]["en"] != "":
                                            size_data.append(
                                                {"name": facet["unitSizeGroupValue"]["en"]}
                                            )
                                        else:
                                            pass
                                    except:
                                        pass
                            else:
                                pass

                            try:
                                if facet["colorName"] != "":
                                    color_list.append({"name": facet["colorName"]})
                            except:
                                pass

                            for facet_attr in facet["attributes"]:
                                for attr in facet_attr["attrlist"]:
                                    try:
                                        if attr["searchable"] == 1:
                                            if attr["attrname"]["en"].upper() != "PRICE":
                                                attr_name.append(attr["attrname"]["en"].upper())
                                                value = []
                                                try:
                                                    if type(attr["value"]["en"]) == str:
                                                        attr_data.append(
                                                            {
                                                                "name": attr["attrname"][
                                                                    "en"
                                                                ].upper(),
                                                                "data": [attr["value"]["en"]],
                                                            }
                                                        )
                                                    else:
                                                        try:
                                                            for x in facet["value"]:
                                                                if type(x["en"]) == int:
                                                                    value.append(x)
                                                                else:
                                                                    try:
                                                                        value.append(
                                                                            x["en"].strip(" ")
                                                                        )
                                                                    except:
                                                                        value.append(x["en"])
                                                            attr_data.append(
                                                                {
                                                                    "name": attr["attrname"][
                                                                        "en"
                                                                    ].upper(),
                                                                    "data": value,
                                                                }
                                                            )
                                                        except:
                                                            for x in facet["value"]:
                                                                if type(x["name"]["en"]) == int:
                                                                    value.append(x)
                                                                else:
                                                                    try:
                                                                        value.append(
                                                                            x["name"]["en"].strip(
                                                                                " "
                                                                            )
                                                                        )
                                                                    except:
                                                                        value.append(
                                                                            x["name"]["en"]
                                                                        )
                                                            attr_data.append(
                                                                {
                                                                    "name": attr["attrname"][
                                                                        "en"
                                                                    ].upper(),
                                                                    "data": value,
                                                                }
                                                            )
                                                except:
                                                    if type(attr["value"]) == dict:
                                                        for x in attr["value"]["en"]:
                                                            if type(x) == int:
                                                                value.append(x)
                                                            else:
                                                                try:
                                                                    value.append(x.strip(" "))
                                                                except:
                                                                    value.append(x)
                                                    else:
                                                        for x in attr["value"]:
                                                            if type(x["name"]["en"]) == int:
                                                                value.append(x)
                                                            else:
                                                                try:
                                                                    value.append(
                                                                        x["name"]["en"].strip(" ")
                                                                    )
                                                                except:
                                                                    value.append(x["name"]["en"])
                                                    attr_data.append(
                                                        {
                                                            "name": attr["attrname"]["en"].upper(),
                                                            "data": value,
                                                        }
                                                    )
                                            else:
                                                pass
                                    except Exception as ex:
                                        template = (
                                            "An exception of type {0} occurred. Arguments:\n{1!r}"
                                        )
                                        message = template.format(type(ex).__name__, ex.args)
                                        (
                                            "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                                            type(ex).__name__,
                                            ex,
                                        )
                                        pass
                        except:
                            pass

                    try:
                        brand_name = i["_source"]["brandTitle"][language]
                    except:
                        try:
                            brand_name = i["_source"]["brandTitle"]["en"]
                        except:
                            brand_name = ""

                    brand_list = []

                    if "categoryList" in i["_source"]:
                        for cat in i["_source"]["categoryList"]:
                            category_list.append(
                                {"name": cat["parentCategory"]["categoryName"]["en"]}
                            )
                # ================================colour================================================================
                if len(color_list) > 0:
                    dataframe = pd.DataFrame(color_list)
                    dataframe["penCount"] = dataframe.groupby("name")["name"].transform("count")
                    dataframe = dataframe.drop_duplicates("name", keep="last")
                    colors_json = dataframe.to_json(orient="records")
                    colors_json = json.loads(colors_json)
                    new_colour_list = sorted(colors_json, key=lambda k: k["name"])
                else:
                    new_colour_list = []
                # ======================================size============================================================
                if len(size_data) > 0:
                    size_dataframe = pd.DataFrame(size_data)
                    # size_dataframe = size_dataframe[size_dataframe.name != '']
                    size_dataframe["penCount"] = size_dataframe.groupby("name")["name"].transform(
                        "count"
                    )
                    size_dataframe = size_dataframe.drop_duplicates("name", keep="last")
                    sizes = size_dataframe.to_json(orient="records")
                    sizes = json.loads(sizes)
                    sizes = sorted(sizes, key=lambda k: k["name"])
                else:
                    sizes = []
                # =====================================brand============================================================
                if len(brand_list) > 0:
                    brand_dataframe = pd.DataFrame(brand_list)
                    brand_dataframe["penCount"] = brand_dataframe.groupby("name")["name"].transform(
                        "count"
                    )
                    brand_dataframe = brand_dataframe.drop_duplicates("name", keep="last")
                    brand_list = brand_dataframe.to_json(orient="records")
                    brand_list = json.loads(brand_list)
                    brand_list = sorted(brand_list, key=lambda k: k["name"])
                else:
                    brand_list = []

                # =======================================category=======================================================
                if len(category_list) > 0:
                    cat_dataframe = pd.DataFrame(category_list)
                    cat_dataframe["penCount"] = cat_dataframe.groupby("name")["name"].transform(
                        "count"
                    )
                    cat_dataframe = cat_dataframe.drop_duplicates("name", keep="last")
                    cat_list = cat_dataframe.to_json(orient="records")
                    cat_list = json.loads(cat_list)
                    cat_list = sorted(cat_list, key=lambda k: k["name"])
                else:
                    cat_list = []

                res_color = {}
                for d in attr_data:
                    res_color.setdefault(d["name"], []).append(
                        {"name": d["name"], "data": d["data"]}
                    )
                attribute_data = []
                for attr in list(set(attr_name)):
                    try:
                        new_data = []
                        for i in res_color[attr]:
                            new_data = i["data"] + new_data
                        attr_dataframe = pd.DataFrame(new_data)
                        attr_dataframe.columns = ["name"]
                        attr_dataframe["penCount"] = attr_dataframe.groupby("name")[
                            "name"
                        ].transform("count")
                        attr_dataframe = attr_dataframe.drop_duplicates("name", keep="last")
                        attr_list = attr_dataframe.to_json(orient="records")
                        attr_list = json.loads(attr_list)

                        attribute_data.append(
                            {"name": attr, "data": attr_list, "selType": 1, "filterType": 8}
                        )
                    except:
                        pass

                brands = {"name": "BRAND", "data": brand_list, "selType": 1, "filterType": 4}

                category = {
                    # "name": "Categories",
                    "name": "CATEGORIES",
                    "data": cat_list,
                    "selType": 2,
                    "level": 1,
                    "filterType": 1,
                }

                colors_data = {
                    "name": "Colour".upper(),
                    "data": new_colour_list,
                    "selType": 4,
                    "filterType": 5,
                }

                rating_value = [
                    {"name": "4★ & above", "value": 4},
                    {"name": "3★ & above", "value": 3},
                    {"name": "2★ & above", "value": 2},
                    {"name": "1★ & above", "value": 1},
                ]
                rating_data = {
                    "name": "CUSTOMER RATINGS",
                    "data": rating_value,
                    "selType": 1,
                }
                try:
                    max_price = max(list(set(units_data)))
                except:
                    max_price = 0
                try:
                    min_price = min(list(set(units_data)))
                except:
                    min_price = 0

                if min_price == max_price:
                    min_price = 0
                p_data = {
                    "name": "PRICE",
                    "data": [{"maxPrice": max_price, "minPrice": min_price}],
                    "selType": 3,
                    "filterType": 7,
                    "currencySymbol": currencySymbol,
                    "currency": currency,
                }
                new_size = []
                # for s in sizes:
                #     try:
                #         new_size.append(
                #             {
                #                 "name": int(s['name']),
                #                 "penCount": s['penCount']
                #             }
                #         )
                #     except:
                #         pass
                # new_category_path = sorted(new_size, key=itemgetter('name'), reverse=False)
                sizes_data = {"name": "SIZE", "data": sizes, "selType": 1, "filterType": 6}
                discount_data = db.discountType.find({"status": 1})
                disc_data = []
                for disc in discount_data:
                    disc_data.append(
                        {
                            "value": disc["discountValue"],
                            "name": disc["name"][language]
                            if language in disc["name"]
                            else disc["name"]["en"],
                        }
                    )
                discount_json = {"name": "DISCOUNT", "data": disc_data, "selType": 1}
                filters_json = []
                filters_json.append(colors_data)
                filters_json.append(sizes_data)
                filters_json.append(p_data)
                filters_json.append(brands)
                filters_json.append(category)
                for atb in attribute_data:
                    filters_json.append(atb)
                Final_output = {
                    "data": {
                        "filters": filters_json,
                        # "currency": currencySymbol,
                        "currencySymbol": currency,
                        "currency": currencySymbol,
                        "unitId": str(ObjectId()),
                        "message": "Filters Found",
                    }
                }

                return JsonResponse(Final_output, safe=False, status=200)

            elif int(filter_level) == 2:
                currencySymbol = "$"
                category_details = db.category.find_one({"categoryName.en": fname, "storeId": "0"})
                if category_details != None:
                    child_category_details = db.category.find(
                        {"parentId": ObjectId(category_details["_id"]), "storeId": "0", "status": 1}
                    )
                    for child_cat in child_category_details:
                        child_data_count = db.category.find(
                            {"parentId": ObjectId(child_cat["_id"]), "status": 1}
                        ).count()
                        if sname != "":
                            if sname == child_cat["categoryName"]["en"]:
                                sub_categpry_list.append(child_cat["categoryName"]["en"])
                            else:
                                pass
                        else:
                            sub_categpry_list.append(child_cat["categoryName"]["en"])

                subcategory = {
                    "name": "subCategories",
                    # "data": sub_cat_list,
                    "data": list(set(sub_categpry_list)),
                    "selType": 2,
                    "level": 2,
                    "filterType": 2,
                }
                filters_json = [subcategory]
                Final_output = {
                    "data": {
                        "filters": filters_json,
                        # "currency": currencySymbol,
                        "currencySymbol": "₹",
                        "currency": "INR",
                        "unitId": str(ObjectId()),
                        "message": "Filters Found",
                    }
                }
                return JsonResponse(Final_output, safe=False, status=200)

            elif int(filter_level) == 3:
                for i in res_filter_parameters["hits"]["hits"]:
                    currencySymbol = "$"
                    if len(i["_source"]["subSubCatName"]) > 0:
                        sub_sub_category_list.append(i["_source"]["subSubCatName"][language])
                    else:
                        pass
                subsubcategory = {
                    "name": "subsubCategories",
                    "data": list(set(sub_sub_category_list)),
                    "selType": 2,
                    "level": 3,
                    "filterType": 3,
                }
                filters_json = [subsubcategory]
                Final_output = {
                    "data": {
                        "filters": filters_json,
                        # "currency": currencySymbol,
                        "currencySymbol": "₹",
                        "currency": "INR",
                        "unitId": str(ObjectId()),
                        "message": "Filters Found",
                    }
                }
                return JsonResponse(Final_output, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class MettamuseFilterList(APIView):
    """
    for popular status
            0 for normal item search
            1 for trending
            2 for popular search
    filter type
        1 for website and app search(central)
        0 for supplier search
    seach type
        0 for category
        1 for sub-category
        2 for sub-sub-category
    seach platform
        0 for website
        1 for ios
        2 for android
    filter type
        category 1
        subcategory 2
        subsubcategory 3
        brand 4
        colour 5
        size 6
        price 7
        attribute 8
        symptoms 9
    """

    @swagger_auto_schema(
        method="get",
        tags=["Metamuss Search & Filter"],
        operation_description="API for getting the category, sub-category, sub-sub-category",
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
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="level",
                default="1",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="level for the category list value is 1, level 2 for get the subcategory filter, level 3 for sub sub category filter ",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default="5cc0846e087d924456427975",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="q",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for the search the item in search bar ex. ni, nik, addi",
            ),
            openapi.Parameter(
                name="page",
                default="1",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="page number. which page number data want to display",
            ),
            openapi.Parameter(
                name="sort",
                default="price_asc",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for the sorting like price high to low value's should be...for low to high price:price_asc, high to low price:price_desc,popularity:recency_desc",
            ),
            openapi.Parameter(
                name="fname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="first category id",
            ),
            openapi.Parameter(
                name="mainCategoryId",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="category id",
            ),
            openapi.Parameter(
                name="sname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="sub category id",
            ),
            openapi.Parameter(
                name="tname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="sub sub category id",
            ),
            openapi.Parameter(
                name="symptoms",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="symptom name for the product which want to display. ex. Nike, H&M",
            ),
            openapi.Parameter(
                name="colour",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="colour name for the which colour product want. ex. White, Green",
            ),
            openapi.Parameter(
                name="bid",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="brand id",
            ),
            openapi.Parameter(
                name="size",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="size for the product which want to display. ex. 8,9,S,M",
            ),
            openapi.Parameter(
                name="maxprice",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="maximum price for the product while applying price filter",
            ),
            openapi.Parameter(
                name="minprice",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="minimum price for the product while applying price filter",
            ),
            openapi.Parameter(
                name="customizable",
                default="0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="apply the filter for get only customizable products. NOTE if need data send value 1",
            ),
            openapi.Parameter(
                name="o_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to displaying particular offers product. ex.5df89d3edd77d6ca2752bd10",
            ),
            openapi.Parameter(
                name="s_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to display particular stores product. ex.5df89d3edd77d6ca2752bd10",
            ),
            openapi.Parameter(
                name="z_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to display particular zones product. ex.5df89d3edd77d6ca2752bd10",
            ),
            openapi.Parameter(
                name="facet_",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while applying filter on attribute it will creating dynamic link. ex. facet_OCCASION, facet_IDEAL FOR",
            ),
        ],
        responses={
            200: "successfully. data found",
            404: "data not found. it might be user not found, product not found",
            401: "Unauthorized. token expired",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request, *args, **kwargs):
        try:
            start_time = time.time()
            token = request.META["HTTP_AUTHORIZATION"]
            query = []
            should_query = []
            facet_value = ""
            facet_key = ""
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            start_time = time.time()
            currencySymbol = "$"
            color_list = []
            brand_list = []
            sym_list = []
            category_list = []
            sub_categpry_list = []
            sub_sub_category_list = []
            finalfilter_responseJson_products = []
            filter_responseJson = []
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            filter_level = request.META["HTTP_LEVEL"] if "HTTP_LEVEL" in request.META else 1
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            login_type = 1
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            # ========================================query parameter====================================================
            # for the search the item in search bar
            search_query = request.GET.get("q", "")
            page = int(request.GET.get("page", 1))  # for the pagination
            # for the sorting like price high to low
            sort = request.GET.get("sort", "")

            f_id = request.GET.get("mainCategoryId", "")  # category id
            b_id = request.GET.get("bid", "")  # brand id
            bname = request.GET.get("bname", "")  # brand name for the search
            fname = request.GET.get("fname", "")  # category-name
            fname = fname.replace("%20", " ")
            fname = fname.replace("+", " ")

            sname = request.GET.get("sname", "")  # sub-category-name
            sname = sname.replace("%20", " ")
            sname = sname.replace("+", " ")

            tname = request.GET.get("tname", "")  # sub-sub-category-name
            tname = tname.replace("%20", " ")
            tname = tname.replace("+", " ")

            colour = request.GET.get("colour", "")  # colour name
            symptoms_name = request.GET.get("symptoms", "")  # brand name for the search
            size = request.GET.get("size", "")  # size name
            max_price = request.GET.get("maxprice", "")  # maximum price
            min_price = request.GET.get("minprice", "")  # minimum price
            customizable = request.GET.get("customizable", "0")  # customizable
            best_deals = request.GET.get("best_deals", "")  # get the best deals
            # get the data for particular offer data
            offer_id = request.GET.get("o_id", "")
            # get the data for particular store data
            store_id = request.GET.get("s_id", "")
            if store_id != "":
                store_id = store_id.replace("Sid%3D", "")
                store_id = store_id.replace("%3F", "")
                store_id = store_id.replace("Sid=", "")
                store_id = store_id.replace("?", "")
            else:
                pass
            zone_id = request.GET.get("z_id", "")
            to_data = 300
            from_data = int(page * 300) - 300

            facet_attribute = request.META["QUERY_STRING"] if "QUERY_STRING" in request.META else ""
            sname = sname.replace("+", " ")

            # ==============================for customizable attributes==================================
            if int(customizable) == 1:
                query.append({"match": {"units.attributes.attrlist.customizable": 1}})

            for facet in facet_attribute.split("&"):
                if "facet_" in facet:
                    if facet_value == "":
                        facet_value = facet_value + ((facet.split("_")[1]).split("=")[1]).replace(
                            "%20", " "
                        )
                    else:
                        facet_value = (
                            facet_value
                            + ", "
                            + ((facet.split("_")[1]).split("=")[1]).replace("%20", " ")
                        )

                    if facet_key == "":
                        facet_key = facet_key + ((facet.split("_")[1]).split("=")[0]).replace(
                            "%20", " "
                        )
                    else:
                        facet_key = (
                            facet_key
                            + ", "
                            + ((facet.split("_")[1]).split("=")[0]).replace("%20", " ")
                        )

            facet_value = facet_value.replace("+", " ")
            facet_value = facet_value.replace("%2F", "/")
            facet_value = facet_value.replace("%2C", ", ")
            facet_value = facet_value.strip()
            facet_value = facet_value.replace("%26", "")
            facet_key = facet_key.replace("+", " ")

            query.append({"match": {"storeCategoryId": store_category_id}})

            if offer_id != "":
                query.append({"match": {"offer.offerId": offer_id}})
                query.append(
                    {"match": {"offer.status": 1}}
                    # {"terms": {"status": ["0", "1"]}}
                )

            if best_deals != "":
                query.append(
                    {"match": {"offer.status": 1}}
                    # {"terms": {"status": ["1"]}}
                )

            if store_id != "":
                query.append(
                    {"match": {"storeId": store_id.replace("=", "")}}
                    # {"terms": {"status": ["1"]}}
                )

            if facet_value != "":
                number_facet_value = ""
                number_facet = re.findall(r"\d+(?:\.\d+)?", facet_value)
                if len(number_facet) > 0:
                    if "," in facet_value or "%2C" in facet_value:
                        should_query.append(
                            {"match": {"units.attributes.attrlist.attrname.en": facet_key}}
                        )
                        should_query.append(
                            {
                                "match": {
                                    "units.attributes.attrlist.attrname.value.en": facet_value.replace(
                                        "%2C", ","
                                    )
                                }
                            }
                        )
                        should_query.append(
                            {"match": {"units.attributes.attrlist.attrname.en": facet_key}}
                        )
                        should_query.append(
                            {"terms": {"units.attributes.attrlist.attrname.value.en": number_facet}}
                        )
                    else:
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.en": facet_key
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.value.en": facet_value.replace(
                                        "%2C", ","
                                    )
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.en": facet_key
                                }
                            }
                        )
                        should_query.append(
                            {"terms": {"units.attributes.attrlist.attrname.value.en": number_facet}}
                        )

                else:
                    if "," in facet_value or "%2C" in facet_value:
                        should_query.append(
                            {"match": {"units.attributes.attrlist.attrname.en": facet_key}}
                        )
                        should_query.append(
                            {
                                "match": {
                                    "units.attributes.attrlist.attrname.value.en": facet_value.replace(
                                        "%2C", ","
                                    )
                                }
                            }
                        )

                    else:
                        query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.en": facet_key
                                }
                            }
                        )
                        query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.attrname.value.en": facet_value.replace(
                                        "%2C", ","
                                    )
                                }
                            }
                        )

            if max_price != "" and min_price != "":
                query.append(
                    {
                        "range": {
                            "units.floatValue": {"gte": float(min_price), "lte": float(max_price)}
                        }
                    }
                )

            if size != "":
                size = size.replace("%2C", ",")
                size = size.replace(",", ", ")
                query.append({"match": {"units.unitSizeGroupValue.en": size.replace("%20", " ")}})
            else:
                pass

            if colour != "":
                if "," in colour or "%2C" in colour:
                    query.append({"match": {"units.colorName": colour.replace("%20", " ")}})
                else:
                    query.append({"term": {"units.colorName.keyword": colour.replace("%20", " ")}})

            ### need to search base on category id
            if f_id != "":
                f_id = f_id.replace("%2C", ",")
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "categoryList.parentCategory.categoryId": f_id
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "categoryList.parentCategory.childCategory.categoryId": f_id
                        }
                    }
                )
            else:
                pass

            if b_id != "":
                query.append(
                    {
                        "match": {
                            "brand": b_id.replace(",", ", ")
                        }
                    }
                )

            if fname != "":
                query.append(
                    {
                        "match": {
                            "categoryList.parentCategory.categoryId": fname
                        }
                    }
                )

            if sname != "":
                query.append(
                    {
                        "match": {
                            "categoryList.parentCategory.childCategory.categoryId": sname
                        }
                    }
                )
            if tname != "":
                query.append(
                    {
                        "match": {
                            "categoryList.parentCategory.childCategory.categoryId": tname
                        }
                    }
                )

            query.append({"match": {"status": 1}})
            number_for = [0]
            number_for.append(int(login_type))
            query.append({"terms": {"productFor": number_for}})

            if search_query != "":
                if len(should_query) > 0:
                    filter_parameters_query = {
                        "query": {
                            "bool": {
                                "must": query,
                                "should": should_query,
                                "minimum_should_match": 1,
                                "must_not": [{"match": {"storeId": "0"}}],
                                "boost": 1.0,
                            }
                        },
                        "size": to_data,
                        "from": from_data,
                    }
                else:
                    filter_parameters_query = {
                        "query": {
                            "bool": {"must": query, "must_not": [{"match": {"storeId": "0"}}]}
                        },
                        "size": to_data,
                        "from": from_data,
                    }
            else:
                if len(should_query) > 0:
                    filter_parameters_query = {
                        "query": {
                            "bool": {
                                "must": query,
                                "should": should_query,
                                "minimum_should_match": 1,
                                "must_not": [{"match": {"storeId": "0"}}],
                                "boost": 1.0,
                            }
                        },
                        "size": to_data,
                        "from": from_data,
                    }
                else:
                    filter_parameters_query = {
                        "query": {
                            "bool": {"must": query, "must_not": [{"match": {"storeId": "0"}}]}
                        },
                        "size": to_data,
                        "from": from_data,
                    }

            units_data = []
            size_data = []
            res = {}
            # Es Query to get all the filter parameters
            res_filter_parameters = es.search(
                index=index_products,
                body=filter_parameters_query,
                filter_path=[
                    "hits.hits._id",
                    "hits.hits._source.units",
                    "hits.hits._source.currencySymbol",
                    "hits.hits._source.brandTitle",
                    "hits.hits._source.catName",
                    "hits.hits._source.subCatName",
                    "hits.hits._source.subSubCatName",
                    "hits.hits._source.currency",
                    "hits.hits._source.units",
                    "hits.hits._source.colour",
                    "hits.hits._source.symptoms",
                    "hits.hits._source.sizes",
                    "hits.hits._source.categoryList",
                ],
            )
            if int(filter_level) == 1:
                if len(res_filter_parameters) == 0:
                    response = {"data": [], "message": "No Data Found"}
                    return JsonResponse(response, safe=False, status=404)

            # gets all the required data for a particular filter type
            if int(filter_level) == 1:
                currencySymbol = "€"
                currency = "EUR"
                attr_data = []
                attr_name = []
                number_attr_name = []
                for i in res_filter_parameters["hits"]["hits"]:
                    facet_data = db.childProducts.find_one({"_id": ObjectId(i["_id"])})
                    tax_value = []
                    if facet_data != None:
                        if type(facet_data["tax"]) == list:
                            for tax in facet_data["tax"]:
                                tax_value.append({"value": tax["taxValue"]})
                        else:
                            if facet_data["tax"] != None:
                                if "taxValue" in facet_data["tax"]:
                                    tax_value.append({"value": facet_data["tax"]["taxValue"]})
                                else:
                                    tax_value.append({"value": facet_data["tax"]})
                            else:
                                pass
                    else:
                        tax_value = []

                    if facet_data is not None:
                        for facet in facet_data["units"]:
                            try:
                                # ==============================price ========================================
                                try:
                                    main_price = facet["b2cPricing"][0]["b2cproductSellingPrice"]
                                except:
                                    main_price = facet["b2cPricing"][0]["floatValue"]
                                # ==================================get currecny rate============================
                                try:
                                    currency_rate = float(
                                        currency_exchange_rate[
                                            str(facet_data["currency"]) + "_" + str(currency_code)
                                        ]
                                    )
                                except:
                                    currency_rate = 0
                                currency_details = db.currencies.find_one(
                                    {"currencyCode": currency_code}
                                )
                                if currency_details is not None:
                                    currencySymbol = currency_details["currencySymbol"]
                                    currency = currency_details["currencyCode"]

                                if currency_rate > 0:
                                    main_price = main_price * currency_rate
                                else:
                                    pass

                                try:
                                    tax_price = 0
                                    if len(tax_value) == 0:
                                        tax_price = 0
                                    else:
                                        for amount in tax_value:
                                            tax_price = tax_price + (int(amount["value"]))
                                    base_price = main_price + ((main_price * tax_price) / 100)
                                    units_data.append(base_price)
                                except:
                                    pass

                                # ======================= colour and size=====================================
                                if "unitSizeGroupValue" in facet:
                                    if facet["unitSizeGroupValue"] != "":
                                        try:
                                            if facet["unitSizeGroupValue"]["en"] != "":
                                                size_data.append(
                                                    {"name": facet["unitSizeGroupValue"]["en"]}
                                                )
                                            else:
                                                pass
                                        except:
                                            pass
                                else:
                                    pass

                                try:
                                    if facet["colorName"] != "":
                                        color_list.append({"name": facet["colorName"]})
                                except:
                                    pass

                                for facet_attr in facet["attributes"]:
                                    for attr in facet_attr["attrlist"]:
                                        try:
                                            if attr["searchable"] == 1:
                                                if attr["attrname"]["en"].upper() != "PRICE":
                                                    attr_name.append(attr["attrname"]["en"].upper())
                                                    value = []
                                                    try:
                                                        if type(attr["value"]["en"]) == str:
                                                            attr_data.append(
                                                                {
                                                                    "name": attr["attrname"][
                                                                        "en"
                                                                    ].upper(),
                                                                    "data": [attr["value"]["en"]],
                                                                }
                                                            )
                                                        else:
                                                            try:
                                                                for x in facet["value"]:
                                                                    if type(x["en"]) == int:
                                                                        value.append(x)
                                                                    else:
                                                                        try:
                                                                            value.append(
                                                                                x["en"].strip(" ")
                                                                            )
                                                                        except:
                                                                            value.append(x["en"])
                                                                attr_data.append(
                                                                    {
                                                                        "name": attr["attrname"][
                                                                            "en"
                                                                        ].upper(),
                                                                        "data": value,
                                                                    }
                                                                )
                                                            except:
                                                                for x in facet["value"]:
                                                                    if type(x["name"]["en"]) == int:
                                                                        value.append(x)
                                                                    else:
                                                                        try:
                                                                            value.append(
                                                                                x["name"]["en"].strip(
                                                                                    " "
                                                                                )
                                                                            )
                                                                        except:
                                                                            value.append(
                                                                                x["name"]["en"]
                                                                            )
                                                                attr_data.append(
                                                                    {
                                                                        "name": attr["attrname"][
                                                                            "en"
                                                                        ].upper(),
                                                                        "data": value,
                                                                    }
                                                                )
                                                    except:
                                                        if type(attr["value"]) == dict:
                                                            for x in attr["value"]["en"]:
                                                                if type(x) == int:
                                                                    value.append(x)
                                                                else:
                                                                    try:
                                                                        value.append(x.strip(" "))
                                                                    except:
                                                                        value.append(x)
                                                        else:
                                                            for x in attr["value"]:
                                                                if type(x["name"]["en"]) == int:
                                                                    value.append(x)
                                                                else:
                                                                    try:
                                                                        value.append(
                                                                            x["name"]["en"].strip(" ")
                                                                        )
                                                                    except:
                                                                        value.append(x["name"]["en"])
                                                        attr_data.append(
                                                            {
                                                                "name": attr["attrname"]["en"].upper(),
                                                                "data": value,
                                                            }
                                                        )
                                                else:
                                                    pass
                                        except Exception as ex:
                                            template = (
                                                "An exception of type {0} occurred. Arguments:\n{1!r}"
                                            )
                                            message = template.format(type(ex).__name__, ex.args)
                                            (
                                                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                                                type(ex).__name__,
                                                ex,
                                            )
                                            pass
                            except:
                                pass

                    try:
                        brand_name = i["_source"]["brandTitle"][language]
                    except:
                        try:
                            brand_name = i["_source"]["brandTitle"]["en"]
                        except:
                            brand_name = ""

                    brand_list = []

                    if "categoryList" in i["_source"]:
                        for cat in i["_source"]["categoryList"]:
                            category_list.append(
                                {"name": cat["parentCategory"]["categoryName"]["en"]}
                            )
                # ================================colour================================================================
                if len(color_list) > 0 and search_query != "":
                    dataframe = pd.DataFrame(color_list)
                    dataframe["penCount"] = dataframe.groupby("name")["name"].transform("count")
                    dataframe = dataframe.drop_duplicates("name", keep="last")
                    colors_json = dataframe.to_json(orient="records")
                    colors_json = json.loads(colors_json)
                    new_colour_list = sorted(colors_json, key=lambda k: k["name"])
                else:
                    new_colour_list = []
                # ======================================size============================================================
                if len(size_data) > 0 and search_query != "":
                    size_dataframe = pd.DataFrame(size_data)
                    # size_dataframe = size_dataframe[size_dataframe.name != '']
                    size_dataframe["penCount"] = size_dataframe.groupby("name")["name"].transform(
                        "count"
                    )
                    size_dataframe = size_dataframe.drop_duplicates("name", keep="last")
                    sizes = size_dataframe.to_json(orient="records")
                    sizes = json.loads(sizes)
                    sizes = sorted(sizes, key=lambda k: k["name"])
                else:
                    sizes = []
                # =====================================brand============================================================
                if len(brand_list) > 0:
                    brand_dataframe = pd.DataFrame(brand_list)
                    brand_dataframe["penCount"] = brand_dataframe.groupby("name")["name"].transform(
                        "count"
                    )
                    brand_dataframe = brand_dataframe.drop_duplicates("name", keep="last")
                    brand_list = brand_dataframe.to_json(orient="records")
                    brand_list = json.loads(brand_list)
                    brand_list = sorted(brand_list, key=lambda k: k["name"])
                else:
                    brand_list = []

                # =======================================category=======================================================
                if len(category_list) > 0 and search_query != "":
                    cat_dataframe = pd.DataFrame(category_list)
                    cat_dataframe["penCount"] = cat_dataframe.groupby("name")["name"].transform(
                        "count"
                    )
                    cat_dataframe = cat_dataframe.drop_duplicates("name", keep="last")
                    cat_list = cat_dataframe.to_json(orient="records")
                    cat_list = json.loads(cat_list)
                    cat_list = sorted(cat_list, key=lambda k: k["name"])
                else:
                    cat_list = []

                res_color = {}
                for d in attr_data:
                    res_color.setdefault(d["name"], []).append(
                        {"name": d["name"], "data": d["data"]}
                    )
                attribute_data = []
                for attr in list(set(attr_name)):
                    try:
                        new_data = []
                        for i in res_color[attr]:
                            new_data = i["data"] + new_data
                        attr_dataframe = pd.DataFrame(new_data)
                        attr_dataframe.columns = ["name"]
                        attr_dataframe["penCount"] = attr_dataframe.groupby("name")[
                            "name"
                        ].transform("count")
                        attr_dataframe = attr_dataframe.drop_duplicates("name", keep="last")
                        attr_list = attr_dataframe.to_json(orient="records")
                        attr_list = json.loads(attr_list)

                        attribute_data.append(
                            {"name": attr, "data": attr_list, "selType": 1, "filterType": 8}
                        )
                    except:
                        pass

                brands = {"name": "BRAND", "data": brand_list, "selType": 1, "filterType": 4}

                category = {
                    # "name": "Categories",
                    "name": "CATEGORIES",
                    "data": cat_list,
                    "selType": 2,
                    "level": 1,
                    "filterType": 1,
                }

                colors_data = {
                    "name": "Colour".upper(),
                    "data": new_colour_list,
                    "selType": 4,
                    "filterType": 5,
                }

                rating_value = [
                    {"name": "4★ & above", "value": 4},
                    {"name": "3★ & above", "value": 3},
                    {"name": "2★ & above", "value": 2},
                    {"name": "1★ & above", "value": 1},
                ]
                rating_data = {
                    "name": "CUSTOMER RATINGS",
                    "data": rating_value,
                    "selType": 1,
                }
                try:
                    max_price = max(list(set(units_data)))
                except:
                    max_price = 0
                try:
                    min_price = min(list(set(units_data)))
                except:
                    min_price = 0

                if min_price == max_price:
                    min_price = 0
                p_data = {
                    "name": "PRICE",
                    "data": [{"maxPrice": max_price, "minPrice": min_price}],
                    "selType": 3,
                    "filterType": 7,
                    "currencySymbol": currencySymbol,
                    "currency": currency,
                }
                new_size = []
                sizes_data = {"name": "SIZE", "data": sizes, "selType": 1, "filterType": 6}
                discount_data = db.discountType.find({"status": 1})
                disc_data = []
                for disc in discount_data:
                    disc_data.append(
                        {
                            "value": disc["discountValue"],
                            "name": disc["name"][language]
                            if language in disc["name"]
                            else disc["name"]["en"],
                        }
                    )
                discount_json = {"name": "DISCOUNT", "data": disc_data, "selType": 1}
                filters_json = []
                filters_json.append(colors_data)
                filters_json.append(sizes_data)
                filters_json.append(p_data)
                filters_json.append(brands)
                filters_json.append(category)
                for atb in attribute_data:
                    filters_json.append(atb)
                Final_output = {
                    "data": {
                        "filters": filters_json,
                        "currencySymbol": currency,
                        "currency": currencySymbol,
                        "unitId": str(ObjectId()),
                        "message": "Filters Found",
                    }
                }

                return JsonResponse(Final_output, safe=False, status=200)

            elif int(filter_level) == 2:
                currencySymbol = "$"
                category_details = db.category.find_one({"categoryName.en": fname, "storeId": "0"})
                if category_details != None:
                    child_category_details = db.category.find(
                        {"parentId": ObjectId(category_details["_id"]), "storeId": "0", "status": 1}
                    )
                    for child_cat in child_category_details:
                        child_data_count = db.category.find(
                            {"parentId": ObjectId(child_cat["_id"]), "status": 1}
                        ).count()
                        if sname != "":
                            if sname == child_cat["categoryName"]["en"]:
                                sub_categpry_list.append(child_cat["categoryName"]["en"])
                            else:
                                pass
                        else:
                            sub_categpry_list.append(child_cat["categoryName"]["en"])

                subcategory = {
                    "name": "subCategories",
                    # "data": sub_cat_list,
                    "data": list(set(sub_categpry_list)),
                    "selType": 2,
                    "level": 2,
                    "filterType": 2,
                }
                filters_json = [subcategory]
                Final_output = {
                    "data": {
                        "filters": filters_json,
                        # "currency": currencySymbol,
                        "currencySymbol": "₹",
                        "currency": "INR",
                        "unitId": str(ObjectId()),
                        "message": "Filters Found",
                    }
                }
                return JsonResponse(Final_output, safe=False, status=200)

            elif int(filter_level) == 3:
                for i in res_filter_parameters["hits"]["hits"]:
                    currencySymbol = "$"
                    if len(i["_source"]["subSubCatName"]) > 0:
                        sub_sub_category_list.append(i["_source"]["subSubCatName"][language])
                    else:
                        pass
                subsubcategory = {
                    "name": "subsubCategories",
                    "data": list(set(sub_sub_category_list)),
                    "selType": 2,
                    "level": 3,
                    "filterType": 3,
                }
                filters_json = [subsubcategory]
                Final_output = {
                    "data": {
                        "filters": filters_json,
                        # "currency": currencySymbol,
                        "currencySymbol": "₹",
                        "currency": "INR",
                        "unitId": str(ObjectId()),
                        "message": "Filters Found",
                    }
                }
                return JsonResponse(Final_output, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class ProductSuggestionsMettamuse(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Product Sugestion"],
        operation_description="API for suggestion the product name based on user search for the catgeory, brand and products wise",
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
                name="currencycode",
                default="INR",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="currencySymbol for the currency..INR, INR, USD",
            ),
            openapi.Parameter(
                name="searchItem",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="searching the products (user search the item. example ni, nik, nike..)",
                default="printed",
            ),
            openapi.Parameter(
                name="page",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                default="1",
                description="page for the pagination",
            ),
            openapi.Parameter(
                name="s_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="store id while need to suggestion from store",
            ),
            openapi.Parameter(
                name="storeCategoryId",
                default="5cc0846e087d924456427975",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
        ],
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            store_details_list = []
            should_query = []
            category_list = []
            store_count_data = []
            token = request.META["HTTP_AUTHORIZATION"]
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
            try:
                print("token", token)
                login_type = json.loads(token)["metaData"]["institutionType"]
            except:
                login_type = 1

            if login_type == 0:
                login_type = 1
            search_item = request.GET["searchItem"] if "searchItem" in request.GET else ""
            search_item = search_item.replace("%20", " ")
            s_id = request.GET.get("s_id", "")
            if search_item == "":
                response_data = {
                    "message": "Invalid Request",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=400)
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"

            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            filter_query = []
            store_json = ["0"]
            store_details = db.stores.find({"storeFrontTypeId": 5})
            for s in store_details:
                store_json.append(str(s["_id"]))
            filter_query.append({"match": {"status": 1}})
            filter_query.append({"match": {"units.isPrimary": True}})
            filter_query.append({"match": {"storeCategoryId": str(store_category_id)}})
            product_for = [0]
            print("login_type", login_type)
            if login_type == 1:
                product_for.append("1")
            else:
                product_for.append("2")
            filter_query.append({"terms": {"productFor": product_for}})
            if s_id != "":
                filter_query.append({"match": {"units.suppliers.id": str(s_id)}})
            else:
                pass

            # ===========================product name========================================
            should_query.append(
                {
                    "match_phrase_prefix": {
                        "pName.en": {
                            "analyzer": "standard",
                            "query": search_item.replace("%20", " "),
                            "boost": 6,
                        }
                    }
                }
            )
            should_query.append(
                {"match": {"pName.en": {"query": search_item.replace("%20", " "), "boost": 6}}}
            )
            # ===========================unit name========================================
            should_query.append(
                {
                    "match_phrase_prefix": {
                        "units.unitName.en": {
                            "analyzer": "standard",
                            "query": search_item.replace("%20", " "),
                            "boost": 5,
                        }
                    }
                }
            )
            should_query.append(
                {
                    "match": {
                        "units.unitName.en": {
                            "query": search_item.replace("%20", " "),
                            "boost": 5,
                        }
                    }
                }
            )
            # ===========================================detail description============================
            should_query.append(
                {
                    "match": {
                        "detailDescription."
                        + language: {"query": search_item.replace("%20", " "), "boost": 0}
                    }
                }
            )
            should_query.append(
                {
                    "match_phrase_prefix": {
                        "detailDescription."
                        + language: {"query": search_item.replace("%20", " "), "boost": 0}
                    }
                }
            )
            # ====================================child category=======================================
            should_query.append(
                {
                    "match": {
                        "categoryList.parentCategory.childCatgory.categoryName."
                        + language: {"query": search_item.replace("%20", " "), "boost": 3}
                    }
                }
            )
            should_query.append(
                {
                    "match_phrase_prefix": {
                        "categoryList.parentCategory.childCategory.categoryName."
                        + language: {"query": search_item.replace("%20", " "), "boost": 3}
                    }
                }
            )
            # ===============================parent category name======================================
            should_query.append(
                {
                    "match": {
                        "categoryList.parentCategory.categoryName."
                        + language: {"query": search_item.replace("%20", " "), "boost": 2}
                    }
                }
            )
            should_query.append(
                {
                    "match_phrase_prefix": {
                        "categoryList.parentCategory.categoryName."
                        + language: {"query": search_item.replace("%20", " "), "boost": 2}
                    }
                }
            )
            # ======================================brand name=======================================
            should_query.append(
                {
                    "match": {
                        "brandTitle."
                        + language: {"query": search_item.replace("%20", " "), "boost": 1}
                    }
                }
            )
            should_query.append(
                {
                    "match_phrase_prefix": {
                        "brandTitle."
                        + language: {"query": search_item.replace("%20", " "), "boost": 1}
                    }
                }
            )

            # =========================================================query for the get the products=======================================
            query = {
                "query": {
                    "bool": {
                        "must": filter_query,
                        "should": should_query,
                        "must_not": [{"terms": {"storeId": store_json}}],
                        "minimum_should_match": 1,
                        "boost": 1.0,
                    }
                },
                "aggs": {
                    "group_by_stores": {
                        "terms": {
                            "field": "storeId.keyword",
                            "size": 20,
                            "order": {"avg_score": "desc"},
                        },
                        "aggs": {
                            "avg_score": {"max": {"script": "_score"}},
                            "group_by_category": {
                                "terms": {"field": "catName.en.keyword", "size": 10}
                            },
                            "top_sales_hits": {
                                "top_hits": {
                                    "_source": {
                                        "includes": [
                                            "_score",
                                            "_id",
                                            "firstCategoryId",
                                            "catName",
                                            "parentProductId",
                                            "secondCategoryId",
                                            "subCatName",
                                            "thirdCategoryId",
                                            "subSubCatName",
                                            "offer",
                                            "images",
                                            "storeId",
                                            "suppliers",
                                            "currencySymbol",
                                            "currency",
                                            "tax",
                                            "units",
                                            "productType",
                                            "storeCategoryId",
                                        ]
                                    },
                                    "size": 20,
                                }
                            },
                        },
                    }
                },
            }
            res = es.search(index=index_products, body=query)
            if len(res["hits"]["hits"]) > 0:
                product_data = []
                for bucket in res["aggregations"]["group_by_stores"]["buckets"]:
                    store_id = bucket["key"]
                    if store_id == "0":
                        pass
                    else:
                        store_score = 0
                        store_details = db.stores.find_one(
                            {"_id": ObjectId(store_id), "storeFrontTypeId": {"$ne": 5}},
                            {
                                "logoImages": 1,
                                "bannerImages": 1,
                                "storeName": 1,
                                "avgRating": 1,
                                "businessLocationAddress": 1,
                                "storeIsOpen": 1,
                                "storeType": 1,
                                "storeTypeId": 1,
                                "storeFrontTypeId": 1,
                                "storeFrontType": 1,
                                "cityName": 1,
                                "sellerType": 1,
                                "sellerTypeId": 1,
                                "billingAddress": 1,
                            },
                        )
                        if store_details is not None:
                            # ====================================================category count========================
                            cat_list = bucket["group_by_category"]["buckets"]
                            for cat in cat_list:
                                category_list.append(
                                    {
                                        "catName": cat["key"] + " " + "/" + " " + search_item,
                                        "totalCount": cat["doc_count"],
                                    }
                                )
                            # ====================================================store details=========================
                            store_lat = float(store_details["businessLocationAddress"]["lat"])
                            store_long = float(store_details["businessLocationAddress"]["long"])
                            address = store_details["businessLocationAddress"]["address"]
                            locality = (
                                store_details["businessLocationAddress"]["locality"]
                                if "locality" in store_details["businessLocationAddress"]
                                else ""
                            )
                            post_code = (
                                store_details["businessLocationAddress"]["postCode"]
                                if "postCode" in store_details["businessLocationAddress"]
                                else ""
                            )
                            state = (
                                store_details["businessLocationAddress"]["state"]
                                if "state" in store_details["businessLocationAddress"]
                                else ""
                            )
                            country = (
                                store_details["businessLocationAddress"]["country"]
                                if "country" in store_details["businessLocationAddress"]
                                else ""
                            )
                            store_name = store_details["storeName"][language]
                            store_type_name = (
                                store_details["sellerType"]
                                if store_details["sellerType"] != ""
                                else "Retailer"
                            )
                            store_id = str(store_details["_id"])
                            area_name = (
                                store_details["cityName"]
                                if "cityName" in store_details
                                else "Banglore"
                            )
                            store_count_data.append(
                                {
                                    "storeName": (
                                        store_type_name.strip()
                                        + " "
                                        + "in"
                                        + " "
                                        + area_name.strip()
                                    ).strip(),
                                    "storeId": store_id,
                                    "avgRating": store_details["avgRating"]
                                    if "avgRating" in store_details
                                    else 0,
                                    "cityName": store_details["cityName"]
                                    if "cityName" in store_details
                                    else "Banglore",
                                    "areaName": store_details["billingAddress"]["areaOrDistrict"]
                                    if "areaOrDistrict" in store_details["billingAddress"]
                                    else "Hebbal",
                                }
                            )
                            seller_rating_count = db.sellerReviewRatings.find(
                                {"sellerId": str(store_id), "rating": {"$ne": 0}}
                            ).count()
                            store_details_json = {
                                "lat": store_lat,
                                "long": store_long,
                                "address": address,
                                "locality": locality,
                                "storeFrontTypeId": store_details["storeFrontTypeId"]
                                if "storeFrontTypeId" in store_details
                                else 0,
                                "storeFrontType": store_details["storeFrontType"]
                                if "storeFrontType" in store_details
                                else "Central",
                                "storeTypeId": store_details["storeTypeId"]
                                if "storeTypeId" in store_details
                                else 0,
                                "storeType": store_details["storeType"]
                                if "storeType" in store_details
                                else "Central",
                                "sellerTypeId": store_details["sellerTypeId"]
                                if "sellerTypeId" in store_details
                                else 0,
                                "sellerType": store_details["sellerType"]
                                if "sellerType" in store_details
                                else "Central",
                                "cityName": store_details["cityName"]
                                if "cityName" in store_details
                                else "Banglore",
                                "areaName": store_details["billingAddress"]["areaOrDistrict"]
                                if "areaOrDistrict" in store_details["billingAddress"]
                                else "Hebbal",
                                "logoImages": store_details["logoImages"],
                                "bannerImages": store_details["bannerImages"],
                                "avgRating": store_details["avgRating"]
                                if "avgRating" in store_details
                                else 0,
                                "storeIsOpen": store_details["storeIsOpen"]
                                if "storeIsOpen" in store_details
                                else False,
                                "postCode": post_code,
                                "ratingCount": seller_rating_count,
                                "state": state,
                                "country": country,
                                "storeName": store_name,
                                "storeId": store_id,
                            }

                            for hits in bucket["top_sales_hits"]["hits"]["hits"]:
                                try:
                                    if hits['_score'] is not None:
                                        store_score = store_score + hits['_score']
                                    else:
                                        pass
                                    tax_value = []
                                    tax_price = 0
                                    offers_details = []
                                    best_supplier = {}
                                    best_supplier["id"] = str(hits["_source"]["storeId"])
                                    best_supplier["productId"] = str(hits["_id"])
                                    # ===========================tax for the product=========================================================

                                    child_product_details = db.childProducts.find_one(
                                        {"_id": ObjectId(best_supplier["productId"])}
                                    )
                                    if child_product_details is not None:
                                        # ============================funcdtion for get currency rate===================
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

                                        try:
                                            best_supplier["retailerPrice"] = child_product_details[
                                                "units"
                                            ][0]["b2cPricing"][0]["b2cproductSellingPrice"]
                                        except:
                                            best_supplier["retailerPrice"] = child_product_details[
                                                "units"
                                            ][0]["floatValue"]

                                        try:
                                            best_supplier[
                                                "distributorPrice"
                                            ] = child_product_details["units"][0]["b2bPricing"][0][
                                                "b2bproductSellingPrice"
                                            ]
                                        except:
                                            best_supplier[
                                                "distributorPrice"
                                            ] = child_product_details["units"][0]["floatValue"]

                                        best_supplier["retailerQty"] = child_product_details[
                                            "units"
                                        ][0]["availableQuantity"]
                                        best_supplier["distributorQty"] = child_product_details[
                                            "units"
                                        ][0]["availableQuantity"]

                                        if "offer" in child_product_details:
                                            for offer in child_product_details["offer"]:
                                                if offer["status"] == 1:
                                                    offers_details.append(offer)
                                        else:
                                            pass

                                        if len(offers_details) > 0:
                                            best_offer = max(
                                                offers_details, key=lambda x: x["discountValue"]
                                            )
                                            discount_type = (
                                                int(best_offer["discountType"])
                                                if "discountType" in best_offer
                                                else 1
                                            )
                                            discount_value = best_offer["discountValue"]
                                        else:
                                            best_offer = {}
                                            discount_type = 2
                                            discount_value = 0

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
                                                pass
                                        # =================================price calculation===================================================================
                                        price = best_supplier["retailerPrice"]
                                        if float(currency_rate) > 0:
                                            price = price * float(currency_rate)
                                        else:
                                            pass
                                        tax_price = 0
                                        if len(tax_value) == 0:
                                            tax_price = 0
                                        else:
                                            for amount in tax_value:
                                                tax_price = tax_price + (int(amount["value"]))
                                        base_price = price + ((price * tax_price) / 100)

                                        if discount_type == 0:
                                            discount_price = float(discount_value)
                                        elif discount_type == 1:
                                            try:
                                                discount_price = (
                                                    float(base_price) * float(discount_value)
                                                ) / 100
                                            except:
                                                discount_price = (
                                                    float(k["price"]["en"]) * float(discount_value)
                                                ) / 100
                                        else:
                                            discount_price = 0

                                        if float(currency_rate) > 0 and discount_type == 0:
                                            discount_price = discount_price * float(currency_rate)
                                        else:
                                            pass

                                        final_price = float(base_price) - discount_price
                                        final_price_list = {
                                            "basePrice": base_price,
                                            "finalPrice": final_price,
                                            "discountPrice": discount_price,
                                            "discountType": discount_type,
                                        }
                                        store_details = db.stores.find_one(
                                            {"_id": ObjectId(best_supplier["id"])}
                                        )
                                        store_count = 0
                                        if store_details is None:
                                            best_supplier["storeName"] = central_store
                                            best_supplier["storeAliasName"] = central_store
                                            best_supplier["storeFrontTypeId"] = 0
                                            best_supplier["storeFrontType"] = "Central"
                                            best_supplier["storeTypeId"] = 0
                                            best_supplier["storeType"] = "Central"
                                            best_supplier["cityName"] = "Banglore"
                                            best_supplier["areaName"] = "Hebbal"
                                            best_supplier["sellerType"] = "Central"
                                            best_supplier["sellerTypeId"] = 1
                                            best_supplier["rating"] = 0
                                        else:
                                            if "businessLocationAddress" in store_details:
                                                if (
                                                    "addressArea"
                                                    in store_details["businessLocationAddress"]
                                                ):
                                                    area_name = store_details[
                                                        "businessLocationAddress"
                                                    ]["addressArea"]
                                                else:
                                                    area_name = "Hebbal"
                                            else:
                                                area_name = "Hebbal"

                                            best_supplier["storeName"] = (
                                                store_details["storeName"][language]
                                                if language in store_details["storeName"]
                                                else store_details["storeName"]["en"]
                                            )
                                            best_supplier["storeAliasName"] = (
                                                store_details["storeAliasName"]
                                                if "storeAliasName" in store_details
                                                else store_details["storeName"]["en"]
                                            )
                                            best_supplier["storeFrontTypeId"] = store_details[
                                                "storeFrontTypeId"
                                            ]
                                            best_supplier["storeFrontType"] = store_details[
                                                "storeFrontType"
                                            ]
                                            best_supplier["storeTypeId"] = store_details[
                                                "storeTypeId"
                                            ]
                                            best_supplier["storeType"] = store_details["storeType"]
                                            best_supplier["cityName"] = (
                                                store_details["cityName"]
                                                if "cityName" in store_details
                                                else "Banglore"
                                            )
                                            best_supplier["areaName"] = area_name
                                            best_supplier["sellerType"] = store_details[
                                                "sellerType"
                                            ]
                                            best_supplier["sellerTypeId"] = store_details[
                                                "sellerTypeId"
                                            ]
                                            best_supplier["rating"] = (
                                                store_details["avgRating"]
                                                if "avgRating" in store_details
                                                else 0
                                            )
                                        if int(best_supplier["retailerQty"]) > 0:
                                            out_of_stock = False
                                        else:
                                            out_of_stock = True

                                        try:
                                            reseller_commission = child_product_details['units'][0]['b2cPricing'][0][
                                                'b2cresellerCommission']
                                        except:
                                            reseller_commission = 0

                                        try:
                                            reseller_commission_type = child_product_details['units'][0]['b2cPricing']['b2cpercentageCommission']
                                        except:
                                            reseller_commission_type = 0
                                        product_data.append(
                                            {
                                                "score": hits['_score'],
                                                "parentProductId": str(hits["_source"]["parentProductId"]),
                                                "resellerCommission": reseller_commission,
                                                "resellerCommissionType": reseller_commission_type,
                                                "childProductId": best_supplier["productId"],
                                                "finalPriceList": final_price_list,
                                                "productName": child_product_details["units"][0][
                                                    "unitName"
                                                ][language],
                                                "productType": child_product_details["productType"] if "productType" in child_product_details else 1,
                                                "images": hits["_source"]["images"],
                                                "currencySymbol": currency_symbol,
                                                "currency": currency,
                                                "availableQuantity": best_supplier["retailerQty"],
                                                "outOfStock": out_of_stock,
                                                "supplier": best_supplier,
                                            }
                                        )
                                except:
                                    pass
                                if len(product_data) > 0:
                                    store_details_json["storeScore"] = store_score
                                    store_details_list.append(store_details_json)

                if len(category_list) > 0:
                    dataframe = pd.DataFrame(category_list)
                    dataframe["penCount"] = dataframe.groupby("catName")["totalCount"].transform(
                        "sum"
                    )
                    dataframe = dataframe.drop_duplicates("catName", keep="last")
                    dataframe = dataframe.drop(["totalCount"], axis=1)
                    category_json = dataframe.to_json(orient="records")
                    category_json = json.loads(category_json)
                else:
                    category_json = []

                # =============================remove duplicate stores========================
                if len(store_details_list) > 0:
                    store_json_new = []
                    new_store_list = sorted(store_details_list, key=lambda k: k["storeScore"], reverse=False)
                    dataframe = pd.DataFrame(new_store_list)
                    dataframe = dataframe.drop_duplicates("storeId", keep="first")
                    store_json_data = dataframe.to_json(orient="records")
                    store_json_data = json.loads(store_json_data)
                    for s_d in store_json_data:
                        store_json_new.append({"storeData": s_d})
                else:
                    store_json_new = []

                # =============================remove duplicate stores from count=============
                if len(store_count_data) > 0:
                    dataframe = pd.DataFrame(store_count_data)
                    dataframe["penCount"] = dataframe.groupby("storeName")["storeName"].transform(
                        "count"
                    )
                    dataframe = dataframe.drop_duplicates("storeName", keep="last")
                    store_count_data_new = dataframe.to_json(orient="records")
                    store_count_data_new = json.loads(store_count_data_new)
                else:
                    store_count_data_new = []

                last_response = {
                    "data": {
                        "storeData": store_json_new,
                        "productsData": product_data,
                        "categoryData": category_json,
                        "sellerList": store_count_data_new,
                    }
                }
                return JsonResponse(last_response, safe=False, status=200)
            else:
                response = {"data": {}, "message": "data not found", "penCount": 0}
                return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class CurrencyList(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Currency List"],
        operation_description="API for get the all currency list",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="Authorization", in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True
            ),
            openapi.Parameter(
                name="search",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="search base on country name, currency symbol",
            ),
        ],
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            store_details_list = []
            should_query = []
            category_list = []
            store_count_data = []
            token = request.META["HTTP_AUTHORIZATION"]
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            else:
                search = request.GET.get("search", "")
                query = {}
                if search != "":
                    query["$or"] = [
                        {"countryCode": {"$regex": search, "$options": "i"}},
                        {"name": {"$regex": search, "$options": "i"}},
                        {"currencyCode": {"$regex": search, "$options": "i"}},
                        {"currencyName": {"$regex": search, "$options": "i"}},
                    ]
                else:
                    pass
                currency_data = db.currencies.find(query).sort([("name", -1)])
                currency_data_count = db.currencies.find(query).count()
                currency_list = []
                if currency_data.count() > 0:
                    for currency in currency_data:
                        currency_list.append(
                            {
                                "name": currency["name"],
                                "countryCode": currency["countryCode"],
                                "currencyCode": currency["currencyCode"],
                                "currencyName": currency["currencyName"],
                                "currencySymbol": currency["currencySymbol"],
                            }
                        )
                    last_response = {"data": currency_list, "penCount": currency_data_count}
                    return JsonResponse(last_response, safe=False, status=200)
                else:
                    response = {"data": [], "message": "data not found", "penCount": 0}
                    return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class MettamuseHomePage(APIView):
    """
    API for Mettamuse homepage.
    """

    # @swagger_auto_schema(
    #     method='get', tags=["Mettamuse-Homepage"],
    #     operation_description="API to get the Mettamuse homepage with currency conversion products.",
    #     required=['AUTHORIZATION', 'currency'],
    #     manual_parameters=APIDocObj.GET_METTAMUSE_HOMEPAGE_MANUAL_PARAMS,
    #     responses=ResponseHandlerObj.GET_METTAMUSE_HOMEPAGE_RESP_OBJ,
    #     operation_summary="Mettamuse Homepage Get")
    # @action(detail=False, methods=['get'])
    def get(self, request):
        try:
            start_time = time.time()
            # check for the authentication
            auth_er = RespObj.check_authentication(request)
            if auth_er:
                return auth_er

            homepageId = request.GET.get("homepageId", "")
            currency_code = request.META.get("HTTP_CURRENCY", "")

            require_param = ["homepageId"]
            error = RespObj.check_req_params(request.GET, require_param)
            if error:
                return error

            rjId = ProObj.generate_rj_id("homepage", homepageId)

            # get json from redis json
            rjData = rj.jsonget(rjId)

            if rjData is None:
                rjData, done = ProObj.check_rj_data_mongo(
                    rjId, {"_id": ObjectId(homepageId)}, db.homepage
                )
                if not done:
                    return JsonResponse(RespObj.JSON_RESPONSE_404, safe=False, status=404)

            currency_rate = ProObj.get_currency_rate(currency_code)

            # convert product prices
            section_names = ["NewArrivals"]
            converted_data = ProObj.convert_product_price(
                currency_rate, rjData, section_names, currency_code
            )

            msg = RespObj.MSG_200 if len(converted_data) else None
            data = RespObj.gen_response_data(converted_data, msg)
            print("response time for mettamuse home page", time.time() - start_time)
            return JsonResponse(data, safe=False, status=200)
        except Exception as ex:
            exception_resp = RespObj.generate_exception(ex, request)
            return JsonResponse(exception_resp, status=500)

    # @swagger_auto_schema(
    #     method='patch', tags=["Mettamuse-Homepage"],
    #     operation_description="API to update the Mettamuse homepage",
    #     required=['AUTHORIZATION'],
    #     request_body=APIDocObj.PATCH_METTAMUSE_HOMEPAGE_REQUEST_BODY,
    #     manual_parameters=APIDocObj.PATCH_METTAMUSE_HOMEPAGE_MANUAL_PARAMS,
    #     responses=ResponseHandlerObj.PATCH_METTAMUSE_HOMEPAGE_RESP_OBJ,
    #     operation_summary="Mettamuse Homepage Update")
    # @action(detail=False, methods=['patch'])
    def patch(self, request):
        try:
            # check for the authentication
            auth_er = RespObj.check_authentication(request)
            if auth_er:
                return auth_er

            homepageId = request.data.get("homepageId", "")
            data = request.data.get("data", "")

            require_param = ["homepageId", "data"]
            error = RespObj.check_req_params(request.data, require_param)
            if error:
                return error

            find_query = {"_id": ObjectId(homepageId)}
            update_data = {"$set": data}
            updated = db.homepage.update(find_query, update_data)

            if int(updated["n"]):
                rjId = ProObj.generate_rj_id("homepage", homepageId)
                ProObj.update_rj_data_mongo(rjId, find_query, db.homepage)

            data = {}
            msg = f"Updated {updated['n']} record(s)."
            data = RespObj.gen_response_data(data, msg)
            return JsonResponse(data, safe=False, status=200)
        except Exception as ex:
            exception_resp = RespObj.generate_exception(ex, request)
            return JsonResponse(exception_resp, status=500)
