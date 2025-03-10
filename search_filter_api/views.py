# -*- coding: utf-8 -*-
from search_api.settings import (
    db,
    es,
    PHARMACY_STORE_CATEGORY_ID,
    ECOMMERCE_STORE_CATEGORY_ID,
    MEAT_STORE_CATEGORY_ID,
    CANNABIS_STORE_CATEGORY_ID,
    LIQUOR_STORE_CATEGORY_ID,
    currency_exchange_rate,
    CHILD_PRODUCT_INDEX,
    CHILD_PRODUCT_DOC_TYPE,
    GROCERY_STORE_CATEGORY_ID,
    DINE_STORE_CATEGORY_ID,
    rj,
    rj_plp,
    CASSANDRA_KEYSPACE,
    session,
)
import base64
import json
from rest_framework.views import APIView
from rest_framework.decorators import action
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from django.http import JsonResponse
from bson.objectid import ObjectId
import time
import re
import pandas as pd
from mongo_query_module.query_module import zone_find
from validations.store_category_validation import validate_store_category
import os
import sys
from search.views import search_read_new, search_read_stores, category_search_logs
import threading
from validations.driver_roaster import next_availbale_driver_roaster
from validations.search_product_validation import search_data_validation
from bs4 import BeautifulSoup
from validations.update_plp_page_data import update_plp_data_from_plp
# from forex_python.converter import CurrencyCodes
# currency_codes = CurrencyCodes()

try:
    session.set_keyspace(CASSANDRA_KEYSPACE)
except:
    pass

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
index_products = CHILD_PRODUCT_INDEX
doc_type_products = CHILD_PRODUCT_DOC_TYPE


class ProductFilter(APIView):
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
        tags=["Search & Filter"],
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
                default=ECOMMERCE_STORE_CATEGORY_ID,
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
                name="justId",
                default="0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while clicking on view more for just in from home page",
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
            token = (
                request.META["HTTP_AUTHORIZATION"]
                if "HTTP_AUTHORIZATION" in request.META
                else "sdfsdfsf"
            )
            query = []
            should_query = []
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            currencySymbol = "₹"
            currency = "INR"
            color_list = []
            brand_list = []
            manufacture_list = []
            cannabis_product_type_list = []
            sym_list = []
            category_list = []
            sub_categpry_list = []
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            filter_level = request.META["HTTP_LEVEL"] if "HTTP_LEVEL" in request.META else 1
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            # ========================================query parameter====================================================
            # for the search the item in search bar
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            search_query = request.GET.get("q", "")
            platform = int(request.GET.get("platform", "1"))
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

            colour = request.GET.get("colour", "")  # colour name
            bname = request.GET.get("bname", "")  # brand name for the search
            symptoms_name = request.GET.get("symptoms", "")  # brand name for the search
            just_id = request.GET.get("justId", "")  # get particular offer data
            best_deals = request.GET.get("best_deals", "")  # get the best deals
            offer_id = request.GET.get("o_id", "")
            offer_id = request.GET.get("o_id", "")  # get particular offer data
            if offer_id != "":
                offer_id = offer_id.split(",")
            store_id = request.GET.get("s_id", "")
            zone_id = request.GET.get("z_id", "")
            attr = request.GET.get("attr", "")
            integration_type = int(request.GET.get("integrationType", 0))
            just_id = request.GET.get("justId", "")  # get particular offer data
            to_data = 100
            from_data = int(page * 100) - 100
            if store_category_id == CANNABIS_STORE_CATEGORY_ID:
                zone_id = ""
            sname = sname.replace("+", " ")

            category_query = {"storeCategory.storeCategoryId": store_category_id}
            if zone_id != "":
                zone_details = zone_find({"_id": ObjectId(zone_id)})
                category_query["_id"] = ObjectId(zone_details["city_ID"])
            elif store_id != "":
                store_details = db.stores.find_one({"_id": ObjectId(store_id)}, {"cityId": 1})
                category_query["_id"] = str(store_details["cityId"])
            else:
                pass

            categoty_details = db.cities.find_one(category_query, {"storeCategory": 1})

            hyperlocal = False
            storelisting = False
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
                                    storelisting = False
                                    hyperlocal = False
                            else:
                                pass
                    else:
                        hyperlocal = False
                        storelisting = False
                else:
                    hyperlocal = False
                    storelisting = False
            must_not_query = []
            store_data_details = []
            if hyperlocal == True and storelisting == False and zone_id != "":
                zone_details = zone_find({"_id": ObjectId(zone_id)})
                store_query = {
                    "categoryId": str(store_category_id),
                    "cityId": zone_details["city_ID"],
                }
                store_data = db.stores.find(store_query)
                store_data_details.append("0")
                if store_data.count() > 0:
                    for store in store_data:
                        store_data_details.append(str(store["_id"]))
                else:
                    pass
            else:
                pass
            try:
                if hyperlocal == True and storelisting == False:
                    query.append({"terms": {"storeId": store_data_details}})
                elif hyperlocal == True and storelisting == True:
                    query.append({"match": {"storeId": store_id}})
                else:
                    if store_id != "":
                        query.append({"match": {"storeId": store_id}})
            except:
                pass
            product_category_id = ""
            parent_category_id = ""
            if fname != "":
                first_category_query = {
                    "categoryName.en": fname.replace("%20", " "),
                    "status": 1,
                    "storeCategory.storeCategoryId": store_category_id,
                }
                first_category_details = db.category.find_one(first_category_query)
                if first_category_details is not None:
                    parent_category_id = str(first_category_details["_id"])
                else:
                    pass
            else:
                pass

            try:
                if hyperlocal == True and storelisting == False:
                    if tname != "":
                        product_category_query = {
                            "categoryName.en": tname.replace("%20", " "),
                            "status": 1,
                            "storeid": {"$in": store_data_details},
                        }
                    elif sname != "":
                        product_category_query = {
                            "categoryName.en": sname.replace("%20", " "),
                            "status": 1,
                            "storeid": {"$in": store_data_details},
                        }
                        if parent_category_id != "":
                            product_category_query["parentId"] = ObjectId(parent_category_id)
                        else:
                            pass
                    elif fname != "":
                        product_category_query = {
                            "categoryName.en": fname.replace("%20", " "),
                            "status": 1,
                            "storeid": {"$in": store_data_details},
                        }
                    else:
                        product_category_query = {}
                    if len(product_category_query) > 0:
                        product_category_details = db.category.find_one(product_category_query)
                        if product_category_details is not None:
                            product_category_id = str(product_category_details["_id"])
                        else:
                            product_category_id = ""
                    else:
                        product_category_id = ""
                elif hyperlocal == True and storelisting == True:
                    if tname != "":
                        product_category_query = {
                            "categoryName.en": tname.replace("%20", " "),
                            "status": 1,
                            "storeid": store_id,
                        }
                    elif sname != "":
                        product_category_query = {
                            "categoryName.en": sname.replace("%20", " "),
                            "status": 1,
                            "storeid": store_id,
                        }
                        if parent_category_id != "":
                            product_category_query["parentId"] = ObjectId(parent_category_id)
                        else:
                            pass
                    elif fname != "":
                        product_category_query = {
                            "categoryName.en": fname.replace("%20", " "),
                            "status": 1,
                            "storeid": store_id,
                        }
                    else:
                        product_category_query = {}

                    if len(product_category_query) > 0:
                        product_category_details = db.category.find_one(product_category_query)
                        if product_category_details is not None:
                            product_category_id = str(product_category_details["_id"])
                        else:
                            product_category_id = ""
                    else:
                        product_category_id = ""
                else:
                    if tname != "":
                        product_category_query = {
                            "categoryName.en": tname.replace("%20", " "),
                            "status": 1,
                        }
                    elif sname != "":
                        product_category_query = {
                            "categoryName.en": sname.replace("%20", " "),
                            "status": 1,
                        }
                        if parent_category_id != "":
                            product_category_query["parentId"] = ObjectId(parent_category_id)
                        else:
                            pass
                    elif fname != "":
                        product_category_query = {
                            "categoryName.en": fname.replace("%20", " "),
                            "status": 1,
                        }
                    else:
                        product_category_query = {}

                    if len(product_category_query) > 0:
                        product_category_details = db.category.find_one(product_category_query)
                        if product_category_details is not None:
                            product_category_id = str(product_category_details["_id"])
                        else:
                            product_category_id = ""
                    else:
                        product_category_id = ""
            except:
                pass
            if symptoms_name != "":
                if "," in symptoms_name or "%2C" in symptoms_name:
                    query.append(
                        {"match": {"symptoms.symptomName": symptoms_name.replace("%20", " ")}}
                    )
                else:
                    query.append(
                        {
                            "match_phrase_prefix": {
                                "symptoms.symptomName": symptoms_name.replace("%20", " ")
                            }
                        }
                    )

            if just_id != "":
                home_page_just_in_data = db.ecomHomePage.find_one({"_id": ObjectId(just_id)})
                product_data = []
                if home_page_just_in_data is not None:
                    for entity in home_page_just_in_data["entity"]:
                        product_details = db.childProducts.find_one(
                            {"_id": ObjectId(entity["parentProductId"])}
                        )
                        if product_details is not None:
                            product_data.append(str(product_details["parentProductId"]))
                else:
                    pass
                if len(product_data) > 0:
                    query.append({"terms": {"parentProductId": product_data}})
                else:
                    pass
            else:
                pass

            if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                query.append({"match": {"units.isPrimary": True}})
            elif store_id != "" and store_category_id != PHARMACY_STORE_CATEGORY_ID:
                query.append({"match": {"units.isPrimary": True}})
            else:
                pass

            # ========================================query for the store category wise getting the products============
            query.append({"match": {"storeCategoryId": store_category_id}})

            if best_deals != "":
                query.append({"match": {"offer.status": 1}})

            if offer_id != "":
                query.append({"terms": {"offer.offerId": offer_id}})

            if attr != "":
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "units.attributes.attrlist.value.en": attr.replace("%20", "")
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "units.attributes.attrlist.value.name.en": attr.replace("%20", "")
                        }
                    }
                )
            if search_query != "":
                # ===========================product name========================================
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "pPName.en": {
                                "analyzer": "standard",
                                "query": search_query.replace("%20", " "),
                                "boost": 6,
                            }
                        }
                    }
                )
                should_query.append(
                    {
                        "match": {
                            "pPName.en": {"query": search_query.replace("%20", " "), "boost": 6}
                        }
                    }
                )
                # ===========================unit name========================================
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "units.unitName.en": {
                                "analyzer": "standard",
                                "query": search_query.replace("%20", " "),
                                "boost": 5,
                            }
                        }
                    }
                )
                should_query.append(
                    {
                        "match": {
                            "units.unitName.en": {
                                "query": search_query.replace("%20", " "),
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
                            + language: {"query": search_query.replace("%20", " "), "boost": 0}
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "detailDescription."
                            + language: {"query": search_query.replace("%20", " "), "boost": 0}
                        }
                    }
                )
                # ====================================child category=======================================
                should_query.append(
                    {
                        "match": {
                            "categoryList.parentCategory.childCatgory.categoryName."
                            + language: {"query": search_query.replace("%20", " "), "boost": 3}
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "categoryList.parentCategory.childCategory.categoryName."
                            + language: {"query": search_query.replace("%20", " "), "boost": 3}
                        }
                    }
                )
                # ===============================parent category name======================================
                should_query.append(
                    {
                        "match": {
                            "categoryList.parentCategory.categoryName."
                            + language: {"query": search_query.replace("%20", " "), "boost": 2}
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "categoryList.parentCategory.categoryName."
                            + language: {"query": search_query.replace("%20", " "), "boost": 2}
                        }
                    }
                )
                # ======================================brand name=======================================
                should_query.append(
                    {
                        "match": {
                            "brandTitle."
                            + language: {"query": search_query.replace("%20", " "), "boost": 1}
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "brandTitle."
                            + language: {"query": search_query.replace("%20", " "), "boost": 1}
                        }
                    }
                )

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
                if store_category_id == LIQUOR_STORE_CATEGORY_ID:
                    if fname != "":
                        fname = fname.replace("%2C", ",")
                        category_query = {"categoryName.en": fname.replace("%20", " ")}
                        if store_id != "":
                            category_query["$or"] = [
                                {"storeid": {"$in": [store_id]}, "storeId": {"$in": [store_id]}}
                            ]
                        category_details = db.category.find_one(category_query)
                        if (
                            category_details is not None
                            and store_category_id != ECOMMERCE_STORE_CATEGORY_ID
                        ):
                            query.append(
                                {
                                    "match": {
                                        "categoryList.parentCategory.categoryName."
                                        + language: fname.replace("%20", " ")
                                    }
                                }
                            )
                        else:
                            query.append(
                                {
                                    "match": {
                                        "categoryList.parentCategory.categoryName."
                                        + language: fname.replace("%20", " ")
                                    }
                                }
                            )
                else:
                    if fname != "":
                        fname = fname.replace("%2C", ",")
                        category_query = {"categoryName.en": fname.replace("%20", " ")}
                        if store_id != "":
                            category_query["$or"] = [
                                {"storeid": {"$in": [store_id]}, "storeId": {"$in": [store_id]}}
                            ]
                        category_details = db.category.find_one(category_query)
                        if (
                            category_details is not None
                            and store_category_id != ECOMMERCE_STORE_CATEGORY_ID
                        ):
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
                    if store_category_id != LIQUOR_STORE_CATEGORY_ID:
                        if sname != "":
                            query.append(
                                {
                                    "match_phrase_prefix": {
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

            query.append({"match": {"status": 1}})

            # ===================check for integration type=========================
            if int(integration_type) == 0:
                pass
            elif int(integration_type) == 1:
                must_not_query.append({"match": {"magentoId": -1}})
                query.append({"exists": {"field": "magentoId"}})
            elif int(integration_type) == 2:
                must_not_query.append({"term": {"shopify_variant_id.keyword": {"value": ""}}})
                query.append({"exists": {"field": "shopify_variant_id"}})
            elif int(integration_type) == 3:
                query.append({"match": {"magentoId": -1}})
                query.append({"term": {"shopify_variant_id.keyword": ""}})

            if search_query != "":
                filter_parameters_query = {
                    "query": {
                        "bool": {
                            "must": query,
                            "should": should_query,
                            "minimum_should_match": 1,
                            "boost": 1.0,
                        }
                    },
                    "size": to_data,
                    "from": from_data,
                }
            else:
                if hyperlocal == False and storelisting == False:
                    if just_id != "":
                        if len(should_query) > 0:
                            filter_parameters_query = {
                                "query": {
                                    "bool": {
                                        "must": query,
                                        "should": should_query,
                                        "minimum_should_match": 1,
                                        "boost": 1.0,
                                    }
                                },
                                "size": to_data,
                                "from": from_data,
                            }
                        else:
                            filter_parameters_query = {
                                "query": {"bool": {"must": query}},
                                "size": to_data,
                                "from": from_data,
                            }
                    else:
                        must_not_query.append({"match": {"storeId": "0"}})
                        if len(should_query) > 0:
                            filter_parameters_query = {
                                "query": {
                                    "bool": {
                                        "must": query,
                                        "should": should_query,
                                        "minimum_should_match": 1,
                                        "boost": 1.0,
                                    }
                                },
                                "size": to_data,
                                "from": from_data,
                            }
                        else:
                            filter_parameters_query = {
                                "query": {"bool": {"must": query}},
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
                                    "boost": 1.0,
                                }
                            },
                            "size": to_data,
                            "from": from_data,
                        }
                    else:
                        filter_parameters_query = {
                            "query": {
                                "bool": {
                                    "must": query,
                                }
                            },
                            "size": to_data,
                            "from": from_data,
                        }
            if len(must_not_query) > 0:
                filter_parameters_query["query"]["bool"]["must_not"] = must_not_query
            units_data = []
            size_data = []
            res_filter_parameters = es.search(
                index=index_products,
                body=filter_parameters_query,
                filter_path=[
                    "hits.hits._id",
                    "hits.hits._source",
                ],
            )
            if int(filter_level) == 1:
                if len(res_filter_parameters) == 0:
                    response = {"data": [], "message": "No Data Found"}
                    return JsonResponse(response, safe=False, status=404)

            is_ecommerce, remove_central, hide_recent_view, store_listing = validate_store_category(
                store_category_id, ECOMMERCE_STORE_CATEGORY_ID
            )
            if is_ecommerce == True:
                store_id = ""

            if store_id != "":
                store_details = db.stores.find_one(
                    {"_id": ObjectId(store_id)}, {"currencySymbol": 1, "currencyCode": 1}
                )
                try:
                    currency = store_details["currencyCode"]
                except:
                    pass
                try:
                    currencySymbol = store_details["currencySymbol"]
                except:
                    pass

            # gets all the required data for a particular filter type
            if int(filter_level) == 1:
                attr_name = []
                for i in res_filter_parameters["hits"]["hits"]:
                    best_supplier = {}
                    best_supplier["productId"] = i["_id"]
                    best_supplier["id"] = str(i["_source"]["storeId"])
                    if len(best_supplier) > 0:
                        child_product_query = {"status": 1}
                        if len(best_supplier) > 0:
                            child_product_id = best_supplier["productId"]
                        else:
                            child_product_id = i["_id"]
                        child_product_query["_id"] = ObjectId(child_product_id)
                        child_product_details = db.childProducts.find_one(child_product_query)
                        if child_product_details != None:
                            if product_category_id != "":
                                try:
                                    for ch in child_product_details["units"][0]["attributes"]:
                                        if "attrlist" in ch:
                                            for attr in ch["attrlist"]:
                                                try:
                                                    attribute_details = (
                                                        db.productAttribute.find_one(
                                                            {
                                                                "_id": ObjectId(
                                                                    attr["attributeId"]
                                                                ),
                                                                "attriubteType": {"$ne": 5},
                                                                "searchable": 1,
                                                            }
                                                        )
                                                    )
                                                    if attribute_details is not None:
                                                        product_attribute_count = db.category.find(
                                                            {
                                                                "_id": ObjectId(
                                                                    product_category_id
                                                                ),
                                                                "status": 1,
                                                                "attributeGroupData.AttributeList.attributeId": str(
                                                                    attribute_details["_id"]
                                                                ),
                                                            }
                                                        ).count()
                                                        attribute_count = 0
                                                        if type(attr["value"]) == list:
                                                            if len(attr["value"]) == 0:
                                                                pass
                                                            else:
                                                                for attr_value in attr["value"]:
                                                                    if language in attr_value:
                                                                        if (
                                                                            attr_value[language]
                                                                            != ""
                                                                        ):
                                                                            attribute_count = (
                                                                                attribute_count + 1
                                                                            )
                                                                        else:
                                                                            pass
                                                                    else:
                                                                        pass
                                                        elif type(attr["value"]) == str:
                                                            if attr["value"] == "":
                                                                pass
                                                            else:
                                                                attribute_count = (
                                                                    attribute_count + 1
                                                                )
                                                        elif type(attr["value"]) == dict:
                                                            if language in attr["value"]:
                                                                if attr["value"][language] != "":
                                                                    attribute_count = (
                                                                        attribute_count + 1
                                                                    )
                                                                else:
                                                                    pass
                                                            else:
                                                                pass
                                                        else:
                                                            pass
                                                        if (
                                                            product_attribute_count > 0
                                                            and attribute_count > 0
                                                        ):
                                                            attr_name.append(
                                                                attr["attrname"]["en"].upper()
                                                            )
                                                        else:
                                                            pass
                                                except:
                                                    pass
                                except:
                                    pass
                            else:
                                pass
                            try:
                                final_price = child_product_details["units"][0]["discountPrice"]
                            except:
                                try:
                                    final_price = child_product_details["units"][0]["floatValue"]
                                except:
                                    final_price = 0
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
                                currencySymbol = currency_details["currencySymbol"]
                                currency = currency_details["currencyCode"]
                            else:
                                currencySymbol = child_product_details["currencySymbol"]
                                currency = child_product_details["currency"]

                            if float(currency_rate) > 0:
                                base_price = final_price * float(currency_rate)
                            else:
                                base_price = final_price
                            try:
                                units_data.append(base_price)
                            except:
                                pass

                            # ======================brand details====================================
                            try:
                                brand_name = i["_source"]["brandTitle"][language]
                            except:
                                brand_name = ""

                            # ======================manufacture details====================================
                            try:
                                manufacture_name = i["_source"]["manufactureName"][language]
                            except:
                                manufacture_name = ""
                            # ==============================currency details=========================

                            if brand_name == "":
                                pass
                            else:
                                brand_list.append(brand_name)

                            if manufacture_name == "":
                                pass
                            else:
                                manufacture_list.append(manufacture_name)

                            # ==================================size data====================================
                            try:
                                if "unitSizeGroupValue" in child_product_details["units"][0]:
                                    if len(child_product_details["units"][0]["unitSizeGroupValue"]):
                                        if (
                                            "en"
                                            in child_product_details["units"][0][
                                                "unitSizeGroupValue"
                                            ]
                                        ):
                                            if (
                                                child_product_details["units"][0][
                                                    "unitSizeGroupValue"
                                                ]["en"]
                                                != ""
                                            ):
                                                size_data.append(
                                                    child_product_details["units"][0][
                                                        "unitSizeGroupValue"
                                                    ]["en"]
                                                )
                                            else:
                                                pass
                                        else:
                                            pass
                                    else:
                                        pass
                                else:
                                    pass
                            except:
                                pass

                            # =========================================colour data==========================
                            try:
                                if "colorName" in child_product_details["units"][0]:
                                    if child_product_details["units"][0]["colorName"]:
                                        if child_product_details["units"][0]["colorName"] != "":
                                            color_list.append(
                                                child_product_details["units"][0]["colorName"]
                                            )
                                    else:
                                        pass
                                else:
                                    pass
                            except:
                                pass

                            # units_data.append(best_supplier['retailerPrice'])

                            if "symptoms" in i["_source"]:
                                for sym in i["_source"]["symptoms"]:
                                    if sym["symptomName"] != "":
                                        sym_list.append(sym["symptomName"])
                            else:
                                pass

                            if "categoryList" in child_product_details:
                                for cat in child_product_details["categoryList"]:
                                    if cat["parentCategory"]["categoryName"]["en"] != "":
                                        category_list.append(
                                            cat["parentCategory"]["categoryName"]["en"]
                                        )
                                    if sname != "" or tname != "":
                                        for child_category in cat["parentCategory"][
                                            "childCategory"
                                        ]:
                                            category_list.append(
                                                child_category["categoryName"]["en"]
                                            )

                            try:
                                if "cannabisProductType" in child_product_details["units"][0]:
                                    if (
                                        child_product_details["units"][0]["cannabisProductType"]
                                        != ""
                                    ):
                                        cannabis_product_type = db.cannabisProductType.find_one(
                                            {
                                                "_id": ObjectId(
                                                    child_product_details["units"][0][
                                                        "cannabisProductType"
                                                    ]
                                                )
                                            }
                                        )
                                        if cannabis_product_type is not None:
                                            cannabis_product_type_list.append(
                                                {
                                                    "name": cannabis_product_type["productType"][
                                                        language
                                                    ]
                                                    if language
                                                    in cannabis_product_type["productType"]
                                                    else cannabis_product_type["productType"]["en"],
                                                    "id": str(cannabis_product_type["_id"]),
                                                }
                                            )
                            except:
                                pass
                attribute_data = []
                if store_category_id == CANNABIS_STORE_CATEGORY_ID and len(attr_name) > 0:
                    attribute_data.append(
                        {
                            "name": "EFFECTS",
                            "data": [],
                            "seqId": 10,
                            "selType": 1,
                            "selectType": 6,
                            "filterType": 8,
                        }
                    )
                else:
                    for attr in list(set(attr_name)):
                        try:
                            attribute_data.append(
                                {
                                    "name": attr,
                                    "data": [],
                                    "seqId": 10,
                                    "selType": 1,
                                    "selectType": 6,
                                    "filterType": 8,
                                }
                            )
                        except:
                            pass

                filters_json = []
                if len(brand_list) > 0 and bname == "":
                    brands = {
                        "name": "BRAND",
                        "data": [],
                        "selType": 1,
                        "seqId": 10,
                        "selectType": 1,
                        "filterType": 4,
                    }
                    filters_json.append(brands)
                else:
                    pass

                # if fname != "" and sname != "" and tname != "":
                #     is_category = False
                # elif fname != "" and sname != "" and tname == "":
                #     is_category = False
                # elif fname != "" and sname == "" and tname == "":
                #     is_category = False
                # elif fname != "" and sname == "" and tname != "":
                #     is_category = False
                # elif fname == "" and sname != "" and tname != "":
                #     is_category = False
                # elif fname == "" and sname == "" and tname == "":
                #     is_category = True
                # else:
                is_category = True #False made this changes because need to show category filter even after we are going through category

                if (
                    len(category_list) > 0 and store_category_id != MEAT_STORE_CATEGORY_ID
                ):  # and platform == 2:
                    if is_category == False:
                        pass
                    else:
                        category = {
                            "name": "CATEGORY",
                            "data": [],
                            "seqId": 1,
                            "selType": 2,
                            "level": 1,
                            "selectType": 2,
                            "filterType": 1,
                        }
                        filters_json.append(category)
                else:
                    pass

                if len(color_list) > 0 and is_category is False:
                    colors_data = {
                        "name": "Colour".upper(),
                        "data": [],
                        "selType": 4,
                        "seqId": 3,
                        "selectType": 3,
                        "filterType": 5,
                    }
                    filters_json.append(colors_data)
                else:
                    pass

                try:
                    max_price = max(list(set(units_data)))
                    min_price = min(list(set(units_data)))
                except:
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
                    "data": [
                        {
                            "maxPrice": float(round(max_price)),
                            "minPrice": float(round(min_price)),
                        }
                    ],
                    "selType": 3,
                    "seqId": 2,
                    "filterType": 7,
                    "selectType": 5,
                    "currency": currency,
                    "currencySymbol": currencySymbol,
                }
                thc_data = {
                    "name": "THC",
                    "data": [{"maxPrice": 100, "minPrice": 0}],
                    "selType": 3,
                    "seqId": 4,
                    "filterType": 7,
                    "selectType": 5,
                    "currency": "%",
                    "currencySymbol": "%",
                }
                type_data = {
                    "name": "TYPES",
                    "data": [],
                    "selType": 1,
                    "seqId": 5,
                    "selectType": 7,
                    "filterType": 4,
                }
                cbd_data = {
                    "name": "CBD",
                    "data": [{"maxPrice": 100, "minPrice": 0}],
                    "selType": 3,
                    "seqId": 6,
                    "filterType": 7,
                    "selectType": 5,
                    "currency": "%",
                    "currencySymbol": "%",
                }
                if len(size_data) > 0 and is_category is False:
                    sizes_data = {
                        "name": "SIZE",
                        "data": [],
                        "seqId": 7,
                        "selType": 1,
                        "selectType": 4,
                        "filterType": 6,
                    }
                    filters_json.append(sizes_data)
                else:
                    pass

                # if store_category_id != MEAT_STORE_CATEGORY_ID:
                filters_json.append(p_data)
                if store_category_id == CANNABIS_STORE_CATEGORY_ID:
                    filters_json.append(cbd_data)
                    filters_json.append(thc_data)
                    if len(cannabis_product_type_list) > 0:
                        filters_json.append(type_data)
                for atb in attribute_data:
                    filters_json.append(atb)
                new_list = sorted(filters_json, key=lambda k: k["seqId"], reverse=False)

                Final_output = {
                    "data": {
                        "filters": new_list,
                        "currency": currency,
                        "currencySymbol": currencySymbol,
                        "unitId": str(ObjectId()),
                        "message": "Filters Found",
                    }
                }
                return JsonResponse(Final_output, safe=False, status=200)

            elif int(filter_level) == 2:
                currencySymbol = "$"
                category_query = {"categoryName.en": fname, "status": 1}
                category_details = db.category.find_one(category_query)
                if category_details != None:
                    child_category_query = {
                        "parentId": ObjectId(category_details["_id"]),
                        "status": 1,
                    }
                    child_category_details = db.category.find(child_category_query)
                    for child_cat in child_category_details:
                        child_data_count = db.category.find(
                            {"parentId": ObjectId(child_cat["_id"]), "status": 1}
                        ).count()
                        sub_categpry_list.append(child_cat["categoryName"]["en"])

                subcategory = {
                    "name": "subCategories",
                    "data": list(set(sub_categpry_list)),
                    "selType": 2,
                    "level": 2,
                    "filterType": 2,
                }
                filters_json = [subcategory]

                Final_output = {
                    "data": {
                        "filters": filters_json,
                        # "currency": currency,
                        "currencySymbol": currencySymbol,
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


class ProductFilterValue(APIView):
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
        3 for subsubcategory
    #! 1 for brand
    #! 2 for category
    #! 3 for colour
    #! 4 for sizes
    #! 5 for price
    #! 6 for attributes
    #! 7 for types (cannibies only)
    """

    @swagger_auto_schema(
        method="get",
        tags=["Search & Filter"],
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
                name="storeCategoryId",
                default=ECOMMERCE_STORE_CATEGORY_ID,
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="search",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for search the value inside filter. brand search, category search, attributes search",
            ),
            openapi.Parameter(
                name="page",
                default="1",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="page number. which page number data want to display",
            ),
            openapi.Parameter(
                name="type",
                default="1",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="type need to send that which type data need to get"
                "type value should be:"
                "1 for brand"
                ", 2 for category"
                ", 3 for colour"
                ", 4 for sizes"
                ", 5 for price"
                ", 6 for attributes"
                ", 7 for types (cannibies only)",
            ),
            openapi.Parameter(
                name="q",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for the search the item in search bar ex. ni, nik, addi",
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
                name="bname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="brand name for the product which want to display. ex. Nike, H&M",
            ),
            openapi.Parameter(
                name="symptoms",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="symptom name for the product which want to display. ex. Nike, H&M",
            ),
            openapi.Parameter(
                name="o_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to displaying particular offers product. ex.5df89d3edd77d6ca2752bd10",
            ),openapi.Parameter(
                name="categoryId",
                default="623c0f7df463e41f821bc576",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="from which category we need to fetch the subcategory.",
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
                name="attribute",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while applying filter on attribute it will creating dynamic link. ex. facet_OCCASION, facet_IDEAL FOR",
            ),
            openapi.Parameter(
                name="justId",
                default="0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while clicking on view more for just in from home page",
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
                name="isGrpBrand",
                default="0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to get brands grouping buy alphabets.."
                "if need data grp by that time value shold be 1 else 0",
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
            token = request.META["HTTP_AUTHORIZATION"]
            query = []
            should_query = []
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)

            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            # ========================================query parameter====================================================
            # for the search the item in search bar
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            filter_type = int(request.GET.get("type", "0"))
            if filter_type == 0:
                response_data = {
                    "message": "filter type is missing",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=404)
            else:
                page = int(request.GET.get("page", 1))  # for the pagination
                fname = request.GET.get("fname", "")  # category-name
                search_text = request.GET.get("search", "")  # category-name
                search_query = request.GET.get("q", "")  # category-name
                fname = fname.replace("%20", " ")
                fname = fname.replace("+", " ")

                sname = request.GET.get("sname", "")  # sub-category-name
                sname = sname.replace("%20", " ")
                sname = sname.replace("+", " ")

                tname = request.GET.get("tname", "")  # sub-sub-category-name
                tname = tname.replace("%20", " ")
                tname = tname.replace("+", " ")
                bname = request.GET.get("bname", "")  # brand name for the search
                just_id = request.GET.get("justId", "")  # brand name for the search
                symptoms_name = request.GET.get("symptoms", "")  # brand name for the search
                best_deals = request.GET.get("best_deals", "")
                category_id = request.GET.get("categoryId", "") # product category id, for return the sub sub category
                offer_id = request.GET.get("o_id", "")
                offer_id = request.GET.get("o_id", "")  # get particular offer data
                if offer_id != "":
                    offer_id = offer_id.split(",")
                store_id = request.GET.get("s_id", "")
                zone_id = request.GET.get("z_id", "")
                attribute = request.GET.get("attribute", "")
                is_grp_brand = request.GET.get("isGrpBrand", "0")
                integration_type = int(request.GET.get("integrationType", 0))
                attr_value = request.GET.get("attr", "")
                attribute = attribute.replace("%20", " ")
                attribute = attribute.replace("+", "")
                from_data = int(page * 300) - 300
                to_data = 300 + from_data
                if filter_type == 6 and attribute == "":
                    response = {"message": "attribute value is mising"}
                    return JsonResponse(response, safe=False, status=422)
                else:
                    if category_id == "":
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
                        storelisting = False
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

                        try:
                            if hyperlocal == True and storelisting == False:
                                zone_details = zone_find({"_id": ObjectId(zone_id)})
                                store_query = {
                                    "categoryId": str(store_category_id),
                                    "cityId": zone_details["city_ID"],
                                }
                                store_data = db.stores.find(store_query)
                                if store_category_id != MEAT_STORE_CATEGORY_ID:
                                    store_data_details = ["0"]
                                else:
                                    store_data_details = []
                                if store_data.count() > 0:
                                    for store in store_data:
                                        store_data_details.append(str(store["_id"]))
                                    query.append({"terms": {"storeId": store_data_details}})
                            elif hyperlocal == True and storelisting == True:
                                if store_id != "":
                                    query.append({"match": {"storeId": store_id}})
                            else:
                                if store_id != "":
                                    query.append({"match": {"storeId": store_id}})
                        except:
                            pass
                        query.append({"match": {"storeCategoryId": store_category_id}})
                        # if store_category_id == ECOMMERCE_STORE_CATEGORY_ID and filter_type != 4:
                        query.append({"match": {"units.isPrimary": True}})
                        # elif store_id != "" and store_category_id != PHARMACY_STORE_CATEGORY_ID:
                        #     query.append({"match": {"units.isPrimary": True}})
                        # else:
                        #     pass

                        if just_id != "":
                            home_page_just_in_data = db.ecomHomePage.find_one(
                                {"_id": ObjectId(just_id)}
                            )
                            product_data = []
                            if home_page_just_in_data is not None:
                                for entity in home_page_just_in_data["entity"]:
                                    product_details = db.childProducts.find_one(
                                        {"_id": ObjectId(entity["parentProductId"])}
                                    )
                                    if product_details is not None:
                                        product_data.append(str(product_details["parentProductId"]))
                            else:
                                pass
                            if len(product_data) > 0:
                                query.append({"terms": {"parentProductId": product_data}})
                            else:
                                pass
                        else:
                            pass

                        if offer_id != "":
                            query.append({"terms": {"offer.offerId": offer_id}})

                        if best_deals != "":
                            query.append({"match": {"offer.status": 1}})

                        if attr_value != "":
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "units.attributes.attrlist.value.en": attr_value.replace(
                                            "%20", ""
                                        )
                                    }
                                }
                            )
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "units.attributes.attrlist.value.name.en": attr_value.replace(
                                            "%20", ""
                                        )
                                    }
                                }
                            )

                        if search_query != "":
                            # ===========================product name========================================
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "pPName.en": {
                                            "analyzer": "standard",
                                            "query": search_query.replace("%20", " "),
                                            "boost": 6,
                                        }
                                    }
                                }
                            )
                            should_query.append(
                                {
                                    "match": {
                                        "pPName.en": {
                                            "query": search_query.replace("%20", " "),
                                            "boost": 6,
                                        }
                                    }
                                }
                            )
                            # ===========================unit name========================================
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "units.unitName.en": {
                                            "analyzer": "standard",
                                            "query": search_query.replace("%20", " "),
                                            "boost": 5,
                                        }
                                    }
                                }
                            )
                            should_query.append(
                                {
                                    "match": {
                                        "units.unitName.en": {
                                            "query": search_query.replace("%20", " "),
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
                                        + language: {
                                            "query": search_query.replace("%20", " "),
                                            "boost": 0,
                                        }
                                    }
                                }
                            )
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "detailDescription."
                                        + language: {
                                            "query": search_query.replace("%20", " "),
                                            "boost": 0,
                                        }
                                    }
                                }
                            )
                            # ====================================child category=======================================
                            should_query.append(
                                {
                                    "match": {
                                        "categoryList.parentCategory.childCatgory.categoryName."
                                        + language: {
                                            "query": search_query.replace("%20", " "),
                                            "boost": 3,
                                        }
                                    }
                                }
                            )
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "categoryList.parentCategory.childCategory.categoryName."
                                        + language: {
                                            "query": search_query.replace("%20", " "),
                                            "boost": 3,
                                        }
                                    }
                                }
                            )
                            # ===============================parent category name======================================
                            should_query.append(
                                {
                                    "match": {
                                        "categoryList.parentCategory.categoryName."
                                        + language: {
                                            "query": search_query.replace("%20", " "),
                                            "boost": 2,
                                        }
                                    }
                                }
                            )
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "categoryList.parentCategory.categoryName."
                                        + language: {
                                            "query": search_query.replace("%20", " "),
                                            "boost": 2,
                                        }
                                    }
                                }
                            )
                            # ======================================brand name=======================================
                            should_query.append(
                                {
                                    "match": {
                                        "brandTitle."
                                        + language: {
                                            "query": search_query.replace("%20", " "),
                                            "boost": 1,
                                        }
                                    }
                                }
                            )
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "brandTitle."
                                        + language: {
                                            "query": search_query.replace("%20", " "),
                                            "boost": 1,
                                        }
                                    }
                                }
                            )

                        if symptoms_name != "":
                            query.append(
                                {
                                    "match_phrase_prefix": {
                                        "symptoms.symptomName": symptoms_name.replace("%20", " ")
                                    }
                                }
                            )

                        if bname != "":
                            query.append(
                                {"match_phrase_prefix": {"brandTitle.en": bname.replace("%20", " ")}}
                            )

                        if fname != "":
                            if store_category_id == LIQUOR_STORE_CATEGORY_ID:
                                if fname != "":
                                    fname = fname.replace("%2C", ",")
                                    category_query = {"categoryName.en": fname.replace("%20", " ")}
                                    if store_id != "":
                                        category_query["$or"] = [
                                            {
                                                "storeid": {"$in": [store_id]},
                                                "storeId": {"$in": [store_id]},
                                            }
                                        ]
                                    category_details = db.category.find_one(category_query)
                                    if (
                                        category_details is not None
                                        and store_category_id != ECOMMERCE_STORE_CATEGORY_ID
                                    ):
                                        query.append(
                                            {
                                                "match": {
                                                    "categoryList.parentCategory.categoryName."
                                                    + language: fname.replace("%20", " ")
                                                }
                                            }
                                        )
                                    else:
                                        query.append(
                                            {
                                                "match": {
                                                    "categoryList.parentCategory.categoryName."
                                                    + language: fname.replace("%20", " ")
                                                }
                                            }
                                        )
                            else:
                                if fname != "":
                                    fname = fname.replace("%2C", ",")
                                    category_query = {"categoryName.en": fname.replace("%20", " ")}
                                    if store_id != "":
                                        category_query["$or"] = [
                                            {
                                                "storeid": {"$in": [store_id]},
                                                "storeId": {"$in": [store_id]},
                                            }
                                        ]
                                    category_details = db.category.find_one(category_query)
                                    if (
                                        category_details is not None
                                        and store_category_id != ECOMMERCE_STORE_CATEGORY_ID
                                    ):
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
                                if store_category_id != LIQUOR_STORE_CATEGORY_ID:
                                    if sname != "":
                                        query.append(
                                            {
                                                "match_phrase_prefix": {
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

                        query.append({"match": {"status": 1}})
                        if search_text != "":
                            if filter_type == 1:
                                query.append({"match_phrase_prefix": {"brandTitle.en": search_text}})
                            elif filter_type == 2:
                                query.append(
                                    {
                                        "match_phrase_prefix": {
                                            "categoryList.parentCategory.categoryName.en": search_text
                                        }
                                    }
                                )
                            elif filter_type == 3:
                                query.append({"match_phrase_prefix": {"units.colorName": search_text}})
                            elif filter_type == 4:
                                query.append(
                                    {
                                        "match_phrase_prefix": {
                                            "units.unitSizeGroupValue.en": search_text
                                        }
                                    }
                                )
                            elif filter_type == 6 and store_category_id != CANNABIS_STORE_CATEGORY_ID:
                                query.append(
                                    {
                                        "match_phrase_prefix": {
                                            "units.attributes.attrlist.value.en": search_text
                                        }
                                    }
                                )
                            else:
                                pass
                        else:
                            pass
                        sort_query = []
                        must_not_query = []

                        # ===================check for integration type=========================
                        if int(integration_type) == 0:
                            pass
                        elif int(integration_type) == 1:
                            must_not_query.append({"match": {"magentoId": -1}})
                            query.append({"exists": {"field": "magentoId"}})
                        elif int(integration_type) == 2:
                            must_not_query.append(
                                {"term": {"shopify_variant_id.keyword": {"value": ""}}}
                            )
                            query.append({"exists": {"field": "shopify_variant_id"}})
                        elif int(integration_type) == 3:
                            query.append({"match": {"magentoId": -1}})
                            query.append({"term": {"shopify_variant_id.keyword": ""}})

                        if filter_type == 1:
                            field_query = "brandTitle.en.keyword"
                            sort_query.append({"brandTitle.en.keyword": {"order": "asc"}})
                            must_not_query.append({"terms": {"brandTitle.en.keyword": [""]}})
                        elif filter_type == 2:
                            # field_query = "categoryList.parentCategory.categoryName.en.keyword"
                            field_query = "categoryList.parentCategory.categoryId.keyword"
                            sub_field_query = (
                                "categoryList.parentCategory.childCategory.categoryId.keyword"
                            )
                            sort_query.append(
                                {
                                    "categoryList.parentCategory.categoryName.en.keyword": {
                                        "order": "asc"
                                    }
                                }
                            )
                            must_not_query.append(
                                {"terms": {"categoryList.parentCategory.categoryName.en.keyword": [""]}}
                            )
                        elif filter_type == 3:
                            field_query = "units.colorName.keyword"
                            sort_query.append({"units.colorName.keyword": {"order": "asc"}})
                            must_not_query.append({"terms": {"units.colorName.keyword": [""]}})
                        elif filter_type == 4:
                            field_query = "units.unitSizeGroupValue.en.keyword"
                            sort_query.append({"units.unitSizeGroupValue.en.keyword": {"order": "asc"}})
                            must_not_query.append(
                                {"terms": {"units.unitSizeGroupValue.en.keyword": [""]}}
                            )
                        elif filter_type == 5:
                            sort_query.append({"units.discountPrice": {"order": "asc"}})
                        elif filter_type == 6 and store_category_id != CANNABIS_STORE_CATEGORY_ID:
                            field_query = "units.attributes.attrlist.attrname.en"
                            sort_query.append(
                                {"units.attributes.attrlist.value.en.keyword": {"order": "asc"}}
                            )
                            must_not_query.append(
                                {"terms": {"units.attributes.attrlist.attrname.en": [""]}}
                            )
                        elif filter_type == 6 and store_category_id == CANNABIS_STORE_CATEGORY_ID:
                            field_query = "units.attributes.attrlist.attrname.en"
                            sort_query.append(
                                {"units.attributes.attrlist.attrname.en.keyword": {"order": "asc"}}
                            )
                            must_not_query.append(
                                {"terms": {"units.attributes.attrlist.attrname.en": [""]}}
                            )
                        elif filter_type == 7:
                            field_query = "units.cannabisProductType.keyword"
                            sort_query.append({"units.cannabisProductType.keyword": {"order": "asc"}})
                            must_not_query.append({"terms": {"units.cannabisProductType": [""]}})
                        if hyperlocal == True and storelisting == False:
                            if len(should_query) > 0:
                                product_search_query = {
                                    "bool": {
                                        "should": should_query,
                                        "minimum_should_match": 1,
                                        "boost": 1.0,
                                        "must": query,
                                        "must_not": must_not_query,
                                    }
                                }
                            else:
                                product_search_query = {
                                    "bool": {"must": query, "must_not": must_not_query}
                                }
                        else:
                            if just_id != "":
                                pass
                            else:
                                must_not_query.append({"match": {"storeId": "0"}})
                            if len(should_query) > 0:
                                product_search_query = {
                                    "bool": {
                                        "should": should_query,
                                        "minimum_should_match": 1,
                                        "boost": 1.0,
                                        "must": query,
                                        "must_not": must_not_query,
                                    }
                                }
                            else:
                                product_search_query = {
                                    "bool": {"must": query, "must_not": must_not_query}
                                }
                        if filter_type not in [2, 5, 6, 4]:
                            search_item_query = {
                                "query": product_search_query,
                                "track_total_hits": True,
                                "sort": sort_query,
                                "aggs": {
                                    "group_by_sub_category": {
                                        "terms": {"field": field_query, "size": 300},
                                        "aggs": {
                                            "top_sales_hits": {
                                                "top_hits": {
                                                    "sort": sort_query,
                                                    "_source": {"includes": ["brandTitle"]},
                                                    "size": 1,
                                                }
                                            }
                                        },
                                    }
                                },
                            }
                        elif filter_type == 2:
                            search_item_query = {
                                "query": product_search_query,
                                "track_total_hits": True,
                                "sort": sort_query,
                                "aggs": {
                                    "group_by_sub_category": {
                                        "terms": {"field": field_query, "size": 3000},
                                        "aggs": {
                                            "top_hits": {
                                                "terms": {"field": sub_field_query, "size": 100},
                                                "aggs": {
                                                    "top_sub_category_hits": {
                                                        "top_hits": {
                                                            "_source": {"includes": ["brandTitle"]},
                                                            "size": 1,
                                                        }
                                                    }
                                                },
                                            }
                                        },
                                    }
                                },
                            }
                        elif filter_type == 6 or filter_type == 4:
                            search_item_query = {
                                "query": product_search_query,
                                "track_total_hits": True,
                                "sort": sort_query,
                                "size": 300,
                            }
                        else:
                            search_item_query = {
                                "query": product_search_query,
                                "track_total_hits": True,
                                "sort": sort_query,
                                "aggs": {
                                    "max_price": {"max": {"field": "units.discountPrice"}},
                                    "min_price": {"min": {"field": "units.discountPrice"}},
                                },
                            }
                        if filter_type == 4:
                            res = es.search(
                                index=index_products,
                                body=search_item_query,
                                filter_path=[
                                    "hits.total",
                                    "hits.hits._id",
                                    "hits.hits._source.units.unitSizeGroupValue",
                                ],
                            )
                        elif filter_type != 6:
                            res = es.search(index=index_products, body=search_item_query)
                        else:
                            res = es.search(
                                index=index_products,
                                body=search_item_query,
                                filter_path=[
                                    "hits.total",
                                    "hits.hits._id",
                                    "hits.hits._source.units.attributes.attrlist.attrname",
                                    "hits.hits._source.units.attributes.attrlist.attributeId",
                                    "hits.hits._source.units.attributes.attrlist.measurementUnitName",
                                    "hits.hits._source.units.attributes.attrlist.value",
                                ],
                            )
                        json_data = []
                        total_count = 0
                        min_price = 0
                        max_price = 0
                        colour_details = db.colors.find_one({})
                        colour_json = {}
                        if colour_details is not None:
                            for color in colour_details["color"]:
                                colour_json[color["name"]] = (
                                    "rgb("
                                    + str(color["R"])
                                    + ","
                                    + str(color["G"])
                                    + ","
                                    + str(color["B"])
                                    + ")"
                                )
                        sub_category_json = []
                        if filter_type == 2:
                            if "aggregations" in res:
                                if "group_by_sub_category" in res["aggregations"]:
                                    if "buckets" in res["aggregations"]["group_by_sub_category"]:
                                        total_count = len(res["aggregations"]["group_by_sub_category"]["buckets"])
                                        for bucket in res["aggregations"]["group_by_sub_category"]["buckets"][from_data:to_data]:
                                            product_categoty_details = db.category.find_one({"_id": ObjectId(bucket["key"])})
                                            if product_categoty_details is not None:
                                                if fname.lower() in product_categoty_details["categoryName"][language].lower(): # == fname.lower():
                                                    json_data.append(
                                                        {
                                                            "name": product_categoty_details["categoryName"][language].lower(),
                                                            "id": bucket["key"].lower(),
                                                            "penCount": bucket["doc_count"],
                                                        }
                                                    )
                                                    main_category_data = db.category.find(
                                                        {
                                                            "parentId": ObjectId(bucket["key"]),
                                                            "status": 1,
                                                            "storeId": "0"
                                                        }
                                                    )
                                                    for inner_bucket in main_category_data:
                                                        sub_category_json.append(
                                                            {
                                                                "parentId": bucket["key"],
                                                                "childId": str(inner_bucket["_id"]),
                                                                "count": bucket["doc_count"],
                                                            }
                                                        )
                        elif filter_type == 7:
                            if "aggregations" in res:
                                if "group_by_sub_category" in res["aggregations"]:
                                    if "buckets" in res["aggregations"]["group_by_sub_category"]:
                                        total_count = len(
                                            res["aggregations"]["group_by_sub_category"]["buckets"]
                                        )
                                        for bucket in res["aggregations"]["group_by_sub_category"][
                                            "buckets"
                                        ][from_data:to_data]:
                                            if bucket["key"] != "":
                                                cannabis_product_type = db.cannabisProductType.find_one(
                                                    {"_id": ObjectId(bucket["key"])}
                                                )
                                                if cannabis_product_type is not None:
                                                    name = (
                                                        cannabis_product_type["productType"][language]
                                                        if language
                                                        in cannabis_product_type["productType"]
                                                        else cannabis_product_type["productType"]["en"]
                                                    )
                                                    json_data.append(
                                                        {
                                                            "name": name.lower(),
                                                            "id": str(cannabis_product_type["_id"]),
                                                            "penCount": bucket["doc_count"],
                                                        }
                                                    )
                        elif filter_type == 4:
                            if "hits" in res:
                                if "hits" in res["hits"]:
                                    attribute_data = []
                                    for bucket in res["hits"]["hits"]:  # [from_data:to_data]:
                                        if "_source" in bucket:
                                            if (
                                                "en"
                                                in bucket["_source"]["units"][0]["unitSizeGroupValue"]
                                            ):
                                                if (
                                                    bucket["_source"]["units"][0]["unitSizeGroupValue"][
                                                        "en"
                                                    ]
                                                    != ""
                                                ):
                                                    attribute_data.append(
                                                        {
                                                            "name": bucket["_source"]["units"][0][
                                                                "unitSizeGroupValue"
                                                            ]["en"].strip()
                                                        }
                                                    )
                                                else:
                                                    pass
                                            else:
                                                pass
                                        else:
                                            pass
                                    if len(attribute_data) > 0:
                                        attribute_dataframe = pd.DataFrame(attribute_data)
                                        attribute_dataframe["penCount"] = attribute_dataframe.groupby(
                                            "name"
                                        )["name"].transform("count")
                                        attribute_dataframe = attribute_dataframe.drop_duplicates(
                                            "name", keep="last"
                                        )
                                        attribute_list = attribute_dataframe.to_json(orient="records")
                                        json_data = json.loads(attribute_list)
                                        total_count = len(json_data)
                                    else:
                                        pass
                                else:
                                    pass
                            else:
                                pass
                        elif filter_type == 3:
                            if "aggregations" in res:
                                if "group_by_sub_category" in res["aggregations"]:
                                    if "buckets" in res["aggregations"]["group_by_sub_category"]:
                                        total_count = len(
                                            res["aggregations"]["group_by_sub_category"]["buckets"]
                                        )
                                        for bucket in res["aggregations"]["group_by_sub_category"][
                                            "buckets"
                                        ][from_data:to_data]:
                                            colour_rgb = ""
                                            if bucket["key"] in colour_json:
                                                colour_rgb = colour_json[bucket["key"]]
                                            if colour_rgb == "":
                                                try:
                                                    colour_rgb = colour_json[(bucket["key"]).upper()]
                                                except:
                                                    colour_rgb = ""
                                            if colour_rgb == "":
                                                colour_rgb = colour_json[(bucket["key"]).title()]
                                            json_data.append(
                                                {
                                                    "rgb": colour_rgb,
                                                    "name": bucket["key"].lower(),
                                                    "penCount": bucket["doc_count"],
                                                }
                                            )
                        elif filter_type not in [5, 6]:
                            if int(is_grp_brand) == 0:
                                if "aggregations" in res:
                                    if "group_by_sub_category" in res["aggregations"]:
                                        if "buckets" in res["aggregations"]["group_by_sub_category"]:
                                            total_count = len(
                                                res["aggregations"]["group_by_sub_category"]["buckets"]
                                            )
                                            for bucket in res["aggregations"]["group_by_sub_category"][
                                                "buckets"
                                            ][from_data:to_data]:
                                                json_data.append(
                                                    {
                                                        "name": bucket["key"].lower(),
                                                        "penCount": bucket["doc_count"],
                                                    }
                                                )
                            else:
                                json_data_new = []
                                if "aggregations" in res:
                                    if "group_by_sub_category" in res["aggregations"]:
                                        if "buckets" in res["aggregations"]["group_by_sub_category"]:
                                            total_count = len(
                                                res["aggregations"]["group_by_sub_category"]["buckets"]
                                            )
                                            for bucket in res["aggregations"]["group_by_sub_category"][
                                                "buckets"
                                            ][from_data:to_data]:
                                                json_data_new.append(
                                                    {
                                                        "name": bucket["key"].lower(),
                                                        "char": bucket["key"][0].upper(),
                                                    }
                                                )
                                        else:
                                            pass
                                    else:
                                        pass
                                else:
                                    pass

                                if len(json_data_new) > 0:
                                    values_by_char = {}
                                    for d in json_data_new:
                                        values_by_char.setdefault(d["char"].upper(), []).append(
                                            d["name"].title()
                                        )
                                    for values in values_by_char:
                                        json_data.append(
                                            {
                                                "name": list(set(values_by_char[values])),
                                                "char": values,
                                                "penCount": 0,
                                            }
                                        )
                                else:
                                    pass
                        elif filter_type == 6 and store_category_id != CANNABIS_STORE_CATEGORY_ID:
                            attribute_data = []
                            if "hits" in res:
                                if "hits" in res["hits"]:
                                    for bucket in res["hits"]["hits"]:  # [from_data:to_data]:
                                        if "_source" in bucket:
                                            try:
                                                for attr in bucket["_source"]["units"][0]["attributes"]:
                                                    for attr_list in attr["attrlist"]:
                                                        if "value" in attr_list:
                                                            if "en" in attr_list["value"]:
                                                                attr_name = attr_list["attrname"][
                                                                    "en"
                                                                ].strip()
                                                                if (
                                                                    attr_name.lower()
                                                                    == attribute.lower()
                                                                ):
                                                                    measurement_unit = (
                                                                        attr_list["measurementUnitName"]
                                                                        if "measurementUnitName"
                                                                        in attr_list
                                                                        else ""
                                                                    )
                                                                    if "en" in attr_list["value"]:
                                                                        if (
                                                                            type(
                                                                                attr_list["value"]["en"]
                                                                            )
                                                                            != list
                                                                        ):
                                                                            if (
                                                                                attr_list["value"]["en"]
                                                                                != ""
                                                                            ):
                                                                                value = (
                                                                                    str(
                                                                                        attr_list[
                                                                                            "value"
                                                                                        ]["en"]
                                                                                    ).lower()
                                                                                    + measurement_unit
                                                                                )
                                                                                attribute_data.append(
                                                                                    {
                                                                                        "name": value,
                                                                                    }
                                                                                )
                                                                            else:
                                                                                pass
                                                                        else:
                                                                            for (
                                                                                inner_value
                                                                            ) in attr_list["value"][
                                                                                "en"
                                                                            ]:
                                                                                value = str(
                                                                                    str(
                                                                                        inner_value
                                                                                    ).lower()
                                                                                    + measurement_unit
                                                                                )
                                                                                if value != "":
                                                                                    attribute_data.append(
                                                                                        {
                                                                                            "name": value,
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
                                            except:
                                                pass
                                        else:
                                            pass

                                    if len(attribute_data) > 0:
                                        attribute_dataframe = pd.DataFrame(attribute_data)
                                        attribute_dataframe["penCount"] = attribute_dataframe.groupby(
                                            "name"
                                        )["name"].transform("count")
                                        attribute_dataframe = attribute_dataframe.drop_duplicates(
                                            "name", keep="last"
                                        )
                                        attribute_list = attribute_dataframe.to_json(orient="records")
                                        json_data = json.loads(attribute_list)
                                        total_count = len(json_data)
                                    else:
                                        pass
                                else:
                                    pass
                            else:
                                pass
                        elif filter_type == 6 and store_category_id == CANNABIS_STORE_CATEGORY_ID:
                            attribute_data = []
                            if "hits" in res:
                                if "hits" in res["hits"]:
                                    for bucket in res["hits"]["hits"]:  # [from_data:to_data]:
                                        if "_source" in bucket:
                                            for attr in bucket["_source"]["units"][0]["attributes"]:
                                                for attr_list in attr["attrlist"]:
                                                    try:
                                                        attribute_details = (
                                                            db.productAttribute.find_one(
                                                                {
                                                                    "_id": ObjectId(
                                                                        attr_list["attributeId"]
                                                                    ),
                                                                    "attriubteType": {"$ne": 5},
                                                                    "searchable": 1,
                                                                }
                                                            )
                                                        )
                                                        if attribute_details is not None:
                                                            if "en" in attr_list["attrname"]:
                                                                attribute_data.append(
                                                                    {
                                                                        "name": attr_list["attrname"][
                                                                            language
                                                                        ],
                                                                    }
                                                                )
                                                            else:
                                                                pass
                                                    except:
                                                        pass
                                        else:
                                            pass

                                    if len(attribute_data) > 0:
                                        attribute_dataframe = pd.DataFrame(attribute_data)
                                        attribute_dataframe["penCount"] = attribute_dataframe.groupby(
                                            "name"
                                        )["name"].transform("count")
                                        attribute_dataframe = attribute_dataframe.drop_duplicates(
                                            "name", keep="last"
                                        )
                                        attribute_list = attribute_dataframe.to_json(orient="records")
                                        json_data = json.loads(attribute_list)
                                        total_count = len(json_data)
                                    else:
                                        pass
                                else:
                                    pass
                            else:
                                pass
                        else:
                            if "aggregations" in res:
                                if "max_price" in res["aggregations"]:
                                    if "value" in res["aggregations"]["max_price"]:
                                        max_price = res["aggregations"]["max_price"]["value"]
                                    else:
                                        pass
                                else:
                                    pass

                                if "min_price" in res["aggregations"]:
                                    if "value" in res["aggregations"]["min_price"]:
                                        min_price = res["aggregations"]["min_price"]["value"]
                                    else:
                                        pass
                                else:
                                    pass

                        if min_price >= 0 and max_price != 0:
                            try:
                                currency_rate = currency_exchange_rate[
                                    str("INR") + "_" + str(currency_code)
                                ]
                            except:
                                currency_rate = 0
                            currency_details = db.currencies.find_one({"currencyCode": currency_code})
                            if currency_details is not None:
                                currencySymbol = currency_details["currencySymbol"]
                                currency = currency_details["currencyCode"]
                            else:
                                currencySymbol = "INR"
                                currency = res["hits"]["hits"][0]["_source"]["currency"]
                            if float(currency_rate) > 0:
                                min_price_new = min_price * float(currency_rate)
                                max_price_new = max_price * float(currency_rate)
                            else:
                                min_price_new = min_price
                                max_price_new = max_price
                        else:
                            try:
                                currencySymbol = res["hits"]["hits"][0]["_source"]["currencySymbol"]
                                currency = res["hits"]["hits"][0]["_source"]["currency"]
                            except:
                                currencySymbol = "₹"
                                currency = "INR"

                        if len(json_data) > 0 and filter_type != 5:
                            if len(json_data) > 0 and int(is_grp_brand) == 0:
                                dataframe = pd.DataFrame(json_data)
                                dataframe = dataframe.drop_duplicates("name", keep="last")
                                colors_json = dataframe.to_json(orient="records")
                                colors_json = json.loads(colors_json)
                                category_json = sorted(colors_json, key=lambda k: k["name"])
                                new_colour_list = []
                                if filter_type == 2:
                                    # ! for loop on main category list to get the child catgeory list for the category
                                    for new_cat in category_json:
                                        # ! check if category is available in list of the sub categories
                                        if not any(
                                            d["parentId"] == new_cat["id"] for d in sub_category_json
                                        ):
                                            pass
                                        else:
                                            sub_category_data = []
                                            for sub_cat in sub_category_json:
                                                if sub_cat["parentId"] == new_cat["id"]:
                                                    child_category = db.category.find(
                                                        {
                                                            "parentId": ObjectId(sub_cat["parentId"]),
                                                            "status": 1,
                                                            "_id": ObjectId(sub_cat["childId"]),
                                                        }
                                                    )
                                                    if child_category.count() > 0:
                                                        for child in child_category:
                                                            child_child_category = db.category.find(
                                                                {
                                                                    "parentId": ObjectId(child["_id"]),
                                                                    "status": 1,
                                                                }
                                                            ).count()
                                                            sub_category_data.append(
                                                                {
                                                                    "name": child["categoryName"][
                                                                        language
                                                                    ],
                                                                    "id": str(child["_id"]),
                                                                    "childCount": child_child_category,
                                                                    "penCount": sub_cat["count"],
                                                                }
                                                            )
                                                    else:
                                                        pass
                                                else:
                                                    pass
                                            if len(sub_category_data) > 0:
                                                new_colour_list.append(
                                                    {
                                                        "name": new_cat["name"],
                                                        "id": new_cat["id"],
                                                        "categoryData": sub_category_data,
                                                        "penCount": new_cat["penCount"],
                                                    }
                                                )
                                            else:
                                                pass
                                else:
                                    new_colour_list = category_json
                            elif int(is_grp_brand) == 1:
                                category_json = sorted(json_data, key=lambda k: k["char"])
                                new_colour_list = category_json
                            else:
                                new_colour_list = []
                            data = {
                                "message": "data found",
                                "data": new_colour_list,
                                "penCount": total_count,
                            }
                            return JsonResponse(data, safe=False, status=200)
                        elif filter_type == 5:
                            price_data = []
                            price_data.append(
                                {
                                    "maxPrice": round(max_price_new, 2),
                                    "minPrice": round(min_price_new, 2),
                                }
                            )
                            data = {
                                "message": "data found",
                                "data": price_data,
                                "maxPrice": round(max_price_new, 2),
                                "minPrice": round(min_price_new, 2),
                                "currencySymbol": currencySymbol,
                                "currency": currency,
                                "penCount": total_count,
                            }
                            return JsonResponse(data, safe=False, status=200)
                        else:
                            data = {"message": "data not found", "data": [], "penCount": 0}
                            return JsonResponse(data, safe=False, status=404)
                    else:
                        product_category_query = {
                            "parentId": ObjectId(category_id),
                            "status": 1
                        }
                        product_category_details = db.category.find(product_category_query)
                        category_data = []
                        for pro_cat in product_category_details:
                            doc_count = db.category.find(
                                {
                                    "parentId": ObjectId(pro_cat['_id']),
                                    "status": 1
                                }
                            ).count()
                            category_data.append(
                                {
                                    "childCount": doc_count,
                                    "id": str(pro_cat['_id']),
                                    "name": pro_cat['categoryName']["en"],
                                    "penCount": doc_count
                                }
                            )
                        if len(category_data) > 0:
                            data = {
                                "message": "data found",
                                "data": category_data
                            }
                            return JsonResponse(data, safe=False, status=200)
                        else:
                            data = {
                                "message": "data not found",
                                "data": []
                            }
                            return JsonResponse(data, safe=False, status=404)

        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class ProductFilterNew(APIView):
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
        tags=["Search & Filter"],
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
                default=ECOMMERCE_STORE_CATEGORY_ID,
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
                name="bid",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="brand id while searching brand or applying filter on brand",
            ),
            openapi.Parameter(
                name="mainCategoryId",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="category id while searching category",
            ),
            openapi.Parameter(
                name="facet_",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while applying filter on attribute it will creating dynamic link. ex. facet_OCCASION, facet_IDEAL FOR",
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
                name="justId",
                default="0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while clicking on view more for just in from home page",
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
            token = (
                request.META["HTTP_AUTHORIZATION"]
                if "HTTP_AUTHORIZATION" in request.META
                else "sdfsdfsf"
            )
            query = []
            should_query = []
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            try:
                currencySymbol = request.META["HTTP_CURRENCYSYMBOL"] if "HTTP_CURRENCYSYMBOL" in request.META else "₹"
                currency = request.META["HTTP_CURRENCYCOODE"] if "HTTP_CURRENCYCOODE" in request.META else "INR"
                if currencySymbol != "₹":
                    currencySymbol = base64.b64decode(currencySymbol).decode('utf-8')
                # currencySymbol = currency_codes.get_symbol(currency)
            except:
                currencySymbol = "₹"
                currency = "INR"
            color_list = []
            brand_list = []
            manufacture_list = []
            cannabis_product_type_list = []
            sym_list = []
            category_list = []
            sub_categpry_list = []
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            filter_level = request.META["HTTP_LEVEL"] if "HTTP_LEVEL" in request.META else 1
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            # ========================================query parameter====================================================
            # for the search the item in search bar
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            search_query = request.GET.get("q", "")
            platform = int(request.GET.get("platform", "1"))
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

            colour = request.GET.get("colour", "")  # colour name
            bname = request.GET.get("bname", "")  # brand name for the search
            symptoms_name = request.GET.get("symptoms", "")  # brand name for the search
            just_id = request.GET.get("justId", "")  # get particular offer data
            b_id = request.GET.get("bid", "")  # brand id
            category_id = request.GET.get("mainCategoryId", "")  # category id
            best_deals = request.GET.get("best_deals", "")  # get the best deals
            offer_id = request.GET.get("o_id", "")
            offer_id = request.GET.get("o_id", "")  # get particular offer data
            if offer_id != "":
                offer_id = offer_id.split(",")
            store_id = request.GET.get("s_id", "")
            zone_id = request.GET.get("z_id", "")
            attr = request.GET.get("attr", "")
            integration_type = int(request.GET.get("integrationType", 0))
            just_id = request.GET.get("justId", "")  # get particular offer data
            QUERY_STRING = request.META["QUERY_STRING"]
            to_data = 100
            from_data = int(page * 100) - 100
            if store_category_id == CANNABIS_STORE_CATEGORY_ID:
                zone_id = ""
            sname = sname.replace("+", " ")

            rjId = ""
            if store_id != "":
                if rjId == "":
                    rjId = store_id
                else:
                    rjId = rjId + "_" + store_id
            else:
                pass

            if fname != "":
                if rjId == "":
                    rjId = fname
                else:
                    rjId = rjId + "_" + fname
            else:
                pass

            if sname != "":
                if rjId == "":
                    rjId = sname
                else:
                    rjId = rjId + "_" + sname
            else:
                pass

            if tname != "":
                if rjId == "":
                    rjId = tname
                else:
                    rjId = rjId + "_" + tname
            else:
                pass

            if b_id != "":
                if rjId == "":
                    rjId = b_id
                else:
                    rjId = rjId + "_" + b_id
            else:
                pass

            try:
                redis_response_data = REDIS_FILTER_LABLE_DB.jsonget(rjId)
            except:
                redis_response_data = {}
            if redis_response_data is None:
                redis_response_data = {}
            redis_response_data = {}
            if len(redis_response_data) > 0:
                Final_output = {
                    "data": redis_response_data['data']
                }
                return JsonResponse(Final_output, safe=False, status=redis_response_data['status'])
            else:
                category_query = {"storeCategory.storeCategoryId": store_category_id}
                if zone_id != "":
                    zone_details = zone_find({"_id": ObjectId(zone_id)})
                    category_query["_id"] = ObjectId(zone_details["city_ID"])
                elif store_id != "":
                    store_details = db.stores.find_one({"_id": ObjectId(store_id)}, {"cityId": 1})
                    category_query["_id"] = str(store_details["cityId"])
                else:
                    pass

                categoty_details = db.cities.find_one(category_query, {"storeCategory": 1})

                hyperlocal = False
                storelisting = False
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
                                        storelisting = False
                                        hyperlocal = False
                                else:
                                    pass
                        else:
                            hyperlocal = False
                            storelisting = False
                    else:
                        hyperlocal = False
                        storelisting = False
                must_not_query = []
                store_data_details = []
                if hyperlocal == True and storelisting == False and zone_id != "":
                    zone_details = zone_find({"_id": ObjectId(zone_id)})
                    store_query = {
                        "categoryId": str(store_category_id),
                        "cityId": zone_details["city_ID"],
                    }
                    store_data = db.stores.find(store_query, {"storeName": 1})
                    # store_data_details.append("0")
                    if store_data.count() > 0:
                        for store in store_data:
                            store_data_details.append(str(store["_id"]))
                    else:
                        pass
                else:
                    pass
                try:
                    if hyperlocal == True and storelisting == False:
                        query.append({"terms": {"storeId": store_data_details}})
                    elif hyperlocal == True and storelisting == True:
                        query.append({"match": {"storeId": store_id}})
                    else:
                        if store_id != "":
                            query.append({"match": {"storeId": store_id}})
                except:
                    pass
                product_category_id = ""
                parent_category_id = ""
                if fname != "":
                    first_category_query = {
                        "categoryName.en": fname.replace("%20", " "),
                        "status": 1,
                        "storeCategory.storeCategoryId": store_category_id,
                    }
                    first_category_details = db.category.find_one(first_category_query, {"categoryName": 1})
                    if first_category_details is not None:
                        parent_category_id = str(first_category_details["_id"])
                    else:
                        pass
                else:
                    pass

                try:
                    if hyperlocal == True and storelisting == False:
                        if tname != "":
                            product_category_query = {
                                "categoryName.en": tname.replace("%20", " "),
                                "status": 1,
                                "storeid": {"$in": store_data_details},
                            }
                        elif sname != "":
                            product_category_query = {
                                "categoryName.en": sname.replace("%20", " "),
                                "status": 1,
                                "storeid": {"$in": store_data_details},
                            }
                            if parent_category_id != "":
                                product_category_query["parentId"] = ObjectId(parent_category_id)
                            else:
                                pass
                        elif fname != "":
                            product_category_query = {
                                "categoryName.en": fname.replace("%20", " "),
                                "status": 1,
                                "storeid": {"$in": store_data_details},
                            }
                        else:
                            product_category_query = {}
                        if len(product_category_query) > 0:
                            product_category_details = db.category.find_one(product_category_query, {"categoryName": 1})
                            if product_category_details is not None:
                                product_category_id = str(product_category_details["_id"])
                            else:
                                product_category_id = ""
                        else:
                            product_category_id = ""
                    elif hyperlocal == True and storelisting == True:
                        if tname != "":
                            product_category_query = {
                                "categoryName.en": tname.replace("%20", " "),
                                "status": 1,
                                "storeid": store_id,
                            }
                        elif sname != "":
                            product_category_query = {
                                "categoryName.en": sname.replace("%20", " "),
                                "status": 1,
                                "storeid": store_id,
                            }
                            if parent_category_id != "":
                                product_category_query["parentId"] = ObjectId(parent_category_id)
                            else:
                                pass
                        elif fname != "":
                            product_category_query = {
                                "categoryName.en": fname.replace("%20", " "),
                                "status": 1,
                                "storeid": store_id,
                            }
                        else:
                            product_category_query = {}

                        if len(product_category_query) > 0:
                            product_category_details = db.category.find_one(product_category_query, {"categoryName": 1})
                            if product_category_details is not None:
                                product_category_id = str(product_category_details["_id"])
                            else:
                                product_category_id = ""
                        else:
                            product_category_id = ""
                    else:
                        if tname != "":
                            product_category_query = {
                                "categoryName.en": tname.replace("%20", " "),
                                "status": 1,
                            }
                        elif sname != "":
                            product_category_query = {
                                "categoryName.en": sname.replace("%20", " "),
                                "status": 1,
                            }
                            if parent_category_id != "":
                                product_category_query["parentId"] = ObjectId(parent_category_id)
                            else:
                                pass
                        elif fname != "":
                            product_category_query = {
                                "categoryName.en": fname.replace("%20", " "),
                                "status": 1,
                            }
                        else:
                            product_category_query = {}

                        if len(product_category_query) > 0:
                            product_category_details = db.category.find_one(product_category_query, {"categoryName": 1})
                            if product_category_details is not None:
                                product_category_id = str(product_category_details["_id"])
                            else:
                                product_category_id = ""
                        else:
                            product_category_id = ""
                except:
                    pass
                if symptoms_name != "":
                    if "," in symptoms_name or "%2C" in symptoms_name:
                        query.append(
                            {"match": {"symptoms.symptomName": symptoms_name.replace("%20", " ")}}
                        )
                    else:
                        query.append(
                            {
                                "match_phrase_prefix": {
                                    "symptoms.symptomName": symptoms_name.replace("%20", " ")
                                }
                            }
                        )

                if just_id != "":
                    home_page_just_in_data = db.ecomHomePage.find_one({"_id": ObjectId(just_id)})
                    product_data = []
                    if home_page_just_in_data is not None:
                        for entity in home_page_just_in_data["entity"]:
                            product_details = db.childProducts.find_one(
                                {"_id": ObjectId(entity["parentProductId"])}
                            )
                            if product_details is not None:
                                product_data.append(str(product_details["parentProductId"]))
                    else:
                        pass
                    if len(product_data) > 0:
                        query.append({"terms": {"parentProductId": product_data}})
                    else:
                        pass
                else:
                    pass

                if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                    query.append({"match": {"units.isPrimary": True}})
                elif store_id != "" and store_category_id != PHARMACY_STORE_CATEGORY_ID:
                    query.append({"match": {"units.isPrimary": True}})
                else:
                    pass

                # ========================================query for the store category wise getting the products============
                query.append({"match": {"storeCategoryId": store_category_id}})

                if best_deals != "":
                    query.append({"match": {"offer.status": 1}})

                if offer_id != "":
                    query.append({"terms": {"offer.offerId": offer_id}})

                ### need to search base on category id
                if category_id != "":
                    categoryid = category_id.replace("%2C", ",")
                    should_query.append(
                        {
                            "match_phrase_prefix": {
                                "categoryList.parentCategory.categoryId": categoryid
                            }
                        }
                    )
                    should_query.append(
                        {
                            "match_phrase_prefix": {
                                "categoryList.parentCategory.childCategory.categoryId": categoryid
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

                if attr != "":
                    should_query.append(
                        {
                            "match_phrase_prefix": {
                                "units.attributes.attrlist.value.en": attr.replace("%20", "")
                            }
                        }
                    )
                    should_query.append(
                        {
                            "match_phrase_prefix": {
                                "units.attributes.attrlist.value.name.en": attr.replace("%20", "")
                            }
                        }
                    )
                if search_query != "":
                    should_query.append(
                        {
                            "span_first": {
                                "match": {
                                    "span_term": {
                                        "pName.en": search_query.replace("%20", " ")
                                    }
                                },
                                "end": 3
                            }
                        }
                    )
                    should_query.append(
                        {
                            "match": {
                                "pName.en": {
                                    "analyzer": "standard",
                                    "query": search_query.replace("%20", " "),
                                    "boost": 5
                                }
                            }
                        }
                    )
                    should_query.append(
                        {
                            "match": {
                                "pName.en": {
                                    "analyzer": "edgengram_analyzer",
                                    "query": search_query.replace("%20", " "),
                                    "boost": 5
                                }
                            }
                        }
                    )
                    # ===========================unit name========================================
                    should_query.append(
                        {
                            "match_phrase_prefix": {
                                "units.unitName.en": {
                                    "analyzer": "whitespace",
                                    "query": search_query.replace("%20", " "),
                                    "boost": 4,
                                }
                            }
                        }
                    )
                    # ====================================child category=======================================
                    should_query.append(
                        {
                            "multi_match": {
                                "analyzer": "standard",
                                "query": search_query.replace("%20", " "),
                                "fields": [
                                    "categoryList.parentCategory.categoryName." + language,
                                    "categoryList.parentCategory.childCategory.categoryName." + language
                                ],
                                "boost": 3
                            }
                        }
                    )
                    # ======================================brand name=======================================
                    should_query.append(
                        {
                            "match": {
                                "brandTitle." + language:
                                    {
                                        "analyzer": "standard",
                                        "query": search_query.replace("%20", " "),
                                        "boost": 2
                                    }
                            }
                        }
                    )
                    # ===========================================detail description============================
                    should_query.append(
                        {
                            "match_phrase_prefix": {
                                "detailDescription." + language: {
                                    "analyzer": "whitespace",
                                    "query": search_query.replace("%20", " "),
                                    "boost": 1
                                }
                            }
                        }
                    )

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

                # ===================check for integration type=========================
                if int(integration_type) == 0:
                    pass
                elif int(integration_type) == 1:
                    must_not_query.append({"match": {"magentoId": -1}})
                    query.append({"exists": {"field": "magentoId"}})
                elif int(integration_type) == 2:
                    must_not_query.append({"term": {"shopify_variant_id.keyword": {"value": ""}}})
                    query.append({"exists": {"field": "shopify_variant_id"}})
                elif int(integration_type) == 3:
                    query.append({"match": {"magentoId": -1}})
                    query.append({"term": {"shopify_variant_id.keyword": ""}})

                if search_query != "":
                    filter_parameters_query = {
                        "query": {
                            "bool": {
                                "must": query,
                                "should": should_query,
                                "minimum_should_match": 1,
                                "boost": 1.0,
                            }
                        },
                        "size": to_data,
                        "from": from_data,
                    }
                else:
                    if hyperlocal == False and storelisting == False:
                        if just_id != "":
                            if len(should_query) > 0:
                                filter_parameters_query = {
                                    "query": {
                                        "bool": {
                                            "must": query,
                                            "should": should_query,
                                            "minimum_should_match": 1,
                                            "boost": 1.0,
                                        }
                                    },
                                    "size": to_data,
                                    "from": from_data,
                                }
                            else:
                                filter_parameters_query = {
                                    "query": {"bool": {"must": query}},
                                    "size": to_data,
                                    "from": from_data,
                                }
                        else:
                            must_not_query.append({"match": {"storeId": "0"}})
                            if len(should_query) > 0:
                                filter_parameters_query = {
                                    "query": {
                                        "bool": {
                                            "must": query,
                                            "should": should_query,
                                            "minimum_should_match": 1,
                                            "boost": 1.0,
                                        }
                                    },
                                    "size": to_data,
                                    "from": from_data,
                                }
                            else:
                                filter_parameters_query = {
                                    "query": {"bool": {"must": query}},
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
                                        "boost": 1.0,
                                    }
                                },
                                "size": to_data,
                                "from": from_data,
                            }
                        else:
                            filter_parameters_query = {
                                "query": {
                                    "bool": {
                                        "must": query,
                                    }
                                },
                                "size": to_data,
                                "from": from_data,
                            }
                if len(must_not_query) > 0:
                    filter_parameters_query["query"]["bool"]["must_not"] = must_not_query
                units_data = []
                size_data = []
                res_filter_parameters = es.search(
                    index=index_products,
                    body=filter_parameters_query,
                    filter_path=[
                        "hits.hits._id",
                        "hits.hits._source",
                    ],
                )

                ### for price we need to fire to get min and max price
                filter_parameters_query["_source"] = ["pName.en"]
                filter_parameters_query["size"] = 1
                filter_parameters_query['aggs'] = {
                    "max_price": {
                        "max": {
                            "field": "units.b2cPricing.b2cproductSellingPrice"
                        }
                    },
                    "min_price": {
                        "min": {
                            "field": "units.b2cPricing.b2cproductSellingPrice"
                        }
                    }
                }
                print(filter_parameters_query)
                price_filter_parameters = es.search(
                    index=index_products,
                    body=filter_parameters_query
                )
                if int(filter_level) == 1:
                    if len(res_filter_parameters) == 0:
                        response = {"data": [], "message": "No Data Found"}
                        try:
                            REDIS_FILTER_LABLE_DB.jsonset(QUERY_STRING, Path.rootPath(), {
                                "data": [],
                                "status": 404
                            })
                            REDIS_FILTER_LABLE_DB.expire(QUERY_STRING, 172800)
                        except Exception as ex:
                            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
                        return JsonResponse(response, safe=False, status=404)

                is_ecommerce, remove_central, hide_recent_view, store_listing = validate_store_category(
                    store_category_id, ECOMMERCE_STORE_CATEGORY_ID
                )
                if is_ecommerce == True:
                    store_id = ""

                if store_id != "":
                    store_details = db.stores.find_one(
                        {"_id": ObjectId(store_id)}, {"currencySymbol": 1, "currencyCode": 1}
                    )
                    try:
                        currency = store_details["currencyCode"]
                    except:
                        pass
                    try:
                        currencySymbol = store_details["currencySymbol"]
                    except:
                        pass

                # gets all the required data for a particular filter type
                if int(filter_level) == 1:
                    attr_name = []
                    for i in res_filter_parameters["hits"]["hits"]:
                        best_supplier = {}
                        best_supplier["productId"] = i["_id"]
                        best_supplier["id"] = str(i["_source"]["storeId"])
                        if len(best_supplier) > 0:
                            child_product_query = {"status": 1}
                            if len(best_supplier) > 0:
                                child_product_id = best_supplier["productId"]
                            else:
                                child_product_id = i["_id"]
                            child_product_query["_id"] = ObjectId(child_product_id)
                            child_product_details = db.childProducts.find_one(
                                child_product_query,
                                {
                                    "units": 1, "categoryList": 1
                                }
                            )
                            if child_product_details != None:
                                if product_category_id != "":
                                    try:
                                        for ch in child_product_details["units"][0]["attributes"]:
                                            if "attrlist" in ch:
                                                for attr in ch["attrlist"]:
                                                    try:
                                                        attribute_count = 0
                                                        if type(attr["value"]) == list:
                                                            if len(attr["value"]) == 0:
                                                                pass
                                                            else:
                                                                for attr_value in attr["value"]:
                                                                    if language in attr_value:
                                                                        if attr_value[language] != "":
                                                                            attribute_count = attribute_count + 1
                                                                        else:
                                                                            pass
                                                                    else:
                                                                        pass
                                                        elif type(attr["value"]) == str:
                                                            if attr["value"] == "":
                                                                pass
                                                            else:
                                                                attribute_count = attribute_count + 1
                                                        elif type(attr["value"]) == dict:
                                                            if language in attr["value"]:
                                                                if attr["value"][language] != "":
                                                                    attribute_count = attribute_count + 1
                                                                else:
                                                                    pass
                                                            else:
                                                                pass
                                                        else:
                                                            pass
                                                        if attribute_count > 0 and attr['searchable'] == 1:
                                                            attr_name.append(attr["attrname"]["en"].upper())
                                                        else:
                                                            pass
                                                    except:
                                                        pass
                                    except:
                                        pass
                                else:
                                    pass

                                # ======================brand details====================================
                                try:
                                    brand_name = i["_source"]["brandTitle"][language]
                                except:
                                    brand_name = ""

                                # ======================manufacture details====================================
                                try:
                                    manufacture_name = i["_source"]["manufactureName"][language]
                                except:
                                    manufacture_name = ""
                                # ==============================currency details=========================

                                if brand_name == "":
                                    pass
                                else:
                                    brand_list.append(brand_name)

                                if manufacture_name == "":
                                    pass
                                else:
                                    manufacture_list.append(manufacture_name)

                                # ==================================size data====================================
                                try:
                                    if "unitSizeGroupValue" in child_product_details["units"][0]:
                                        if len(child_product_details["units"][0]["unitSizeGroupValue"]):
                                            if (
                                                "en"
                                                in child_product_details["units"][0][
                                                    "unitSizeGroupValue"
                                                ]
                                            ):
                                                if (
                                                    child_product_details["units"][0][
                                                        "unitSizeGroupValue"
                                                    ]["en"]
                                                    != ""
                                                ):
                                                    size_data.append(
                                                        child_product_details["units"][0][
                                                            "unitSizeGroupValue"
                                                        ]["en"]
                                                    )
                                                else:
                                                    pass
                                            else:
                                                pass
                                        else:
                                            pass
                                    else:
                                        pass
                                except:
                                    pass

                                # =========================================colour data==========================
                                try:
                                    if "colorName" in child_product_details["units"][0]:
                                        if child_product_details["units"][0]["colorName"]:
                                            if child_product_details["units"][0]["colorName"] != "":
                                                color_list.append(
                                                    child_product_details["units"][0]["colorName"]
                                                )
                                        else:
                                            pass
                                    else:
                                        pass
                                except:
                                    pass

                                # units_data.append(best_supplier['retailerPrice'])

                                if "symptoms" in i["_source"]:
                                    for sym in i["_source"]["symptoms"]:
                                        if sym["symptomName"] != "":
                                            sym_list.append(sym["symptomName"])
                                else:
                                    pass

                                if "categoryList" in child_product_details:
                                    for cat in child_product_details["categoryList"]:
                                        if cat["parentCategory"]["categoryName"]["en"] != "":
                                            category_list.append(
                                                cat["parentCategory"]["categoryName"]["en"]
                                            )
                                        if sname != "" or tname != "":
                                            for child_category in cat["parentCategory"][
                                                "childCategory"
                                            ]:
                                                category_list.append(
                                                    child_category["categoryName"]["en"]
                                                )

                                try:
                                    if "cannabisProductType" in child_product_details["units"][0]:
                                        if (
                                            child_product_details["units"][0]["cannabisProductType"]
                                            != ""
                                        ):
                                            cannabis_product_type = db.cannabisProductType.find_one(
                                                {
                                                    "_id": ObjectId(
                                                        child_product_details["units"][0][
                                                            "cannabisProductType"
                                                        ]
                                                    )
                                                }
                                            )
                                            if cannabis_product_type is not None:
                                                cannabis_product_type_list.append(
                                                    {
                                                        "name": cannabis_product_type["productType"][
                                                            language
                                                        ]
                                                        if language
                                                        in cannabis_product_type["productType"]
                                                        else cannabis_product_type["productType"]["en"],
                                                        "id": str(cannabis_product_type["_id"]),
                                                    }
                                                )
                                except:
                                    pass

                    #### price calculation
                    if "aggregations" in price_filter_parameters:
                        currency_rate = 0
                        # currencySymbol = "₹"
                        # currency = "INR"
                        max_price_value = price_filter_parameters['aggregations']['max_price']['value']
                        min_price_value = price_filter_parameters['aggregations']['min_price']['value']
                        try:
                            units_data.append(max_price_value)
                            units_data.append(min_price_value)
                        except:
                            pass
                    # predefined sorting for the meolaa filters
                    category_seq = 1
                    brand_seq = 2
                    price_seq = 3
                    discount_seq = 4
                    customer_review = 5
                    size_seq = 6
                    color_seq = 7
                    fabric_seq = 8
                    other_seq = 10

                    attribute_data = []
                    for attr in list(set(attr_name)):
                        try:
                            attribute_data.append(
                                {
                                    "name": attr,
                                    "data": [],
                                    "seqId": fabric_seq if "fabric" in attr.lower() else other_seq,
                                    "selType": 1,
                                    "selectType": 6,
                                    "filterType": 17,
                                }
                            )
                        except:
                            pass

                    filters_json = []
                    if len(brand_list) > 0: # and bname == "":
                        brands = {
                            "name": "BRAND",
                            "data": [],
                            "selType": 1,
                            "seqId": brand_seq,
                            "selectType": 1,
                            "filterType": 6,
                        }
                        filters_json.append(brands)
                    else:
                        pass

                    is_category = False #False made this changes because need to show category filter even after we are going through category

                    if len(category_list) > 0:
                        if is_category == False:
                            pass
                        else:
                            category = {
                                "name": "CATEGORY",
                                "data": [],
                                "seqId": category_seq,
                                "selType": 2,
                                "level": 1,
                                "selectType": 2,
                                "filterType": 7,
                            }
                            filters_json.append(category)
                    else:
                        pass

                    if len(color_list) > 0 and is_category is False:
                        colors_data = {
                            "name": "COLOUR",
                            "data": [],
                            "selType": 4,
                            "seqId": color_seq,
                            "selectType": 3,
                            "filterType": 8,
                        }
                        filters_json.append(colors_data)
                    else:
                        pass

                    try:
                        max_price = max(list(set(units_data)))
                        min_price = min(list(set(units_data)))
                    except:
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

                    filters_json.append({
                        "name": "DISCOUNT RANGE",
                        "data": [
                            { "value": "5_100", "name": "5% and above" },
                            { "value": "10_100", "name": "10% and above" },
                            { "value": "20_100", "name": "20% and above" },
                            { "value": "30_100", "name": "30% and above" },
                            { "value": "40_100", "name": "40% and above" },
                            { "value": "50_100", "name": "50% and above" },
                            { "value": "60_100", "name": "60% and above" },
                            { "value": "70_100", "name": "70% and above" },
                            { "value": "80_100", "name": "80% and above" },
                            { "value": "90_100", "name": "90% and above" },
                        ],
                        "seqId": discount_seq,
                        "selType": 1,
                        "selectType": 4,
                        "filterType": 9,
                    })
                    filters_json.append({
                        "name": "CUSTOMER RATINGS",
                        "data": [
                            { "value": "4_5", "name": "4 & Above" },
                            { "value": "3_5", "name": "3 & Above" },
                            { "value": "2_5", "name": "2 & Above" },
                            { "value": "1_5", "name": "1 & Above" }
                        ],
                        "seqId": customer_review,
                        "selType": 1,
                        "selectType": 4,
                        "filterType": 10,
                    })

                    p_data = {
                        "name": "PRICE",
                        "data": [
                            {
                                "maxPrice": float(round(max_price)),
                                "minPrice": float(round(min_price)),
                            }
                        ],
                        "selType": 3,
                        "seqId": price_seq,
                        "filterType": 11,
                        "selectType": 5,
                        "currency": currency,
                        "currencySymbol": currencySymbol,
                    }
                    thc_data = {
                        "name": "THC",
                        "data": [{"maxPrice": 100, "minPrice": 0}],
                        "selType": 3,
                        "seqId": 11,
                        "filterType": 12,
                        "selectType": 5,
                        "currency": "%",
                        "currencySymbol": "%",
                    }
                    type_data = {
                        "name": "TYPES",
                        "data": [],
                        "selType": 1,
                        "seqId": 11,
                        "selectType": 7,
                        "filterType": 13,
                    }
                    cbd_data = {
                        "name": "CBD",
                        "data": [{"maxPrice": 100, "minPrice": 0}],
                        "selType": 3,
                        "seqId": 11,
                        "filterType": 14,
                        "selectType": 5,
                        "currency": "%",
                        "currencySymbol": "%",
                    }
                    if len(size_data) > 0 and is_category is False:
                        sizes_data = {
                            "name": "SIZE",
                            "data": [],
                            "seqId": size_seq,
                            "selType": 1,
                            "selectType": 4,
                            "filterType": 15,
                        }
                        filters_json.append(sizes_data)
                    else:
                        pass

                    # if store_category_id != MEAT_STORE_CATEGORY_ID:
                    filters_json.append(p_data)
                    if store_category_id == CANNABIS_STORE_CATEGORY_ID:
                        filters_json.append(cbd_data)
                        filters_json.append(thc_data)
                        if len(cannabis_product_type_list) > 0:
                            filters_json.append(type_data)
                    for atb in attribute_data:
                        filters_json.append(atb)
                    new_list = sorted(filters_json, key=lambda k: k["seqId"], reverse=False)

                    Final_output = {
                        "data": {
                            "filters": new_list,
                            "currency": currency,
                            "currencySymbol": currencySymbol,
                            "unitId": str(ObjectId()),
                            "message": "Filters Found",
                        }
                    }
                    try:
                        REDIS_FILTER_LABLE_DB.jsonset(rjId, Path.rootPath(), {
                            "data": Final_output['data'],
                            "status": 200
                        })
                    except Exception as ex:
                        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
                    return JsonResponse(Final_output, safe=False, status=200)

                elif int(filter_level) == 2:
                    # currencySymbol = "$"
                    category_query = {"_id": ObjectId(fname), "status": 1}
                    category_details = db.category.find_one(category_query)
                    if category_details != None:
                        child_category_query = {
                            "parentId": ObjectId(category_details["_id"]),
                            "status": 1,
                        }
                        child_category_details = db.category.find(child_category_query)
                        for child_cat in child_category_details:
                            child_data_count = db.category.find(
                                {"parentId": ObjectId(child_cat["_id"]), "status": 1}
                            ).count()
                            sub_categpry_list.append(child_cat["categoryName"]["en"])

                    subcategory = {
                        "name": "subCategories",
                        "data": list(set(sub_categpry_list)),
                        "selType": 2,
                        "level": 2,
                        "filterType": 16,
                    }
                    filters_json = [subcategory]

                    Final_output = {
                        "data": {
                            "filters": filters_json,
                            # "currency": currency,
                            "currencySymbol": currencySymbol,
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


class ProductFilterValueNew(APIView):
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
        3 for subsubcategory
    #! 1 for brand
    #! 2 for category
    #! 3 for colour
    #! 4 for sizes
    #! 5 for price
    #! 6 for attributes
    #! 7 for types (cannibies only)
    """

    @swagger_auto_schema(
        method="get",
        tags=["Search & Filter"],
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
                name="storeCategoryId",
                default=ECOMMERCE_STORE_CATEGORY_ID,
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
            ),
            openapi.Parameter(
                name="search",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for search the value inside filter. brand search, category search, attributes search",
            ),
            openapi.Parameter(
                name="page",
                default="1",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="page number. which page number data want to display",
            ),
            openapi.Parameter(
                name="type",
                default="1",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="type need to send that which type data need to get"
                "type value should be:"
                "1 for brand"
                ", 2 for category"
                ", 3 for colour"
                ", 4 for sizes"
                ", 5 for price"
                ", 6 for attributes"
                ", 7 for types (cannibies only)",
            ),
            openapi.Parameter(
                name="q",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="for the search the item in search bar ex. ni, nik, addi",
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
                name="bname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="brand name for the product which want to display. ex. Nike, H&M",
            ),
            openapi.Parameter(
                name="symptoms",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="symptom name for the product which want to display. ex. Nike, H&M",
            ),
            openapi.Parameter(
                name="o_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to displaying particular offers product. ex.5df89d3edd77d6ca2752bd10",
            ),openapi.Parameter(
                name="categoryId",
                default="623c0f7df463e41f821bc576",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="from which category we need to fetch the subcategory.",
            ),openapi.Parameter(
                name="mainCategoryId",
                default="623c0f7df463e41f821bc576",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="from which category we need to fetch the products.",
            ),openapi.Parameter(
                name="bid",
                default="623c0f7df463e41f821bc576",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="from which brand we need to fetch the products.",
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
                name="attribute",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while applying filter on attribute it will creating dynamic link. ex. facet_OCCASION, facet_IDEAL FOR",
            ),
            openapi.Parameter(
                name="justId",
                default="0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while clicking on view more for just in from home page",
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
                name="isGrpBrand",
                default="0",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while need to get brands grouping buy alphabets.."
                "if need data grp by that time value shold be 1 else 0",
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
            token = request.META["HTTP_AUTHORIZATION"]
            query = []
            should_query = []
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)

            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            # ========================================query parameter====================================================
            # for the search the item in search bar
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            filter_type = int(request.GET.get("type", "0"))
            if filter_type == 0:
                response_data = {
                    "message": "filter type is missing",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=404)
            else:
                page = int(request.GET.get("page", 1))  # for the pagination
                fname = request.GET.get("fname", "")  # category-name
                search_text = request.GET.get("search", "")  # category-name
                search_query = request.GET.get("q", "")  # category-name
                fname = fname.replace("%20", " ")
                fname = fname.replace("+", " ")

                sname = request.GET.get("sname", "")  # sub-category-name
                sname = sname.replace("%20", " ")
                sname = sname.replace("+", " ")

                tname = request.GET.get("tname", "")  # sub-sub-category-name
                tname = tname.replace("%20", " ")
                tname = tname.replace("+", " ")
                bname = request.GET.get("bname", "")  # brand name for the search
                just_id = request.GET.get("justId", "")  # brand name for the search
                b_id = request.GET.get("bid", "")  # brand id for the search
                parent_category_id = request.GET.get("mainCategoryId", "")  # category id for the search
                symptoms_name = request.GET.get("symptoms", "")  # brand name for the search
                best_deals = request.GET.get("best_deals", "")
                category_id = request.GET.get("categoryId", "") # product category id, for return the sub sub category
                offer_id = request.GET.get("o_id", "")
                offer_id = request.GET.get("o_id", "")  # get particular offer data
                if offer_id != "":
                    offer_id = offer_id.split(",")
                store_id = request.GET.get("s_id", "")
                zone_id = request.GET.get("z_id", "")
                attribute = request.GET.get("attribute", "")
                is_grp_brand = request.GET.get("isGrpBrand", "0")
                integration_type = int(request.GET.get("integrationType", 0))
                attr_value = request.GET.get("attr", "")
                attribute = attribute.replace("%20", " ")
                attribute = attribute.replace("+", "")
                from_data = int(page * 300) - 300
                to_data = 300 + from_data
                if filter_type == 6 and attribute == "":
                    response = {"message": "attribute value is mising"}
                    return JsonResponse(response, safe=False, status=422)
                else:
                    if category_id == "":
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
                        storelisting = False
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

                        try:
                            if hyperlocal == True and storelisting == False:
                                zone_details = zone_find({"_id": ObjectId(zone_id)})
                                store_query = {
                                    "categoryId": str(store_category_id),
                                    "cityId": zone_details["city_ID"],
                                }
                                store_data = db.stores.find(store_query, {"storeName": 1})
                                if store_category_id != MEAT_STORE_CATEGORY_ID:
                                    store_data_details = ["0"]
                                else:
                                    store_data_details = []
                                if store_data.count() > 0:
                                    for store in store_data:
                                        store_data_details.append(str(store["_id"]))
                                    query.append({"terms": {"storeId": store_data_details}})
                            elif hyperlocal == True and storelisting == True:
                                if store_id != "":
                                    query.append({"match": {"storeId": store_id}})
                            else:
                                if store_id != "":
                                    query.append({"match": {"storeId": store_id}})
                        except:
                            pass
                        query.append({"match": {"storeCategoryId": store_category_id}})
                        # if store_category_id == ECOMMERCE_STORE_CATEGORY_ID and filter_type != 4:
                        # query.append({"match": {"units.isPrimary": True}})
                        # elif store_id != "" and store_category_id != PHARMACY_STORE_CATEGORY_ID:
                        #     query.append({"match": {"units.isPrimary": True}})
                        # else:
                        #     pass

                        if just_id != "":
                            home_page_just_in_data = db.ecomHomePage.find_one(
                                {"_id": ObjectId(just_id)}, {"entity": 1}
                            )
                            product_data = []
                            if home_page_just_in_data is not None:
                                for entity in home_page_just_in_data["entity"]:
                                    product_details = db.childProducts.find_one(
                                        {"_id": ObjectId(entity["parentProductId"])}, {"units": 1}
                                    )
                                    if product_details is not None:
                                        product_data.append(str(product_details["parentProductId"]))
                            else:
                                pass
                            if len(product_data) > 0:
                                query.append({"terms": {"parentProductId": product_data}})
                            else:
                                pass
                        else:
                            pass

                        ### need to search base on category id
                        if parent_category_id != "":
                            categoryid = parent_category_id.replace("%2C", ",")
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "categoryList.parentCategory.categoryId": categoryid
                                    }
                                }
                            )
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "categoryList.parentCategory.childCategory.categoryId": categoryid
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

                        if offer_id != "":
                            query.append({"terms": {"offer.offerId": offer_id}})

                        if best_deals != "":
                            query.append({"match": {"offer.status": 1}})

                        if attr_value != "":
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "units.attributes.attrlist.value.en": attr_value.replace(
                                            "%20", ""
                                        )
                                    }
                                }
                            )
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "units.attributes.attrlist.value.name.en": attr_value.replace(
                                            "%20", ""
                                        )
                                    }
                                }
                            )

                        if search_query != "":
                            should_query.append(
                                {
                                    "span_first": {
                                        "match": {
                                            "span_term": {
                                                "pName.en": search_query.replace("%20", " ")
                                            }
                                        },
                                        "end": 3
                                    }
                                }
                            )
                            should_query.append(
                                {
                                    "match": {
                                        "pName.en": {
                                            "analyzer": "standard",
                                            "query": search_query.replace("%20", " "),
                                            "boost": 5
                                        }
                                    }
                                }
                            )
                            should_query.append(
                                {
                                    "match": {
                                        "pName.en": {
                                            "analyzer": "edgengram_analyzer",
                                            "query": search_query.replace("%20", " "),
                                            "boost": 5
                                        }
                                    }
                                }
                            )
                            # ===========================unit name========================================
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "units.unitName.en": {
                                            "analyzer": "whitespace",
                                            "query": search_query.replace("%20", " "),
                                            "boost": 4,
                                        }
                                    }
                                }
                            )
                            # ====================================child category=======================================
                            should_query.append(
                                {
                                    "multi_match": {
                                        "analyzer": "standard",
                                        "query": search_query.replace("%20", " "),
                                        "fields": [
                                            "categoryList.parentCategory.categoryName." + language,
                                            "categoryList.parentCategory.childCategory.categoryName." + language
                                        ],
                                        "boost": 3
                                    }
                                }
                            )
                            # ======================================brand name=======================================
                            should_query.append(
                                {
                                    "match": {
                                        "brandTitle." + language:
                                            {
                                                "analyzer": "standard",
                                                "query": search_query.replace("%20", " "),
                                                "boost": 2
                                            }
                                    }
                                }
                            )
                            # ===========================================detail description============================
                            should_query.append(
                                {
                                    "match_phrase_prefix": {
                                        "detailDescription." + language: {
                                            "analyzer": "whitespace",
                                            "query": search_query.replace("%20", " "),
                                            "boost": 1
                                        }
                                    }
                                }
                            )

                        if symptoms_name != "":
                            query.append(
                                {
                                    "match_phrase_prefix": {
                                        "symptoms.symptomName": symptoms_name.replace("%20", " ")
                                    }
                                }
                            )

                        if bname != "":
                            query.append(
                                {"match_phrase_prefix": {"brandTitle.en": bname.replace("%20", " ")}}
                            )

                        query.append(
                            {
                                "match": {"units.isPrimary": True}
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
                        if search_text != "":
                            if filter_type == 1:
                                query.append({"match_phrase_prefix": {"brandTitle.en": search_text}})
                            elif filter_type == 2:
                                query.append(
                                    {
                                        "match_phrase_prefix": {
                                            "categoryList.parentCategory.categoryName.en": search_text
                                        }
                                    }
                                )
                            elif filter_type == 3:
                                query.append({"match_phrase_prefix": {"units.colorName": search_text}})
                            elif filter_type == 4:
                                query.append(
                                    {
                                        "match_phrase_prefix": {
                                            "units.unitSizeGroupValue.en": search_text
                                        }
                                    }
                                )
                            elif filter_type == 6 and store_category_id != CANNABIS_STORE_CATEGORY_ID:
                                query.append(
                                    {
                                        "match_phrase_prefix": {
                                            "units.attributes.attrlist.value.en": search_text
                                        }
                                    }
                                )
                            else:
                                pass
                        else:
                            pass
                        sort_query = []
                        must_not_query = []
                        # ===================check for integration type=========================
                        if int(integration_type) == 0:
                            pass
                        elif int(integration_type) == 1:
                            must_not_query.append({"match": {"magentoId": -1}})
                            query.append({"exists": {"field": "magentoId"}})
                        elif int(integration_type) == 2:
                            must_not_query.append(
                                {"term": {"shopify_variant_id.keyword": {"value": ""}}}
                            )
                            query.append({"exists": {"field": "shopify_variant_id"}})
                        elif int(integration_type) == 3:
                            query.append({"match": {"magentoId": -1}})
                            query.append({"term": {"shopify_variant_id.keyword": ""}})
                        source = ["brandTitle.en", "brand"]
                        if filter_type == 1:
                            source = ["brandTitle.en", "brand"]
                            field_query = "brandTitle.en.keyword"
                            sort_query.append({"brandTitle.en.keyword": {"order": "asc"}})
                            must_not_query.append({"terms": {"brandTitle.en.keyword": [""]}})
                        elif filter_type == 2:
                            # field_query = "categoryList.parentCategory.categoryName.en.keyword"
                            source = ["categoryList"]
                            field_query = "categoryList.parentCategory.categoryId.keyword"
                            sub_field_query = (
                                "categoryList.parentCategory.childCategory.categoryId.keyword"
                            )
                            sort_query.append(
                                {
                                    "categoryList.parentCategory.categoryName.en.keyword": {
                                        "order": "asc"
                                    }
                                }
                            )
                            must_not_query.append(
                                {"terms": {"categoryList.parentCategory.categoryName.en.keyword": [""]}}
                            )
                        elif filter_type == 3:
                            source = ["units.colorName"]
                            field_query = "units.colorName.keyword"
                            sort_query.append({"units.colorName.keyword": {"order": "asc"}})
                            must_not_query.append({"terms": {"units.colorName.keyword": [""]}})
                        elif filter_type == 4:
                            source = ["units.unitSizeGroupValue.en"]
                            field_query = "units.unitSizeGroupValue.en.keyword"
                            sort_query.append({"units.unitSizeGroupValue.en.keyword": {"order": "asc"}})
                            must_not_query.append(
                                {"terms": {"units.unitSizeGroupValue.en.keyword": [""]}}
                            )
                        elif filter_type == 5:
                            source = ["units"]
                            sort_query.append({"units.discountPrice": {"order": "asc"}})
                        elif filter_type == 6 and store_category_id != CANNABIS_STORE_CATEGORY_ID:
                            source = ["units"]
                            field_query = "units.attributes.attrlist.attrname.en"
                            sort_query.append(
                                {"units.attributes.attrlist.value.en.keyword": {"order": "asc"}}
                            )
                            must_not_query.append(
                                {"terms": {"units.attributes.attrlist.attrname.en": [""]}}
                            )
                        elif filter_type == 6 and store_category_id == CANNABIS_STORE_CATEGORY_ID:
                            source = ["units"]
                            field_query = "units.attributes.attrlist.attrname.en"
                            sort_query.append(
                                {"units.attributes.attrlist.attrname.en.keyword": {"order": "asc"}}
                            )
                            must_not_query.append(
                                {"terms": {"units.attributes.attrlist.attrname.en": [""]}}
                            )
                        elif filter_type == 7:
                            source = ["units"]
                            field_query = "units.cannabisProductType.keyword"
                            sort_query.append({"units.cannabisProductType.keyword": {"order": "asc"}})
                            must_not_query.append({"terms": {"units.cannabisProductType": [""]}})
                        if hyperlocal == True and storelisting == False:
                            if len(should_query) > 0:
                                product_search_query = {
                                    "bool": {
                                        "should": should_query,
                                        "minimum_should_match": 1,
                                        "boost": 1.0,
                                        "must": query,
                                        "must_not": must_not_query,
                                    }
                                }
                            else:
                                product_search_query = {
                                    "bool": {"must": query, "must_not": must_not_query}
                                }
                        else:
                            if just_id != "":
                                pass
                            else:
                                must_not_query.append({"match": {"storeId": "0"}})
                            if len(should_query) > 0:
                                product_search_query = {
                                    "bool": {
                                        "should": should_query,
                                        "minimum_should_match": 1,
                                        "boost": 1.0,
                                        "must": query,
                                        "must_not": must_not_query,
                                    }
                                }
                            else:
                                product_search_query = {
                                    "bool": {"must": query, "must_not": must_not_query}
                                }
                        if filter_type not in [2, 5, 6, 4]:
                            search_item_query = {
                                "size": 2000,
                                "_source": source,
                                "query": product_search_query,
                                "track_total_hits": True,
                                "sort": sort_query,
                            }
                        elif filter_type == 2:
                            search_item_query = {
                                "query": product_search_query,
                                "track_total_hits": True,
                                "sort": sort_query,
                                "aggs": {
                                    "group_by_sub_category": {
                                        "terms": {"field": field_query, "size": 3000},
                                        "aggs": {
                                            "top_hits": {
                                                "terms": {"field": sub_field_query, "size": 100},
                                                "aggs": {
                                                    "top_sub_category_hits": {
                                                        "top_hits": {
                                                            "_source": {"includes": ["brandTitle"]},
                                                            "size": 1,
                                                        }
                                                    }
                                                },
                                            }
                                        },
                                    }
                                },
                            }
                        elif filter_type == 6 or filter_type == 4:
                            search_item_query = {
                                "query": product_search_query,
                                "track_total_hits": True,
                                "sort": sort_query,
                                "size": 300,
                            }
                        else:
                            search_item_query = {
                                "query": product_search_query,
                                "track_total_hits": True,
                                "sort": sort_query,
                                "aggs": {
                                    "max_price": {"max": {"field": "units.discountPrice"}},
                                    "min_price": {"min": {"field": "units.discountPrice"}},
                                },
                            }
                        if filter_type == 4:
                            res = es.search(
                                index=index_products,
                                body=search_item_query,
                                filter_path=[
                                    "hits.total",
                                    "hits.hits._id",
                                    "hits.hits._source.units.unitSizeGroupValue",
                                ],
                            )
                        elif filter_type != 6:
                            res = es.search(index=index_products, body=search_item_query)
                        else:
                            res = es.search(
                                index=index_products,
                                body=search_item_query,
                                filter_path=[
                                    "hits.total",
                                    "hits.hits._id",
                                    "hits.hits._source.units.attributes.attrlist.attrname",
                                    "hits.hits._source.units.attributes.attrlist.attributeId",
                                    "hits.hits._source.units.attributes.attrlist.measurementUnitName",
                                    "hits.hits._source.units.attributes.attrlist.value",
                                ],
                            )
                        print("search_item_query", search_item_query)
                        json_data = []
                        total_count = 0
                        min_price = 0
                        max_price = 0
                        colour_details = db.colors.find_one({})
                        colour_json = {}
                        if colour_details is not None:
                            for color in colour_details["color"]:
                                colour_json[color["name"]] = (
                                    "rgb("
                                    + str(color["R"])
                                    + ","
                                    + str(color["G"])
                                    + ","
                                    + str(color["B"])
                                    + ")"
                                )
                        sub_category_json = []
                        if filter_type == 2:
                            if "aggregations" in res:
                                if "group_by_sub_category" in res["aggregations"]:
                                    if "buckets" in res["aggregations"]["group_by_sub_category"]:
                                        total_count = len(res["aggregations"]["group_by_sub_category"]["buckets"])
                                        for bucket in res["aggregations"]["group_by_sub_category"]["buckets"][from_data:to_data]:
                                            product_categoty_details = db.category.find_one({"_id": ObjectId(bucket["key"])}, {"categoryName": 1})
                                            if product_categoty_details is not None:
                                                if fname in str(product_categoty_details["_id"]): # == fname.lower():
                                                    json_data.append(
                                                        {
                                                            "name": product_categoty_details["categoryName"][language].lower(),
                                                            "id": bucket["key"].lower(),
                                                            "penCount": bucket["doc_count"],
                                                        }
                                                    )
                                                    main_category_data = db.category.find(
                                                        {
                                                            "parentId": ObjectId(bucket["key"]),
                                                            "status": 1,
                                                            "storeId": "0"
                                                        }
                                                    )
                                                    for inner_bucket in main_category_data:
                                                        if sname != "": ### this change we made because in filter we need to show only those category which we clicked on for subcategory
                                                            if str(inner_bucket["_id"]) == sname:
                                                                sub_category_json.append(
                                                                    {
                                                                        "parentId": bucket["key"],
                                                                        "childId": str(inner_bucket["_id"]),
                                                                        "count": bucket["doc_count"],
                                                                    }
                                                                )
                                                            else:
                                                                pass
                                                        else:
                                                            sub_category_json.append(
                                                                {
                                                                    "parentId": bucket["key"],
                                                                    "childId": str(inner_bucket["_id"]),
                                                                    "count": bucket["doc_count"],
                                                                }
                                                            )
                        elif filter_type == 7:
                            if "aggregations" in res:
                                if "group_by_sub_category" in res["aggregations"]:
                                    if "buckets" in res["aggregations"]["group_by_sub_category"]:
                                        total_count = len(
                                            res["aggregations"]["group_by_sub_category"]["buckets"]
                                        )
                                        for bucket in res["aggregations"]["group_by_sub_category"][
                                            "buckets"
                                        ][from_data:to_data]:
                                            if bucket["key"] != "":
                                                cannabis_product_type = db.cannabisProductType.find_one(
                                                    {"_id": ObjectId(bucket["key"])}, {"productType": 1}
                                                )
                                                if cannabis_product_type is not None:
                                                    name = (
                                                        cannabis_product_type["productType"][language]
                                                        if language
                                                        in cannabis_product_type["productType"]
                                                        else cannabis_product_type["productType"]["en"]
                                                    )
                                                    json_data.append(
                                                        {
                                                            "name": name.lower(),
                                                            "id": str(cannabis_product_type["_id"]),
                                                            "penCount": bucket["doc_count"],
                                                        }
                                                    )
                        elif filter_type == 4:
                            if "hits" in res:
                                if "hits" in res["hits"]:
                                    attribute_data = []
                                    for bucket in res["hits"]["hits"]:  # [from_data:to_data]:
                                        if "_source" in bucket:
                                            if (
                                                "en"
                                                in bucket["_source"]["units"][0]["unitSizeGroupValue"]
                                            ):
                                                if (
                                                    bucket["_source"]["units"][0]["unitSizeGroupValue"][
                                                        "en"
                                                    ]
                                                    != ""
                                                ):
                                                    attribute_data.append(
                                                        {
                                                            "name": bucket["_source"]["units"][0][
                                                                "unitSizeGroupValue"
                                                            ]["en"].strip()
                                                        }
                                                    )
                                                else:
                                                    pass
                                            else:
                                                pass
                                        else:
                                            pass
                                    if len(attribute_data) > 0:
                                        attribute_dataframe = pd.DataFrame(attribute_data)
                                        attribute_dataframe["penCount"] = attribute_dataframe.groupby(
                                            "name"
                                        )["name"].transform("count")
                                        attribute_dataframe = attribute_dataframe.drop_duplicates(
                                            "name", keep="last"
                                        )
                                        attribute_list = attribute_dataframe.to_json(orient="records")
                                        json_data = json.loads(attribute_list)
                                        total_count = len(json_data)
                                    else:
                                        pass
                                else:
                                    pass
                            else:
                                pass
                        elif filter_type == 3:
                            if "hits" in res:
                                if "hits" in res["hits"]:
                                    try:
                                        total_count = res["hits"]["total"]["value"]
                                    except:
                                        total_count = 0
                                    for bucket in res["hits"]['hits'][from_data:to_data]:
                                        colour_rgb = ""
                                        if bucket["_source"]['units'][0]['colorName'] in colour_json:
                                            colour_rgb = colour_json[bucket["_source"]['units'][0]['colorName']]
                                        if colour_rgb == "":
                                            try:
                                                colour_rgb = colour_json[(bucket["_source"]['units'][0]['colorName']).upper()]
                                            except:
                                                colour_rgb = ""
                                        if colour_rgb == "":
                                            try:
                                                colour_rgb = colour_json[(bucket["_source"]['units'][0]['colorName']).title()]
                                            except:
                                                pass

                                        json_data.append(
                                            {
                                                "rgb": colour_rgb,
                                                "name": bucket["_source"]['units'][0]['colorName'].lower(),
                                                "penCount": 0,
                                            }
                                        )
                        elif filter_type == 1:
                            if "hits" in res:
                                if "hits" in res["hits"]:
                                    for brand in res["hits"]['hits']:
                                        json_data.append(
                                            {
                                                "name": brand["_source"]['brandTitle']['en'].lower(),
                                                "_id": brand["_source"]['brand']
                                            }
                                        )
                            if len(json_data) > 0:
                                dataframe = pd.DataFrame(json_data)
                                dataframe.columns = ['name', "_id"]
                                dataframe['penCount'] = dataframe.groupby('name')['name'].transform('count')
                                dataframe = dataframe.drop_duplicates("name", keep="last")
                                json_data = dataframe.to_json(orient='records')
                                json_data = json.loads(json_data)
                                # dataframe = dataframe.groupby('name').sum()

                        elif filter_type not in [5, 6]:
                            if int(is_grp_brand) == 0:
                                if "aggregations" in res:
                                    if "group_by_sub_category" in res["aggregations"]:
                                        if "buckets" in res["aggregations"]["group_by_sub_category"]:
                                            total_count = len(
                                                res["aggregations"]["group_by_sub_category"]["buckets"]
                                            )
                                            for bucket in res["aggregations"]["group_by_sub_category"][
                                                "buckets"
                                            ][from_data:to_data]:
                                                json_data.append(
                                                    {
                                                        "name": bucket["key"].lower(),
                                                        "penCount": bucket["doc_count"],
                                                    }
                                                )
                            else:
                                json_data_new = []
                                if "aggregations" in res:
                                    if "group_by_sub_category" in res["aggregations"]:
                                        if "buckets" in res["aggregations"]["group_by_sub_category"]:
                                            total_count = len(
                                                res["aggregations"]["group_by_sub_category"]["buckets"]
                                            )
                                            for bucket in res["aggregations"]["group_by_sub_category"][
                                                "buckets"
                                            ][from_data:to_data]:
                                                json_data_new.append(
                                                    {
                                                        "name": bucket["key"].lower(),
                                                        "char": bucket["key"][0].upper(),
                                                    }
                                                )
                                        else:
                                            pass
                                    else:
                                        pass
                                else:
                                    pass

                                if len(json_data_new) > 0:
                                    values_by_char = {}
                                    for d in json_data_new:
                                        values_by_char.setdefault(d["char"].upper(), []).append(
                                            d["name"].title()
                                        )
                                    for values in values_by_char:
                                        json_data.append(
                                            {
                                                "name": list(set(values_by_char[values])),
                                                "char": values,
                                                "penCount": 0,
                                            }
                                        )
                                else:
                                    pass
                        
                        elif filter_type == 6 and store_category_id != CANNABIS_STORE_CATEGORY_ID:
                            print('res--',res)
                            attribute_data = []
                            if "hits" in res:
                                if "hits" in res["hits"]:
                                    for bucket in res["hits"]["hits"]:  # [from_data:to_data]:
                                        if "_source" in bucket:
                                            try:
                                                for attr in bucket["_source"]["units"][0]["attributes"]:
                                                    for attr_list in attr["attrlist"]:
                                                        if "value" in attr_list:
                                                            if "en" in attr_list["value"]:
                                                                attr_name = attr_list["attrname"]["en"].strip()
                                                                if attr_name.lower() == attribute.lower():
                                                                    measurement_unit = attr_list["measurementUnitName"] if "measurementUnitName" in attr_list else ""
                                                                    if "en" in attr_list["value"]:
                                                                        if type(attr_list["value"]["en"]) != list:
                                                                            if attr_list["value"]["en"] != "":
                                                                                value = (
                                                                                    str(
                                                                                        attr_list[
                                                                                            "value"
                                                                                        ]["en"]
                                                                                    ).lower()
                                                                                    + measurement_unit
                                                                                )
                                                                                attribute_data.append(
                                                                                    {
                                                                                        "name": value,
                                                                                    }
                                                                                )
                                                                            else:
                                                                                pass
                                                                        else:
                                                                            for (
                                                                                inner_value
                                                                            ) in attr_list["value"][
                                                                                "en"
                                                                            ]:
                                                                                value = str(
                                                                                    str(
                                                                                        inner_value
                                                                                    ).lower()
                                                                                    + measurement_unit
                                                                                )
                                                                                if value != "":
                                                                                    attribute_data.append(
                                                                                        {
                                                                                            "name": value,
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
                                            except:
                                                pass
                                        else:
                                            pass

                                    if len(attribute_data) > 0:
                                        attribute_dataframe = pd.DataFrame(attribute_data)
                                        attribute_dataframe["penCount"] = attribute_dataframe.groupby(
                                            "name"
                                        )["name"].transform("count")
                                        attribute_dataframe = attribute_dataframe.drop_duplicates(
                                            "name", keep="last"
                                        )
                                        attribute_list = attribute_dataframe.to_json(orient="records")
                                        json_data = json.loads(attribute_list)
                                        total_count = len(json_data)
                                    else:
                                        pass
                                else:
                                    pass
                            else:
                                pass
                        else:
                            print('res--222',res)
                            if "aggregations" in res:
                                if "max_price" in res["aggregations"]:
                                    if "value" in res["aggregations"]["max_price"]:
                                        max_price = res["aggregations"]["max_price"]["value"]
                                    else:
                                        pass
                                else:
                                    pass

                                if "min_price" in res["aggregations"]:
                                    if "value" in res["aggregations"]["min_price"]:
                                        min_price = res["aggregations"]["min_price"]["value"]
                                    else:
                                        pass
                                else:
                                    pass


                        if min_price is not None and max_price is not None:
                            if min_price >= 0 and max_price != 0:
                                try:
                                    currency_rate = currency_exchange_rate[
                                        str("INR") + "_" + str(currency_code)
                                    ]
                                except:
                                    currency_rate = 0
                                currency_details = db.currencies.find_one({"currencyCode": currency_code}, {"currencySymbol": 1, "currencyCode": 1})
                                if currency_details is not None:
                                    currencySymbol = currency_details["currencySymbol"]
                                    currency = currency_details["currencyCode"]
                                else:
                                    currencySymbol = "INR"
                                    currency = res["hits"]["hits"][0]["_source"]["currency"]
                                if float(currency_rate) > 0:
                                    min_price_new = min_price * float(currency_rate)
                                    max_price_new = max_price * float(currency_rate)
                                else:
                                    min_price_new = min_price
                                    max_price_new = max_price
                            else:
                                min_price_new = 0
                                max_price_new = 100000
                        else:
                            try:
                                currencySymbol = res["hits"]["hits"][0]["_source"]["currencySymbol"]
                                currency = res["hits"]["hits"][0]["_source"]["currency"]
                            except:
                                currencySymbol = "₹"
                                currency = "INR"
                            min_price_new = 0
                            max_price_new = 100000

                        if len(json_data) > 0 and filter_type != 5:
                            if len(json_data) > 0 and int(is_grp_brand) == 0:
                                dataframe = pd.DataFrame(json_data)
                                dataframe = dataframe.drop_duplicates("name", keep="last")
                                colors_json = dataframe.to_json(orient="records")
                                colors_json = json.loads(colors_json)
                                category_json = sorted(colors_json, key=lambda k: k["name"])
                                new_colour_list = []
                                if filter_type == 2:
                                    # ! for loop on main category list to get the child catgeory list for the category
                                    for new_cat in category_json:
                                        # ! check if category is available in list of the sub categories
                                        if not any(
                                            d["parentId"] == new_cat["id"] for d in sub_category_json
                                        ):
                                            pass
                                        else:
                                            sub_category_data = []
                                            for sub_cat in sub_category_json:
                                                if sub_cat["parentId"] == new_cat["id"]:
                                                    child_category = db.category.find(
                                                        {
                                                            "parentId": ObjectId(sub_cat["parentId"]),
                                                            "status": 1,
                                                            "_id": ObjectId(sub_cat["childId"]),
                                                        }
                                                    )
                                                    if child_category.count() > 0:
                                                        for child in child_category:
                                                            child_child_category = db.category.find(
                                                                {
                                                                    "parentId": ObjectId(child["_id"]),
                                                                    "status": 1,
                                                                }
                                                            ).count()
                                                            sub_category_data.append(
                                                                {
                                                                    "name": child["categoryName"][
                                                                        language
                                                                    ],
                                                                    "id": str(child["_id"]),
                                                                    "childCount": child_child_category,
                                                                    "penCount": sub_cat["count"],
                                                                }
                                                            )
                                                    else:
                                                        pass
                                                else:
                                                    pass
                                            if len(sub_category_data) > 0:
                                                new_colour_list.append(
                                                    {
                                                        "name": new_cat["name"],
                                                        "id": new_cat["id"],
                                                        "categoryData": sub_category_data,
                                                        "penCount": new_cat["penCount"],
                                                    }
                                                )
                                            else:
                                                pass
                                else:
                                    new_colour_list = category_json
                            elif int(is_grp_brand) == 1:
                                category_json = sorted(json_data, key=lambda k: k["char"])
                                new_colour_list = category_json
                            else:
                                new_colour_list = []
                            data = {
                                "message": "data found",
                                "data": new_colour_list,
                                "penCount": total_count,
                            }
                            return JsonResponse(data, safe=False, status=200)
                        elif filter_type == 5:
                            price_data = []
                            price_data.append(
                                {
                                    "maxPrice": round(max_price_new, 2),
                                    "minPrice": round(min_price_new, 2),
                                }
                            )
                            data = {
                                "message": "data found",
                                "data": price_data,
                                "maxPrice": round(max_price_new, 2),
                                "minPrice": round(min_price_new, 2),
                                "currencySymbol": currencySymbol,
                                "currency": currency,
                                "penCount": total_count,
                            }
                            return JsonResponse(data, safe=False, status=200)
                        else:
                            data = {"message": "data not found", "data": [], "penCount": 0}
                            return JsonResponse(data, safe=False, status=404)
                    else:
                        product_category_query = {
                            "parentId": ObjectId(category_id),
                            "status": 1
                        }
                        product_category_details = db.category.find(product_category_query, {"categoryName": 1})
                        category_data = []
                        for pro_cat in product_category_details:
                            doc_count = db.category.find(
                                {
                                    "parentId": ObjectId(pro_cat['_id']),
                                    "status": 1
                                }
                            ).count()
                            category_data.append(
                                {
                                    "childCount": doc_count,
                                    "id": str(pro_cat['_id']),
                                    "name": pro_cat['categoryName']["en"],
                                    "penCount": doc_count
                                }
                            )
                        if len(category_data) > 0:
                            data = {
                                "message": "data found",
                                "data": category_data
                            }
                            return JsonResponse(data, safe=False, status=200)
                        else:
                            data = {
                                "message": "data not found",
                                "data": []
                            }
                            return JsonResponse(data, safe=False, status=404)

        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


class ProductSuggestionsNew(APIView):
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
                name="zoneId", in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=False
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
                name="searchItem",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="searching the products (user search the item. example ni, nik, nike..)",
                default="mango",
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
                name="storeCategoryId",
                default="5df8766e8798dc2f236c95fa",
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
            start_time = time.time()
            page = int(request.GET.get("page", 1))  # for the pagination
            should_query = []
            to_data = 30  # page*30
            from_data = int(page * 30) - 30
            token = request.META["HTTP_AUTHORIZATION"]
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)

            searchItem = request.GET["searchItem"] if "searchItem" in request.GET else ""
            searchItem = searchItem.replace("%20", " ")
            if searchItem == "":
                response_data = {
                    "message": "Invalid Request",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=400)
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"

            search_type = request.META["HTTP_TYPE"] if "HTTP_TYPE" in request.META else "2"
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            filter_query = []
            brand_data_query = []
            fname = request.GET.get("fname", "")  # category-name
            fname = fname.replace("%20", " ")
            fname = fname.replace("+", " ")

            sname = request.GET.get("sname", "")  # sub-category-name
            sname = sname.replace("%20", " ")
            sname = sname.replace("+", " ")

            tname = request.GET.get("tname", "")  # sub-sub-category-name
            tname = tname.replace("%20", " ")
            tname = tname.replace("+", " ")

            zone_id = str(request.META["HTTP_ZONEID"]) if "HTTP_ZONEID" in request.META else ""
            # zone_id = ""
            store_list = []
            store_data = db.stores.find(
                {"serviceZones.zoneId": zone_id, "status": 1, "categoryId": store_category_id}
            )
            if store_data.count() > 0:
                for store in store_data:
                    store_list.append(str(store["_id"]))
                if len(store_list) > 0:
                    filter_query.append({"terms": {"storeId": store_list}})

            filter_query.append({"match": {"status": 1}})
            filter_query.append({"match": {"storeCategoryId": str(store_category_id)}})

            if fname != "":
                if "," in fname or "%2C" in fname:
                    filter_query.append(
                        {"match": {"catName." + language: fname.replace("%20", " ")}}
                    )
                    brand_data_query.append(
                        {"match": {"catName." + language: fname.replace("%20", " ")}}
                    )
                else:
                    if fname != "":
                        filter_query.append(
                            {
                                "match_phrase_prefix": {
                                    "catName." + language: fname.replace("%20", " ")
                                }
                            }
                        )
                        brand_data_query.append(
                            {
                                "match_phrase_prefix": {
                                    "catName." + language: fname.replace("%20", " ")
                                }
                            }
                        )

            if sname != "":
                if "," in sname or "%2C" in sname:
                    filter_query.append(
                        {"match": {"subCatName." + language: sname.replace("%20", " ")}}
                    )
                    brand_data_query.append(
                        {"match": {"subCatName." + language: sname.replace("%20", " ")}}
                    )
                else:
                    if sname != "":
                        filter_query.append(
                            {
                                "term": {
                                    "subCatName." + language + ".keyword": sname.replace("%20", " ")
                                }
                            }
                        )
                        brand_data_query.append(
                            {
                                "term": {
                                    "subCatName." + language + ".keyword": sname.replace("%20", " ")
                                }
                            }
                        )
            if tname != "":
                if "," in tname or "%2C" in tname:
                    filter_query.append(
                        {"match": {"subSubCatName." + language: tname.replace("%20", " ")}}
                    )
                    brand_data_query.append(
                        {"match": {"subSubCatName." + language: tname.replace("%20", " ")}}
                    )
                else:
                    filter_query.append(
                        {
                            "match_phrase_prefix": {
                                "subSubCatName." + language: tname.replace("%20", " ")
                            }
                        }
                    )
                    brand_data_query.append(
                        {
                            "match_phrase_prefix": {
                                "subSubCatName." + language: tname.replace("%20", " ")
                            }
                        }
                    )

            # ===========================product name========================================
            should_query.append(
                {
                    "match_phrase_prefix": {
                        "pName.en": {
                            "analyzer": "standard",
                            "query": searchItem.replace("%20", " "),
                            "boost": 5,
                        }
                    }
                }
            )
            should_query.append(
                {"match": {"pName.en": {"query": searchItem.replace("%20", " "), "boost": 5}}}
            )
            # ===========================unit name========================================
            should_query.append(
                {
                    "match_phrase_prefix": {
                        "units.unitName.en": {
                            "analyzer": "standard",
                            "query": searchItem.replace("%20", " "),
                            "boost": 6,
                        }
                    }
                }
            )
            should_query.append(
                {
                    "match": {
                        "units.unitName.en": {"query": searchItem.replace("%20", " "), "boost": 6}
                    }
                }
            )

            query = {
                "query": {
                    "bool": {
                        "must": filter_query,
                        "should": should_query,
                        "must_not": [{"match": {"storeId": "0"}}],
                        "minimum_should_match": 1,
                        "boost": 1.0,
                    }
                },
                "track_total_hits": True,
                "sort": [
                    {"isCentral": {"order": "desc"}},
                    {"isInStock": {"order": "desc"}},
                    {"units.floatValue": {"order": "asc"}},
                    {"_score": {"order": "desc"}},
                ],
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
                                    "sort": [
                                        {"isCentral": {"order": "desc"}},
                                        {"isInStock": {"order": "desc"}},
                                        {"units.floatValue": {"order": "asc"}},
                                        {"_score": {"order": "desc"}},
                                    ],
                                    "_source": {
                                        "includes": [
                                            "_id",
                                            "_score",
                                            "pName",
                                            "storeId",
                                            "parentProductId",
                                            "firstCategoryName",
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
            res = es.search(index=index_products, body=query)
            product_data = []
            category_data = []
            total_count = []
            brand_data = []
            try:
                total_product_count = res["hits"]["total"]["value"]
            except:
                total_product_count = res["hits"]["total"]

            # =============================product suggestion============================================
            if total_product_count > 0:
                total_count.append(total_product_count)
                if "aggregations" in res:
                    if "buckets" in res["aggregations"]["group_by_sub_category"]:
                        if len(res["aggregations"]["group_by_sub_category"]["buckets"]) > 0:
                            for bucket in res["aggregations"]["group_by_sub_category"]["buckets"]:
                                if "top_sales_hits" in bucket:
                                    if "hits" in bucket["top_sales_hits"]:
                                        if "hits" in bucket["top_sales_hits"]["hits"]:
                                            for i in bucket["top_sales_hits"]["hits"]["hits"]:
                                                try:
                                                    last_name = (
                                                        i["_source"]["firstCategoryName"]
                                                        + "'s"
                                                        + " "
                                                        + i["_source"]["secondCategoryName"]
                                                        if "secondCategoryName" in i["_source"]
                                                        else ""
                                                    )
                                                    product_data.append(
                                                        {
                                                            "score": i["sort"][3],
                                                            "isProduct": True,
                                                            "productId": str(
                                                                i["_source"]["parentProductId"]
                                                            ),
                                                            "childProductId": i["_id"],
                                                            "productName": i["_source"]["pName"][
                                                                language
                                                            ],
                                                            "images": i["_source"]["images"],
                                                            "currencySymbol": i["_source"][
                                                                "currencySymbol"
                                                            ]
                                                            if "currencySymbol" in i["_source"]
                                                            else "$",
                                                            "currency": i["_source"]["currency"],
                                                            "storeType": i["_source"]["storeType"]
                                                            if "storeType" in i["_source"]
                                                            else "8",
                                                            "catName": i["_source"][
                                                                "firstCategoryName"
                                                            ]
                                                            if "firstCategoryName" in i["_source"]
                                                            else "",
                                                            "brandTitle": i["_source"][
                                                                "brandTitle"
                                                            ][language]
                                                            if language in i["_source"]
                                                            else "",
                                                            "subCatName": i["_source"][
                                                                "secondCategoryName"
                                                            ]
                                                            if "secondCategoryName" in i["_source"]
                                                            else "",
                                                            "subSubCatName": "",
                                                            "seqId": 2,
                                                            "inSection": last_name,
                                                        }
                                                    )
                                                except:
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
                else:
                    pass
            else:
                pass

            # =====================================brand data====================================================================
            brand_query = {
                "storeCategoryId": store_category_id,
                "status": 1,
                "name." + language: {"$regex": searchItem, "$options": "i"},
            }
            brand_data_counter = (
                db.brands.find(brand_query).sort([("_id", -1)]).skip(from_data).limit(to_data)
            )
            brand_count = db.brands.find(brand_query).count()
            if brand_data_counter.count() == 0:
                pass
            else:
                for brand in brand_data_counter:
                    brand_data.append(
                        {
                            "productId": "",
                            "childProductId": "",
                            "score": 1,
                            "isProduct": False,
                            "productName": brand["name"][language]
                            if language in brand["name"]
                            else brand["name"]["en"],
                            "images": [],
                            "currencySymbol": "$",
                            "currency": "USD",
                            "storeType": "8",
                            "catName": "",
                            "brandTitle": "",
                            "subCatName": "",
                            "subSubCatName": "",
                            "seqId": 1,
                            "inSection": "",
                        }
                    )
            # ===============================================category data================================================================
            category_query = {
                "storeCategory.storeCategoryId": store_category_id,
                "status": 1,
                "categoryName." + language: {"$regex": searchItem, "$options": "i"},
            }
            category_data_counter = (
                db.category.find(category_query).sort([("_id", -1)]).skip(from_data).limit(to_data)
            )
            category_count = db.category.find(category_query).count()
            if category_data_counter.count() == 0:
                pass
            else:
                for cat in category_data_counter:
                    category_data.append(
                        {
                            "productId": "",
                            "childProductId": "",
                            "score": 0,
                            "isProduct": False,
                            "productName": cat["categoryName"][language]
                            if language in cat["categoryName"]
                            else cat["categoryName"]["en"],
                            "images": [],
                            "currencySymbol": "$",
                            "currency": "USD",
                            "storeType": "8",
                            "catName": "",
                            "brandTitle": "",
                            "subCatName": "",
                            "subSubCatName": "",
                            "seqId": 1,
                            "inSection": "",
                        }
                    )
            if len(product_data) > 0:
                dataframe = pd.DataFrame(product_data)
                dataframe = dataframe.drop_duplicates(subset="productName", keep="last")
                product_list = dataframe.to_dict(orient="records")
                new_product_list = sorted(product_list, key=lambda k: k["score"], reverse=True)
            else:
                new_product_list = []

            if len(category_data) > 0:
                dataframe_category = pd.DataFrame(category_data)
                dataframe_category = dataframe_category.drop_duplicates(
                    subset="productName", keep="last"
                )
                category_list = dataframe_category.to_dict(orient="records")
                new_category_list = sorted(category_list, key=lambda k: k["score"], reverse=True)
            else:
                new_category_list = []

            if len(brand_data) > 0:
                dataframe_brand = pd.DataFrame(brand_data)
                dataframe_brand = dataframe_brand.drop_duplicates(subset="productName", keep="last")
                brand_list = dataframe_brand.to_dict(orient="records")
                new_brand_list = sorted(brand_list, key=lambda k: k["score"], reverse=True)
            else:
                new_brand_list = []

            if (
                len(new_brand_list) == 0
                and len(new_category_list) == 0
                and len(new_product_list) == 0
            ):
                finalSuggestions = {
                    "data": {
                        "productData": {},
                        "categoryData": {},
                        "brandData": {},
                        "message": "Product Suggestions Not Found",
                    }
                }
                return JsonResponse(finalSuggestions, safe=False, status=404)
            else:
                finalSuggestions = {
                    "data": {
                        "productData": {"products": new_product_list, "penCount": len(total_count)},
                        "categoryData": {"products": new_category_list, "penCount": category_count},
                        "brandData": {"products": new_brand_list, "penCount": brand_count},
                        "message": "Product Suggestions Found",
                    }
                }
                return JsonResponse(finalSuggestions, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class SearchNew(APIView):
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
        tags=["Search & Filter"],
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
                default=ECOMMERCE_STORE_CATEGORY_ID,
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)",
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
                description="for the sorting like price high to low value's should be...for low to high price:price_asc, high to low price:price_desc,popularity:recency_desc, name: name_asc, name_desc",
            ),
            openapi.Parameter(
                name="categoryName",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="category name of the product..ex. Men, Women",
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
                name="attr",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="attribute name for apply the filter on atribute value",
            ),
            openapi.Parameter(
                name="symptoms",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="symptom name for the product which want to display. ex. Nike, H&M",
            ),
            openapi.Parameter(
                name="serves",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="serves name for the product which want to display. ex. 2 Person, 2-3 Person",
            ),
            openapi.Parameter(
                name="nopieces",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="nopieces name for the product which want to display. ex. 1 Pcs, 10 Pcs",
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
            openapi.Parameter(
                name="menuType",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while applying filter on types in cannabies, this is for only cannabies store category",
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
                name="discountFilter",
                default="10_100",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while applying discount filter for the search data",
            ),
            openapi.Parameter(
                name="ratingFilter",
                default="1_5",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while applying rating filter for the search data",
            ),
            openapi.Parameter(
                name="productType",
                default="1",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="which type we need to get, 0 for all, 1 for normal products and 2 for combo products",
            ),
            openapi.Parameter(
                name="justId",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="while clicking on view more for just in from home page",
            ),
            openapi.Parameter(
                name="newlyAddedBeforeDays",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                description="Timestamp from the date when the products where created, i.e. 15",
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
        # try:
        api_start_time = time.time() ## api start time, this is for use the start time in newlyAddedBeforeDays query
        token = request.META["HTTP_AUTHORIZATION"]
        query = [] # list of all the must query which need to use in elstic search query
        must_not = [] # list of all the must not query which need to use in elstic search query
        should_query = [] # list of all the should query which need to use in elstic search query
        facet_value = "" # this variable we are using for the attribute value while applying filter on attributes
        facet_key = "" # this variable we are using for the attribute key while applying filter on attributes
        QUERY_STRING = request.META["QUERY_STRING"]
        if token == "":
            response_data = {
                "message": "unauthorized",
                "data": [],
            }
            return JsonResponse(response_data, safe=False, status=401)

        user_id = request.GET.get("userId", "") ### fetch the user id from request param
        if user_id == "":
            try:
                user_id = json.loads(token)["userId"]
            except:
                user_id = ""
        else:
            pass

        try:
            session_id = json.loads(token)["sessionId"]
        except:
            session_id = ""

        finalfilter_responseJson_products = [] ## list of the product data, final response for the api
        filter_responseJson = [] ## list of the product data, final response for the api
        language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
        currency_code = (request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else "")
        if currency_code == "":
            currency_code = request.GET.get("currencycode", "")

        'login type we are using for the check which type user is currectly logged in app or web app' \
        '0 for guest user, 1 for normal user and 2 for b2b(institute) buyers'
        try:
            login_type = json.loads(token)["metaData"]["institutionType"]
        except:
            login_type = 1

        if login_type == 0:
            login_type = 1
        popular_status = request.GET.get("popularDtatus", "")
        ip_address = request.META["HTTP_IPADDRESS"] if "HTTP_IPADDRESS" in request.META else ""
        seach_platform = request.META["HTTP_PLATFORM"] if "HTTP_PLATFORM" in request.META else "0"
        city_name = request.META["HTTP_CITY"] if "HTTP_CITY" in request.META else ""
        city_id = request.GET.get("cityId", "5df7b7218798dc2c1114e6bf")
        country_name = request.META["HTTP_COUNTRY"] if "HTTP_COUNTRY" in request.META else ""
        try:
            latitude = (
                float(request.META["HTTP_LATITUDE"]) if "HTTP_LATITUDE" in request.META else 0
            )
        except:
            latitude = 0
        try:
            longitude = (
                float(request.META["HTTP_LONGITUDE"]) if "HTTP_LONGITUDE" in request.META else 0
            )
        except:
            longitude = 0
        '''
            search type based on click on category, subcategory, subcategory or click on searched result. 
            values should be 1 for category, 2 for subcategory, 3 for subsubcstegory, 4 for searched result click.
        '''
        search_type = (int(request.META["HTTP_SEARCHTYPE"]) if "HTTP_SEARCHTYPE" in request.META else 100)

        ##search In for the data which on clicked.example In AppleMobile, In Mens
        search_in = request.META["HTTP_SEARCHIN"] if "HTTP_SEARCHIN" in request.META else ""
        store_category_id = str(request.META["HTTP_STORECATEGORYID"])
        manager_data = db.managers.find_one({"_id": ObjectId(user_id), "linkedWith": 2})
        if manager_data is not None:
            margin_price = False
        else:
            margin_price = True

        # ========================================query parameter====================================================
        # for the search the item in search bar
        search_query = request.GET.get("q", "")
        search_query = search_query.replace("?", "")

        page = int(request.GET.get("page", 1))  # for the pagination
        product_type = int(request.GET.get("productType", 0))  # for the product type which type product need to fetch
        discount_filter = request.GET.get("discountFilter", "")
        rating_filter = request.GET.get("ratingFilter", "")
        f_id = request.GET.get("mainCategoryId", "")  # all type of category ids, first level category id, second level category id, etc

        colour = request.GET.get("colour", "")  # colour name
        b_id = request.GET.get("bid", "")  # brand id
        if b_id == "":
            b_id = request.GET.get("b_id", "")
        bname = request.GET.get("bname", "")  # brand name for the search
        fname = request.GET.get("fname", "")  # category-name, first level category name
        fname = fname.replace("%20", " ")
        fname = fname.replace("+", " ")

        sname = request.GET.get("sname", "")  # sub-category-name, second level category name
        sname = sname.replace("%20", " ")
        sname = sname.replace("+", " ")

        tname = request.GET.get("tname", "")  # sub-sub-category-name, third level category name
        tname = tname.replace("%20", " ")
        tname = tname.replace("+", " ")

        colour = request.GET.get("colour", "")  # colour name
        symptoms_name = request.GET.get("symptoms", "")  # symptoms name for the filter the products base on symptoms
        size = request.GET.get("size", "")  # size name
        serves = request.GET.get("serves", "")
        nopieces = request.GET.get("nopieces", "")
        max_price = request.GET.get("maxprice", "")  # maximum price
        min_price = request.GET.get("minprice", "")  # minimum price
        max_cbd = float(request.GET.get("maxCBD", 0))  # maximum cbd
        min_cbd = float(request.GET.get("minCBD", 0))  # minimum cbd
        max_thc = float(request.GET.get("maxTHC", 0))  # maximum thc
        min_thc = float(request.GET.get("minTHC", 0))  # minimum thc
        integration_type = float(request.GET.get("integrationType", 0))  # minimum thc

        # for the sorting like price high to low value's should be.
        # for low to high price:price_asc, high to low price:price_desc,popularity:recency_desc, name: name_asc, name_desc
        sort_data = request.GET.get("sort", "")

        # number of days for newly added product
        newlyAddedBeforeDays = request.GET.get("newlyAddedBeforeDays", "")
        api_called = int(request.GET.get("called", 0))
        # get the list of all products which have offers
        best_deals = request.GET.get("best_deals", "") # for the apply filter on offers, offer id need to get from request param
        attr_value = request.GET.get("attr", "") # get the attribute value from the query param
        offer_id = request.GET.get("o_id", "")  # get particular offer data
        if offer_id != "":
            offer_id = offer_id.split(",")
        store_id = request.GET.get("s_id", "")  # get particular store data base on store id
        menu_type = request.GET.get("menuType", "")  # get particular menu data (for resturant or dine flow)
        zone_id = request.GET.get("z_id", "")
        just_id = request.GET.get("justId", "") # just in id, while clicking on justIn from the home page need to send the id of just in this variable
        from_data = int(page * 20) - 20
        to_data = 20  # int(page*30)
        to_data_product = int(page * 20)
        facet_attribute = request.META["QUERY_STRING"]

        ### need to add category or search logs in cassandra
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

        # to apply the filter on servers
        if serves != "":
            serves = serves.replace("+%2C", ",")
            serves = serves.replace("+", " ")
            serves = serves.replace("%2F", "/")
            serves = serves.strip()
            serves = serves.replace("%26", "")
            serves = serves.replace("%28", "(")
            serves = serves.replace("%29", ")")
            serves = serves.replace("%25", "%")
            serves = serves.replace("Person", "")
        else:
            pass

        # for apply the filter on no pieces
        if nopieces != "":
            nopieces = nopieces.replace("+%2C", ",")
            nopieces = nopieces.replace("+", " ")
            nopieces = nopieces.replace("%2F", "/")
            nopieces = nopieces.strip()
            nopieces = nopieces.replace("%26", "")
            nopieces = nopieces.replace("%28", "(")
            nopieces = nopieces.replace("%29", ")")
            nopieces = nopieces.replace("%25", "%")
            nopieces = nopieces.replace("Pcs", "")
        else:
            pass


        ## section for the get the store category details
        ## to find the storelisting and hyperlocal values for the store category in the requested city
        category_query = {"storeCategory.storeCategoryId": store_category_id}
        if zone_id != "":
            zone_details = zone_find({"_id": ObjectId(zone_id)})
            category_query["_id"] = ObjectId(zone_details["city_ID"])
        elif store_id != "":
            store_details = db.stores.find_one({"_id": ObjectId(store_id)}, {"cityId": 1})
            category_query["_id"] = str(store_details["cityId"])
        else:
            pass

        categoty_details = db.cities.find_one(category_query, {"storeCategory": 1})
        remove_central = False # to check need to remove central store products or not
        hyperlocal = False # to check store category is hyper local or not, which means store category is setup for city or for global
        storelisting = False # to check store category type is storelisting or not, if storelisting true means need to show store listing in app and web app
        STORE_TYPE = 8
        if store_category_id != "":
            store_category_details = db.storeCategory.find_one({"_id": ObjectId(store_category_id)})
        else:
            store_category_details = None
        if store_category_details is not None:
            STORE_TYPE = store_category_details["type"]
        else:
            pass
        if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
            remove_central = False
            hyperlocal = False
            storelisting = False
        elif STORE_TYPE == 12:
            remove_central = False
            hyperlocal = True
            storelisting = False
        else:
            if categoty_details is not None:
                if "storeCategory" in categoty_details:
                    for cat in categoty_details["storeCategory"]:
                        if cat["storeCategoryId"] == store_category_id:
                            if cat["hyperlocal"] == True and cat["storeListing"] == 1:
                                remove_central = True
                                hyperlocal = True
                                STORE_TYPE = cat["type"]
                                storelisting = True
                                store_id = store_id
                            elif cat["hyperlocal"] == True and cat["storeListing"] == 0:
                                hyperlocal = True
                                storelisting = False
                                STORE_TYPE = cat["type"]
                                remove_central = True
                            else:
                                STORE_TYPE = 8
                                remove_central = False
                                hyperlocal = False
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


        ## get the next available driver shift and available time
        if hyperlocal == True and storelisting == False:
            try:
                if zone_id != "":
                    driver_roaster = next_availbale_driver_roaster(zone_id)
                    next_availbale_driver_time = driver_roaster["productText"]
                else:
                    next_availbale_driver_time = ""
            except:
                next_availbale_driver_time = ""
        else:
            next_availbale_driver_time = ""

        ### filter apply on attributes, which we need to add in elastic search query
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
                        facet_key + ", " + ((facet.split("_")[1]).split("=")[0]).replace("%20", " ")
                    )

        ### filter base on attribute values
        facet_value = facet_value.replace("+%2C", ",")
        facet_value = facet_value.replace("%2C", ",")
        facet_value = facet_value.replace("+", " ")
        facet_value = facet_value.replace("%2F", "/")
        facet_value = facet_value.strip()
        facet_value = facet_value.replace("%26", "")
        facet_value = facet_value.replace("%28", "(")
        facet_value = facet_value.replace("%29", ")")
        facet_value = facet_value.replace("%25", "%")
        facet_key = facet_key.replace("+", " ")
        facet_value_data = ""
        if facet_value_data == "":
            facet_value_data = facet_value
        else:
            facet_value_data = facet_value_data

        ### find the store list in the zone, for hyperlocal true and storelisting false
        try:
            if (
                hyperlocal == True
                and storelisting == False
                and store_category_id != MEAT_STORE_CATEGORY_ID
            ):
                zone_details = zone_find({"_id": ObjectId(zone_id)})
                store_query = {
                    "categoryId": str(store_category_id),
                    "serviceZones.zoneId": str(zone_details["_id"]),
                    "status": 1,
                    "storeFrontTypeId": {"$ne": 5},
                }
                store_data = db.stores.find(store_query)
                if remove_central == False and zone_id == "":
                    store_data_details = ["0"]
                else:
                    store_data_details = []
                if store_data.count() > 0:
                    for store in store_data:
                        store_data_details.append(str(store["_id"]))
                query.append({"terms": {"storeId": store_data_details}})
            elif (
                hyperlocal == True
                and storelisting == False
                and store_category_id == MEAT_STORE_CATEGORY_ID
            ):
                if zone_id != "":
                    if remove_central == True:
                        store_data_details = []
                    else:
                        store_data_details = ["0"]
                    zone_details = zone_find({"_id": ObjectId(zone_id)})
                    try:
                        store_data_details.append(str(zone_details["DCStoreId"]))
                    except:
                        pass
                    query.append({"terms": {"storeId": store_data_details}})
                else:
                    store_query = None
            elif hyperlocal == True and storelisting == True:
                if store_id != "":
                    query.append({"match": {"storeId": store_id}})
                else:
                    pass
            else:
                if store_id != "":
                    query.append({"match": {"storeId": store_id}})
        except:
            pass
        query.append({"match": {"storeCategoryId": store_category_id}})
        query.append({"terms": {"productFor": [str(login_type)]}})

        ## add the discount query in elastic search find query
        if discount_filter != "":
            dis_list = discount_filter.split(',')
            if len(dis_list) > 0:
                d1 = dis_list[0]
                d2 = dis_list[1]
                min_discount = d1.split('_')[0]
                max_discount = d2.split('_')[1]
            else:
                min_discount = rating_filter.split("_")[0]
                max_discount = rating_filter.split("_")[1]
            query.append(
                {
                    "range": {
                        "offer.discountValue": {
                            "gte": int(min_discount),
                            "lte": int(max_discount),
                        }
                    }
                }
            )
            query.append({"match": {"offer.status": 1}})
        else:
            pass

        ## add the created timestamp query in elastic search find query
        if newlyAddedBeforeDays != "":
            before_days = int(newlyAddedBeforeDays) * 86400
            query.append(
                {
                    "range": {
                        "createdTimestamp": {
                            "lte": int(api_start_time),
                            "gte": int(api_start_time) - int(before_days),
                        }
                    }
                }
            )

        ## add the product rating query in elastic search find query
        if rating_filter != "":
            rat_list = rating_filter.split(',')
            if len(rat_list) > 1:
                i1 = rat_list[0]
                i2 = rat_list[len(rat_list)- 1]
                min_rating = i1.split('_')[0]
                max_rating = i2.split('_')[1]
            else:
                min_rating = rating_filter.split("_")[0]
                max_rating = rating_filter.split("_")[1]
            query.append({"range": {"avgRating": {"gte": int(min_rating), "lte": int(max_rating)}}})
        else:
            pass

        if just_id != "":
            home_page_just_in_data = db.ecomHomePage.find_one({"_id": ObjectId(just_id)})
            product_data = []
            if home_page_just_in_data is not None:
                for entity in home_page_just_in_data["entity"]:
                    product_details = db.childProducts.find_one(
                        {"_id": ObjectId(entity["parentProductId"])}
                    )
                    if product_details is not None:
                        product_data.append(str(product_details["_id"]))
            else:
                pass

            if len(product_data) > 0:
                query.append(
                    {
                        "terms": {
                            # "parentProductId": product_data
                            "_id": product_data
                        }
                    }
                )
            else:
                pass
        else:
            pass

        if offer_id != "":
            query.append({"terms": {"offer.offerId": offer_id}})

        if best_deals != "":
            query.append({"match": {"offer.status": 1}})

        ## add the attributes query in elastic search find query
        if facet_value_data != "":
            if "," in facet_value_data or "%2C" in facet_value_data:
                facet_key = facet_key.title()
                if facet_key != "Effects" and store_category_id != CANNABIS_STORE_CATEGORY_ID:
                    query.append(
                        {
                            "query_string": {
                                "fields": ["units.attributes.attrlist.attrname.en"],
                                "query": facet_key,
                            }
                        }
                    )
                else:
                    facet_value_data = facet_value_data.replace("%2C", ",")
                    query.append(
                        {
                            "terms": {
                                "units.attributes.attrlist.attrname.en.keyword": facet_value_data.split(
                                    ","
                                )
                            }
                        }
                    )
            else:
                facet_key = facet_key.title()
                if facet_key != "Effects" and store_category_id != CANNABIS_STORE_CATEGORY_ID:
                    query.append(
                        {
                            "query_string": {
                                "fields": ["units.attributes.attrlist.attrname.en"],
                                "query": facet_key,
                            }
                        }
                    )
                else:
                    query.append(
                        {
                            "query_string": {
                                "fields": ["units.attributes.attrlist.attrname.en"],
                                "query": facet_value_data.replace("%20", ""),
                            }
                        }
                    )
            attribute_value = facet_value_data.replace("%20", "")
            attribute_value = attribute_value.title()
            query.append(
                {
                    "terms": {
                        "units.attributes.attrlist.value.en.keyword": attribute_value.split(",")
                    }
                }
            )
        if attr_value != "":
            should_query.append(
                {
                    "match_phrase_prefix": {
                        "units.attributes.attrlist.value.en": attr_value.replace("%20", "")
                    }
                }
            )
            should_query.append(
                {
                    "match_phrase_prefix": {
                        "units.attributes.attrlist.value.name.en": attr_value.replace("%20", "")
                    }
                }
            )

        ## add the search text query in elastic search find query
        if search_query != "":
            is_redis_data = False
            search_query = search_query.replace("%20", " ")
            should_query.extend([
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
                    "match": {
                        "pName.en": {
                            "analyzer": "standard",
                            "query": search_query,
                            "boost": 3
                        }
                    }
                },
                {
                    "match": {
                        "pName.en": {
                            "analyzer": "edgengram_analyzer",
                            "query": search_query,
                            "boost": 0.1,
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
                    "multi_match": {
                        "analyzer": "standard",
                        "query": search_query,
                        "fields": [
                            "categoryList.parentCategory.categoryName." + language,
                            "categoryList.parentCategory.childCategory.categoryName." + language
                        ],
                        "boost": 1
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

        ## add the price range query in elastic search find query
        if max_price != "" and min_price != "":
            query.append(
                {
                    "range": {
                        "units.b2cPricing.b2cproductSellingPrice": {
                            "gte": float(min_price),
                            "lte": float(max_price),
                        }
                    }
                }
            )

        if max_cbd != 0 and min_cbd != 0:
            query.append({"range": {"units.CBD": {"gte": float(min_cbd), "lte": float(max_cbd)}}})

        if max_thc != 0 and min_thc != 0:
            query.append({"range": {"units.THC": {"gte": float(min_thc), "lte": float(max_thc)}}})

        ## add the sizes query in elastic search find query
        if size != "":
            size = size.replace("%2C", ",")
            size = size.replace(",", ", ")
            query.append({"match": {"units.unitSizeGroupValue.en": size.replace("%20", " ")}})
        else:
            pass

        ## add the colour query in elastic search find query
        if colour != "":
            if "," in colour or "%2C" in colour:
                query.append({"match": {"units.colorName": colour.replace("%20", " ")}})
            else:
                query.append(
                    {"match_phrase_prefix": {"units.colorName": colour.replace("%20", " ")}}
                )

        if serves != "":
            if "," in serves or "%2C" in serves:
                serves = serves.replace("%20", " ")
                query.append({"terms": {"serversFor.keyword": serves.split(",")}})
            else:
                query.append({"match_phrase_prefix": {"serversFor": serves.replace("%20", " ")}})

        if nopieces != "":
            if "," in nopieces or "%2C" in nopieces:
                nopieces = nopieces.replace("%20", " ")
                query.append({"terms": {"numberOfPcs.keyword": nopieces.split(",")}})
            else:
                query.append({"match_phrase_prefix": {"numberOfPcs": nopieces.replace("%20", " ")}})

        if menu_type != "":
            query.append({"match": {"units.cannabisProductType": menu_type}})

        if symptoms_name != "":
            if "," in symptoms_name or "%2C" in symptoms_name:
                symptoms_name = symptoms_name.replace("%20", " ")
                query.append({"terms": {"symptoms.symptomName.keyword": symptoms_name.split(",")}})
            else:
                query.append(
                    {
                        "match_phrase_prefix": {
                            "symptoms.symptomName": symptoms_name.replace("%20", " ")
                        }
                    }
                )

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

        ## add the brand query in elastic search find query
        if b_id != "":
            query.append(
                {
                    "match": {
                        "brand": b_id.replace(",", ", ")
                    }
                }
            )

        ## add the main category query in elastic search find query
        if fname != "":
            query.append(
                {
                    "match": {
                        "categoryList.parentCategory.categoryId": fname
                    }
                }
            )

        ## add the second category query in elastic search find query
        if sname != "":
            query.append(
                {
                    "match": {
                        "categoryList.parentCategory.childCategory.categoryId": sname
                    }
                }
            )

        ## add the third level category query in elastic search find query
        if tname != "":
            query.append(
                {
                    "match": {
                        "categoryList.parentCategory.childCategory.categoryId": tname
                    }
                }
            )

        # ==== add the query for product fetch on product type=======
        if product_type != 0:
            query.append({"match": {"productType": int(product_type)}})
        else:
            pass

        ## add the sorting base on request in search query
        sort_type = 5
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
                bucket_sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"units.discountPrice": {"order": sort_data.split("_")[1]}},
                ]
            elif "name" in sort_data.split("_")[0]:
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
            elif "recency" in sort_data.split("_")[0]:
                sort_type = 2
                sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"units.discountPrice": {"order": "asc"}},
                ]
                bucket_sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"units.discountPrice": {"order": "asc"}},
                ]
            elif "popular_score" == sort_data.lower() and store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                sort_type = 5  # popularScore descending
                sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"popularScore": {"order": "desc"}},
                ]
                bucket_sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"popularScore": {"order": "desc"}},
                ]
            elif "newest" in sort_data.split("_")[0]:
                sort_type = 6  # newest first
                sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"createdTimestamp": {"order": "desc"}},
                ]
                bucket_sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"createdTimestamp": {"order": "desc"}},
                ]
            else:
                sort_type = 2
                sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"units.discountPrice": {"order": "asc"}},
                ]
                bucket_sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"units.discountPrice": {"order": "asc"}},
                ]
        else:
            if search_query != "":
                sort_type = 2
                sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"units.discountPrice": {"order": "asc"}},
                ]
                bucket_sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"units.discountPrice": {"order": "asc"}},
                    {"_score": {"order": "desc"}},
                ]
            else:
                sort_type = 5  # popularScore descending
                sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"popularScore": {"order": "desc"}},
                ]
                bucket_sort_query = [
                    {"isInStock": {"order": "desc"}},
                    {"popularScore": {"order": "desc"}},
                ]

        # this we added for the sorting the bucket base on request
        '''
            sort type 0 means price sorting in asc
            sort type 1 means price sorting in desc
        '''
        if search_query != "" and sort_type == 0:
            avg_score = {"max": {"script": "_score"}}
            order_score = {"avg_score": "desc"}
        elif search_query != "":
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
        else:
            avg_score = {"max": {"script": "_score"}}
            order_score = {"avg_score": "desc"}

        query.append({"match": {"status": 1}})

        if int(integration_type) == 0:
            pass
        elif int(integration_type) == 1:
            must_not.append({"match": {"magentoId": -1}})
            query.append({"exists": {"field": "magentoId"}})
        elif int(integration_type) == 2:
            must_not.append({"term": {"shopify_variant_id.keyword": {"value": ""}}})
            query.append({"exists": {"field": "shopify_variant_id"}})
        else:
            pass

        ## generate the aggrigation in search query
        aggs_json = {
            "group_by_sub_category": {
                "terms": {
                    "field": "parentProductId.keyword",
                    "order": order_score,
                    "size": int(to_data_product),
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
                                    "secondCategoryName",
                                    "currencySymbol",
                                    "detailDescription",
                                    "prescriptionRequired",
                                    "needsIdProof",
                                    "saleOnline",
                                    "productType",
                                    "uploadProductDetails",
                                    "currency",
                                    "pPName",
                                    "offer",
                                    "tax",
                                    "brandTitle",
                                    "categoryList",
                                    "images",
                                    "avgRating",
                                    "units",
                                    "storeCategoryId",
                                    "manufactureName",
                                    "maxQuantity",
                                    "substitute"
                                ]
                            },
                            "size": 1,
                        }
                    },
                },
            }
        }
        if int(login_type) != 2:
            must_not.append({"match": {"units.b2cPricing.b2cproductSellingPrice": 0}})
        else:
            must_not.append({"match": {"units.b2bPricing.b2bproductSellingPrice": 0}})

        ## make the query base on hyperlocal and storelisting condition
        if hyperlocal == True and storelisting == False:
            if len(should_query) > 0:
                product_search_query = {
                    "bool": {
                        "must": query,
                        "should": should_query,
                        "minimum_should_match": 1,
                        "boost": 1.0,
                    }
                }
            else:
                product_search_query = {
                    "bool": {
                        "must": query,
                    }
                }
        else:
            if just_id != "":
                if len(should_query) > 0:
                    product_search_query = {
                        "bool": {
                            "must": query,
                            "should": should_query,
                            "minimum_should_match": 1,
                            "boost": 1.0,
                        }
                    }
                else:
                    product_search_query = {
                        "bool": {
                            "must": query,
                        }
                    }
            else:
                must_not.append({"match": {"storeId": "0"}})
                if len(should_query) > 0:
                    product_search_query = {
                        "bool": {
                            "must": query,
                            "should": should_query,
                            "minimum_should_match": 1,
                            "boost": 1.0,
                        }
                    }
                else:
                    product_search_query = {"bool": {"must": query}}

        if len(must_not) > 0:
            product_search_query["bool"]["must_not"] = must_not

        if len(should_query) > 0:
            search_item_query = {
                "_source": ["firstCategoryName"],
                "size": 5,
                "query": product_search_query,
                "track_total_hits": True,
                "sort": sort_query,
                "aggs": aggs_json,
            }
            search_item_query_count = {
                "_source": ["firstCategoryName"],
                "size": 5,
                "query": product_search_query,
                "track_total_hits": True,
                "sort": sort_query,
                "aggs": {
                    "group_by_sub_category": {
                        "terms": {
                            "field": "parentProductId.keyword",
                            "order": order_score,
                            "size": 2000,
                        },
                        "aggs": {
                            "avg_score": {"max": {"script": "doc.isInStock"}},
                            "top_sales_hits": {
                                "top_hits": {
                                    "sort": bucket_sort_query,
                                    "_source": {
                                        "includes": [
                                            "_id",
                                            "_score",
                                            "pName",
                                            "detailDescription",
                                            "storeId",
                                            "parentProductId",
                                            "secondCategoryName",
                                            "currencySymbol",
                                            "prescriptionRequired",
                                            "needsIdProof",
                                            "productType",
                                            "saleOnline",
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
                "_source": ["firstCategoryName"],
                "size": 5,
                "query": product_search_query,
                "track_total_hits": True,
                "sort": sort_query,
                "aggs": aggs_json,
            }
            search_item_query_count = {
                "_source": ["firstCategoryName"],
                "size": 5,
                "query": product_search_query,
                "track_total_hits": True,
                "sort": sort_query,
                "aggs": {
                    "group_by_sub_category": {
                        "terms": {
                            "field": "parentProductId.keyword",
                            "order": order_score,
                            "size": 2000,
                        },
                        "aggs": {
                            "avg_score": {"max": {"script": "doc.isInStock"}},
                            "top_sales_hits": {
                                "top_hits": {
                                    "sort": bucket_sort_query,
                                    "_source": {
                                        "includes": [
                                            "_id",
                                            "_score",
                                            "productType",
                                            "pName",
                                            "detailDescription",
                                            "prescriptionRequired",
                                            "needsIdProof",
                                            "saleOnline",
                                            "uploadProductDetails",
                                            "storeId",
                                            "parentProductId",
                                            "secondCategoryName",
                                            "currencySymbol",
                                            "currency",
                                            "pPName",
                                            "tax",
                                            "brandTitle",
                                            "categoryList",
                                            "images",
                                            "avgRating",
                                            "units",
                                            "offer",
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
        """
            get the data from redis json if the data is available in redis json
            else execute the same code for the get the data and store into redis json
        """
        PLP_QUERY_STRING = QUERY_STRING
        QUERY_STRING = QUERY_STRING.replace("&userId", "")
        QUERY_STRING = QUERY_STRING.replace("&called=1", "")
        QUERY_STRING = QUERY_STRING.replace("%20", "")
        QUERY_STRING = QUERY_STRING.replace("+", "")
        QUERY_STRING = QUERY_STRING.replace("&", "")
        rjId = "plp_" + QUERY_STRING + "_" + currency_code + "_" + str(login_type)
        start_count_time = time.time()
        try:
            ## get the data from redis base on key (rjId)
            redis_response_data = rj_plp.jsonget(rjId)
        except:
            redis_response_data = []

        if store_category_id == MEAT_STORE_CATEGORY_ID:
            redis_response_data = None
        if redis_response_data is None:
            redis_response_data = []
        else:
            redis_response_data = redis_response_data

        ### if data is coming from redis need to add other details in response data
        ## values needs to be add in response data is favourite
        redis_response_data = []
        if len(redis_response_data) > 0 and api_called == 0:
            if len(redis_response_data["products"]) > 0:
                currency_details = db.currencies.find_one({"currencyCode": currency_code})
                if currency_details is not None:
                    currency_symbol = currency_details["currencySymbol"]
                    currency = currency_details["currencyCode"]
                else:
                    currency_symbol = "₹"
                    currency = "INR"
                main_products = []

                ### find the favourite product details from cassandra base on user_id
                for p_p in redis_response_data["products"]:
                    p_p["currencySymbol"] = currency_symbol
                    response_casandra = session.execute(
                        """SELECT * FROM favouriteproductsuserwise where userid=%(userid)s AND childproductid=%(productid)s ALLOW FILTERING""",
                        {"userid": user_id, "productid": str(p_p["childProductId"])},
                    )

                    isFavourite = False
                    if not response_casandra:
                        isFavourite = False
                    else:
                        for fav in response_casandra:
                            isFavourite = True
                    p_p["isFavourite"] = isFavourite
                    main_products.append(p_p)
                data = {
                    "data": {
                        "products": main_products,
                        "penCount": redis_response_data["penCount"],
                        "offerBanner": [],
                    },
                    "message": "Got the details",
                }
            else:
                ### if data not found in redis need to execute the elastic search query
                res_first_category = es.search(index=index_products, body=search_item_query, scroll="1m")
                res = es.search(index=index_products, body=search_item_query, scroll="1m")
                res_count = es.search(index=index_products, body=search_item_query_count, scroll="1m")
                if "sort" in search_item_query:
                    if "units.floatValue.keyword" in search_item_query["sort"]:
                        if search_item_query["sort"]["units.floatValue.keyword"]["order"] == "asc":
                            sort = 0
                        elif (
                            search_item_query["sort"]["units.floatValue.keyword"]["order"] == "desc"
                        ):
                            sort = 1
                    else:
                        sort = 2
                else:
                    sort = 3
                if store_category_id == MEAT_STORE_CATEGORY_ID:  # for meaty flow call this function
                    data = search_read_new(
                        res,
                        api_start_time,
                        language,
                        filter_responseJson,
                        finalfilter_responseJson_products,
                        popular_status,
                        sort,
                        login_type,
                        store_id,
                        sort_type,
                        store_category_id,
                        int(from_data),
                        int(to_data),
                        user_id,
                        remove_central,
                        zone_id,
                        offer_id,
                        next_availbale_driver_time,
                        QUERY_STRING,
                        city_id,
                        currency_code,
                    )
                else:
                    ### function for get the product details and generate the response for the api
                    data = search_read_stores(
                        res,
                        api_start_time,
                        language,
                        filter_responseJson,
                        finalfilter_responseJson_products,
                        popular_status,
                        sort,
                        login_type,
                        store_id,
                        sort_type,
                        store_category_id,
                        int(from_data),
                        int(to_data_product),
                        user_id,
                        remove_central,
                        zone_id,
                        min_price,
                        max_price,
                        margin_price,
                        currency_code,
                        search_query,
                        token,
                        hyperlocal,
                        storelisting,
                        QUERY_STRING,
                    )
        else:
            print(search_item_query)
            res_first_category = es.search(index=index_products, body=search_item_query, scroll="1m")
            res = es.search(index=index_products, body=search_item_query, scroll="1m")
            res_count = es.search(index=index_products, body=search_item_query_count, scroll="1m")
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
            if store_category_id == MEAT_STORE_CATEGORY_ID:  # for meaty flow call this function
                data = search_read_new(
                    res,
                    api_start_time,
                    language,
                    filter_responseJson,
                    finalfilter_responseJson_products,
                    popular_status,
                    sort,
                    login_type,
                    store_id,
                    sort_type,
                    store_category_id,
                    int(from_data),
                    int(to_data),
                    user_id,
                    remove_central,
                    zone_id,
                    offer_id,
                    next_availbale_driver_time,
                    QUERY_STRING,
                    city_id,
                    currency_code,
                )
            else:
                ### function for get the product details and generate the response for the api
                data = search_read_stores(
                    res,
                    api_start_time,
                    language,
                    filter_responseJson,
                    finalfilter_responseJson_products,
                    popular_status,
                    sort,
                    login_type,
                    store_id,
                    sort_type,
                    store_category_id,
                    int(from_data),
                    int(to_data_product),
                    user_id,
                    remove_central,
                    zone_id,
                    min_price,
                    max_price,
                    margin_price,
                    currency_code,
                    search_query,
                    token,
                    hyperlocal,
                    storelisting,
                    QUERY_STRING,
                )
        # for brand need to fetch the footer details
        footer_details = {}
        if bname != "":
            try:
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
                    {"_id": ObjectId(b_id)}, {"description": 1, "name": 1, "logoImage": 1}
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
                    footer_details["image"] = (
                        brand_details["logoImage"]
                        if "logoImage" in brand_details
                        else ""
                    )
            except Exception as e:
                print(e)
                pass
        elif ObjectId.is_valid(tname):
            brand_details = db.category.find_one({"_id": ObjectId(tname)}, {"categoryDesc": 1, "categoryName": 1, "websiteImage": 1})
            if brand_details is not None:
                soup = BeautifulSoup(brand_details["categoryDesc"]["en"])
                footer_details["description"] = soup.get_text()
                footer_details["title"] = (
                    brand_details["categoryName"][language]
                    if language in brand_details["categoryName"]
                    else brand_details["categoryName"]["en"]
                )
                footer_details["image"] = (
                    brand_details["websiteImage"]
                    if "websiteImage" in brand_details
                    else ""
                )
        elif ObjectId.is_valid(sname):
            brand_details = db.category.find_one({"_id": ObjectId(sname)}, {"categoryDesc": 1, "categoryName": 1, "websiteImage": 1})
            if brand_details is not None:
                soup = BeautifulSoup(brand_details["categoryDesc"]["en"])
                footer_details["description"] = soup.get_text()
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
                }, {"categoryDesc": 1, "categoryName": 1, "websiteImage": 1}
            )
            if brand_details is not None:
                soup = BeautifulSoup(brand_details["categoryDesc"]["en"])
                footer_details["description"] = soup.get_text()
                footer_details["title"] = (
                    brand_details["categoryName"][language]
                    if language in brand_details["categoryName"]
                    else brand_details["categoryName"]["en"]
                )
            else:
                pass
        else:
            pass

        try:
            try:
                pen_count = len(res_count["aggregations"]["group_by_sub_category"]["buckets"])
            except:
                res_count = es.search(index=index_products, body=search_item_query_count, scroll="1m")
                pen_count = len(res_count["aggregations"]["group_by_sub_category"]["buckets"])
        except Exception as e:
            print(e)
            pen_count = 20

        ### api is called from superadmin while adding the
        # products or category then need to add the plp response in redis
        if api_called == 0:
            threading.Thread(
                target=update_plp_data_from_plp,
                args=(
                    PLP_QUERY_STRING,
                    currency_code,
                    search_type,
                    store_category_id,
                ),
            ).start()
        if len(data["data"]["products"]) == 0:
            return JsonResponse(data, safe=False, status=404)
        else:
            data["data"]["penCount"] = pen_count
            if data["data"]["penCount"] == 0:
                return JsonResponse(data, safe=False, status=404)
            else:
                data["data"]["penCount"] = pen_count
                data["data"]["footer"] = footer_details
                print("total time for api response with request for......!!!!!!!", time.time() - start_count_time, "request for", api_called)
                return JsonResponse(data, safe=False, status=200)

    # except Exception as ex:
    #     template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    #     message = template.format(type(ex).__name__, ex.args)
    #     print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
    #     with open("erorrlogs.txt", "a+") as myfile:
    #         myfile.write(message)
    #     error = {"data": [], "message": message}
    #     return JsonResponse(error, safe=False, status=500)


class Filter(APIView):
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
        tags=["Search & Filter"],
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
                default=ECOMMERCE_STORE_CATEGORY_ID,
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
                name="attr",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="attribute name for apply the filter on atribute value",
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
            token = (
                request.META["HTTP_AUTHORIZATION"]
                if "HTTP_AUTHORIZATION" in request.META
                else "sdfsdfsf"
            )
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
            currencySymbol = "₹"
            currency = "INR"
            color_list = []
            serve_data = []
            type_data = []
            number_of_data = []
            weight_data = []
            brand_list = []
            manufacture_list = []
            sym_list = []
            category_list = []
            sub_categpry_list = []
            sub_sub_category_list = []
            finalfilter_responseJson_products = []
            filter_responseJson = []
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            filter_level = request.META["HTTP_LEVEL"] if "HTTP_LEVEL" in request.META else 1
            login_type = (
                int(request.META["HTTP_LOGINTYPE"]) if "HTTP_LOGINTYPE" in request.META else 1
            )
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            # ========================================query parameter====================================================
            # for the search the item in search bar
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            search_query = request.GET.get("q", "")
            platform = int(request.GET.get("platform", "1"))
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
            max_cbd = int(request.GET.get("maxCBD", 0))  # maximum cbd
            min_cbd = int(request.GET.get("minCBD", 0))  # minimum cbd
            max_thc = int(request.GET.get("maxTHC", 0))  # maximum thc
            min_thc = int(request.GET.get("minTHC", 0))  # minimum thc
            best_deals = request.GET.get("best_deals", "")  # get the best deals
            offer_id = request.GET.get("o_id", "")
            attr_value = request.GET.get("attr", "")
            store_id = request.GET.get("s_id", "")
            if store_id == "":
                store_id = request.GET.get("store_id", "")  # get particular offer data
            main_store_id = request.GET.get("store_id", "")
            zone_id = request.GET.get("z_id", "")
            to_data = 300
            from_data = int(page * 300) - 300
            if store_category_id == CANNABIS_STORE_CATEGORY_ID:
                zone_id = ""
            facet_attribute = request.META["QUERY_STRING"] if "QUERY_STRING" in request.META else ""
            sname = sname.replace("+", " ")
            must_not_query = []
            must_not_query.append({"term": {"brandTitle.en.keyword": ""}})
            must_not_query.append({"term": {"category.en.keyword": ""}})

            if symptoms_name != "":
                if "," in symptoms_name or "%2C" in symptoms_name:
                    query.append(
                        {"match": {"symptoms.symptomName": symptoms_name.replace("%20", " ")}}
                    )
                else:
                    query.append(
                        {
                            "match_phrase_prefix": {
                                "symptoms.symptomName": symptoms_name.replace("%20", " ")
                            }
                        }
                    )
            if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                query.append({"match": {"units.isPrimary": True}})
            elif zone_id != "" and store_category_id == MEAT_STORE_CATEGORY_ID:
                zone_details = zone_find({"_id": ObjectId(zone_id)})
                store_query = {
                    "categoryId": str(store_category_id),
                    "cityId": zone_details["city_ID"],
                }
                store_data = db.stores.find(store_query)
                store_data_details = ["0"]
                if store_data.count() > 0:
                    for store in store_data:
                        store_data_details.append(str(store["_id"]))
                    query.append(
                        {
                            "terms": {
                                # "units.suppliers.id": store_data_details
                                "storeId": store_data_details
                            }
                        }
                    )
            elif (
                zone_id != "" and store_id != "" and store_category_id != PHARMACY_STORE_CATEGORY_ID
            ):
                store_data_details = []
                # store_data_details.append(store_id)
                store_data = db.stores.find(
                    {"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id}
                )
                if store_data.count() > 0:
                    for store in store_data:
                        store_data_details.append(str(store["_id"]))
                    query.append(
                        {
                            "terms": {
                                # "units.suppliers.id": store_data_details
                                "storeId": store_data_details
                            }
                        }
                    )
            elif zone_id != "" and store_category_id == PHARMACY_STORE_CATEGORY_ID:
                zone_details = zone_find({"_id": ObjectId(zone_id)})
                store_query = {
                    "categoryId": str(store_category_id),
                    "cityId": zone_details["city_ID"],
                }
                store_data = db.stores.find(store_query)
                store_data_details = []
                if store_data.count() > 0:
                    for store in store_data:
                        store_data_details.append(str(store["_id"]))
                    query.append(
                        {
                            "terms": {
                                # "units.suppliers.id": store_data_details
                                "storeId": store_data_details
                            }
                        }
                    )
            elif (
                zone_id != ""
                and store_id == ""
                and store_category_id != ECOMMERCE_STORE_CATEGORY_ID
            ):
                store_data_details = []
                store_data_details.append(store_id)
                store_data = db.stores.find(
                    {"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id}
                )
                if store_data.count() > 0:
                    for store in store_data:
                        store_data_details.append(str(store["_id"]))
                    query.append(
                        {
                            "terms": {
                                # "units.suppliers.id": store_data_details
                                "storeId": store_data_details
                            }
                        }
                    )
            elif store_id != "" and store_category_id != PHARMACY_STORE_CATEGORY_ID:
                query.append({"match": {"storeId": store_id}})
            else:
                pass

            if main_store_id != "":
                query.append({"match": {"storeId": main_store_id}})

            if facet_attribute != "":
                for facet in facet_attribute.split("&"):
                    if "facet_" in facet:
                        if facet_value == "":
                            facet_value = facet_value + (
                                (facet.split("_")[1]).split("=")[1]
                            ).replace("%20", " ")
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

            # ========================================query for the store category wise getting the products============
            query.append({"match": {"storeCategoryId": store_category_id}})

            if best_deals != "":
                query.append({"match": {"offer.status": 1}})

            if offer_id != "":
                query.append({"match": {"offer.offerId": offer_id}})

            if attr_value != "":
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "units.attributes.attrlist.value.en": attr_value.replace("%20", "")
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "units.attributes.attrlist.value.name.en": attr_value.replace("%20", "")
                        }
                    }
                )

            if facet_value != "":
                number_facet_value = ""
                number_facet = re.findall(r"\d+(?:\.\d+)?", facet_value)
                if len(number_facet) > 0:
                    if "," in facet_value or "%2C" in facet_value:
                        query.append(
                            {
                                "nested": {
                                    "path": "string_facet",
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {"match": {"string_facet.facet-name": facet_key}},
                                                {
                                                    "match": {
                                                        "string_facet.facet-value": facet_value.replace(
                                                            "%2C", ","
                                                        )
                                                    }
                                                },
                                            ]
                                        }
                                    },
                                }
                            }
                        )

                    else:
                        query.append(
                            {
                                "nested": {
                                    "path": "string_facet",
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {
                                                    "match_phrase_prefix": {
                                                        "string_facet.facet-name": facet_key
                                                    }
                                                },
                                                {
                                                    "match_phrase_prefix": {
                                                        "string_facet.facet-value": facet_value.replace(
                                                            "%2C", ","
                                                        )
                                                    }
                                                },
                                            ]
                                        }
                                    },
                                }
                            }
                        )

                else:
                    if "," in facet_value or "%2C" in facet_value:
                        query.append(
                            {
                                "nested": {
                                    "path": "string_facet",
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {"match": {"string_facet.facet-name": facet_key}},
                                                {
                                                    "match": {
                                                        "string_facet.facet-value": facet_value.replace(
                                                            "%2C", ","
                                                        )
                                                    }
                                                },
                                            ]
                                        }
                                    },
                                }
                            }
                        )

                    else:
                        query.append(
                            {
                                "nested": {
                                    "path": "string_facet",
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {
                                                    "match_phrase_prefix": {
                                                        "string_facet.facet-name": facet_key
                                                    }
                                                },
                                                {
                                                    "match_phrase_prefix": {
                                                        "string_facet.facet-value": facet_value.replace(
                                                            "%2C", ","
                                                        )
                                                    }
                                                },
                                            ]
                                        }
                                    },
                                }
                            }
                        )

            if search_query != "":
                # ===========================product name========================================
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "pPName.en": {
                                "analyzer": "standard",
                                "query": search_query.replace("%20", " "),
                                "boost": 5,
                            }
                        }
                    }
                )
                should_query.append(
                    {
                        "match": {
                            "pPName.en": {"query": search_query.replace("%20", " "), "boost": 5}
                        }
                    }
                )
                # ===========================================detail description============================
                should_query.append(
                    {
                        "match": {
                            "detailDescription."
                            + language: {"query": search_query.replace("%20", " "), "boost": 0}
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "detailDescription."
                            + language: {"query": search_query.replace("%20", " "), "boost": 0}
                        }
                    }
                )
                # ====================================child category=======================================
                should_query.append(
                    {
                        "match": {
                            "categoryList.parentCategory.childCatgory.categoryName."
                            + language: {"query": search_query.replace("%20", " "), "boost": 3}
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "categoryList.parentCategory.childCatgory.categoryName."
                            + language: {"query": search_query.replace("%20", " "), "boost": 3}
                        }
                    }
                )
                # ===============================parent category name======================================
                should_query.append(
                    {
                        "match": {
                            "categoryList.parentCategory.categoryName."
                            + language: {"query": search_query.replace("%20", " "), "boost": 2}
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "categoryList.parentCategory.categoryName."
                            + language: {"query": search_query.replace("%20", " "), "boost": 2}
                        }
                    }
                )
                # ======================================brand name=======================================
                should_query.append(
                    {
                        "match": {
                            "brandTitle."
                            + language: {"query": search_query.replace("%20", " "), "boost": 1}
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "brandTitle."
                            + language: {"query": search_query.replace("%20", " "), "boost": 1}
                        }
                    }
                )

            if max_price != "" and min_price != "":
                query.append(
                    {
                        "range": {
                            "units.b2cPricing.b2cproductSellingPrice": {"gte": float(min_price), "lte": float(max_price)}
                        }
                    }
                )

            if max_cbd != 0 and min_cbd != 0:
                query.append(
                    {"range": {"units.CBD": {"gte": float(min_price), "lte": float(max_price)}}}
                )

            if max_thc != 0 and min_thc != 0:
                query.append(
                    {"range": {"units.THC": {"gte": float(min_price), "lte": float(max_price)}}}
                )

            if size != "":
                size = size.replace("%2C", ",")
                size = size.replace(",", ", ")
                query.append({"match": {"sizes": size.replace("%20", " ")}})

            if colour != "":
                if "," in colour or "%2C" in colour:
                    query.append({"match": {"colour.name": colour.replace("%20", " ")}})
                else:
                    query.append(
                        {"match_phrase_prefix": {"colour.name": colour.replace("%20", " ")}}
                    )

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
                if store_category_id == LIQUOR_STORE_CATEGORY_ID:
                    if fname != "":
                        fname = fname.replace("%2C", ",")
                        category_query = {"categoryName.en": fname.replace("%20", " ")}
                        if store_id != "":
                            category_query["$or"] = [
                                {"storeid": {"$in": [store_id]}, "storeId": {"$in": [store_id]}}
                            ]
                        category_details = db.category.find_one(category_query)
                        if (
                            category_details is not None
                            and store_category_id != ECOMMERCE_STORE_CATEGORY_ID
                        ):
                            query.append(
                                {
                                    "match": {
                                        "categoryList.parentCategory.categoryName."
                                        + language: fname.replace("%20", " ")
                                    }
                                }
                            )
                        else:
                            query.append(
                                {
                                    "match": {
                                        "categoryList.parentCategory.categoryName."
                                        + language: fname.replace("%20", " ")
                                    }
                                }
                            )
                else:
                    if fname != "":
                        fname = fname.replace("%2C", ",")
                        category_query = {"categoryName.en": fname.replace("%20", " ")}
                        if store_id != "":
                            category_query["$or"] = [
                                {"storeid": {"$in": [store_id]}, "storeId": {"$in": [store_id]}}
                            ]
                        category_details = db.category.find_one(category_query)
                        # if category_details is not None and store_category_id != ECOMMERCE_STORE_CATEGORY_ID:
                        #     query.append({
                        #         "match_phrase_prefix": {
                        #             "categoryList.parentCategory.categoryName." + language: fname.replace("%20", " ")
                        #         }
                        #     })
                        # else:
                        #     query.append({
                        #         "match_phrase_prefix": {
                        #             "categoryList.parentCategory.categoryName." + language: fname.replace("%20", " ")
                        #         }
                        #     })
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "categoryList.parentCategory.categoryName."
                                    + language: fname.replace("%20", " ")
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match": {
                                    "categoryList.parentCategory.categoryName."
                                    + language: fname.replace("%20", " ")
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase": {
                                    "categoryList.parentCategory.categoryName."
                                    + language: fname.replace("%20", " ")
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase": {
                                    "categoryList.parentCategory.childCategory.categoryName."
                                    + language: fname.replace("%20", " ")
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match": {
                                    "categoryList.parentCategory.childCategory.categoryName."
                                    + language: fname.replace("%20", " ")
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "categoryList.parentCategory.childCategory.categoryName."
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
                    if store_category_id != LIQUOR_STORE_CATEGORY_ID:
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

            query.append({"match": {"status": 1}})

            if search_query != "":
                filter_parameters_query = {
                    "query": {
                        "bool": {
                            "must": query,
                            "should": should_query,
                            "must_not": [{"match": {"storeId": "0"}}],
                            "minimum_should_match": 1,
                            "boost": 1.0,
                        }
                    },
                    "size": to_data,
                    "from": from_data,
                }
            else:
                if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                    if len(should_query) > 0:
                        filter_parameters_query = {
                            "query": {
                                "bool": {
                                    "must": query,
                                    "should": should_query,
                                    "must_not": [{"match": {"storeId": "0"}}],
                                    "minimum_should_match": 1,
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
                                    "must_not": [{"match": {"storeId": "0"}}],
                                    "minimum_should_match": 1,
                                    "boost": 1.0,
                                }
                            },
                            "size": to_data,
                            "from": from_data,
                        }
                    else:
                        filter_parameters_query = {
                            "query": {
                                "bool": {
                                    "must": query,
                                    "must_not": [{"match": {"storeId": "0"}}],
                                }
                            },
                            "size": to_data,
                            "from": from_data,
                        }

            units_data = []
            size_data = []
            res = {}
            res_filter_parameters = es.search(
                index=index_products,
                body=filter_parameters_query,
                filter_path=[
                    "hits.hits._id",
                    "hits.hits._source",
                ],
            )
            if int(filter_level) == 1:
                if len(res_filter_parameters) == 0:
                    response = {"data": [], "message": "No Data Found"}
                    return JsonResponse(response, safe=False, status=404)

            is_ecommerce, remove_central, hide_recent_view, store_listing = validate_store_category(
                store_category_id, ECOMMERCE_STORE_CATEGORY_ID
            )
            if is_ecommerce == True:
                store_id = ""
                zone_id = ""

            if store_id != "":
                store_details = db.stores.find_one(
                    {"_id": ObjectId(store_id)}, {"currencySymbol": 1, "currencyCode": 1}
                )
                try:
                    currency = store_details["currencyCode"]
                except:
                    pass
                try:
                    currencySymbol = store_details["currencySymbol"]
                except:
                    pass

            # gets all the required data for a particular filter type
            if int(filter_level) == 1:
                attr_data = []
                attr_name = []
                number_attr_name = []
                count = 145
                for i in res_filter_parameters["hits"]["hits"]:
                    count = count - 1
                    supplier_list = []
                    best_supplier = {}
                    child_product_count = db.childProducts.find({"_id": ObjectId(i["_id"])}).count()
                    if child_product_count > 0:
                        best_supplier["productId"] = i["_id"]
                        best_supplier["id"] = str(i["_source"]["storeId"])
                    if len(best_supplier) > 0:
                        child_product_query = {}
                        if len(best_supplier) > 0:
                            child_product_id = best_supplier["productId"]
                        else:
                            child_product_id = i["_id"]
                        child_product_query["_id"] = ObjectId(child_product_id)
                        if (
                            best_supplier["id"] == "0"
                            and store_category_id == ECOMMERCE_STORE_CATEGORY_ID
                        ):
                            child_product_query["storeId"] = best_supplier["id"]
                        elif best_supplier["id"] == "0" or best_supplier["id"] == "":
                            child_product_query["storeId"] = best_supplier["id"]
                        else:
                            child_product_query["storeId"] = ObjectId(best_supplier["id"])
                        child_product_details = db.childProducts.find_one(child_product_query)
                        offers_details = []
                        if child_product_details != None:
                            for ch in i["_source"]["units"][0]["attributes"]:
                                if "attrlist" in ch:
                                    for attr in ch["attrlist"]:
                                        try:
                                            attr_details_db = db.productAttribute.find_one(
                                                {"_id": ObjectId(attr["attributeId"])},
                                                {"measurementUnit": 1, "searchable": 1},
                                            )
                                            if (
                                                attr_details_db["searchable"] == 1
                                                and attr["attriubteType"] != 5
                                            ):
                                                attr_name.append(attr["attrname"]["en"].upper())
                                                value = []
                                                if attr_details_db != None:
                                                    measurement_unit = (
                                                        attr_details_db["measurementUnit"]
                                                        if "measurementUnit" in attr_details_db
                                                        else ""
                                                    )
                                                else:
                                                    measurement_unit = (
                                                        attr_details_db["measurementUnit"]
                                                        if "measurementUnit" in attr_details_db
                                                        else ""
                                                    )
                                                if type(attr["value"]) == dict:
                                                    if attr["value"]["en"] != "":
                                                        if type(attr["value"]["en"]) == list:
                                                            value_data = (
                                                                str(attr["value"]["en"][0])
                                                                + " "
                                                                + measurement_unit
                                                            )
                                                            attr_data.append(
                                                                {
                                                                    "name": attr["attrname"][
                                                                        "en"
                                                                    ].upper(),
                                                                    "data": [
                                                                        value_data.replace(
                                                                            "% %", "%"
                                                                        )
                                                                    ],
                                                                }
                                                            )
                                                        elif type(attr["value"]) == list:
                                                            for v_v in attr["value"]:
                                                                value_data = (
                                                                    str(v_v["value"]["name"]["en"])
                                                                    + " "
                                                                    + measurement_unit
                                                                )
                                                                attr_data.append(
                                                                    {
                                                                        "name": attr["attrname"][
                                                                            "en"
                                                                        ].upper(),
                                                                        "data": [
                                                                            value_data.replace(
                                                                                "% %", "%"
                                                                            )
                                                                        ],
                                                                    }
                                                                )
                                                        else:
                                                            value_data = (
                                                                str(attr["value"]["en"])
                                                                + " "
                                                                + measurement_unit
                                                            )
                                                            attr_data.append(
                                                                {
                                                                    "name": attr["attrname"][
                                                                        "en"
                                                                    ].upper(),
                                                                    "data": [
                                                                        value_data.replace(
                                                                            "% %", "%"
                                                                        )
                                                                    ],
                                                                }
                                                            )
                                                else:
                                                    try:
                                                        for x in attr["value"]:
                                                            if type(x) == int:
                                                                if x != "":
                                                                    value.append(
                                                                        (str(x)).replace("% %", "%")
                                                                        + " "
                                                                        + measurement_unit
                                                                    )
                                                            elif "name" in x:
                                                                if x["name"]["en"] != "":
                                                                    value.append(
                                                                        (
                                                                            str(x["name"]["en"])
                                                                        ).replace("% %", "%")
                                                                        + " "
                                                                        + measurement_unit
                                                                    )
                                                            else:
                                                                if x != "":
                                                                    try:
                                                                        value.append(
                                                                            (
                                                                                str(x.strip(" "))
                                                                            ).replace("% %", "%")
                                                                            + " "
                                                                            + measurement_unit
                                                                        )
                                                                    except:
                                                                        value.append(
                                                                            (str(x)).replace(
                                                                                "% %", "%"
                                                                            )
                                                                            + " "
                                                                            + measurement_unit
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
                                                        pass
                                        except:
                                            pass
                            if "cannabisProductType" in child_product_details["units"][0]:
                                if child_product_details["units"][0]["cannabisProductType"] != "":
                                    cannabis_details = db.cannabisProductType.find_one(
                                        {
                                            "_id": ObjectId(
                                                child_product_details["units"][0][
                                                    "cannabisProductType"
                                                ]
                                            )
                                        }
                                    )
                                    if cannabis_details is not None:
                                        type_data.append(
                                            {
                                                "name": cannabis_details["productType"]["en"],
                                                "id": child_product_details["units"][0][
                                                    "cannabisProductType"
                                                ],
                                            }
                                        )

                            if "offer" in child_product_details:
                                for offer in child_product_details["offer"]:
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
                                        "storeId": best_supplier["id"],
                                    }
                                ).count()
                                if offer_details != 0:
                                    best_offer = best_offer
                                else:
                                    best_offer = {}
                            else:
                                best_offer = {}

                            try:
                                final_price = child_product_details["units"][0]["discountPrice"]
                            except:
                                final_price = child_product_details["units"][0]["floatValue"]
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
                                currencySymbol = currency_details["currencySymbol"]
                                currency = currency_details["currencyCode"]
                            else:
                                currencySymbol = child_product_details["currencySymbol"]
                                currency = child_product_details["currency"]

                            if float(currency_rate) > 0:
                                base_price = final_price * float(currency_rate)
                            else:
                                base_price = final_price
                            try:
                                units_data.append(base_price)
                            except:
                                pass

                            # ======================brand details====================================
                            try:
                                brand_name = (
                                    i["_source"]["brandTitle"][language]
                                    if language in i["_source"]["brandTitle"]
                                    else i["_source"]["brandTitle"]["en"]
                                )
                            except:
                                brand_name = ""

                            if "Fossil  " in brand_name:
                                brand_name = ""
                            # ======================manufacture details====================================
                            try:
                                manufacture_name = i["_source"]["manufactureName"][language]
                            except:
                                try:
                                    manufacture_name = i["_source"]["manufactureName"]["en"]
                                except:
                                    manufacture_name = ""
                            # ==============================currency details=========================

                            if brand_name == "":
                                pass
                            else:
                                brand_list.append({"name": brand_name.upper()})

                            if manufacture_name == "":
                                pass
                            else:
                                manufacture_list.append({"name": manufacture_name.upper()})

                            # ==================================size data====================================
                            try:
                                if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                                    if "unitSizeGroupValue" in i["_source"]["units"][0]:
                                        if len(i["_source"][0]["unitSizeGroupValue"]) > 0:
                                            if (
                                                i["_source"]["units"][0]["unitSizeGroupValue"]["en"]
                                                != ""
                                            ):
                                                size_data.append(
                                                    {
                                                        "name": i["_source"]["units"][0][
                                                            "unitSizeGroupValue"
                                                        ]["en"].upper()
                                                    }
                                                )
                                        else:
                                            pass
                                    else:
                                        pass
                                else:
                                    if "unitSizeGroupValue" in child_product_details["units"][0]:
                                        if len(
                                            child_product_details["units"][0]["unitSizeGroupValue"]
                                        ):
                                            if (
                                                child_product_details["units"][0][
                                                    "unitSizeGroupValue"
                                                ]["en"]
                                                != ""
                                            ):
                                                size_data.append(
                                                    {
                                                        "name": child_product_details["units"][0][
                                                            "unitSizeGroupValue"
                                                        ]["en"].upper()
                                                    }
                                                )
                                        else:
                                            pass
                                    else:
                                        pass
                            except:
                                pass

                            # =========================================colour data==========================
                            try:
                                if "colorName" in child_product_details["units"][0]:
                                    if child_product_details["units"][0]["colorName"]:
                                        if child_product_details["units"][0]["colorName"] != "":
                                            color_list.append(
                                                {
                                                    "name": child_product_details["units"][0][
                                                        "colorName"
                                                    ].upper(),
                                                    "rgb": "rgb("
                                                    + child_product_details["units"][0]["color"]
                                                    + ")",
                                                }
                                            )
                                    else:
                                        pass
                                else:
                                    pass
                            except:
                                pass

                            # units_data.append(best_supplier['retailerPrice'])

                            # ==============================for static data only for meat==================================
                            if "serversFor" in child_product_details:
                                if (
                                    str(child_product_details["serversFor"]) != "0"
                                    and str(child_product_details["serversFor"]) != ""
                                ):
                                    servers_for = (
                                        str(child_product_details["serversFor"]) + " Person"
                                    )
                                else:
                                    servers_for = ""
                            else:
                                servers_for = ""

                            if servers_for != "":
                                serve_data.append({"name": servers_for})

                            if "numberOfPcs" in child_product_details:
                                if (
                                    str(child_product_details["numberOfPcs"]) != "0"
                                    and str(child_product_details["numberOfPcs"]) != ""
                                ):
                                    number_of_pcs = (
                                        str(child_product_details["numberOfPcs"]) + " Pcs"
                                    )
                                else:
                                    number_of_pcs = ""
                            else:
                                number_of_pcs = ""

                            if number_of_pcs != "":
                                number_of_data.append({"name": number_of_pcs})

                            if "Weight" in child_product_details:
                                if (
                                    str(child_product_details["Weight"]) != "0"
                                    and str(child_product_details["Weight"]) != ""
                                ):
                                    Weight = str(child_product_details["Weight"]) + " gms"
                                else:
                                    Weight = ""
                            else:
                                Weight = ""

                            if Weight != "":
                                weight_data.append({"name": Weight})

                            if "symptoms" in i["_source"]:
                                for sym in i["_source"]["symptoms"]:
                                    if sym["symptomName"] != "":
                                        sym_list.append({"name": sym["symptomName"].upper()})
                            else:
                                pass

                            if "categoryList" in child_product_details:
                                for cat in child_product_details["categoryList"]:
                                    if cat["parentCategory"]["categoryName"]["en"] != "":
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

                # =====================================cannbies type====================================================
                if len(type_data) > 0:
                    type_dataframe = pd.DataFrame(type_data)
                    type_dataframe["penCount"] = type_dataframe.groupby("name")["name"].transform(
                        "count"
                    )
                    type_dataframe = type_dataframe.drop_duplicates("name", keep="last")
                    type_list = type_dataframe.to_json(orient="records")
                    type_list = json.loads(type_list)
                    type_list = sorted(type_list, key=lambda k: k["name"])
                else:
                    type_list = []

                # =====================================serve============================================================
                if len(serve_data) > 0:
                    serve_dataframe = pd.DataFrame(serve_data)
                    serve_dataframe["penCount"] = serve_dataframe.groupby("name")["name"].transform(
                        "count"
                    )
                    serve_dataframe = serve_dataframe.drop_duplicates("name", keep="last")
                    serve_list = serve_dataframe.to_json(orient="records")
                    serve_list = json.loads(serve_list)
                    serve_list = sorted(serve_list, key=lambda k: k["name"])
                else:
                    serve_list = []

                # =====================================number of people============================================================
                if len(number_of_data) > 0:
                    number_of_data_dataframe = pd.DataFrame(number_of_data)
                    number_of_data_dataframe["penCount"] = number_of_data_dataframe.groupby("name")[
                        "name"
                    ].transform("count")
                    number_of_data_dataframe = number_of_data_dataframe.drop_duplicates(
                        "name", keep="last"
                    )
                    number_of_data_list = number_of_data_dataframe.to_json(orient="records")
                    number_of_data_list = json.loads(number_of_data_list)
                    number_of_data_list = sorted(number_of_data_list, key=lambda k: k["name"])
                else:
                    number_of_data_list = []

                # =====================================symptoms=========================================================
                if len(sym_list) > 0:
                    sym_dataframe = pd.DataFrame(sym_list)
                    sym_dataframe["penCount"] = sym_dataframe.groupby("name")["name"].transform(
                        "count"
                    )
                    sym_dataframe = sym_dataframe.drop_duplicates("name", keep="last")
                    sym_data = sym_dataframe.to_json(orient="records")
                    sym_data = json.loads(sym_data)
                    sym_data = sorted(sym_data, key=lambda k: k["name"])
                else:
                    sym_data = []
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
                    # brand_list = list(filter(None, brand_list))
                res_color = {}
                for d in attr_data:
                    res_color.setdefault(d["name"], []).append(
                        {"name": d["name"], "data": d["data"]}
                    )
                attribute_data = []
                for attr in list(set(attr_name)):
                    new_data = []
                    try:
                        for i in res_color[attr]:
                            new_data = i["data"] + new_data
                        if len(new_data) > 0:
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

                filters_json = []
                if len(brand_list) > 0:
                    brands = {"name": "BRAND", "data": brand_list, "selType": 1, "filterType": 4}
                    filters_json.append(brands)
                else:
                    pass

                serve_last_json = {
                    "name": "SERVES",
                    "data": serve_list,
                    "selType": 0,
                    "filterType": 4,
                }
                number_of_data_last_json = {
                    "name": "NO. OF PIECES",
                    "data": number_of_data_list,
                    "selType": 0,
                    "filterType": 4,
                }
                if len(sym_data) > 0 and store_category_id != MEAT_STORE_CATEGORY_ID:
                    symptoms = {"name": "SYMPTOMS", "data": sym_data, "selType": 1, "filterType": 9}
                    filters_json.append(symptoms)
                else:
                    pass

                if len(type_list) > 0 and store_category_id == CANNABIS_STORE_CATEGORY_ID:
                    symptoms = {"name": "TYPES", "data": type_list, "selType": 1, "filterType": 11}
                    filters_json.append(symptoms)
                else:
                    pass

                if (
                    len(cat_list) > 0
                    and store_category_id != MEAT_STORE_CATEGORY_ID
                    and platform == 2
                ):
                    category = {
                        # "name": "Categories",
                        "name": "CATEGORIES",
                        "data": cat_list,
                        "selType": 2,
                        "level": 1,
                        "filterType": 1,
                    }
                    filters_json.append(category)
                else:
                    pass

                if len(new_colour_list) > 0 and store_category_id != MEAT_STORE_CATEGORY_ID:
                    colors_data = {
                        "name": "Colour".upper(),
                        "data": new_colour_list,
                        "selType": 4,
                        "filterType": 5,
                    }
                    filters_json.append(colors_data)
                else:
                    pass

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
                    min_price = min(list(set(units_data)))
                except:
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
                    "data": [
                        {
                            "maxPrice": float(round(max_price, 2)),
                            "minPrice": float(round(min_price, 2)),
                        }
                    ],
                    "selType": 3,
                    "filterType": 7,
                    "currency": currency,
                    "currencySymbol": currencySymbol,
                }
                thc_data = {
                    "name": "THC",
                    "data": [{"maxPrice": 100, "minPrice": 0}],
                    "selType": 3,
                    "filterType": 9,
                    "currency": "%",
                    "currencySymbol": "%",
                }
                cbd_data = {
                    "name": "CBD",
                    "data": [{"maxPrice": 100, "minPrice": 0}],
                    "selType": 3,
                    "filterType": 10,
                    "currency": "%",
                    "currencySymbol": "%",
                }
                potency_data = {
                    "name": "POTENCY",
                    "data": [],
                    "selType": 3,
                    "filterType": 10,
                    "currency": "%",
                    "currencySymbol": "%",
                }
                if store_category_id != MEAT_STORE_CATEGORY_ID and len(sizes) > 0:
                    sizes_data = {"name": "SIZE", "data": sizes, "selType": 1, "filterType": 6}
                    filters_json.append(sizes_data)
                else:
                    pass

                if store_category_id == CANNABIS_STORE_CATEGORY_ID:
                    filters_json.append(thc_data)
                    filters_json.append(cbd_data)
                    filters_json.append(potency_data)
                if store_category_id != MEAT_STORE_CATEGORY_ID:
                    filters_json.append(p_data)
                if store_category_id == MEAT_STORE_CATEGORY_ID:
                    filters_json.append(serve_last_json)
                    filters_json.append(number_of_data_last_json)
                for atb in attribute_data:
                    filters_json.append(atb)
                new_list = sorted(filters_json, key=lambda k: k["name"], reverse=False)
                Final_output = {
                    "data": {
                        "filters": new_list,
                        "currency": currency,
                        "currencySymbol": currencySymbol,
                        "unitId": str(ObjectId()),
                        "message": "Filters Found",
                    }
                }

                return JsonResponse(Final_output, safe=False, status=200)

            elif int(filter_level) == 2:
                currencySymbol = "$"
                category_query = {"categoryName.en": fname, "status": 1}
                if store_id != "":
                    category_query["$or"] = [{"storeid": {"$in": [store_id]}}]
                category_details = db.category.find_one(category_query)
                if category_details != None:
                    child_category_query = {
                        "parentId": ObjectId(category_details["_id"]),
                        "status": 1,
                    }
                    if store_id != "":
                        child_category_query["$or"] = [{"storeid": {"$in": [store_id]}}]
                    child_category_details = db.category.find(child_category_query)
                    for child_cat in child_category_details:
                        child_data_count = db.category.find(
                            {"parentId": ObjectId(child_cat["_id"]), "status": 1}
                        ).count()
                        sub_categpry_list.append(child_cat["categoryName"]["en"])

                subcategory = {
                    "name": "subCategories",
                    "data": list(set(sub_categpry_list)),
                    "selType": 2,
                    "level": 2,
                    "filterType": 2,
                }
                filters_json = [subcategory]
                Final_output = {
                    "data": {
                        "filters": filters_json,
                        # "currency": currency,
                        "currencySymbol": currencySymbol,
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


"""
    :type 0 for store admin search
          1 for website or app or central search
          3 for suppliers admin search
"""


class ProductSuggestions(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Product Sugestion"],
        operation_description="API for suggestion the product name based on user search",
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
                default="nike",
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
                name="fname",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="category name of the product..ex. Men, Women",
            ),
            openapi.Parameter(
                name="requestFrom",
                default=0,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example=0,
                description="requestFrom, from which platform the request is coming, 0 for app and 1 for website",
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
                name="s_id",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="need to get suggestion form any particular store",
            ),
            openapi.Parameter(
                name="zoneId",
                default="",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="need to get suggestion form any particular zone",
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
                name="storeCategoryId",
                default=ECOMMERCE_STORE_CATEGORY_ID,
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
            start_time = time.time()
            page = int(request.GET.get("page", 1))  # for the pagination
            should_query = []
            to_data = 10  # page*30
            from_data = int(page * 10) - 10
            token = request.META["HTTP_AUTHORIZATION"]
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            searchItem = request.GET["searchItem"] if "searchItem" in request.GET else ""
            child_product_ids = request.GET["childProductId"] if "childProductId" in request.GET else ""
            store_id = request.GET["storeid"] if "storeid" in request.GET else ""
            if store_id == "":
                store_id = request.GET["s_id"] if "s_id" in request.GET else ""
            zone_id = request.GET["zoneId"] if "zoneId" in request.GET else ""
            if zone_id == "":
                zone_id = request.GET["z_id"] if "z_id" in request.GET else ""
            currency_code = (
                request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            )
            searchItem = searchItem.replace("%20", " ")
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            search_type = request.META["HTTP_TYPE"] if "HTTP_TYPE" in request.META else "2"
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            cart_id = request.GET.get('cartId', "")
            if store_category_id == "0":
                store_category_id = MEAT_STORE_CATEGORY_ID
            filter_query = []
            brand_data_query = []
            request_from = int(request.GET.get("requestFrom", "0"))
            fname = request.GET.get("fname", "")  # category-name
            integration_type = int(request.GET.get("integrationType", 0))  # integrationType
            fname = fname.replace("%20", " ")
            fname = fname.replace("+", " ")
            sname = request.GET.get("sname", "")  # sub-category-name
            sname = sname.replace("%20", " ")
            sname = sname.replace("+", " ")

            tname = request.GET.get("tname", "")  # sub-sub-category-name
            tname = tname.replace("%20", " ")
            tname = tname.replace("+", " ")
            # ========================= add cart products ===================
            cart_product_ids = []
            try:
                card_data = db.cart.find_one({'_id': ObjectId(cart_id)})
                if 'products' in card_data and len(card_data['products']) > 0:
                    cart_product_ids.append(str(card_data['_id']))
            except Exception as e:
                print(e)
                pass
            # =============================================store category configuration=======================
            category_query = {"storeCategory.storeCategoryId": store_category_id}
            if zone_id != "" and store_id == "":
                zone_details = zone_find({"_id": ObjectId(zone_id)})
                category_query["_id"] = ObjectId(zone_details["city_ID"])
            elif store_id != "" and store_id != "0":
                store_details = db.stores.find_one({"_id": ObjectId(store_id)}, {"cityId": 1})
                category_query["_id"] = ObjectId(store_details["cityId"])
            else:
                pass

            categoty_details = db.cities.find_one(category_query, {"storeCategory": 1})
            remove_central = False
            hyperlocal = False
            storelisting = False
            if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                remove_central = False
                hyperlocal = False
                storelisting = False
            else:
                if categoty_details is not None:
                    if "storeCategory" in categoty_details:
                        for cat in categoty_details["storeCategory"]:
                            if cat["storeCategoryId"] == store_category_id:
                                if cat["hyperlocal"] == True and cat["storeListing"] == 1:
                                    remove_central = True
                                    hyperlocal = True
                                    storelisting = True
                                    store_id = store_id
                                elif cat["hyperlocal"] == True and cat["storeListing"] == 0:
                                    hyperlocal = True
                                    storelisting = False
                                    remove_central = True
                                else:
                                    remove_central = False
                                    hyperlocal = False
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

            filter_query.append({"match": {"status": 1}})

            # ==============================store data===========================================
            try:
                if hyperlocal == True and storelisting == False:
                    store_data_details = []
                    if zone_id != "":
                        zone_details = zone_find({"_id": ObjectId(zone_id)})
                        store_query = {
                            "categoryId": str(store_category_id),
                            "serviceZones.zoneId": str(zone_details["_id"]),
                        }
                        store_data = db.stores.find(store_query)
                        store_data_details = ["0"]
                        if store_data.count() > 0:
                            for store in store_data:
                                store_data_details.append(str(store["_id"]))
                    elif store_id != "":
                        store_data_details.append(store_id)
                    else:
                        pass
                    if len(store_data_details) > 0:
                        filter_query.append({"terms": {"storeId": store_data_details}})
                    else:
                        pass
                elif hyperlocal == True and storelisting == True:
                    filter_query.append({"match": {"storeId": store_id}})
                elif hyperlocal == False and storelisting == False:
                    pass
                else:
                    if store_id != "":
                        filter_query.append({"match": {"storeId": store_id}})
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                print(
                    "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                    type(ex).__name__,
                    ex,
                )
                pass
            filter_query.append({"match": {"storeCategoryId": str(store_category_id)}})

            # ===========================product name========================================
            if searchItem != "":
                # ===========================product name========================================
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            f"pPName.{language}": {
                                "analyzer": "standard",
                                "query": searchItem.replace("%20", " "),
                                "boost": 6,
                            }
                        }
                    }
                )
                should_query.append(
                    {"match": {f"pPName.{language}": {"query": searchItem.replace("%20", " "), "boost": 6}}}
                )
                # ===========================unit name========================================
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            f"units.unitName.{language}": {
                                "analyzer": "standard",
                                "query": searchItem.replace("%20", " "),
                                "boost": 5,
                            }
                        }
                    }
                )
                should_query.append(
                    {
                        "match": {
                           f"units.unitName.{language}": {
                                "query": searchItem.replace("%20", " "),
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
                            + language: {"query": searchItem.replace("%20", " "), "boost": 0}
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "detailDescription."
                            + language: {"query": searchItem.replace("%20", " "), "boost": 0}
                        }
                    }
                )
                # ====================================child category=======================================
                should_query.append(
                    {
                        "match": {
                            "categoryList.parentCategory.childCatgory.categoryName."
                            + language: {"query": searchItem.replace("%20", " "), "boost": 3}
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "categoryList.parentCategory.childCategory.categoryName."
                            + language: {"query": searchItem.replace("%20", " "), "boost": 3}
                        }
                    }
                )
                # ===============================parent category name======================================
                should_query.append(
                    {
                        "match": {
                            "categoryList.parentCategory.categoryName."
                            + language: {"query": searchItem.replace("%20", " "), "boost": 2}
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "categoryList.parentCategory.categoryName."
                            + language: {"query": searchItem.replace("%20", " "), "boost": 2}
                        }
                    }
                )
                # ======================================brand name=======================================
                should_query.append(
                    {
                        "match": {
                            "brandTitle."
                            + language: {"query": searchItem.replace("%20", " "), "boost": 1}
                        }
                    }
                )
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "brandTitle."
                            + language: {"query": searchItem.replace("%20", " "), "boost": 1}
                        }
                    }
                )
            aggs_json = {
                "group_by_sub_category": {
                    "terms": {
                        "field": "parentProductId.keyword",
                        "order": {"avg_score": "desc"},
                        "size": int(to_data),
                    },
                    "aggs": {
                        "avg_score": {"max": {"script": "_score"}},
                        "top_sales_hits": {
                            "top_hits": {
                                "sort": [
                                    {"isCentral": {"order": "desc"}},
                                    {"isInStock": {"order": "desc"}},
                                    {"units.discountPrice": {"order": "asc"}},
                                    {"_score": {"order": "desc"}},
                                ],
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
                                        "uploadProductDetails",
                                        "currency",
                                        "firstCategoryName",
                                        "secondCategoryName",
                                        "brandTitle",
                                        "pPName",
                                        "tax",
                                        "brandTitle",
                                        "categoryList",
                                        "images",
                                        "avgRating",
                                        "units",
                                        "storeCategoryId",
                                        "manufactureName",
                                        "productType",
                                        "maxQuantity",
                                    ]
                                },
                                "size": 1,
                            }
                        },
                    },
                }
            }
            must_not = []
            if child_product_ids != "":
                must_not.append({"term":{"_id":child_product_ids}})
            if len(cart_product_ids) > 0:
                must_not.append({"term":{"_id":cart_product_ids}})
            if int(integration_type) == 0:
                pass
            elif int(integration_type) == 1:
                must_not.append({"match": {"magentoId": -1}})
                filter_query.append({"exists": {"field": "magentoId"}})
            elif int(integration_type) == 2:
                must_not.append({"term": {"shopify_variant_id.keyword": {"value": ""}}})
                filter_query.append({"exists": {"field": "shopify_variant_id"}})
            elif int(integration_type) == 3:
                filter_query.append({"match": {"magentoId": -1}})
                filter_query.append({"term": {"shopify_variant_id.keyword": ""}})
            if hyperlocal == False and storelisting == False:
                must_not.append({"match": {"storeId": "0"}})
                
                query = {
                    "query": {
                        "bool": {
                            "must": filter_query,
                            "should": should_query,
                            "minimum_should_match": 1,
                            "boost": 1.0,
                        }
                    },
                    "track_total_hits": True,
                    "sort": [
                        {"isCentral": {"order": "desc"}},
                        {"isInStock": {"order": "desc"}},
                        {"units.discountPrice": {"order": "asc"}},
                        {"_score": {"order": "desc"}},
                    ],
                    "aggs": aggs_json,
                }
            elif hyperlocal == False and storelisting == True:
                must_not.append({"match": {"storeId": "0"}})
                query = {
                    "query": {
                        "bool": {
                            "must": filter_query,
                            "should": should_query,
                            "minimum_should_match": 1,
                            "boost": 1.0,
                        }
                    },
                    "track_total_hits": True,
                    "sort": [
                        {"isCentral": {"order": "desc"}},
                        {"isInStock": {"order": "desc"}},
                        {"units.floatValue": {"order": "asc"}},
                        {"_score": {"order": "desc"}},
                    ],
                    "aggs": aggs_json,
                }
            else:
                query = {
                    "query": {
                        "bool": {
                            "must": filter_query,
                            "should": should_query,
                            "minimum_should_match": 1,
                            "boost": 1.0,
                        }
                    },
                    "track_total_hits": True,
                    "sort": [
                        {"isCentral": {"order": "desc"}},
                        {"isInStock": {"order": "desc"}},
                        {"units.floatValue": {"order": "asc"}},
                        {"_score": {"order": "desc"}},
                    ],
                    "aggs": aggs_json,
                }

            if len(must_not) > 0:
                query["query"]["bool"]["must_not"] = must_not
            res_first_category = es.search(index=index_products, body=query)
            try:
                second_category_name = res_first_category["aggregations"]["group_by_sub_category"][
                    "buckets"
                ][0]["top_sales_hits"]["hits"]["hits"][0]["_source"]["secondCategoryName"]
            except:
                second_category_name = ""

            if second_category_name != "":
                should_query.append(
                    {
                        "more_like_this": {
                            "fields": ["pName.en", "secondCategoryName"],
                            "like": [searchItem.replace("%20", " "), second_category_name],
                            "boost": 1,
                            "min_term_freq": 1,
                            "max_query_terms": 2,
                        }
                    }
                )
            res = es.search(index=index_products, body=query)
            # res["aggregations"]["group_by_sub_category"]["buckets"][0]["top_sales_hits"]["hits"]["hits"][0]["_source"][
            #     "secondCategoryName"]
            productData = []
            categoryData = []
            total_count = []
            try:
                total_product_count = res["hits"]["total"]["value"]
            except:
                total_product_count = res["hits"]["total"]
            if (
                total_product_count > 0 and int(request_from) == 0
            ):  # and store_category_id != ECOMMERCE_STORE_CATEGORY_ID:
                total_count.append(total_product_count)
                if "aggregations" in res:
                    if "buckets" in res["aggregations"]["group_by_sub_category"]:
                        if len(res["aggregations"]["group_by_sub_category"]["buckets"]) > 0:
                            for bucket in res["aggregations"]["group_by_sub_category"]["buckets"]:
                                if "top_sales_hits" in bucket:
                                    if "hits" in bucket["top_sales_hits"]:
                                        if "hits" in bucket["top_sales_hits"]["hits"]:
                                            for i in bucket["top_sales_hits"]["hits"]["hits"]:
                                                tax_price = 0
                                                try:
                                                    best_supplier = {
                                                        "productId": i["_id"],
                                                        "id": i["_source"]["storeId"],
                                                    }
                                                    product_id = best_supplier["productId"]
                                                    tax_details = db.childProducts.find_one(
                                                        {"_id": ObjectId(product_id), "status": 1}
                                                    )
                                                    without_tax_price = ""
                                                    if tax_details is not None:
                                                        try:
                                                            base_price = tax_details["units"][0][
                                                                "b2cPricing"
                                                            ][0]["b2cproductSellingPrice"]
                                                            without_tax_price = tax_details["units"][0][
                                                                "b2cPricing"
                                                            ][0]["b2cproductSellingPrice"]
                                                            with_out_margin_base_price = (
                                                                tax_details["units"][0][
                                                                    "b2cPricing"
                                                                ][0]["b2cpriceWithTax"]
                                                            )
                                                        except:
                                                            try:
                                                                base_price = tax_details["units"][
                                                                    0
                                                                ]["floatValue"]
                                                                with_out_margin_base_price = (
                                                                    tax_details["units"][0][
                                                                        "floatValue"
                                                                    ]
                                                                )
                                                                without_tax_price = tax_details["units"][0]["floatValue"]
                                                            except:
                                                                base_price = 0
                                                                with_out_margin_base_price = 0

                                                        try:
                                                            currency_rate = currency_exchange_rate[
                                                                str(tax_details["currency"])
                                                                + "_"
                                                                + str(currency_code)
                                                            ]
                                                        except:
                                                            currency_rate = 0
                                                        currency_details = db.currencies.find_one(
                                                            {"currencyCode": currency_code}, {"currencySymbol": 1, "currency": 1, "currencyCode":1}
                                                        )
                                                        if currency_details is not None:
                                                            currency_symbol = currency_details[
                                                                "currencySymbol"
                                                            ]
                                                            currency = currency_details[
                                                                "currencyCode"
                                                            ]
                                                        else:
                                                            currency_symbol = tax_details[
                                                                "currencySymbol"
                                                            ]
                                                            currency = tax_details["currency"]

                                                        if float(currency_rate) > 0:
                                                            base_price = base_price * float(
                                                                currency_rate
                                                            )
                                                        else:
                                                            pass

                                                        # ==========================unit data================================================================
                                                        tax_value = []
                                                        attribute_data = []
                                                        if tax_details is not None:
                                                            if type(tax_details["tax"]) == list:
                                                                for tax in tax_details["tax"]:
                                                                    tax_value.append(
                                                                        {"value": tax["taxValue"]}
                                                                    )
                                                            else:
                                                                if tax_details["tax"] is not None:
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

                                                            # ================================offer details===================================
                                                            offers_details = []
                                                            if "offer" in tax_details:
                                                                for offer in tax_details["offer"]:
                                                                    if offer["status"] == 1:
                                                                        offer_terms = db.offers.find_one(
                                                                            {
                                                                                "_id": ObjectId(
                                                                                    offer["offerId"]
                                                                                )
                                                                            }
                                                                        )
                                                                        if offer_terms != None:
                                                                            offer[
                                                                                "termscond"
                                                                            ] = offer_terms[
                                                                                "termscond"
                                                                            ]
                                                                            if offer_terms[
                                                                                "startDateTime"
                                                                            ] <= int(time.time()):
                                                                                offer[
                                                                                    "name"
                                                                                ] = offer[
                                                                                    "offerName"
                                                                                ][
                                                                                    "en"
                                                                                ]
                                                                                offers_details.append(
                                                                                    offer
                                                                                )
                                                                    else:
                                                                        pass
                                                            else:
                                                                pass

                                                            # =============================================end==============================
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
                                                                        "storeId": best_supplier[
                                                                            "id"
                                                                        ],
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
                                                                final_offer = None
                                                            else:
                                                                try:
                                                                    final_offer = {
                                                                        "offerName": best_offer['offerName'][
                                                                            language] if language in best_offer[
                                                                            'offerName'] else best_offer['offerName'][
                                                                            "en"],
                                                                        "offerId": best_offer['offerId'],
                                                                        "offerType": best_offer['offerType'] if 'offerType' in best_offer else 0,
                                                                        "discountType": best_offer['discountType'],
                                                                        "discountValue": best_offer['discountValue']
                                                                    }
                                                                except:
                                                                    final_offer = None
                                                                if "discountType" in best_offer:
                                                                    if (
                                                                        best_offer["discountType"]
                                                                        == 0
                                                                    ):
                                                                        percentage = 0
                                                                        discount_type = 0
                                                                    else:
                                                                        percentage = int(
                                                                            best_offer[
                                                                                "discountValue"
                                                                            ]
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
                                                                discount_value = best_offer[
                                                                    "discountValue"
                                                                ]
                                                            else:
                                                                discount_type = 2

                                                            if base_price == 0 or base_price == "":
                                                                final_price = 0
                                                                discount_price = 0
                                                                final_without_margin_price = 0
                                                            else:
                                                                if int(discount_type) == 0:
                                                                    discount_price = float(
                                                                        discount_value
                                                                    )
                                                                elif int(discount_type) == 1:
                                                                    discount_price = (
                                                                        float(base_price)
                                                                        * float(discount_value)
                                                                    ) / 100
                                                                else:
                                                                    discount_price = 0
                                                                base_price = base_price + (
                                                                    (base_price * tax_price) / 100
                                                                )
                                                                with_out_margin_base_price = with_out_margin_base_price + (
                                                                    (
                                                                        with_out_margin_base_price
                                                                        * tax_price
                                                                    )
                                                                    / 100
                                                                )
                                                                final_price = (
                                                                    base_price - discount_price
                                                                )
                                                                final_without_margin_price = with_out_margin_base_price - discount_price

                                                            final_price_list = {
                                                                "withOutTaxPrice": round(float(without_tax_price), 2),
                                                                "basePrice": round(base_price, 2),
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
                                                            try:
                                                                last_name = i["_source"][
                                                                    "categoryList"
                                                                ][0]["parentCategory"][
                                                                    "categoryName"
                                                                ][
                                                                    "en"
                                                                ]
                                                            except:
                                                                try:
                                                                    last_name = i["_source"][
                                                                        "catName"
                                                                    ]["en"]
                                                                except:
                                                                    last_name = ""
                                                            needs_weighed = (
                                                                tax_details["needsWeighed"]
                                                                if "needsWeighed" in tax_details
                                                                else False
                                                            )

                                                            # ---------------------------attribute details------------------------------
                                                            if (
                                                                type(tax_details["units"][0])
                                                                == list
                                                            ):
                                                                attribute_json = tax_details[
                                                                    "units"
                                                                ][0][0]["attributes"]
                                                            else:
                                                                attribute_json = tax_details[
                                                                    "units"
                                                                ][0]["attributes"]
                                                            for attr in attribute_json:
                                                                for att in attr["attrlist"]:
                                                                    if "linkedtounit" in att:
                                                                        if att["linkedtounit"] == 0:
                                                                            pass
                                                                        else:
                                                                            if att["value"] == None:
                                                                                pass
                                                                            else:
                                                                                if (
                                                                                    "measurementUnit"
                                                                                    in att
                                                                                ):
                                                                                    measurement_unit = att[
                                                                                        "measurementUnit"
                                                                                    ]
                                                                                else:
                                                                                    measurement_unit = (
                                                                                        ""
                                                                                    )
                                                                                try:
                                                                                    image = (
                                                                                        res[
                                                                                            "images"
                                                                                        ][0][
                                                                                            "small"
                                                                                        ]
                                                                                        if "images"
                                                                                        in res
                                                                                        else product[
                                                                                            "image"
                                                                                        ][
                                                                                            0
                                                                                        ][
                                                                                            "medium"
                                                                                        ]
                                                                                    )
                                                                                except:
                                                                                    image = ""
                                                                                try:
                                                                                    attr_value = (
                                                                                        str(
                                                                                            att[
                                                                                                "value"
                                                                                            ][
                                                                                                language
                                                                                            ]
                                                                                        )
                                                                                        + " "
                                                                                        + str(
                                                                                            measurement_unit
                                                                                        )
                                                                                    )
                                                                                except Exception as ex:
                                                                                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                                                                                    message = template.format(
                                                                                        type(
                                                                                            ex
                                                                                        ).__name__,
                                                                                        ex.args,
                                                                                    )
                                                                                    attr_value = ""
                                                                                attribute_data.append(
                                                                                    {
                                                                                        "attrname": att[
                                                                                            "attrname"
                                                                                        ][
                                                                                            language
                                                                                        ]
                                                                                        if language
                                                                                        in att[
                                                                                            "attrname"
                                                                                        ]
                                                                                        else att[
                                                                                            "attrname"
                                                                                        ]["en"],
                                                                                        "value": attr_value,
                                                                                        "name": att[
                                                                                            "attrname"
                                                                                        ][language]
                                                                                        if language
                                                                                        in att[
                                                                                            "attrname"
                                                                                        ]
                                                                                        else att[
                                                                                            "attrname"
                                                                                        ]["en"],
                                                                                    }
                                                                                )

                                                            try:
                                                                product_name = (
                                                                    tax_details["pName"][language]
                                                                    if language
                                                                    in tax_details["pName"]
                                                                    else tax_details["pName"][
                                                                        language
                                                                    ]
                                                                )
                                                                # product_name = tax_details['units'][0]["unitName"][language] if language in tax_details['units'][0]["unitName"] else tax_details['pName']["en"]
                                                            except:
                                                                product_name = ""
                                                            if (
                                                                len(best_supplier) > 0
                                                                and product_name != ""
                                                                and last_name != ""
                                                            ):
                                                                productData.append(
                                                                    {
                                                                        "score": bucket[
                                                                            "avg_score"
                                                                        ]["value"],
                                                                        "isProduct": False,
                                                                        "productBestOffer": final_offer,
                                                                        "productId": str(
                                                                            tax_details[
                                                                                "parentProductId"
                                                                            ]
                                                                        ),
                                                                        "parentProductId": str(
                                                                            tax_details[
                                                                                "parentProductId"
                                                                            ]
                                                                        ),
                                                                        "variantData": attribute_data,
                                                                        "childProductId": product_id,
                                                                        "productName": product_name,
                                                                        "images": tax_details[
                                                                            "images"
                                                                        ],
                                                                        "currencySymbol": currency_symbol,
                                                                        "currency": currency,
                                                                        "storeType": i["_source"][
                                                                            "storeType"
                                                                        ]
                                                                        if "storeType"
                                                                        in i["_source"]
                                                                        else "8",
                                                                        "needsWeighed": needs_weighed,
                                                                        "catName": "",
                                                                        "finalPriceList": final_price_list,
                                                                        "brandTitle": i["_source"][
                                                                            "brandTitle"
                                                                        ][language]
                                                                        if language
                                                                        in i["_source"][
                                                                            "brandTitle"
                                                                        ]
                                                                        else i["_source"][
                                                                            "brandTitle"
                                                                        ]["en"],
                                                                        "productType": i["_source"][
                                                                            "productType"
                                                                        ]
                                                                        if "productType"
                                                                        in i["_source"]
                                                                        else 1,
                                                                        "subCatName": "",
                                                                        "subSubCatName": "",
                                                                        "seqId": 2,
                                                                        "inSection": last_name,
                                                                    }
                                                                )
                                                except Exception as ex:
                                                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                                                    message = template.format(
                                                        type(ex).__name__, ex.args
                                                    )
                                                    print(
                                                        "Error on line {}".format(
                                                            sys.exc_info()[-1].tb_lineno
                                                        ),
                                                        type(ex).__name__,
                                                        ex,
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
            elif (
                total_product_count > 0 and int(request_from) == 1
            ):  # and store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                total_count.append(total_product_count)
                if "aggregations" in res:
                    if "buckets" in res["aggregations"]["group_by_sub_category"]:
                        if len(res["aggregations"]["group_by_sub_category"]["buckets"]) > 0:
                            for bucket in res["aggregations"]["group_by_sub_category"]["buckets"]:
                                if "top_sales_hits" in bucket:
                                    if "hits" in bucket["top_sales_hits"]:
                                        if "hits" in bucket["top_sales_hits"]["hits"]:
                                            for i in bucket["top_sales_hits"]["hits"]["hits"]:
                                                try:
                                                    product_name = (
                                                        i["_source"]["pName"][language]
                                                        if language in i["_source"]["pName"]
                                                        else i["_source"]["pName"][language]
                                                    )
                                                except:
                                                    product_name = ""
                                                if "firstCategoryName" in i["_source"]:
                                                    category_name = i["_source"][
                                                        "firstCategoryName"
                                                    ]
                                                    second_category_name = (
                                                        i["_source"]["secondCategoryName"]
                                                        if "secondCategoryName" in i["_source"]
                                                        else ""
                                                    )
                                                    if second_category_name != "":
                                                        # categoryData.append(category_name+"'"+"s "+second_category_name)
                                                        categoryData.append(category_name)
                                                    elif category_name != "":
                                                        categoryData.append(category_name)
                                                else:
                                                    pass

                                                if "brandTitle" in i["_source"]:
                                                    category_name = (
                                                        i["_source"]["firstCategoryName"]
                                                        if "firstCategoryName" in i["_source"]
                                                        else ""
                                                    )
                                                    second_category_name = (
                                                        i["_source"]["secondCategoryName"]
                                                        if "secondCategoryName" in i["_source"]
                                                        else ""
                                                    )
                                                    brand_name = i["_source"]["brandTitle"][
                                                        language
                                                    ]
                                                    if second_category_name != "":
                                                        if brand_name != "":
                                                            # string = brand_name + " "+ second_category_name
                                                            # categoryData.append(" ".join(string.split()))
                                                            categoryData.append(brand_name)
                                                        else:
                                                            pass
                                                    elif category_name != "":
                                                        if brand_name != "":
                                                            # string = brand_name + " "+ category_name
                                                            # categoryData.append(" ".join(string.split()))
                                                            categoryData.append(brand_name)
                                                        else:
                                                            pass
                                                    else:
                                                        if brand_name != "":
                                                            categoryData.append(brand_name)
                                                        else:
                                                            pass
                                                else:
                                                    pass

                                                if product_name != "":
                                                    productData.append(
                                                        {
                                                            "score": bucket["avg_score"]["value"],
                                                            "isProduct": False,
                                                            "productId": "",
                                                            "variantData": [],
                                                            "childProductId": "",
                                                            "parentProductId": "",
                                                            "productName": product_name,
                                                            "images": [],
                                                            "catName": "",
                                                            "brandTitle": i["_source"][
                                                                "brandTitle"
                                                            ][language],
                                                            "productType": i["_source"][
                                                                "productType"
                                                            ]
                                                            if "productType" in i["_source"]
                                                            else 1,
                                                            "subCatName": "",
                                                            "subSubCatName": "",
                                                            "seqId": 2,
                                                            "inSection": "",
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
            else:
                pass

            if len(productData) > 0:
                # newlist = sorted(product_list, key=lambda k: k['score'], reverse=True)
                dataframe = pd.DataFrame(productData)
                dataframe = dataframe.drop_duplicates(subset="productName", keep="last")
                product_list = dataframe.to_dict(orient="records")
                newlist = sorted(product_list, key=lambda k: k["score"], reverse=True)
                finalSuggestions = {
                    "data": {
                        "penCount": max(total_count),
                        "data": newlist,
                        "text": "Popular Products",
                        "message": "Product Suggestions Found",
                    },
                    "categoryData": {
                        "data": list(set(categoryData))[0:10],
                        "text": "Categories and brands",
                        "message": "Product Suggestions Found",
                    },
                }

                return JsonResponse(finalSuggestions, safe=False, status=200)
            else:
                error = {"data": [], "message": "No Products Found"}
                return JsonResponse(error, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)
