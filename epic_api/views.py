# Create your views here.
import os
from pickle import FALSE
import sys
import asyncio, requests

from cassandra.query import ValueSequence

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from bson.objectid import ObjectId
from django.http import JsonResponse
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.decorators import action
from rest_framework.views import APIView
from search_api.settings import (db, ECOMMERCE_STORE_CATEGORY_ID, es, CHILD_PRODUCT_INDEX, CENRAL_STORE_NAME,
                                 DINE_STORE_CATEGORY_ID, STORE_PRODUCT_INDEX, STORE_PRODUCT_DOC_TYPE, conv_fac, session,
                                 SWAGGER_URL, PYTHON_PRODUCT_URL)
from search.views import home_units_data
import pandas as pd
import json
import time
from validations.product_best_offer_redis import product_get_best_offer_data, product_best_offer_data
from validations.product_variant_validation import validate_variant
from search.views import get_linked_unit_attribute
from validations.combo_special_validation import combo_special_type_validation
import threading
from search.views import category_search_logs
from epic_api.utils import *

index_products = CHILD_PRODUCT_INDEX
central_store = CENRAL_STORE_NAME

index_store = STORE_PRODUCT_INDEX
doc_type_store = STORE_PRODUCT_DOC_TYPE

Ar = u"\u2192"

'''
    Function for the search from the central
    Function for the modify data based on request
    :res ---> data which we are getting from es
    : language ---> return data in particular language
'''


def product_search_data_new(res, start_time, language, filter_responseJson,
                            finalfilter_responseJson_products, popularstatus, sort, login_type,
                            store_id, sort_type, store_category_id, from_data,
                            to_data, user_id, remove_central, zone_id, min_price, max_price,
                            margin_price, currency_code, search_query, token):
    resData = []
    product_tag = ""
    for i in res["hits"]['hits']:
        try:
            start_time = time.time()
            best_supplier = {}
            score = i['_score'] if i['_score'] is not None else 0
            variant_data = []
            available_qty = i['_source']['units'][0]['availableQuantity'] if "availableQuantity" in \
                                                                             i['_source']['units'][0] else 0
            if available_qty == 0:
                try:
                    available_qty = i['_source']['units'][0]['availableQuantity'] if "availableQuantity" in \
                                                                                     i['_source']['units'][0][
                                                                                         'seller'] else 0
                except:
                    available_qty = 0
            if available_qty == "":
                available_qty = 0
            best_supplier['productId'] = i['_id']
            best_supplier['id'] = i['_source']['storeId']
            best_supplier['retailerQty'] = available_qty

            if len(best_supplier) > 0:
                if len(best_supplier) > 0:
                    child_product_id = best_supplier['productId']
                else:
                    child_product_id = i['_id']

                if int(login_type) == 1 or int(login_type) != 2:
                    if "availableQuantity" in i['_source']['units'][0]:
                        qty = int(i['_source']['units'][0]['availableQuantity'])
                    else:
                        qty = 0

                    if qty == "":
                        qty = 0
                    if qty > 0:
                        outOfStock = False
                    else:
                        outOfStock = True
                else:
                    if best_supplier['distributorQty'] != 0:
                        outOfStock = False
                    else:
                        outOfStock = True

                if "allowOrderOutOfStock" in i['_source']:
                    allow_order_out_of_stock = i['_source']['allowOrderOutOfStock']
                else:
                    allow_order_out_of_stock = False

                if "b2cpackingNoofUnits" in i['_source']:
                    if type(i['_source']['b2cpackingNoofUnits']) == int:
                        b2c_packing_no_units = i['_source']['b2cpackingNoofUnits']
                    else:
                        b2c_packing_no_units = 1
                else:
                    b2c_packing_no_units = 1

                if "b2cpackingPackageUnits" in i['_source']:
                    if "en" in i['_source']['b2cpackingPackageUnits']:
                        b2c_packing_package_units = i['_source']['b2cpackingPackageUnits']['en']
                    else:
                        b2c_packing_package_units = ""
                else:
                    b2c_packing_package_units = ""

                if "b2cpackingPackageType" in i['_source']:
                    if "en" in i['_source']['b2cpackingPackageType']:
                        b2c_packing_units = i['_source']['b2cpackingPackageType']['en']
                    else:
                        b2c_packing_units = ""
                else:
                    b2c_packing_units = ""

                if "containsMeat" in i['_source']:
                    contains_Meat = i['_source']['containsMeat']
                else:
                    contains_Meat = False

                try:
                    if b2c_packing_package_units != "" and b2c_packing_units != "":
                        mou_data = str(
                            b2c_packing_no_units) + " " + b2c_packing_package_units + " per " + b2c_packing_units
                    elif b2c_packing_package_units == "" and b2c_packing_units != "":
                        mou_data = str(b2c_packing_no_units) + " " + b2c_packing_package_units
                    else:
                        mou_data = ""
                except:
                    mou_data = ""

                    # ========================= for the get the linked the unit data====================================
                if type(i['_source']['units'][0]) == list:
                    attribute_list = i['_source']['units'][0][0]['attributes']
                else:
                    attribute_list = i['_source']['units'][0]['attributes']
                for link_unit in attribute_list:
                    if "attrlist" in link_unit:
                        for attrlist in link_unit['attrlist']:
                            if attrlist is None:
                                pass
                            else:
                                if type(attrlist) == str:
                                    pass
                                else:
                                    try:
                                        if attrlist['linkedtounit'] == 1:
                                            if attrlist['measurementUnit'] == "":
                                                attr_name = attrlist['value'][language] if language in attrlist[
                                                    'value'] else attrlist['value']['en']
                                            else:
                                                attr_name = attrlist['value'][language] if language in attrlist[
                                                    'value'] else attrlist['value']['en'] + " " + attrlist[
                                                    'measurementUnit']
                                            variant_data.append(
                                                {
                                                    "value": str(attr_name)
                                                }
                                            )
                                        else:
                                            pass
                                    except:
                                        pass
                    else:
                        pass
                    # ======================================product seo======================================================
                if "productSeo" in i['_source']:
                    if len(i['_source']['productSeo']) > 0:
                        try:
                            if len(i['_source']['productSeo']['title']) > 0:
                                title = i['_source']['productSeo']['title'][language] if language in i['_source'][
                                    'productSeo']['title'] else i['_source']['productSeo']['title']["en"]
                            else:
                                title = ""
                        except:
                            title = ""

                        try:
                            if len(i['_source']['productSeo']['description']) > 0:
                                description = i['_source']['productSeo']['description'][language] if language in \
                                                                                                     i['_source'][
                                                                                                         'productSeo'][
                                                                                                         'description'] else \
                                    i['_source']['productSeo']['description']["en"]
                            else:
                                description = ""
                        except:
                            description = ""

                        try:
                            if len(i['_source']['productSeo']['metatags']) > 0:
                                metatags = i['_source']['productSeo']['metatags'][language] if language in \
                                                                                               i['_source'][
                                                                                                   'productSeo'][
                                                                                                   'metatags'] else \
                                    i['_source']['productSeo']['metatags']["en"]
                            else:
                                metatags = ""
                        except:
                            metatags = ""

                        try:
                            if len(i['_source']['productSeo']['slug']) > 0:
                                slug = i['_source']['productSeo']['slug'][language] if language in i['_source'][
                                    'productSeo']['slug'] else i['_source']['productSeo']['slug']["en"]
                            else:
                                slug = ""
                        except:
                            slug = ""

                        product_seo = {
                            "title": title,
                            "description": description,
                            "metatags": metatags,
                            "slug": slug
                        }
                    else:
                        product_seo = {
                            "title": "",
                            "description": "",
                            "metatags": "",
                            "slug": ""
                        }
                else:
                    product_seo = {
                        "title": "",
                        "description": "",
                        "metatags": "",
                        "slug": ""
                    }

                if len(best_supplier) > 0:
                    if "productId" in best_supplier:
                        product_id = best_supplier['productId']
                    else:
                        product_id = i["_id"]
                else:
                    product_id = i["_id"]

                    # ===============================offer data======================================
                best_offer = product_get_best_offer_data(str(i['_id']), zone_id)
                if len(best_offer) == 0:
                    best_offer = product_best_offer_data(str(i['_id']))
                else:
                    pass
                # ===============================variant data=================================================
                if type(best_supplier['id']) == str:
                    best_supplier_id = best_supplier['id']
                else:
                    best_supplier_id = best_supplier['id'][0]
                variant_count = validate_variant(
                    str(i['_source']['parentProductId']),
                    best_supplier_id, zone_id, store_category_id)
                # ======================for tax ================================================================
                tax_value = []
                if i['_source'] is not None:
                    if type(i['_source']['tax']) == list:
                        for tax in i['_source']['tax']:
                            tax_value.append({"value": tax['taxValue']})
                    else:
                        if i['_source']['tax'] is not None:
                            if "taxValue" in i['_source']['tax']:
                                tax_value.append({"value": i['_source']['tax']['taxValue']})
                            else:
                                tax_value.append({"value": i['_source']['tax']})
                        else:
                            pass
                else:
                    tax_value = []

                store_details = db.stores.find_one({"_id": ObjectId(best_supplier_id)})
                store_count = 0
                if store_details is None:
                    best_supplier['storeName'] = central_store
                    best_supplier['storeAliasName'] = central_store
                    best_supplier['storeFrontTypeId'] = 0
                    best_supplier['storeFrontType'] = "Central"
                    best_supplier['storeTypeId'] = 0
                    best_supplier['storeType'] = "Central"
                    best_supplier['cityName'] = "Banglore"
                    best_supplier['areaName'] = "Hebbal"
                    best_supplier['sellerType'] = "Central"
                    best_supplier['sellerTypeId'] = 1
                    best_supplier['rating'] = 0
                else:
                    if "businessLocationAddress" in store_details:
                        if "addressArea" in store_details['businessLocationAddress']:
                            area_name = store_details['businessLocationAddress']['addressArea']
                        else:
                            area_name = "Hebbal"
                    else:
                        area_name = "Hebbal"

                    best_supplier['storeName'] = store_details['storeName'][language] if language in store_details[
                        'storeName'] else store_details['storeName']['en']
                    best_supplier['storeAliasName'] = store_details[
                        'storeAliasName'] if "storeAliasName" in store_details else store_details['storeName']['en']
                    best_supplier['storeFrontTypeId'] = store_details['storeFrontTypeId']
                    best_supplier['storeFrontType'] = store_details['storeFrontType']
                    best_supplier['storeTypeId'] = store_details['storeTypeId']
                    best_supplier['storeType'] = store_details['storeType']
                    best_supplier['cityName'] = store_details['cityName'] if 'cityName' in store_details else "Banglore"
                    best_supplier['areaName'] = area_name
                    best_supplier['sellerType'] = store_details['sellerType']
                    best_supplier['sellerTypeId'] = store_details['sellerTypeId']
                    best_supplier['rating'] = store_details['avgRating'] if "avgRating" in store_details else 0

                if len(i['_source']["images"]) > 0:
                    image_data = i['_source']["images"]
                else:
                    image_data = [
                        {
                            "extraLarge": "",
                            "medium": "",
                            "altText": "",
                            "large": "",
                            "small": ""
                        }
                    ]
                if "modelImage" in i['_source']["units"][0]:
                    if len(i['_source']["units"][0]['modelImage']) > 0:
                        model_data = i['_source']["units"][0]['modelImage']
                    else:
                        model_data = [
                            {
                                "extraLarge": "",
                                "medium": "",
                                "altText": "",
                                "large": "",
                                "small": ""
                            }
                        ]
                else:
                    model_data = [
                        {
                            "extraLarge": "",
                            "medium": "",
                            "altText": "",
                            "large": "",
                            "small": ""
                        }
                    ]
                if i["_source"]['storeCategoryId'] == ECOMMERCE_STORE_CATEGORY_ID:
                    isShoppingList = True
                else:
                    shoppinglist_product = db.userShoppingList.find(
                        {"userId": user_id, "products.centralProductId": i["_source"]['parentProductId'],
                         "products.childProductId": product_id})

                    if shoppinglist_product.count() == 0:
                        isShoppingList = False
                    else:
                        isShoppingList = True

                # =========================================pharmacy details=========================================
                if "prescriptionRequired" in i['_source']:
                    if i['_source']["prescriptionRequired"] == 0:
                        prescription_required = False
                    else:
                        prescription_required = True
                else:
                    prescription_required = False

                if "needsIdProof" in i['_source']:
                    if not i['_source']["needsIdProof"]:
                        needsIdProof = False
                    else:
                        needsIdProof = True
                else:
                    needsIdProof = False

                if "saleOnline" in i['_source']:
                    if i['_source']["saleOnline"] == 0:
                        sales_online = False
                    else:
                        sales_online = True
                else:
                    sales_online = False

                if "uploadProductDetails" in i['_source']:
                    upload_details = i['_source']["uploadProductDetails"]
                else:
                    upload_details = ""
                    # ==================================================================================================

                    # =========================for max quantity=================================================
                if "maxQuantity" in i['_source']:
                    if i['_source']['maxQuantity'] != "":
                        max_quantity = int(i['_source']['maxQuantity'])
                    else:
                        max_quantity = 30
                else:
                    max_quantity = 30

                # ==========================================================================================
                if "addOns" in i['_source']["units"][0]:
                    if len(i['_source']["units"][0]['addOns']) > 0:
                        addons_count = True
                    else:
                        addons_count = False
                else:
                    addons_count = False

                # ================================currency=================================================
                currency_symbol = i['_source']['currencySymbol']
                currency = i['_source']['currency']
                best_supplier['currency'] = currency
                best_supplier['currencySymbol'] = currency_symbol
                try:
                    best_supplier['retailerPrice'] = i['_source']["units"][0]['floatValue']
                except:
                    best_supplier['retailerPrice'] = i['_source']["units"][0][0]['floatValue']
                # avg_rating = i['_source']['avgRating'] if "avgRating" in i['_source'] else 0
                avg_rating = 0
                product_rating = db.reviewRatings.aggregate(
                    [
                        {"$match": {
                            "productId": str(i['_source']['parentProductId']),
                            "rating": {"$ne": 0}
                        }},
                        {
                            "$group":
                                {
                                    "_id": "$productId",
                                    "avgRating": {"$avg": "$rating"}
                                }
                        }
                    ]
                )
                for avg_rat in product_rating:
                    avg_rating = avg_rat['avgRating']
                try:
                    product_name = i['_source']['pName'][language] if language in i['_source']['pName'] else \
                        i['_source']['pName']["en"]
                except:
                    product_name = i['_source']['pPName'][language] if language in i['_source'][
                        'pPName'] else i['_source']['pPName']["en"]
                try:
                    linked_attribute = get_linked_unit_attribute(i['_source']['units'])
                except:
                    linked_attribute = []

                # ==================================get currecny rate============================
                try:
                    currency_rate = currency_exchange_rate[str(currency) + "_" + str(currency_code)]
                except:
                    currency_rate = 0
                currency_details = db.currencies.find_one({"currencyCode": currency_code})
                if currency_details is not None:
                    currency_symbol = currency_details['currencySymbol']
                    currency = currency_details['currencyCode']
                product_type = combo_special_type_validation(product_id)
                child_product_details = db.childProducts.find_one({"_id": ObjectId(product_id)}, {"units": 1})
                resData.append(
                    {"childProductId": product_id, "score": score, "isShoppingList": isShoppingList,
                     "maxQuantity": max_quantity, "linkedAttribute": linked_attribute, "productName": product_name,
                     "brandName": i['_source']['brandTitle'][language]
                     if language in i['_source']["brandTitle"] else "",
                     "manufactureName": i['_source']['manufactureName'][language]
                     if language in i['_source']["manufactureName"] else "",
                     "parentProductId": i["_source"]['parentProductId'],
                     "productSeo": product_seo, "TotalStarRating": avg_rating,
                     "storeCategoryId": i['_source']['storeCategoryId']
                     if "storeCategoryId" in i['_source'] else "",
                     "prescriptionRequired": prescription_required, "needsIdProof": needsIdProof,
                     "modelImage": model_data,
                     "productType": product_type,
                     "saleOnline": sales_online, "currencyRate": currency_rate, "containsMeat": contains_Meat,
                     "uploadProductDetails": upload_details, "sizes": [],
                     "productTag": product_tag, "tax": tax_value, "variantData": variant_data,
                     "variantCount": variant_count, "addOnsCount": addons_count, "currencySymbol": currency_symbol,
                     "currency": currency, "mobileImage": image_data, "mouDataUnit": mou_data,
                     "units": child_product_details['units'] if child_product_details is not None else i['_source'][
                         "units"],
                     "unitId": product_id,  # i['_source']["units"][0]['unitId'],
                     "allowOrderOutOfStock": allow_order_out_of_stock, "moUnit": "Pcs", "offer": best_offer,
                     "popularstatus": popularstatus, "suppliers": best_supplier, "outOfStock": outOfStock,
                     "storeCount": store_count})
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            pass

    if len(resData) > 0:
        dataframe = pd.DataFrame(resData)
        dataframe["popularstatus"] = popularstatus
        dataframe["unitsData"] = dataframe.apply(home_units_data, lan=language, sort=sort,
                                                 status=1, axis=1, logintype=login_type,
                                                 store_category_id=store_category_id, margin_price=margin_price,
                                                 city_id="")
        # dataframe = dataframe.drop_duplicates("childProductId", keep="first")
        details = dataframe.to_json(orient="records")
        data = json.loads(details)
        for k in data:
            if k["unitsData"]['basePrice'] == 0:
                pass
            else:
                try:
                    base_price = k["unitsData"]['basePrice']
                    final_price = k["unitsData"]['finalPrice']
                    discount_price = k["unitsData"]['discountPrice']
                    if store_id != "":
                        outOfStock = k['unitsData']['outOfStock']
                    else:
                        outOfStock = k['unitsData']['outOfStock']
                    availableQuantity = int(k['unitsData']['availableQuantity'])
                    mou = ""
                    mou_unit = ""
                    minimum_qty = 0
                except:
                    base_price = 0
                    final_price = 0
                    discount_price = 0
                    availableQuantity = 0
                    outOfStock = True
                    minimum_qty = 0
                    mou = None
                    mou_unit = None

                # discount_price = 0
                if len(k['offer']) == 0:
                    percentage = 0
                    discount_type = 0
                else:
                    if "discountType" in k['offer']:
                        if k['offer']['discountType'] == 0:
                            percentage = 0
                            discount_type = 0
                        else:
                            percentage = int(k['offer']['discountValue'])
                            discount_type = k['offer']['discountType']
                    else:
                        percentage = 0
                        discount_type = 0

                filter_responseJson.append(
                    {
                        "outOfStock": outOfStock,
                        "score": k['score'] if "score" in k else 0,
                        "childProductId": k["childProductId"],
                        "productName": k["productName"],
                        "brandName": k["brandName"],
                        "manufactureName": k["manufactureName"],
                        "isShoppingList": k["isShoppingList"],
                        "unitId": k['unitId'],
                        "storeCategoryId": k['storeCategoryId'],
                        "productType": k['productType'] if "productType" in k else 1,
                        "linkedAttribute": k['linkedAttribute'],
                        "parentProductId": k["parentProductId"],
                        "TotalStarRating": k['TotalStarRating'],
                        "offers": k['offer'],
                        "mouDataUnit": k['mouDataUnit'],
                        "variantCount": k['variantCount'],
                        "productTag": k['productTag'],
                        "maxQuantity": k['maxQuantity'],
                        "availableQuantity": availableQuantity,
                        "images": k["mobileImage"],
                        "addOnsCount": k["addOnsCount"],
                        "productSeo": k['productSeo'],
                        "containsMeat": k['containsMeat'],
                        "variantData": k['variantData'],
                        "allowOrderOutOfStock": k['allowOrderOutOfStock'],
                        "moUnit": "Pcs",
                        "discountPrice": discount_price,
                        "discountType": discount_type,
                        "price": final_price,
                        "finalPriceList": {
                            "basePrice": round(base_price, 2),
                            "finalPrice": round(final_price, 2),
                            "discountPrice": round(discount_price, 2),
                            "discountPercentage": percentage
                        },
                        "currencySymbol": k["currencySymbol"],
                        "currency": k["currency"],
                        "supplier": k["suppliers"],
                        "prescriptionRequired": k["prescriptionRequired"],
                        "needsIdProof": k["needsIdProof"] if "needsIdProof" in k else False,
                        "modelImage": k["modelImage"] if "modelImage" in k else [],
                        "saleOnline": k["saleOnline"],
                        "uploadProductDetails": k["uploadProductDetails"],
                        "storeCount": k["storeCount"],
                        "mouData": {
                            "mou": mou,
                            "mouUnit": mou_unit,
                            "mouQty": minimum_qty,
                            "minimumPurchaseUnit": k['unitsData']['minimumPurchaseUnit'] if "minimumPurchaseUnit" in k[
                                'unitsData'] else ""
                        }
                    }
                )
        if int(sort_type) == 0:
            newlist = sorted(filter_responseJson, key=lambda k: k['price'], reverse=False)
        elif int(sort_type) == 1:
            newlist = sorted(filter_responseJson, key=lambda k: k['price'], reverse=True)
        else:
            if search_query == "":
                newlist = sorted(filter_responseJson, key=lambda k: k['availableQuantity'], reverse=True)
            else:
                newlist = sorted(filter_responseJson, key=lambda k: k['score'], reverse=True)
        try:
            if "value" in res["hits"]["total"]:
                pen_count = res["hits"]["total"]['value']
            else:
                pen_count = res["hits"]["total"]
        except:
            pen_count = res["hits"]["total"]
        if pen_count > 20:
            pen_count = pen_count
        else:
            pen_count = len(newlist)
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


def product_search_read(
        res,
        start_time,
        language,
        filter_responseJson,
        finalfilter_responseJson_products,
        popularstatus,
        sort, login_type, store_id, sort_type,
        store_category_id, from_data, to_data, user_id, remove_central, zone_id, min_price,
        max_price, margin_price, currency_code, search_query, token):
    try:
        if len(res) <= 0:
            error = {"data": [], "message": "No Products Found"}
            return error
        else:
            last_data = product_search_data_new(res, start_time, language, filter_responseJson,
                                                finalfilter_responseJson_products, popularstatus, sort, login_type,
                                                store_id, sort_type, store_category_id, from_data, to_data,
                                                user_id, remove_central, zone_id, min_price, max_price,
                                                margin_price, currency_code, search_query, token)
            finalSearchResults = {
                "data": last_data,
                "message": "Got the details",
            }
            return finalSearchResults
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print('Error on line {}'.format(sys.exc_info()
                                        [-1].tb_lineno), type(ex).__name__, ex)
        finalResponse = {
            "message": message,
            "data": []
        }
        return JsonResponse(finalResponse, safe=False, status=500)


'''
    Function for the search from the central
    Function for the modify data based on request
    :res ---> data which we are getting from es
    : language ---> return data in particular language
'''


def store_search_data_new(res, start_time, language, filter_responseJson,
                          finalfilter_responseJson_products, popularstatus, sort, login_type,
                          store_id, sort_type, store_category_id, from_data,
                          to_data, user_id, remove_central, zone_id, min_price, max_price,
                          margin_price, currency_code, search_query, token):
    resData = []
    product_tag = ""
    for i in res["hits"]['hits']:
        try:
            start_time = time.time()
            best_supplier = {}
            score = 0
            variant_data = []
            available_qty = i['_source']['units'][0]['availableQuantity'] if "availableQuantity" in \
                                                                             i['_source']['units'][0] else 0
            if available_qty == 0:
                try:
                    available_qty = i['_source']['units'][0]['availableQuantity'] if "availableQuantity" in \
                                                                                     i['_source']['units'][0][
                                                                                         'seller'] else 0
                except:
                    available_qty = 0
            if available_qty == "":
                available_qty = 0
            best_supplier['productId'] = i['_id']
            best_supplier['id'] = i['_source']['storeId']
            best_supplier['retailerQty'] = available_qty
            # else:
            #     pass

            if len(best_supplier) > 0:
                if len(best_supplier) > 0:
                    child_product_id = best_supplier['productId']
                else:
                    child_product_id = i['_id']

                if int(login_type) == 1 or int(login_type) != 2:
                    if "availableQuantity" in i['_source']['units'][0]:
                        qty = int(i['_source']['units'][0]['availableQuantity'])
                    else:
                        qty = 0

                    if qty == "":
                        qty = 0
                    if qty > 0:
                        outOfStock = False
                    else:
                        outOfStock = True
                else:
                    if best_supplier['distributorQty'] != 0:
                        outOfStock = False
                    else:
                        outOfStock = True

                if "allowOrderOutOfStock" in i['_source']:
                    allow_order_out_of_stock = i['_source']['allowOrderOutOfStock']
                else:
                    allow_order_out_of_stock = False

                if "b2cpackingNoofUnits" in i['_source']:
                    if type(i['_source']['b2cpackingNoofUnits']) == int:
                        b2c_packing_no_units = i['_source']['b2cpackingNoofUnits']
                    else:
                        b2c_packing_no_units = 1
                else:
                    b2c_packing_no_units = 1

                if "b2cpackingPackageUnits" in i['_source']:
                    if "en" in i['_source']['b2cpackingPackageUnits']:
                        b2c_packing_package_units = i['_source']['b2cpackingPackageUnits']['en']
                    else:
                        b2c_packing_package_units = ""
                else:
                    b2c_packing_package_units = ""

                if "b2cpackingPackageType" in i['_source']:
                    if "en" in i['_source']['b2cpackingPackageType']:
                        b2c_packing_units = i['_source']['b2cpackingPackageType']['en']
                    else:
                        b2c_packing_units = ""
                else:
                    b2c_packing_units = ""

                if "containsMeat" in i['_source']:
                    contains_Meat = i['_source']['containsMeat']
                else:
                    contains_Meat = False

                try:
                    if b2c_packing_package_units != "" and b2c_packing_units != "":
                        mou_data = str(
                            b2c_packing_no_units) + " " + b2c_packing_package_units + " per " + b2c_packing_units
                    elif b2c_packing_package_units == "" and b2c_packing_units != "":
                        mou_data = str(b2c_packing_no_units) + " " + b2c_packing_package_units
                    else:
                        mou_data = ""
                except:
                    mou_data = ""

                    # ========================= for the get the linked the unit data====================================
                if type(i['_source']['units'][0]) == list:
                    attribute_list = i['_source']['units'][0][0]['attributes']
                else:
                    attribute_list = i['_source']['units'][0]['attributes']
                for link_unit in attribute_list:
                    if "attrlist" in link_unit:
                        for attrlist in link_unit['attrlist']:
                            if attrlist is None:
                                pass
                            else:
                                if type(attrlist) == str:
                                    pass
                                else:
                                    try:
                                        if attrlist['linkedtounit'] == 1:
                                            if attrlist['measurementUnit'] == "":
                                                attr_name = attrlist['value'][language] if language in attrlist[
                                                    'value'] else attrlist['value']['en']
                                            else:
                                                attr_name = attrlist['value'][language] if language in attrlist[
                                                    'value'] else attrlist['value']['en'] + " " + attrlist[
                                                    'measurementUnit']
                                            variant_data.append(
                                                {
                                                    "value": str(attr_name)
                                                }
                                            )
                                        else:
                                            pass
                                    except:
                                        pass
                    else:
                        pass
                    # ======================================product seo======================================================
                if "productSeo" in i['_source']:
                    if len(i['_source']['productSeo']) > 0:
                        try:
                            if len(i['_source']['productSeo']['title']) > 0:
                                title = i['_source']['productSeo']['title'][language] if language in i['_source'][
                                    'productSeo']['title'] else i['_source']['productSeo']['title']["en"]
                            else:
                                title = ""
                        except:
                            title = ""

                        try:
                            if len(i['_source']['productSeo']['description']) > 0:
                                description = i['_source']['productSeo']['description'][language] if language in \
                                                                                                     i['_source'][
                                                                                                         'productSeo'][
                                                                                                         'description'] else \
                                    i['_source']['productSeo']['description']["en"]
                            else:
                                description = ""
                        except:
                            description = ""

                        try:
                            if len(i['_source']['productSeo']['metatags']) > 0:
                                metatags = i['_source']['productSeo']['metatags'][language] if language in \
                                                                                               i['_source'][
                                                                                                   'productSeo'][
                                                                                                   'metatags'] else \
                                    i['_source']['productSeo']['metatags']["en"]
                            else:
                                metatags = ""
                        except:
                            metatags = ""

                        try:
                            if len(i['_source']['productSeo']['slug']) > 0:
                                slug = i['_source']['productSeo']['slug'][language] if language in i['_source'][
                                    'productSeo']['slug'] else i['_source']['productSeo']['slug']["en"]
                            else:
                                slug = ""
                        except:
                            slug = ""

                        product_seo = {
                            "title": title,
                            "description": description,
                            "metatags": metatags,
                            "slug": slug
                        }
                    else:
                        product_seo = {
                            "title": "",
                            "description": "",
                            "metatags": "",
                            "slug": ""
                        }
                else:
                    product_seo = {
                        "title": "",
                        "description": "",
                        "metatags": "",
                        "slug": ""
                    }

                if len(best_supplier) > 0:
                    if "productId" in best_supplier:
                        product_id = best_supplier['productId']
                    else:
                        product_id = i["_id"]
                else:
                    product_id = i["_id"]

                    # ===============================offer data======================================
                best_offer = product_get_best_offer_data(str(i['_id']), zone_id)
                if len(best_offer) == 0:
                    best_offer = product_best_offer_data(str(i['_id']))
                else:
                    pass
                # ===============================variant data=================================================
                if type(best_supplier['id']) == str:
                    best_supplier_id = best_supplier['id']
                else:
                    best_supplier_id = best_supplier['id'][0]
                variant_count = validate_variant(
                    str(i['_source']['parentProductId']),
                    best_supplier_id, zone_id, store_category_id)
                # ======================for tax ================================================================
                tax_value = []
                if i['_source'] is not None:
                    if type(i['_source']['tax']) == list:
                        for tax in i['_source']['tax']:
                            tax_value.append({"value": tax['taxValue']})
                    else:
                        if i['_source']['tax'] is not None:
                            if "taxValue" in i['_source']['tax']:
                                tax_value.append({"value": i['_source']['tax']['taxValue']})
                            else:
                                tax_value.append({"value": i['_source']['tax']})
                        else:
                            pass
                else:
                    tax_value = []

                try:
                    store_details = db.stores.find_one({"_id": ObjectId(i['_source']['storeId'])})
                except:
                    store_details = None
                store_count = 0
                if store_details is not None:
                    best_supplier['storeName'] = store_details['storeName'][language] if language in store_details[
                        'storeName'] else store_details['storeName']['en']
                    best_supplier['storeFrontTypeId'] = store_details['storeFrontTypeId']
                    best_supplier['storeFrontType'] = store_details['storeFrontType']
                    best_supplier['storeTypeId'] = store_details['storeTypeId']
                    best_supplier['storeType'] = store_details['storeType']
                    best_supplier['cityName'] = store_details['cityName'] if "cityName" in store_details else "Banglore"
                    try:
                        best_supplier['areaName'] = store_details['businessLocationAddress'][
                            'areaOrDistrict'] if "areaOrDistrict" in store_details[
                            'businessLocationAddress'] else "Hebbal"
                    except:
                        best_supplier['areaName'] = "Hebbal"
                    best_supplier['sellerType'] = store_details['sellerType']
                    best_supplier['sellerTypeId'] = store_details['sellerTypeId']
                    best_supplier['rating'] = store_details['avgRating'] if "avgRating" in store_details else 0
                else:
                    best_supplier['storeName'] = central_store
                    best_supplier['storeFrontTypeId'] = 0
                    best_supplier['storeFrontType'] = "Central"
                    best_supplier['storeTypeId'] = 0
                    best_supplier['storeType'] = "Central"
                    best_supplier['cityName'] = "Banglore"
                    best_supplier['areaName'] = "Hebbal"
                    best_supplier['sellerType'] = "Central"
                    best_supplier['sellerTypeId'] = 1
                    best_supplier['rating'] = 0

                if len(i['_source']["images"]) > 0:
                    image_data = i['_source']["images"]
                else:
                    image_data = [
                        {
                            "extraLarge": "",
                            "medium": "",
                            "altText": "",
                            "large": "",
                            "small": ""
                        }
                    ]
                if "modelImage" in i['_source']["units"][0]:
                    if len(i['_source']["units"][0]['modelImage']) > 0:
                        model_data = i['_source']["units"][0]['modelImage']
                    else:
                        model_data = [
                            {
                                "extraLarge": "",
                                "medium": "",
                                "altText": "",
                                "large": "",
                                "small": ""
                            }
                        ]
                else:
                    model_data = [
                        {
                            "extraLarge": "",
                            "medium": "",
                            "altText": "",
                            "large": "",
                            "small": ""
                        }
                    ]
                if i["_source"]['storeCategoryId'] == ECOMMERCE_STORE_CATEGORY_ID:
                    isShoppingList = True
                else:
                    shoppinglist_product = db.userShoppingList.find(
                        {"userId": user_id, "products.centralProductId": i["_source"]['parentProductId'],
                         "products.childProductId": product_id})

                    if shoppinglist_product.count() == 0:
                        isShoppingList = False
                    else:
                        isShoppingList = True

                # =========================================pharmacy details=========================================
                if "prescriptionRequired" in i['_source']:
                    if i['_source']["prescriptionRequired"] == 0:
                        prescription_required = False
                    else:
                        prescription_required = True
                else:
                    prescription_required = False

                if "needsIdProof" in i['_source']:
                    if not i['_source']["needsIdProof"]:
                        needsIdProof = False
                    else:
                        needsIdProof = True
                else:
                    needsIdProof = False

                if "saleOnline" in i['_source']:
                    if i['_source']["saleOnline"] == 0:
                        sales_online = False
                    else:
                        sales_online = True
                else:
                    sales_online = False

                if "uploadProductDetails" in i['_source']:
                    upload_details = i['_source']["uploadProductDetails"]
                else:
                    upload_details = ""
                    # ==================================================================================================

                    # =========================for max quantity=================================================
                if "maxQuantity" in i['_source']:
                    if i['_source']['maxQuantity'] != "":
                        max_quantity = int(i['_source']['maxQuantity'])
                    else:
                        max_quantity = 30
                else:
                    max_quantity = 30

                # ==========================================================================================
                if "addOns" in i['_source']["units"][0]:
                    if len(i['_source']["units"][0]['addOns']) > 0:
                        addons_count = True
                    else:
                        addons_count = False
                else:
                    addons_count = False

                # ================================currency=================================================
                currency_symbol = i['_source']['currencySymbol']
                currency = i['_source']['currency']
                best_supplier['currency'] = currency
                best_supplier['currencySymbol'] = currency_symbol
                try:
                    best_supplier['retailerPrice'] = i['_source']["units"][0]['floatValue']
                except:
                    best_supplier['retailerPrice'] = i['_source']["units"][0][0]['floatValue']
                avg_rating = i['_source']['avgRating'] if "avgRating" in i['_source'] else 0
                try:
                    product_name = i['_source']['pName'][language] if language in i['_source']['pName'] else \
                        i['_source']['pName']["en"]
                except:
                    product_name = i['_source']['pPName'][language] if language in i['_source'][
                        'pPName'] else i['_source']['pPName']["en"]
                try:
                    linked_attribute = get_linked_unit_attribute(i['_source']['units'])
                except:
                    linked_attribute = []

                # ==================================get currecny rate============================
                try:
                    currency_rate = currency_exchange_rate[str(currency) + "_" + str(currency_code)]
                except:
                    currency_rate = 0
                currency_details = db.currencies.find_one({"currencyCode": currency_code})
                if currency_details is not None:
                    currency_symbol = currency_details['currencySymbol']
                    currency = currency_details['currencyCode']
                child_product_details = db.childProducts.find_one({"_id": ObjectId(product_id)},
                                                                  {"units": 1, "storeName": 1})
                resData.append(
                    {"childProductId": product_id, "score": score, "isShoppingList": isShoppingList,
                     "maxQuantity": max_quantity, "linkedAttribute": linked_attribute, "productName": product_name,
                     "brandName": i['_source']['brandTitle'][language]
                     if language in i['_source']["brandTitle"] else "",
                     "manufactureName": i['_source']['manufactureName'][language]
                     if language in i['_source']["manufactureName"] else "",
                     "parentProductId": i["_source"]['parentProductId'],
                     "productSeo": product_seo, "TotalStarRating": avg_rating,
                     "storeCategoryId": i['_source']['storeCategoryId']
                     if "storeCategoryId" in i['_source'] else "",
                     "prescriptionRequired": prescription_required, "needsIdProof": needsIdProof,
                     "modelImage": model_data,
                     "saleOnline": sales_online, "currencyRate": currency_rate, "containsMeat": contains_Meat,
                     "uploadProductDetails": upload_details, "sizes": [],
                     "productTag": product_tag, "tax": tax_value, "variantData": variant_data,
                     "variantCount": variant_count, "addOnsCount": addons_count, "currencySymbol": currency_symbol,
                     "currency": currency, "mobileImage": image_data, "mouDataUnit": mou_data,
                     "units": child_product_details['units'] if child_product_details is not None else i['_source'][
                         "units"],
                     "unitId": product_id,  # i['_source']["units"][0]['unitId'],
                     "storeName": child_product_details["storeName"][
                         'en'] if child_product_details is not None else "3Embed Pvt Ltd",
                     "allowOrderOutOfStock": allow_order_out_of_stock, "moUnit": "Pcs", "offer": best_offer,
                     "popularstatus": popularstatus, "suppliers": best_supplier, "outOfStock": outOfStock,
                     "storeCount": store_count})
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            pass

    if len(resData) > 0:
        dataframe = pd.DataFrame(resData)
        dataframe["popularstatus"] = popularstatus
        dataframe["unitsData"] = dataframe.apply(home_units_data, lan=language, sort=sort,
                                                 status=1, axis=1, logintype=login_type,
                                                 store_category_id=store_category_id, margin_price=margin_price,
                                                 city_id="")
        dataframe = dataframe.drop_duplicates("childProductId", keep="first")
        details = dataframe.to_json(orient="records")
        data = json.loads(details)
        for k in data:
            if k["unitsData"]['basePrice'] == 0:
                pass
            else:
                try:
                    base_price = k["unitsData"]['basePrice']
                    final_price = k["unitsData"]['finalPrice']
                    discount_price = k["unitsData"]['discountPrice']
                    if store_id != "":
                        outOfStock = k['unitsData']['outOfStock']
                    else:
                        outOfStock = k['unitsData']['outOfStock']
                    availableQuantity = int(k['unitsData']['availableQuantity'])
                    mou = ""
                    mou_unit = ""
                    minimum_qty = 0
                except:
                    base_price = 0
                    final_price = 0
                    discount_price = 0
                    availableQuantity = 0
                    outOfStock = True
                    minimum_qty = 0
                    mou = None
                    mou_unit = None

                # discount_price = 0
                if len(k['offer']) == 0:
                    percentage = 0
                    discount_type = 0
                else:
                    if "discountType" in k['offer']:
                        if k['offer']['discountType'] == 0:
                            percentage = 0
                            discount_type = 0
                        else:
                            percentage = int(k['offer']['discountValue'])
                            discount_type = k['offer']['discountType']
                    else:
                        percentage = 0
                        discount_type = 0

                filter_responseJson.append(
                    {
                        "outOfStock": outOfStock,
                        "score": k['score'] if "score" in k else 0,
                        "childProductId": k["childProductId"],
                        "productName": k["productName"],
                        "brandName": k["brandName"],
                        "manufactureName": k["manufactureName"],
                        "isShoppingList": k["isShoppingList"],
                        "unitId": k['unitId'],
                        "storeCategoryId": k['storeCategoryId'],
                        "linkedAttribute": k['linkedAttribute'],
                        "parentProductId": k["parentProductId"],
                        "TotalStarRating": k['TotalStarRating'],
                        "offers": k['offer'],
                        "mouDataUnit": k['mouDataUnit'],
                        "variantCount": k['variantCount'],
                        "productTag": k['productTag'],
                        "maxQuantity": k['maxQuantity'],
                        "availableQuantity": availableQuantity,
                        "images": k["mobileImage"],
                        "addOnsCount": k["addOnsCount"],
                        "productSeo": k['productSeo'],
                        "containsMeat": k['containsMeat'],
                        "variantData": k['variantData'],
                        "allowOrderOutOfStock": k['allowOrderOutOfStock'],
                        "moUnit": "Pcs",
                        "discountPrice": discount_price,
                        "discountType": discount_type,
                        "price": final_price,
                        "finalPriceList": {
                            "basePrice": round(base_price, 2),
                            "finalPrice": round(final_price, 2),
                            "discountPrice": round(discount_price, 2),
                            "discountPercentage": percentage
                        },
                        "currencySymbol": k["currencySymbol"],
                        "storeName": k["storeName"] if "storeName" in k else "3Embed Pvt Ltd",
                        "currency": k["currency"],
                        "supplier": k["suppliers"],
                        "prescriptionRequired": k["prescriptionRequired"],
                        "needsIdProof": k["needsIdProof"] if "needsIdProof" in k else False,
                        "modelImage": k["modelImage"] if "modelImage" in k else [],
                        "saleOnline": k["saleOnline"],
                        "uploadProductDetails": k["uploadProductDetails"],
                        "storeCount": k["storeCount"],
                        "mouData": {
                            "mou": mou,
                            "mouUnit": mou_unit,
                            "mouQty": minimum_qty,
                            "minimumPurchaseUnit": k['unitsData']['minimumPurchaseUnit'] if "minimumPurchaseUnit" in k[
                                'unitsData'] else ""
                        }
                    }
                )
        if int(sort_type) == 0:
            newlist = sorted(filter_responseJson, key=lambda k: k['price'], reverse=False)
        elif int(sort_type) == 1:
            newlist = sorted(filter_responseJson, key=lambda k: k['price'], reverse=True)
        else:
            if search_query == "":
                newlist = sorted(filter_responseJson, key=lambda k: k['availableQuantity'], reverse=True)
            else:
                newlist = sorted(filter_responseJson, key=lambda k: k['score'], reverse=True)
        try:
            if "value" in res["hits"]["total"]:
                pen_count = res["hits"]["total"]['value']
            else:
                pen_count = res["hits"]["total"]
        except:
            pen_count = res["hits"]["total"]
        if pen_count > 20:
            pen_count = pen_count
        else:
            pen_count = len(newlist)
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


async def search_read_new(res, start_time, language, filter_responseJson, finalfilter_responseJson_products,
                          popularstatus, sort, login_type, store_id, sort_type, store_category_id,
                          currency_code, from_data, to_data, user_id, remove_central, zone_id, min_price, max_price,
                          margin_price, search_query, token, city_id):
    try:
        if len(res) <= 0:
            error = {"data": [], "message": "No Products Found"}
            return error
        else:
            last_data = store_search_data_new(res, start_time, language, filter_responseJson,
                                              finalfilter_responseJson_products, popularstatus, sort, login_type,
                                              store_id, sort_type, store_category_id, from_data, to_data,
                                              user_id, remove_central, zone_id, min_price, max_price,
                                              margin_price, currency_code, search_query, token)
            finalSearchResults = {
                "data": last_data,
                "message": "Got the details",
            }
            return finalSearchResults
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print('Error on line {}'.format(sys.exc_info()
                                        [-1].tb_lineno), type(ex).__name__, ex)
        finalResponse = {
            "message": message,
            "data": []
        }
        return finalResponse


'''
    API for get all the banners for social banners
    :parameters
        --> limit
        --> skip
'''


class SocialBannersList(APIView):
    @swagger_auto_schema(method='get', tags=["Banners"],
                         operation_description="Api get all social banners list",
                         required=['AUTHORIZATION'],
                         manual_parameters=[
                             openapi.Parameter(
                                 name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
                                 description="authorization token"),
                             openapi.Parameter(
                                 name='skip',
                                 required=True,
                                 default="0",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="skip, how much data needs to skip"
                             ),
                             openapi.Parameter(
                                 name='limit',
                                 required=True,
                                 default="10",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="limit, how much data needs to show after skip"
                             )
                         ],
                         responses={
                             200: 'successfully.',
                             404: 'data not found.',
                             401: 'Unauthorized. token expired',
                             500: 'Internal Server Error. if server is not working that time'
                         },
                         )
    @action(detail=False, methods=['get'])
    def get(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            if token == "":
                response_data = {
                    "message": "Unauthorized"
                }
                return JsonResponse(response_data, safe=False, status=401)
            else:
                skip_data = int(request.GET.get("skip", "0"))
                limit_data = int(request.GET.get("limit", "10"))
                mongo_query = {"status": 1}
                banner_data = db.dublyBannerImage.find(mongo_query).sort([("seqId", -1)]).skip(skip_data).limit(
                    limit_data)
                banner_data_count = db.dublyBannerImage.find(mongo_query).count()
                if banner_data.count() == 0:
                    response = {
                        "message": "data not found",
                        "penCount": 0,
                        "total_count": 0,
                        "data": []
                    }
                    return JsonResponse(response, safe=False, status=404)
                else:
                    banner_details = []
                    for banner in banner_data:
                        try:
                            banner_details.append(
                                {
                                    "_id": str(banner['_id']),
                                    "activeimage": banner['activeimage'],
                                    "starName": banner['starName'],
                                    "userId": banner['userId'],
                                }
                            )
                        except:
                            pass
                    if len(banner_details) > 0:
                        response = {
                            "message": "banner found",
                            "total_count": banner_data_count,
                            "penCount": banner_data_count,
                            "data": banner_details
                        }
                        return JsonResponse(response, safe=False, status=200)
                    else:
                        response = {
                            "message": "banner not found",
                            "total_count": 0,
                            "penCount": 0,
                            "data": []
                        }
                        return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error_message = {
                "error": "Invalid request",
                "total_count": 0,
                "message": message
            }
            return JsonResponse(error_message, status=500)


'''
    API for get all the banners for social banners
    :parameters
        --> limit
        --> skip
'''


class TaggedProductList(APIView):
    @swagger_auto_schema(method='get', tags=["Tagged Products"],
                         operation_description="Api get all tagged product list which "
                                               "user tagged in the post",
                         required=['AUTHORIZATION'],
                         manual_parameters=[
                             openapi.Parameter(
                                 name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
                                 description="authorization token"),
                             openapi.Parameter(
                                 name='language', default="en", in_=openapi.IN_HEADER, type=openapi.TYPE_STRING,
                                 required=True, description="language, for language support"),
                             openapi.Parameter(
                                 name='userId',
                                 required=True,
                                 default="600a3cd7dc61ff5b7464063c",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="skip, how much data needs to skip"
                             ),
                             openapi.Parameter(
                                 name='skip',
                                 required=True,
                                 default="0",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="skip, how much data needs to skip"
                             ),
                             openapi.Parameter(
                                 name='limit',
                                 required=True,
                                 default="10",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="limit, how much data needs to show after skip"
                             )
                         ],
                         responses={
                             200: 'successfully.',
                             404: 'data not found.',
                             401: 'Unauthorized. token expired',
                             500: 'Internal Server Error. if server is not working that time'
                         },
                         )
    @action(detail=False, methods=['get'])
    def get(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            if token == "":
                response_data = {
                    "message": "Unauthorized"
                }
                return JsonResponse(response_data, safe=False, status=401)
            else:
                user_id = request.GET.get("userId", "")
                skip_data = int(request.GET.get("skip", "0"))
                limit_data = int(request.GET.get("limit", "10"))
                recent_data = []
                if user_id == "":
                    response_data = {
                        "message": "user id is missing"
                    }
                    return JsonResponse(response_data, safe=False, status=422)
                else:
                    mongo_query = [
                        {
                            '$match':
                                {
                                    "postStatus": 1
                                }
                        },
                        {
                            '$unwind': '$productData'
                        },
                        {
                            '$match':
                                {
                                    'productData.status': 1,
                                }
                        },
                        {
                            '$match':
                                {
                                    "userId": ObjectId(user_id)
                                }
                        },
                        {
                            "$sort": {"_id": -1}
                        },
                        {
                            "$group": {
                                "_id": "$productData.productId",
                            }
                        },
                        {
                            '$project':
                                {
                                    'product_data':
                                        {
                                            '_id': 1
                                        },
                                    '_id': '$_id'
                                }
                        },
                        {"$skip": int(skip_data)},
                        {"$limit": int(limit_data)}
                    ]
                    mongo_query_count = [
                        {
                            '$match':
                                {
                                    "postStatus": 1
                                }
                        },
                        {
                            '$unwind': '$productData'
                        },
                        {
                            '$match':
                                {
                                    'productData.status': 1,
                                }
                        },
                        {
                            '$match':
                                {
                                    "userId": ObjectId(user_id)
                                }
                        },
                        {
                            "$sort": {"_id": -1}
                        },
                        {
                            "$group": {
                                "_id": "$productData.productId",
                            }
                        },
                        {
                            '$project':
                                {
                                    'product_data':
                                        {
                                            '_id': 1
                                        },
                                    '_id': '$_id'
                                }
                        },
                        {"$count": "passing_scores"}
                    ]
                    resData = []
                    post_product_data = db.posts.aggregate(mongo_query)
                    post_product_count = db.posts.aggregate(mongo_query_count)
                    total_count = []
                    for count_data in post_product_count:
                        total_count.append(count_data['passing_scores'])
                    for post in post_product_data:
                        try:
                            central_query = {
                                "_id": ObjectId(post['_id'])
                            }
                            product_details = db.childProducts.find_one(central_query)
                            if product_details is not None:
                                try:
                                    best_supplier = {
                                        "productId": str(product_details['_id']),
                                        "id": str(product_details['storeId']),
                                        "retailerQty": product_details['units'][0]['availableQuantity'],
                                    }
                                    if best_supplier['retailerQty'] > 0:
                                        outOfStock = False
                                        availableQuantity = best_supplier['retailerQty']
                                    else:
                                        outOfStock = True
                                        availableQuantity = 0

                                    offers_details = []
                                    if 'offer' in product_details:
                                        for offer in product_details['offer']:
                                            if offer['status'] == 1:
                                                offers_details.append(offer)
                                            else:
                                                pass

                                    if len(offers_details) > 0:
                                        best_offer = max(
                                            offers_details, key=lambda x: x['discountValue'])
                                    else:
                                        best_offer = {}

                                    product_id = str(product_details["_id"])
                                    rating_count = db.reviewRatings.find(
                                        {"productId": product_details['parentProductId'], "rating": {"$ne": 0}}).count()
                                    tax_value = []
                                    if type(product_details['tax']) == list:
                                        for tax in product_details['tax']:
                                            tax_value.append(
                                                {"value": tax['taxValue']})
                                    else:
                                        if product_details['tax'] is not None:
                                            if "taxValue" in product_details['tax']:
                                                tax_value.append(
                                                    {"value": product_details['tax']['taxValue']})
                                            else:
                                                tax_value.append(
                                                    {"value": product_details['tax']})
                                        else:
                                            tax_value = []

                                    seller_details = db.stores.find_one({"_id": ObjectId(product_details['storeId'])})
                                    if seller_details is not None:
                                        seller_name = seller_details['storeName']['en']
                                    else:
                                        seller_name = ""

                                    resData.append({
                                        "childProductId": product_id,
                                        "availableQuantity": availableQuantity,
                                        "productName": product_details['units'][0]['unitName'][language] if language in
                                                                                                            product_details[
                                                                                                                'units'][
                                                                                                                0][
                                                                                                                'unitName'] else
                                        product_details['units'][0]['unitName']['en'],
                                        "parentProductId": product_details['parentProductId'],
                                        "brandName": product_details['brandTitle'][language] if language in
                                                                                                product_details[
                                                                                                    'brandTitle'] else
                                        product_details['brandTitle']['en'],
                                        "outOfStock": outOfStock,
                                        "timestamp": product_details[
                                            'createdtimestamp'] if "createdtimestamp" in product_details else 0,
                                        # product['createdtimestamp'],
                                        "suppliers": best_supplier,
                                        "storeName": seller_name,
                                        "tax": tax_value,
                                        "variantData": [],
                                        "currencySymbol": product_details['currencySymbol'],
                                        "currency": product_details[
                                            'currency'] if "currency" in product_details else "$",
                                        "images": product_details['units'][0]['image'] if "image" in
                                                                                          product_details['units'][
                                                                                              0] else
                                        product_details['units'][0]['images'],
                                        "finalPriceList": product_details['units'],
                                        "units": product_details['units'],
                                        "unitId": product_details['units'][0]['unitId'],
                                        "offer": best_offer,
                                        "avgRating": product_details[
                                            'avgRating'] if "avgRating" in product_details else 0,
                                        "totalRatingCount": rating_count
                                    })
                                except Exception as ex:
                                    print('Error on line {}'.format(sys.exc_info()
                                                                    [-1].tb_lineno), type(ex).__name__, ex)
                            else:
                                pass
                        except Exception as ex:
                            print('Error on line {}'.format(sys.exc_info()
                                                            [-1].tb_lineno), type(ex).__name__, ex)
                    if len(resData) > 0:
                        dataframe = pd.DataFrame(resData)
                        dataframe["unitsData"] = dataframe.apply(home_units_data, lan=language, sort=0, status=0,
                                                                 axis=1, store_category_id=ECOMMERCE_STORE_CATEGORY_ID,
                                                                 logintype=1, margin_price=True, city_id="")
                        details = dataframe.to_json(orient='records')
                        data = json.loads(details)
                        for k in data:
                            if len(k['offer']) != 0:
                                if "discountType" in k['offer']:
                                    if k['offer']['discountType'] == 0:
                                        percentage = 0
                                    else:
                                        percentage = int(
                                            k['offer']['discountValue'])
                                else:
                                    percentage = 0
                            else:
                                percentage = 0

                            mou = ""
                            mou_unit = ""
                            minimum_qty = 0

                            recent_data.append({
                                "outOfStock": k['outOfStock'],
                                "unitId": k['unitId'],
                                "parentProductId": k['parentProductId'],
                                "childProductId": k['childProductId'],
                                "productName": k['productName'],
                                "availableQuantity": k['availableQuantity'],
                                "images": k['images'],
                                "timestamp": k['timestamp'],
                                "supplier": k['suppliers'],
                                "brandName": k['brandName'],
                                "variantData": k['variantData'],
                                "discountType": k['offer']['discountType'] if "discountType" in k['offer'] else 0,
                                "finalPrice": k['unitsData']['finalPrice'],
                                "finalPriceList": {
                                    "basePrice": k['unitsData']['basePrice'],
                                    "finalPrice": k['unitsData']['finalPrice'],
                                    "discountPrice": k['unitsData']['discountPrice'],
                                    "discountPercentage": percentage
                                },
                                "totalRatingCount": k['totalRatingCount'],
                                "mouData": {
                                    "mou": mou,
                                    "mouUnit": mou_unit,
                                    "mouQty": minimum_qty,
                                    "minimumPurchaseUnit": ""
                                },
                                "currencySymbol": k['currencySymbol'],
                                "currency": k['currency'],
                                "avgRating": k['avgRating'],
                                "offers": k['offer']
                            })

                        sorted_data = sorted(recent_data, key=lambda k: k['availableQuantity'], reverse=True)
                        if len(sorted_data) > 0:
                            response = {
                                "data": sorted_data,
                                "penCount": sum(total_count),
                                "total_count": sum(total_count),
                                "message": "data found...!!!"
                            }
                            return_response = {"data": response}
                            return JsonResponse(return_response, safe=False, status=200)
                        else:
                            response = {
                                "data": [],
                                "penCount": 0,
                                "total_count": 0,
                                "message": "data not found...!!!"
                            }
                            return_response = {"data": response}
                            return JsonResponse(return_response, safe=False, status=404)
                    else:
                        response = {
                            "data": [],
                            "penCount": 0,
                            "total_count": 0,
                            "message": "data not found...!!!"
                        }
                        return_response = {"data": response}
                        return JsonResponse(return_response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error_message = {
                "error": "Invalid request",
                "total_count": 0,
                "message": message
            }
            return JsonResponse(error_message, status=500)


'''
    API for get all the product list based on request
    :parameters
        --> page (page number. which page number data want to display)
        --> q (search products base on store name or product name)
        --> s_id (store id if need to get products from one store)
'''


class ProductList(APIView):
    @swagger_auto_schema(method='get', tags=["Tagged Products"],
                         operation_description="Api for get all the product list based on request",
                         required=['AUTHORIZATION'],
                         manual_parameters=[
                             openapi.Parameter(
                                 name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
                                 description="authorization token"),
                             openapi.Parameter(
                                 name='language', default="en", in_=openapi.IN_HEADER, type=openapi.TYPE_STRING,
                                 required=True, description="language, for language support"),
                             openapi.Parameter(
                                 name='s_id',
                                 required=False,
                                 default="600a3cd7dc61ff5b7464063c",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="store id if need to get products from one store"
                             ),
                             openapi.Parameter(
                                 name='q',
                                 required=False,
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="search products base on store name or product name"
                             ), openapi.Parameter(
                                 name='integrationType',
                                 default="0",
                                 required=True,
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="for filter out the products base on product setup configuration, "
                                             "value should be "
                                             "0 for All products, "
                                             "1 for Only Magento Products, "
                                             "2 for Only Shopify Products, "
                                             "3 for Only Roadyo or shopar products"
                             ),
                             openapi.Parameter(
                                 name='page',
                                 default="1",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="page number. which page number data want to display"
                             )
                         ],
                         responses={
                             200: 'successfully.',
                             404: 'data not found.',
                             401: 'Unauthorized. token expired',
                             500: 'Internal Server Error. if server is not working that time'
                         },
                         )
    @action(detail=False, methods=['get'])
    def get(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            if token == "":
                response_data = {
                    "message": "Unauthorized"
                }
                return JsonResponse(response_data, safe=False, status=401)
            else:
                start_time = time.time()
                store_id = request.GET.get("s_id", "")
                search_query = request.GET.get("q", "")
                page = int(request.GET.get("page", 1))
                integration_type = float(request.GET.get("integrationType", 0))  # minimum thc
                from_data = int(page * 20) - 20
                to_data = 20  # int(page*30)
                store_category_id = ECOMMERCE_STORE_CATEGORY_ID
                query = []
                should_query = []
                if store_id != "":
                    query.append(
                        {
                            "terms":
                                {
                                    "storeId": [store_id]
                                }
                        }
                    )
                else:
                    pass
                query.append(
                    {
                        "match":
                            {
                                "storeCategoryId": store_category_id
                            }
                    }
                )
                query.append(
                    {
                        "match":
                            {
                                "units.isPrimary": True
                            }
                    }
                )
                if int(integration_type) == 0:
                    pass
                elif int(integration_type) == 1:
                    must_not.append(
                        {
                            "match": {
                                "magentoId": -1
                            }
                        }
                    )
                    query.append(
                        {
                            "exists": {
                                "field": "magentoId"
                            }
                        }
                    )
                elif int(integration_type) == 2:
                    must_not.append(
                        {
                            "term": {
                                "shopify_variant_id.keyword":
                                    {
                                        "value": ""
                                    }
                            }
                        }
                    )
                    query.append(
                        {
                            "exists": {
                                "field": "shopify_variant_id"
                            }
                        }
                    )
                elif int(integration_type) == 3:
                    query.append(
                        {
                            "match": {
                                "magentoId": -1
                            }
                        }
                    )
                    query.append(
                        {
                            "term": {
                                "shopify_variant_id.keyword": ""
                            }
                        }
                    )
                if search_query != "":
                    # ===========================product name========================================
                    should_query.append(
                        {
                            "match_phrase_prefix": {
                                "pName.en": {
                                    "analyzer": "standard",
                                    "query": search_query.replace("%20", " "),
                                    "boost": 6
                                }
                            }
                        })
                    should_query.append({
                        "match": {
                            "pName.en": {
                                "query": search_query.replace("%20", " "),
                                "boost": 6
                            }
                        }
                    })
                    # ===========================unit name========================================
                    should_query.append(
                        {
                            "match_phrase_prefix": {
                                "units.unitName.en": {
                                    "analyzer": "standard",
                                    "query": search_query.replace("%20", " "),
                                    "boost": 5
                                }
                            }
                        })
                    should_query.append({
                        "match": {
                            "units.unitName.en": {
                                "query": search_query.replace("%20", " "),
                                "boost": 5
                            }
                        }
                    })
                    # ===========================================detail description============================
                    should_query.append({
                        "match": {
                            "detailDescription." + language: {
                                "query": search_query.replace("%20", " "),
                                "boost": 4
                            }
                        }
                    })
                    should_query.append({
                        "match_phrase_prefix": {
                            "detailDescription." + language: {
                                "query": search_query.replace("%20", " "),
                                "boost": 4
                            }
                        }
                    })
                    # ====================================child category=======================================
                    should_query.append({
                        "match": {
                            "categoryList.parentCategory.childCatgory.categoryName." + language: {
                                "query": search_query.replace("%20", " "),
                                "boost": 3
                            }
                        }
                    })
                    should_query.append({
                        "match_phrase_prefix": {
                            "categoryList.parentCategory.childCatgory.categoryName." + language: {
                                "query": search_query.replace("%20", " "),
                                "boost": 3
                            }
                        }
                    })
                    # ===============================parent category name======================================
                    should_query.append({
                        "match": {
                            "categoryList.parentCategory.categoryName." + language: {
                                "query": search_query.replace("%20", " "),
                                "boost": 2
                            }
                        }
                    })
                    should_query.append({
                        "match_phrase_prefix": {
                            "categoryList.parentCategory.categoryName." + language: {
                                "query": search_query.replace("%20", " "),
                                "boost": 2
                            }
                        }
                    })
                    # ======================================brand name=======================================
                    should_query.append({
                        "match": {
                            "brandTitle." + language: {
                                "query": search_query.replace("%20", " "),
                                "boost": 1
                            }
                        }
                    })
                    should_query.append({
                        "match_phrase_prefix": {
                            "brandTitle." + language: {
                                "query": search_query.replace("%20", " "),
                                "boost": 1
                            }
                        }
                    })

                sort_type = 2
                sort_query = [
                    {
                        "isInStock": {
                            "order": "desc"
                        }
                    }
                ]
                query.append({
                    "match":
                        {
                            "status": 1
                        }
                }
                )
                if len(should_query) > 0:
                    search_item_query = {
                        "query":
                            {
                                "bool":
                                    {
                                        "must": query,
                                        "should": should_query,
                                        "must_not": [
                                            {
                                                "match": {"storeId": "0"}
                                            }
                                        ],
                                        "minimum_should_match": 1,
                                        "boost": 1.0
                                    }
                            },
                        "track_total_hits": True,
                        "sort": sort_query,
                        "size": to_data,
                        "from": from_data
                    }
                else:
                    search_item_query = {
                        "query":
                            {
                                "bool":
                                    {
                                        "must": query,
                                        "must_not": [
                                            {
                                                "match": {"storeId": "0"}
                                            }
                                        ]
                                    }
                            },
                        "track_total_hits": True,
                        "size": to_data,
                        "from": from_data,
                        "sort": sort_query
                    }
                res = es.search(index=index_products, body=search_item_query)
                try:
                    if "value" in res['hits']['total']:
                        if res['hits']['total'] == 0 or "hits" not in res['hits']:
                            final_json = {"data": [], "message": "No Data Found"}
                            return JsonResponse(final_json, safe=False, status=404)
                    else:
                        if res['hits']['total'] == 0 or "hits" not in res['hits']:
                            final_json = {"data": [], "message": "No Data Found"}
                            return JsonResponse(final_json, safe=False, status=404)
                except:
                    if res['hits']['total'] == 0 or "hits" not in res['hits']:
                        final_json = {"data": [], "message": "No Data Found"}
                        return JsonResponse(final_json, safe=False, status=404)
                sort = 3
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
                currency_code = "INR"
                loop = asyncio.new_event_loop()
                try:
                    user_id = json.loads(token)['userId']
                except:
                    user_id = ""
                event_loop = asyncio.set_event_loop(loop)
                data = loop.run_until_complete(
                    asyncio.gather(search_read_new(
                        res,
                        time.time(),
                        language,
                        [],
                        [],
                        1,
                        sort, 0, store_id, sort_type, store_category_id, currency_code,
                        from_data, to_data, user_id, False, "", 0, 0, True, "", token
                    )
                    )
                )
                if len(data[0]) == 0:
                    return JsonResponse(data[0], safe=False, status=404)
                else:
                    print("total time taken by the tagged product....!!!!", int(time.time() - int(start_time)))
                    return JsonResponse(data[0], safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error_message = {
                "error": "Invalid request",
                "total_count": 0,
                "message": message
            }
            return JsonResponse(error_message, status=500)


'''
    API for get recommended products list from the db, recent added first needs to return
    :parameters
        --> limit
        --> skip
'''


class RecommendedProductList(APIView):
    @swagger_auto_schema(method='get', tags=["Recommended Product List"],
                         operation_description="API for get recommended products list from the db, recent added first needs to return",
                         required=['AUTHORIZATION'],
                         manual_parameters=[
                             openapi.Parameter(
                                 name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
                                 description="authorization token"),
                             openapi.Parameter(
                                 name='language', default="en", in_=openapi.IN_HEADER, type=openapi.TYPE_STRING,
                                 required=True, description="language, for language support"),
                             openapi.Parameter(
                                 name='skip',
                                 required=True,
                                 default="0",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="skip, how much data needs to skip"
                             ),
                             openapi.Parameter(
                                 name='limit',
                                 required=True,
                                 default="10",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="limit, how much data needs to show after skip"
                             )
                         ],
                         responses={
                             200: 'successfully.',
                             404: 'data not found.',
                             401: 'Unauthorized. token expired',
                             500: 'Internal Server Error. if server is not working that time'
                         },
                         )
    @action(detail=False, methods=['get'])
    def get(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            if token == "":
                response_data = {
                    "message": "Unauthorized"
                }
                return JsonResponse(response_data, safe=False, status=401)
            else:
                skip_data = int(request.GET.get("skip", "0"))
                limit_data = int(request.GET.get("limit", "20"))
                recent_data = []
                mongo_query = {"postStatus": 1, 'productData.productId': {"$exists": True}}
                resData = []
                print(mongo_query)
                post_product_data = db.posts.find(mongo_query).sort([("_id", -1)]).skip(skip_data).limit(limit_data)
                post_product_count = db.posts.find(mongo_query).count()
                print(post_product_data.count())
                for post in post_product_data:
                    try:
                        user_details = db.customer.find_one(
                            {"_id": ObjectId(post['userId'])},
                            {"firstName": 1, "lastName": 1, "userName": 1, "starRequest": 1, "profilePic": 1}
                        )
                        if user_details is not None:
                            if "starRequest" in user_details:
                                if "starUserKnownBy" in user_details["starRequest"]:
                                    user_name = user_details["starRequest"]['starUserKnownBy']
                                else:
                                    user_name = user_details['userName'] if "userName" in user_details else ""
                            else:
                                user_name = user_details['userName'] if "userName" in user_details else ""
                            profile_pic = user_details['profilePic'] if "profilePic" in user_details else ""
                            first_name = user_details['firstName'] if "firstName" in user_details else ""
                            last_name = user_details['lastName'] if "lastName" in user_details else ""
                            customer_name = user_name  # first_name + " " + last_name
                        else:
                            user_name = post['userName'] if "userName" in post else ""
                            profile_pic = ""
                            customer_name = ""
                        for post_product in post['productData']:
                            if post_product['status'] == 1:
                                central_query = {
                                    "_id": ObjectId(post_product['productId']),
                                    "status": 1
                                }
                                print("central", central_query)
                                product_details = db.childProducts.find_one(central_query)
                                if product_details is not None:
                                    try:
                                        best_supplier = {
                                            "productId": str(product_details['_id']),
                                            "id": str(product_details['storeId']),
                                            "retailerQty": product_details['units'][0]['availableQuantity'],
                                        }
                                        if best_supplier['retailerQty'] > 0:
                                            outOfStock = False
                                            availableQuantity = best_supplier['retailerQty']
                                        else:
                                            outOfStock = True
                                            availableQuantity = 0

                                        offers_details = []
                                        if 'offer' in product_details:
                                            for offer in product_details['offer']:
                                                if offer['status'] == 1:
                                                    offers_details.append(offer)
                                                else:
                                                    pass

                                        if len(offers_details) > 0:
                                            best_offer = max(
                                                offers_details, key=lambda x: x['discountValue'])
                                        else:
                                            best_offer = {}

                                        product_id = str(product_details["_id"])
                                        rating_count = db.reviewRatings.find(
                                            {"productId": product_details['parentProductId'],
                                             "rating": {"$ne": 0}}).count()
                                        tax_value = []
                                        if type(product_details['tax']) == list:
                                            for tax in product_details['tax']:
                                                tax_value.append(
                                                    {"value": tax['taxValue']})
                                        else:
                                            if product_details['tax'] is not None:
                                                if "taxValue" in product_details['tax']:
                                                    tax_value.append(
                                                        {"value": product_details['tax']['taxValue']})
                                                else:
                                                    tax_value.append(
                                                        {"value": product_details['tax']})
                                            else:
                                                tax_value = []

                                        seller_details = db.stores.find_one(
                                            {"_id": ObjectId(product_details['storeId'])})
                                        if seller_details is not None:
                                            seller_name = seller_details['storeName']['en']
                                        else:
                                            seller_name = ""

                                        resData.append({
                                            "childProductId": product_id,
                                            "availableQuantity": availableQuantity,
                                            "productName": product_details['units'][0]['unitName'][
                                                language] if language in product_details['units'][0]['unitName'] else
                                            product_details['units'][0]['unitName']['en'],
                                            "parentProductId": product_details['parentProductId'],
                                            "brandName": product_details['brandTitle'][language] if language in
                                                                                                    product_details[
                                                                                                        'brandTitle'] else
                                            product_details['brandTitle']['en'],
                                            "outOfStock": outOfStock,
                                            "timestamp": product_details[
                                                'createdtimestamp'] if "createdtimestamp" in product_details else 0,
                                            # product['createdtimestamp'],
                                            "suppliers": best_supplier,
                                            "storeName": seller_name,
                                            "profilePic": profile_pic,
                                            "tax": tax_value,
                                            "variantData": [],
                                            "customerName": customer_name,
                                            "userName": user_name,
                                            "currencySymbol": product_details['currencySymbol'],
                                            "currency": product_details[
                                                'currency'] if "currency" in product_details else "$",
                                            "images": product_details['units'][0]['image'] if "image" in
                                                                                              product_details['units'][
                                                                                                  0] else
                                            product_details['units'][0]['images'],
                                            "finalPriceList": product_details['units'],
                                            "units": product_details['units'],
                                            "unitId": product_details['units'][0]['unitId'],
                                            "offer": best_offer,
                                            "avgRating": product_details[
                                                'avgRating'] if "avgRating" in product_details else 0,
                                            "totalRatingCount": rating_count
                                        })
                                    except Exception as ex:
                                        print('Error on line {}'.format(sys.exc_info()
                                                                        [-1].tb_lineno), type(ex).__name__, ex)
                                else:
                                    pass
                            else:
                                pass
                    except Exception as ex:
                        print('Error on line {}'.format(sys.exc_info()
                                                        [-1].tb_lineno), type(ex).__name__, ex)
                if len(resData) > 0:
                    dataframe = pd.DataFrame(resData)
                    dataframe["unitsData"] = dataframe.apply(home_units_data, lan=language, sort=0, status=0, axis=1,
                                                             store_category_id=ECOMMERCE_STORE_CATEGORY_ID, logintype=1,
                                                             margin_price=True, city_id="")
                    details = dataframe.to_json(orient='records')
                    data = json.loads(details)
                    for k in data:
                        if len(k['offer']) != 0:
                            if "discountType" in k['offer']:
                                if k['offer']['discountType'] == 0:
                                    percentage = 0
                                else:
                                    percentage = int(
                                        k['offer']['discountValue'])
                            else:
                                percentage = 0
                        else:
                            percentage = 0

                        mou = ""
                        mou_unit = ""
                        minimum_qty = 0

                        recent_data.append({
                            "outOfStock": k['outOfStock'],
                            "unitId": k['unitId'],
                            "parentProductId": k['parentProductId'],
                            "childProductId": k['childProductId'],
                            "productName": k['productName'],
                            "availableQuantity": k['availableQuantity'],
                            "images": k['images'],
                            "timestamp": k['timestamp'],
                            "supplier": k['suppliers'],
                            "brandName": k['brandName'],
                            "variantData": k['variantData'],
                            "customerName": k['customerName'],
                            "userName": k['userName'],
                            "discountType": k['offer']['discountType'] if "discountType" in k['offer'] else 0,
                            "profilePic": k['profilePic'] if "profilePic" in k else "",
                            "finalPrice": k['unitsData']['finalPrice'],
                            "finalPriceList": {
                                "basePrice": k['unitsData']['basePrice'],
                                "finalPrice": k['unitsData']['finalPrice'],
                                "discountPrice": k['unitsData']['discountPrice'],
                                "discountPercentage": percentage
                            },
                            "totalRatingCount": k['totalRatingCount'],
                            "mouData": {
                                "mou": mou,
                                "mouUnit": mou_unit,
                                "mouQty": minimum_qty,
                                "minimumPurchaseUnit": ""
                            },
                            "currencySymbol": k['currencySymbol'],
                            "currency": k['currency'],
                            "avgRating": k['avgRating'],
                            "offers": k['offer']
                        })

                    sorted_data = sorted(recent_data, key=lambda k: k['availableQuantity'], reverse=True)
                    if len(sorted_data) > 0:
                        response = {
                            "data": sorted_data,
                            "penCount": post_product_count,
                            "total_count": post_product_count,
                            "message": "data found...!!!"
                        }
                        return_response = {"data": response}
                        return JsonResponse(return_response, safe=False, status=200)
                    else:
                        response = {
                            "data": [],
                            "penCount": 0,
                            "total_count": 0,
                            "message": "data not found...!!!"
                        }
                        return_response = {"data": response}
                        return JsonResponse(return_response, safe=False, status=404)
                else:
                    response = {
                        "data": [],
                        "penCount": 0,
                        "total_count": 0,
                        "message": "data not found...!!!"
                    }
                    return_response = {"data": response}
                    return JsonResponse(return_response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error_message = {
                "error": "Invalid request",
                "total_count": 0,
                "message": message
            }
            return JsonResponse(error_message, status=500)


class ProductSearch(APIView):
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
"""

    @swagger_auto_schema(method='get', tags=["Search & Filter"],
                         operation_description="API for getting the category, sub-category, sub-sub-category and search and filter",
                         required=['AUTHORIZATION', 'language'],
                         manual_parameters=[
                             openapi.Parameter(
                                 name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
                                 description="authorization token"),
                             openapi.Parameter(
                                 name='language', default="en", required=True, in_=openapi.IN_HEADER,
                                 type=openapi.TYPE_STRING, description="language"),
                             openapi.Parameter(
                                 name='currencycode', default="INR", required=True, in_=openapi.IN_HEADER,
                                 type=openapi.TYPE_STRING,
                                 description="currencySymbol for the currency..INR, INR, USD"),
                             openapi.Parameter(
                                 name='loginType', default="1", required=False, in_=openapi.IN_HEADER,
                                 type=openapi.TYPE_STRING,
                                 description="login type of the user. value should be 1 for retailer and 2 for distributor"),
                             openapi.Parameter(
                                 name='ipAddress', default="124.40.244.94", required=False, in_=openapi.IN_HEADER,
                                 type=openapi.TYPE_STRING,
                                 description="ip address of the network"),
                             openapi.Parameter(
                                 name='platform', default="0", required=False, in_=openapi.IN_HEADER,
                                 type=openapi.TYPE_STRING,
                                 description="from which platform requested for data.values will be 0 for website, 1 for iOS and 2 for android"),
                             openapi.Parameter(
                                 name='city', default="5df7b7218798dc2c1114e6bf", required=False, in_=openapi.IN_HEADER,
                                 type=openapi.TYPE_STRING,
                                 description="city id of the user where browser or app opened if not there value should be empty string"),
                             openapi.Parameter(
                                 name='country', default="5df7b7218798dc2c1114e6c0", required=False,
                                 in_=openapi.IN_HEADER, type=openapi.TYPE_STRING,
                                 description="country id of the user where browser or app opened if not there value should be empty string"),

                             openapi.Parameter(
                                 name='searchType', default="1", required=True, in_=openapi.IN_HEADER,
                                 type=openapi.TYPE_STRING,
                                 description="search type based on click on category, subcategory, subcategory or click on searched result. values should be 1 for category, 2 for subcategory, 3 for subsubcstegory, 4 for searched result click..Note if filter apply that time value should be 100"),
                             openapi.Parameter(
                                 name='searchIn', default="", required=False, in_=openapi.IN_HEADER,
                                 type=openapi.TYPE_STRING,
                                 description="search In for the data which on clicked.example In AppleMobile, In Mens"),
                             openapi.Parameter(
                                 name='latitude', default="12.9716", required=False, in_=openapi.IN_HEADER,
                                 type=openapi.TYPE_STRING,
                                 description="latitude of the user where website or application opened"),

                             openapi.Parameter(
                                 name='longitude', default="77.5946", required=False, in_=openapi.IN_HEADER,
                                 type=openapi.TYPE_STRING,
                                 description="longitude of the user where website or application opened"),

                             openapi.Parameter(
                                 name='storeCategoryId',
                                 default=ECOMMERCE_STORE_CATEGORY_ID,
                                 required=True,
                                 in_=openapi.IN_HEADER,
                                 type=openapi.TYPE_STRING,
                                 description="store category id for getting the products from only that category(eg. Ecommerce, dine, groccery)"
                             ), openapi.Parameter(
                                 name='cityId',
                                 default="5df7b7218798dc2c1114e6bf",
                                 in_=openapi.IN_HEADER,
                                 type=openapi.TYPE_STRING,
                                 description="fetching the data in particular city"
                             ),
                             openapi.Parameter(
                                 name='q',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="for the search the item in search bar ex. ni, nik, addi"
                             ),
                             openapi.Parameter(
                                 name='page',
                                 default="1",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="page number. which page number data want to display"
                             ),
                             openapi.Parameter(
                                 name='sort',
                                 default="price_asc",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="for the sorting like price high to low value's should be...for low to high price:price_asc, high to low price:price_desc,popularity:recency_desc"
                             ), openapi.Parameter(
                                 name='fname',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="category name of the product..ex. Men, Women"
                             ), openapi.Parameter(
                                 name='sname',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="sub category name of the product while getting data through subcategory that time category name mandatory..ex. Footware"
                             ), openapi.Parameter(
                                 name='tname',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="sub sub category name of the product while getting data through subsubcategory that time category name and subcategory mandatory..ex. Footware"
                             ), openapi.Parameter(
                                 name='colour',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="colour name for the which colour product want. ex. White, Green"
                             ), openapi.Parameter(
                                 name='bname',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="brand name for the product which want to display. ex. Nike, H&M"
                             ), openapi.Parameter(
                                 name='symptoms',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="symptom name for the product which want to display. ex. Nike, H&M"
                             ), openapi.Parameter(
                                 name='serves',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="serves name for the product which want to display. ex. 2 Person, 2-3 Person"
                             ), openapi.Parameter(
                                 name='nopieces',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="nopieces name for the product which want to display. ex. 1 Pcs, 10 Pcs"
                             ), openapi.Parameter(
                                 name='size',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="size for the product which want to display. ex. 8,9,S,M"
                             ), openapi.Parameter(
                                 name='maxprice',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="maximum price for the product while applying price filter"
                             ), openapi.Parameter(
                                 name='minprice',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="minimum price for the product while applying price filter"
                             ), openapi.Parameter(
                                 name='o_id',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="while need to displaying particular offers product. ex.5df89d3edd77d6ca2752bd10"
                             ), openapi.Parameter(
                                 name='s_id',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="while need to display particular stores product. ex.5df89d3edd77d6ca2752bd10"
                             ), openapi.Parameter(
                                 name='z_id',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="while need to display particular zone product. ex.5df8b7ad8798dc19da1a4b0e"
                             ), openapi.Parameter(
                                 name='facet_',
                                 default="",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="while applying filter on attribute it will creating dynamic link. ex. facet_OCCASION, facet_IDEAL FOR"
                             )
                         ],
                         responses={
                             200: 'successfully. data found',
                             404: 'data not found. it might be product not found',
                             401: 'Unauthorized. token expired',
                             500: 'Internal Server Error. if server is not working that time'
                         },
                         )
    @action(detail=False, methods=['get'])
    def get(self, request, *args, **kwargs):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            query = []
            start_time = time.time()
            should_query = []
            facet_value = ""
            facet_key = ""
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            try:
                user_id = json.loads(token)['userId']
            except:
                user_id = "5d92f959fc2045620ce36c92"
            try:
                session_id = json.loads(token)['sessionId']
            except:
                session_id = ""

            start_time = time.time()
            finalfilter_responseJson_products = []
            filter_responseJson = []
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            currency_code = request.META["HTTP_CURRENCYCODE"] if "HTTP_CURRENCYCODE" in request.META else ""
            login_type = 1
            ip_address = request.META["HTTP_IPADDRESS"] if "HTTP_IPADDRESS" in request.META else ""
            seach_platform = request.META["HTTP_PLATFORM"] if "HTTP_PLATFORM" in request.META else "0"
            city_name = request.META["HTTP_CITY"] if "HTTP_CITY" in request.META else ""
            country_name = request.META["HTTP_COUNTRY"] if "HTTP_COUNTRY" in request.META else ""
            latitude = float(
                request.META["HTTP_LATITUDE"]) if "HTTP_LATITUDE" in request.META else 0
            longitude = float(
                request.META["HTTP_LONGITUDE"]) if "HTTP_LONGITUDE" in request.META else 0
            popular_status = int(
                request.META["HTTP_POPULARSTATUS"]) if "HTTP_POPULARSTATUS" in request.META else 0
            search_type = int(
                request.META["HTTP_SEARCHTYPE"]) if "HTTP_SEARCHTYPE" in request.META else 100
            search_in = request.META["HTTP_SEARCHIN"] if "HTTP_SEARCHIN" in request.META else ""
            store_category_id = str(request.META["HTTP_STORECATEGORYID"])
            manager_data = db.managers.find_one({"_id": ObjectId(user_id), "linkedWith": 2})
            if manager_data is not None:
                margin_price = False
            else:
                margin_price = True

            # ========================================query parameter====================================================
            # for the search the item in search bar
            search_query = request.GET.get('q', "")
            page = int(request.GET.get('page', 1))  # for the pagination

            fname = request.GET.get('fname', "")  # category-name
            fname = fname.replace("%20", " ")
            fname = fname.replace("+", " ")

            sname = request.GET.get('sname', "")  # sub-category-name
            sname = sname.replace("%20", " ")
            sname = sname.replace("+", " ")

            tname = request.GET.get('tname', "")  # sub-sub-category-name
            tname = tname.replace("%20", " ")
            tname = tname.replace("+", " ")

            colour = request.GET.get('colour', "")  # colour name
            bname = request.GET.get('bname', "")  # brand name for the search
            symptoms_name = request.GET.get('symptoms', "")  # brand name for the search
            size = request.GET.get('size', "")  # size name
            serves = request.GET.get('serves', "")
            nopieces = request.GET.get('nopieces', "")
            max_price = request.GET.get('maxprice', "")  # maximum price
            min_price = request.GET.get("minprice", "")  # minimum price
            # sorting based on lowest price, newest first
            sort_data = request.GET.get("sort", "")
            # get the list of all products which have offers
            best_deals = request.GET.get("best_deals", "")
            offer_id = request.GET.get("o_id", "")  # get particular offer data
            store_id = request.GET.get("s_id", "")  # get particular offer data
            zone_id = request.GET.get("z_id", "")  # get particular offer data
            if store_id != "":
                zone_id = ""

            # started thread for inserting the data for search and category click data stored
            if int(search_type) != 100:
                thread_logs = threading.Thread(target=category_search_logs,
                                               args=(fname, sname, tname, str(search_type),
                                                     user_id, seach_platform, ip_address, latitude, longitude,
                                                     city_name, country_name, search_query, store_category_id,
                                                     search_in, session_id, store_id, False, "", ""))
                thread_logs.start()
            else:
                pass
            from_data = int(page * 30) - 30
            to_data = 30  # int(page*30)
            to_data_product = int(page * 30)
            facet_attribute = request.META['QUERY_STRING']
            # =========================================serves========================================================
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

            # =========================================nopieces========================================================
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

            categoty_details = db.cities.find_one(
                {"storeCategory.storeCategoryId": store_category_id},
                {"storeCategory": 1})
            remove_central = False
            if store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                remove_central = False
                zone_id = ""
            else:
                if categoty_details is not None:
                    if "storeCategory" in categoty_details:
                        for cat in categoty_details['storeCategory']:
                            if cat['storeCategoryId'] == store_category_id:
                                if cat['hyperlocal'] == True and cat['storeListing'] == 1:
                                    remove_central = True
                                    store_id = store_id
                                    zone_id = ""
                                elif cat['hyperlocal'] == True and cat['storeListing'] == 0:
                                    zone_id = zone_id
                                    remove_central = True
                                else:
                                    remove_central = False
                            else:
                                pass
                    else:
                        remove_central = False
                else:
                    remove_central = False

            next_availbale_driver_time = ""

            for facet in facet_attribute.split("&"):
                if "facet_" in facet:
                    if facet_value == "":
                        facet_value = facet_value + \
                                      ((facet.split("_")[1]).split(
                                          "=")[1]).replace("%20", " ")
                    else:
                        facet_value = facet_value + ", " + \
                                      ((facet.split("_")[1]).split(
                                          "=")[1]).replace("%20", " ")

                    if facet_key == "":
                        facet_key = facet_key + \
                                    ((facet.split("_")[1]).split(
                                        "=")[0]).replace("%20", " ")
                    else:
                        facet_key = facet_key + ", " + \
                                    ((facet.split("_")[1]).split(
                                        "=")[0]).replace("%20", " ")

            facet_value = facet_value.replace("+%2C", ",")
            facet_value = facet_value.replace("+", " ")
            facet_value = facet_value.replace("%2F", "/")
            facet_value = facet_value.strip()
            facet_value = facet_value.replace("%26", "")
            facet_value = facet_value.replace("%28", "(")
            facet_value = facet_value.replace("%29", ")")
            facet_value = facet_value.replace("%25", "%")
            # facet_value = facet_value.replace("ml", "")
            facet_key = facet_key.replace("+", " ")
            facet_value_data = ""
            if facet_value_data == "":
                facet_value_data = facet_value
            else:
                facet_value_data = facet_value_data

            if zone_id != "":
                zone_details = zone_find({"_id": ObjectId(zone_id)})
                store_query = {"categoryId": str(store_category_id), "cityId": zone_details['city_ID']}
                store_data = db.stores.find(store_query)
                store_data_details = ["0"]
                if store_data.count() > 0:
                    for store in store_data:
                        store_data_details.append(str(store['_id']))
                    query.append(
                        {
                            "terms":
                                {
                                    "storeId": store_data_details
                                    # "units.suppliers.id": store_data_details
                                }
                        }
                    )
            elif zone_id != "" and store_id != "":
                store_data_details = []
                store_data_details.append(store_id)
                store_data = db.stores.find(
                    {"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id})
                if store_data.count() > 0:
                    for store in store_data:
                        store_data_details.append(str(store['_id']))
                    query.append(
                        {
                            "terms":
                                {
                                    "storeId": store_data_details
                                }
                        }
                    )
            elif zone_id != "" and store_id == "":
                store_data_details = []
                store_data = db.stores.find(
                    {"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id})
                if store_data.count() > 0:
                    for store in store_data:
                        store_data_details.append(str(store['_id']))
                    query.append(
                        {
                            "terms":
                                {
                                    # "units.suppliers.id": store_data_details
                                    "storeId": store_data_details
                                }
                        }
                    )
            elif store_category_id == ECOMMERCE_STORE_CATEGORY_ID:
                pass
            else:
                if store_id != "":
                    query.append(
                        {
                            "match":
                                {
                                    "storeId": store_id
                                }
                        }
                    )
            query.append(
                {
                    "match":
                        {
                            "storeCategoryId": store_category_id
                        }
                }
            )

            if offer_id != "":
                query.append(
                    {
                        "match":
                            {
                                "offer.offerId": offer_id
                            }
                    }
                )

            if best_deals != "":
                query.append(
                    {
                        "match":
                            {
                                "offer.status": 1
                            }
                    }
                )

            if facet_value_data != "":
                number_facet = re.findall(r'\d+(?:\.\d+)?', facet_value_data)
                if len(number_facet) > 0:
                    # if "," in facet_value_data or "%2C" in facet_value_data:
                    facet_key = facet_key.title()
                    query.append(
                        {
                            "terms": {
                                "units.attributes.attrlist.attrname.en.keyword": facet_key.split(",")
                            }
                        }

                    )
                    facet_value_data = facet_value_data.replace("%2C", ",")
                    should_query.append(
                        {
                            "terms": {
                                "units.attributes.attrlist.value.en.keyword": number_facet
                                # facet_value_data.split(",")
                            }
                        }

                    )
                    should_query.append(
                        {
                            "terms": {
                                "units.attributes.attrlist.value.name.en.keyword": number_facet
                                # facet_value_data.split(",")
                            }
                        }

                    )
                else:
                    if "," in facet_value_data or "%2C" in facet_value_data:
                        facet_key = facet_key.title()
                        query.append(
                            {

                                "query_string": {
                                    "fields": ["units.attributes.attrlist.attrname.en"],
                                    "query": facet_key
                                }
                            }
                        )
                        facet_value_data = facet_value_data.replace("%2C", ",")
                        should_query.append(
                            {
                                "terms": {
                                    "units.attributes.attrlist.value.en.keyword": facet_value_data.split(",")
                                }
                            }

                        )
                        should_query.append(
                            {
                                "terms": {
                                    "units.attributes.attrlist.value.name.en.keyword": facet_value_data.split(",")
                                }
                            }

                        )
                    else:
                        facet_key = facet_key.title()
                        query.append(
                            {

                                "query_string": {
                                    "fields": ["units.attributes.attrlist.attrname.en"],
                                    "query": facet_key
                                }
                            }
                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.value.en": facet_value_data.replace("%20", "")
                                }
                            }

                        )
                        should_query.append(
                            {
                                "match_phrase_prefix": {
                                    "units.attributes.attrlist.value.name.en": facet_value_data.replace("%20", "")
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
                                "boost": 6
                            }
                        }
                    })
                should_query.append({
                    "match": {
                        "pPName.en": {
                            "query": search_query.replace("%20", " "),
                            "boost": 6
                        }
                    }
                })
                # ===========================unit name========================================
                should_query.append(
                    {
                        "match_phrase_prefix": {
                            "units.unitName.en": {
                                "analyzer": "standard",
                                "query": search_query.replace("%20", " "),
                                "boost": 5
                            }
                        }
                    })
                should_query.append({
                    "match": {
                        "units.unitName.en": {
                            "query": search_query.replace("%20", " "),
                            "boost": 5
                        }
                    }
                })
                # ===========================================detail description============================
                should_query.append({
                    "match": {
                        "detailDescription." + language: {
                            "query": search_query.replace("%20", " "),
                            "boost": 4
                        }
                    }
                })
                should_query.append({
                    "match_phrase_prefix": {
                        "detailDescription." + language: {
                            "query": search_query.replace("%20", " "),
                            "boost": 4
                        }
                    }
                })
                # ====================================child category=======================================
                should_query.append({
                    "match": {
                        "categoryList.parentCategory.childCatgory.categoryName." + language: {
                            "query": search_query.replace("%20", " "),
                            "boost": 3
                        }
                    }
                })
                should_query.append({
                    "match_phrase_prefix": {
                        "categoryList.parentCategory.childCatgory.categoryName." + language: {
                            "query": search_query.replace("%20", " "),
                            "boost": 3
                        }
                    }
                })
                # ===============================parent category name======================================
                should_query.append({
                    "match": {
                        "categoryList.parentCategory.categoryName." + language: {
                            "query": search_query.replace("%20", " "),
                            "boost": 2
                        }
                    }
                })
                should_query.append({
                    "match_phrase_prefix": {
                        "categoryList.parentCategory.categoryName." + language: {
                            "query": search_query.replace("%20", " "),
                            "boost": 2
                        }
                    }
                })
                # ======================================brand name=======================================
                should_query.append({
                    "match": {
                        "brandTitle." + language: {
                            "query": search_query.replace("%20", " "),
                            "boost": 1
                        }
                    }
                })
                should_query.append({
                    "match_phrase_prefix": {
                        "brandTitle." + language: {
                            "query": search_query.replace("%20", " "),
                            "boost": 1
                        }
                    }
                })

            if max_price != "" and min_price != "":
                if store_id == "":
                    query.append(
                        {
                            "range":
                                {
                                    "units.discountPrice": {
                                        "gte": float(min_price),
                                        "lte": float(max_price)
                                    }
                                }
                        }
                    )
                else:
                    query.append(
                        {
                            "range":
                                {
                                    "units.discountPrice": {
                                        "gte": float(min_price),
                                        "lte": float(max_price)
                                    }
                                }
                        }
                    )

            if size != "":
                size = size.replace("%2C", ",")
                size = size.replace(",", ", ")
                query.append({
                    "match": {
                        "units.unitSizeGroupValue.en": size.replace("%20", " ")
                    }
                })
            else:
                pass

            if colour != "":
                if "," in colour or "%2C" in colour:
                    query.append({
                        "match": {
                            "units.colorName": colour.replace("%20", " ")
                        }
                    })
                else:
                    query.append({
                        "term": {
                            "units.colorName.keyword": colour.replace("%20", " ")
                        }
                    })

            if bname != "":
                if "," in bname or "%2C" in bname:
                    bname = bname.replace("%20", " ")
                    query.append({
                        "terms": {
                            "brandTitle." + language + ".keyword": bname.split(",")  # bname.replace("%20", " ")
                        }
                    })
                else:
                    query.append({
                        "match_phrase_prefix": {
                            "brandTitle." + language: bname.replace("%20", " ")
                        }
                    })

            if serves != "":
                if "," in serves or "%2C" in serves:
                    serves = serves.replace("%20", " ")
                    query.append({
                        "terms": {
                            "serversFor.keyword": serves.split(",")
                        }
                    })
                else:
                    query.append({
                        "match_phrase_prefix": {
                            "serversFor": serves.replace("%20", " ")
                        }
                    })

            if nopieces != "":
                if "," in nopieces or "%2C" in nopieces:
                    nopieces = nopieces.replace("%20", " ")
                    query.append({
                        "terms": {
                            "numberOfPcs.keyword": nopieces.split(",")
                        }
                    })
                else:
                    query.append({
                        "match_phrase_prefix": {
                            "numberOfPcs": nopieces.replace("%20", " ")
                        }
                    })

            if symptoms_name != "":
                if "," in symptoms_name or "%2C" in symptoms_name:
                    symptoms_name = symptoms_name.replace("%20", " ")
                    query.append({
                        "terms": {
                            "symptoms.symptomName.keyword": symptoms_name.split(",")
                        }
                    })
                else:
                    query.append({
                        "match_phrase_prefix": {
                            "symptoms.symptomName": symptoms_name.replace("%20", " ")
                        }
                    })

            if fname != "":
                fname = fname.replace("%2C", ",")
                category_query = {"categoryName.en": fname.replace("%20", " ")}
                if store_id != "":
                    category_query["$or"] = [{"storeid": {"$in": [store_id]}, "storeId": {"$in": [store_id]}}]
                category_details = db.category.find_one(category_query)
                query.append({
                    "match_phrase_prefix": {
                        "categoryList.parentCategory.categoryName." + language: fname.replace("%20", " ")
                    }
                })

            if sname != "":
                if "," in sname or "%2C" in sname:
                    query.append(
                        {
                            "match": {
                                "categoryList.parentCategory.childCategory.categoryName." + language: sname.replace(
                                    "%20", " ")
                            }
                        }
                    )
                else:
                    query.append(
                        {"term": {
                            "categoryList.parentCategory.childCategory.categoryName." + language + ".keyword": sname.replace(
                                "%20", " ")}})
            if tname != "":
                if "," in tname or "%2C" in tname:
                    query.append(
                        {
                            "match": {
                                "categoryList.parentCategory.childCategory.categoryName." + language: tname.replace(
                                    "%20", " ")
                            }
                        }
                    )
                else:
                    query.append(
                        {
                            "match_phrase_prefix": {
                                "categoryList.parentCategory.childCategory.categoryName." + language: tname.replace(
                                    "%20", " ")
                            }
                        }
                    )

            if "_" in sort_data:
                if "price" in sort_data.split("_")[0]:
                    if sort_data.split("_")[1] == "desc":
                        sort_type = 1
                    else:
                        sort_type = 0

                    sort_query = [
                        {
                            "isInStock": {
                                "order": "desc"
                            }
                        },
                        {
                            "units.discountPrice": {
                                "order": sort_data.split("_")[1]
                            }
                        }
                    ]
                elif "recency" in sort_data.split("_")[0]:
                    sort_type = 2
                    sort_query = [
                        {
                            "isInStock": {
                                "order": "desc"
                            }
                        },
                        {
                            "units.discountPrice": {
                                "order": "asc"
                            }
                        }
                    ]
                else:
                    sort_type = 2
                    sort_query = [
                        {
                            "isInStock": {
                                "order": "desc"
                            }
                        },
                        {
                            "units.discountPrice": {
                                "order": "asc"
                            }
                        }
                    ]
            else:
                sort_type = 2
                sort_query = [
                    {
                        "isInStock": {
                            "order": "desc"
                        }
                    },
                    {
                        "units.discountPrice": {
                            "order": "asc"
                        }
                    }
                ]
            query.append({
                "match":
                    {
                        "status": 1
                    }
            }
            )
            number_for = [0]
            number_for.append(int(login_type))
            query.append({
                "terms": {
                    "productFor": number_for
                }
            })
            if len(should_query) > 0:
                search_item_query = {
                    "query":
                        {
                            "bool":
                                {
                                    "must": query,
                                    "should": should_query,
                                    "must_not": [
                                        {
                                            "match": {"storeId": "0"}
                                        }
                                    ],
                                    "minimum_should_match": 1,
                                    "boost": 1.0
                                }
                        },
                    "track_total_hits": True,
                    "sort": sort_query,
                }
            else:
                search_item_query = {
                    "query":
                        {
                            "bool":
                                {
                                    "must": query,
                                    "must_not": [
                                        {
                                            "match": {"storeId": "0"}
                                        }
                                    ]
                                }
                        },
                    "track_total_hits": True,
                    "sort": sort_query
                }
            print(search_item_query)
            if type(search_item_query) == str or type(search_item_query) == "":
                search_item_query = json.loads(search_item_query)
            res = es.search(
                index=index_products,
                body=search_item_query,
                filter_path=[
                    "hits.hits._id",
                    "hits.total",
                    "hits.hits._score",
                    "hits.hits._source",
                ]
            )
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
            data = product_search_read(
                res,
                start_time,
                language,
                filter_responseJson,
                finalfilter_responseJson_products,
                popular_status,
                sort, login_type, store_id, sort_type, store_category_id, int(
                    from_data), int(to_data_product), user_id, remove_central,
                zone_id, min_price, max_price, margin_price, currency_code, search_query, token
            )
            if len(data["data"]) == 0:
                return JsonResponse(data, safe=False, status=404)
            else:
                if data['data']['penCount'] == 0:
                    return JsonResponse(data, safe=False, status=404)
                else:
                    return JsonResponse(data, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)


'''
    API to get the nearest store(s)
'''


class GetNearestStores(APIView):
    @swagger_auto_schema(
        method='get', tags=["CUAPP"],
        operation_description="API for get all outlet near you", required=['AUTHORIZATION'],
        manual_parameters=[openapi.Parameter(
            name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
            description="authorization token"),
            openapi.Parameter(
                name='language', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
                description="language", default="en"),
            openapi.Parameter(
                name='storeCategoryId', in_=openapi.IN_QUERY, type=openapi.TYPE_STRING, required=True,
                description="store category id for get the stores", default=DINE_STORE_CATEGORY_ID),
            openapi.Parameter(
                name='lat', required=True, default="13.05176", in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING, description="latitude of the user's location"),
            openapi.Parameter(
                name='long', default="77.580448", required=True, in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING, description="longitude of the user's location")],
        responses={200: 'Nearest store(s) found successfully.', 404:
            'data not found. it might be store not found or location not found.', 401: 'Unauthorized. token expired',
                   422: 'Fields are missing. required Fields are missing', 500:
                       'Internal Server Error. if server is not working that time'},
        operation_summary="Nearest Store API")
    @action(detail=False, methods=['get'])
    def get(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            language = request.META["HTTP_LANGUAGE"]
            store_category_id = request.GET.get("storeCategoryId", "")
            search_text = request.GET.get("search", "")
            lat = float(request.GET.get("lat", 0))
            long = float(request.GET.get("long", 0))
            cusines_data = []
            if token == "":
                pass
                response_data = {
                    "message": "unauthorized",
                    "data": []
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif lat == "":
                response_data = {
                    "message": "latitude is blank",
                    "data": []
                }
                return JsonResponse(response_data, safe=False, status=422)
            elif long == "":
                response_data = {
                    "message": "longtitude is blank",
                    "data": []
                }
                return JsonResponse(response_data, safe=False, status=422)
            elif store_category_id == "":
                response_data = {
                    "message": "storeCategoryId is blank",
                    "data": []
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                user_id = json.loads(token)['userId']
                # user_id = "5efad21cdb643f6e4b42f2dc"

                # ================================for more stores==================================
                more_must_query = [{"match": {"status": 1}}, {
                    "match": {
                        "categoryId": store_category_id
                    }
                }]
                geo_distance_sort = {
                    "_geo_distance": {
                        "distance_type": "plane",
                        "location": {
                            "lat": float(lat),
                            "lon": float(long)
                        },
                        "order": "asc",
                        "unit": "km"
                    }
                }
                sort_query = [geo_distance_sort]
                more_must_query.append({
                    "geo_distance": {
                        "distance": "50km",
                        "location": {
                            "lat": float(lat),
                            "lon": float(long)
                        }
                    }
                })

                store_more_geo_query = {
                    "query":
                        {
                            "bool":
                                {
                                    "must": more_must_query,
                                }
                        },
                    "size": 100,
                    "from": 0,
                    "sort": sort_query
                }
                res_more = es.search(
                    index=index_store,
                    body=store_more_geo_query,
                    filter_path=[
                        "hits.hits._id",
                        "hits.hits._source"
                    ],
                )
                # ============================query to get the distance by geo location==============
                store_json = []
                store_ids = []
                if len(res_more) > 0:
                    for seller in res_more['hits']['hits']:
                        cusine_name = ""
                        store_must_query = [{"match": {"_id": str(seller['_id'])}}]
                        geo_distance_sort = {
                            "_geo_distance": {
                                "distance_type": "plane",
                                "location": {
                                    "lat": float(lat),
                                    "lon": float(long)
                                },
                                "order": "asc",
                                "unit": "km"
                            }
                        }
                        sort_query = [geo_distance_sort]
                        store_must_query.append({
                            "geo_distance": {
                                "distance": "50km",
                                "location": {
                                    "lat": float(lat),
                                    "lon": float(long)
                                }
                            }
                        })
                        store_geo_query = {
                            "query":
                                {
                                    "bool":
                                        {
                                            "must": store_must_query,
                                        }
                                },
                            "size": 1,
                            "from": 0,
                            "sort": sort_query
                        }
                        res = es.search(
                            index=index_store,
                            body=store_geo_query,
                            filter_path=[
                                "hits.total",
                                "hits.hits._id",
                                "hits.hits.sort",
                                "hits.hits._source"
                            ],
                        )
                        if res['hits']['total'] > 0:
                            if "hits" in res['hits']:
                                for seller_distance in res['hits']['hits']:
                                    distance_km = round(seller_distance['sort'][0], 2)
                                    distance_miles = round(distance_km * float(conv_fac), 2)
                            else:
                                distance_km = 0
                                distance_miles = 0
                        else:
                            distance_km = 0
                            distance_miles = 0

                        child_stores_data = []
                        if "parentSellerIdOrSupplierId" in seller["_source"] and "parentStore" in seller["_source"]:
                            # check if parent store
                            if int(seller["_source"]["parentStore"]) == 1:
                                parent_store_id = seller["_id"]
                                child_stores = db.stores.find({
                                    "parentSellerIdOrSupplierId": str(parent_store_id),
                                    "status": 1
                                })
                                if child_stores.count():
                                    for cs in child_stores:
                                        child_data = {}
                                        child_data['address'] = cs['businessLocationAddress']['address'] if 'address' in \
                                                                                                            cs[
                                                                                                                'businessLocationAddress'] else ""
                                        child_data['addressArea'] = cs['businessLocationAddress'][
                                            'addressArea'] if "addressArea" in cs['businessLocationAddress'] else ""
                                        child_data['locality'] = cs['businessLocationAddress'][
                                            'locality'] if "locality" in cs['businessLocationAddress'] else ""
                                        child_data['post_code'] = cs['businessLocationAddress'][
                                            'postCode'] if "postCode" in cs['businessLocationAddress'] else ""
                                        child_data['state'] = cs['businessLocationAddress']['state'] if "state" in cs[
                                            'businessLocationAddress'] else ""
                                        child_data['country'] = cs['businessLocationAddress']['country'] if "country" in \
                                                                                                            cs[
                                                                                                                'businessLocationAddress'] else ""
                                        child_data['city'] = cs['businessLocationAddress']['city'] if "city" in cs[
                                            'businessLocationAddress'] else ""
                                        child_data['location'] = cs['location'] if "location" in cs else {}
                                        child_data['child_store_id'] = str(cs['_id'])
                                        child_data['favorite'] = False
                                        child_stores_data.append(child_data)
                                        store_ids.append(str(cs['_id']))

                        address = seller["_source"]['businessLocationAddress']['address'] if "address" in \
                                                                                             seller["_source"][
                                                                                                 'businessLocationAddress'] else ""
                        addressArea = seller["_source"]['businessLocationAddress']['addressArea'] if "addressArea" in \
                                                                                                     seller["_source"][
                                                                                                         'businessLocationAddress'] else ""
                        locality = seller["_source"]['businessLocationAddress']['locality'] if "locality" in \
                                                                                               seller["_source"][
                                                                                                   'businessLocationAddress'] else ""
                        post_code = seller["_source"]['businessLocationAddress']['postCode'] if "postCode" in \
                                                                                                seller["_source"][
                                                                                                    'businessLocationAddress'] else ""
                        state = seller["_source"]['businessLocationAddress']['state'] if "state" in seller["_source"][
                            'businessLocationAddress'] else ""
                        country = seller["_source"]['businessLocationAddress']['country'] if "country" in \
                                                                                             seller["_source"][
                                                                                                 'businessLocationAddress'] else ""
                        city = seller["_source"]['businessLocationAddress']['city'] if "city" in seller["_source"][
                            'businessLocationAddress'] else ""
                        store_ids.append(str(seller['_id']))
                        store_json.append({"address": address, "locality": locality, "addressArea": addressArea,
                                           "logoImages": seller['_source']['logoImages'],
                                           "bannerImages": seller['_source']['bannerImages'],
                                           "cityId": seller['_source']['cityId'],
                                           "minimumOrder": seller['_source']['minimumOrder'],
                                           "nextCloseTime": seller['_source']['nextCloseTime'] if "nextCloseTime" in
                                                                                                  seller[
                                                                                                      '_source'] else "",
                                           "nextOpenTime": seller['_source']['nextOpenTime'] if "nextOpenTime" in
                                                                                                seller[
                                                                                                    '_source'] else "",
                                           "avgRating": seller['_source']['avgRating'] if "avgRating" in seller[
                                               '_source'] else 0,
                                           "storeIsOpen": seller['_source']['storeIsOpen'] if "storeIsOpen" in seller[
                                               '_source'] else False,
                                           "driverTypeId": seller['_source']['driverTypeId'] if "driverTypeId" in
                                                                                                seller[
                                                                                                    '_source'] else 0,
                                           "driverType": seller['_source']['driverType'] if "driverType" in seller[
                                               '_source'] else "",
                                           "storeType": seller['_source']['storeType'] if "storeType" in seller[
                                               '_source'] else "Food",
                                           "location": seller['_source']['location'] if "location" in seller[
                                               '_source'] else {},
                                           "averageDeliveryTime": str(seller["_source"][
                                                                          'averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in
                                                                                                                          seller[
                                                                                                                              "_source"] else "",
                                           "state": state, "country": country,
                                           "city": city, "distanceKm": round(distance_km, 2),
                                           "currencySymbol": seller['_source']['currencyCode'] if 'currencyCode' in
                                                                                                  seller[
                                                                                                      '_source'] else "₹",
                                           "currency": seller['_source']['currencyCode'] if 'currencyCode' in seller[
                                               '_source'] else "INR",
                                           "distanceMiles": distance_miles,
                                           "storeName": seller['_source']['storeName']['en'],
                                           "storeId": seller['_id'], "checkinCount": 0,
                                           "favorite": False,
                                           "location": seller['_source']['location'] if 'location' in seller[
                                               '_source'] else {},
                                           "childStores": child_stores_data if len(child_stores_data) else []})
                    # get checkins from store id
                    checkins = db.checkins.aggregate([{
                        "$group": {
                            "_id": "$storeId",
                            "count": {"$sum": 1}
                        }
                    }])

                    # save checkins to store
                    for check_in in checkins:
                        for store in store_json:
                            if str(store['storeId']) == str(check_in['_id']):
                                store['checkinCount'] = str(check_in['count'])
                            else:
                                store['checkinCount'] = 0

                    # get the favorite details for the user
                    response_cassandra = session.execute(
                        """SELECT * FROM favouritesellersuserwise where userid=%(userid)s AND seller_id IN %(seller_id)s ALLOW FILTERING""",
                        {"userid": user_id, "seller_id": ValueSequence(list(set(store_ids)))})

                    # process the data
                    if response_cassandra:
                        cassandra_data = pd.DataFrame(response_cassandra._current_rows).to_dict("record")

                        for cass_result in cassandra_data:
                            # check for parent store
                            if str(cass_result['seller_id']) in [str(x['storeId']) for x in store_json if
                                                                 'storeId' in x]:
                                for pstore in store_json:
                                    if pstore['storeId'] == str(cass_result['seller_id']):
                                        pstore['favorite'] = True

                            # check for child store
                            if str(cass_result['seller_id']) in [y['storeId'] for x in store_json for y in
                                                                 x['childStores'] if 'storeId' in y]:
                                for pstore in store_json:
                                    for cstore in pstore['childStores']:
                                        if cstore['child_store_id'] == str(cass_result['seller_id']):
                                            cstore['favorite'] = True

                    response = {
                        "data": store_json,
                        "message": "data not found"
                    }
                    return JsonResponse(response, safe=False, status=200)
                else:
                    response = {
                        "data": [],
                        "message": "data not found"
                    }
                    return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error_message = {
                "error": "Invalid request",
                "message": message,
                "rating": 0
            }
            return JsonResponse(error_message, status=500)


class AllEcommerceStoreList(APIView):
    @swagger_auto_schema(
        method='get', tags=["Store List"],
        operation_description="API for get all outlet", required=['AUTHORIZATION'],
        manual_parameters=[openapi.Parameter(
            name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
            description="authorization token"),
            openapi.Parameter(
                name='language', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
                description="language", default="en"),
            openapi.Parameter(
                name='storeCategoryId', in_=openapi.IN_QUERY, type=openapi.TYPE_STRING, required=True,
                description="store category id for get the stores", default=ECOMMERCE_STORE_CATEGORY_ID),
            openapi.Parameter(
                name='page', in_=openapi.IN_QUERY, type=openapi.TYPE_STRING, required=True,
                description="from which page we need to return the stores", default=1),
        ],
        responses={200: 'Nearest store(s) found successfully.', 404:
            'data not found. it might be store not found or location not found.', 401: 'Unauthorized. token expired',
                   422: 'Fields are missing. required Fields are missing', 500:
                       'Internal Server Error. if server is not working that time'},
        operation_summary="Nearest Store API")
    @action(detail=False, methods=['get'])
    def get(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            store_category_id = request.GET.get("storeCategoryId", "")
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "data": []
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif store_category_id == "":
                response_data = {
                    "message": "storeCategoryId is blank",
                    "data": []
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # ================================for more stores==================================
                more_must_query = [
                    {"match": {"status": 1}},
                    {
                        "match": {
                            "categoryId": store_category_id
                        }
                    }]

                store_more_geo_query = {
                    "query":
                        {
                            "bool":
                                {
                                    "must": more_must_query,
                                }
                        },
                    "size": 500,
                    "from": 0,
                    "sort": [
                        {
                            "storeName.en.keyword": {
                                "order": "asc"
                            }
                        }
                    ]
                }
                res_more = es.search(
                    index=index_store,
                    body=store_more_geo_query,
                    filter_path=[
                        "hits.hits._id",
                        "hits.hits._source"
                    ],
                )
                # ============================query to get the distance by geo location==============
                store_json = []
                json_data = []
                if len(res_more) > 0:
                    for seller in res_more['hits']['hits']:
                        child_product_count = db.childProducts.find(
                            {
                                "storeId": ObjectId(seller['_id']),
                                "units.isPrimary": True,
                                "status": 1
                            }
                        ).count()
                        if child_product_count > 0:
                            store_json.append(
                                {
                                    "logoImages": seller['_source']['logoImages'],
                                    "bannerImages": seller['_source']['bannerImages'],
                                    "storeIsOpen": seller['_source']['storeIsOpen'] if "storeIsOpen" in seller[
                                        '_source'] else False,
                                    "storeName": (seller['_source']['storeName']['en']).title(),
                                    "storeId": seller['_id'],
                                    "char": seller['_source']['storeName']['en'][0].upper(),
                                    "productCount": child_product_count
                                }
                            )
                    if len(store_json) > 0:
                        values_by_char = {}
                        for d in store_json:
                            values_by_char.setdefault(d["char"].upper(), []).append(
                                {
                                    "logoImages": d['logoImages'],
                                    "bannerImages": d['bannerImages'],
                                    "storeIsOpen": d['storeIsOpen'],
                                    "storeName": (d['storeName']).title(),
                                    "storeId": d['storeId'],
                                    "char": d['char'],
                                    "productCount": d['productCount']
                                }
                            )
                        for values in values_by_char:
                            json_data.append(
                                {
                                    "stores": values_by_char[values],
                                    "char": values
                                }
                            )
                    else:
                        pass
                    if len(json_data) > 0:
                        newlist = sorted(json_data, key=lambda k: k['char'], reverse=False)
                        response = {
                            "data": newlist,
                            "message": "data found"
                        }
                        return JsonResponse(response, safe=False, status=200)
                    else:
                        response = {
                            "data": [],
                            "message": "data not found"
                        }
                        return JsonResponse(response, safe=False, status=404)
                else:
                    response = {
                        "data": [],
                        "message": "data not found"
                    }
                    return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error_message = {
                "error": "Invalid request",
                "message": message,
                "rating": 0
            }
            return JsonResponse(error_message, status=500)


class NearestStoreFilter(APIView):
    '''CU app nearest store filter.'''

    @swagger_auto_schema(
        method='post', tags=["CUAPP"],
        operation_description="API for nearest store filter.",
        required=['AUTHORIZATION'],
        manual_parameters=POST_NEAREST_STORE_FILTER_MANUAL_PARAM,
        request_body=POST_NEAREST_STORE_FILTER_REQUEST_BODY,
        responses=NEAREST_STORE_FILTER_RESPONSES,
        operation_summary="CU App homepage filter")
    @action(detail=False, methods=['post'])
    def post(self, request):
        try:
            err = UtilsObj.check_authentication(request)
            if err: return err

            token = UtilsObj.get_auth_token(request)
            language = UtilsObj.get_lang(request)
            store_category_ids = request.data.get("storeCategoryIds", [])
            project_ids = request.data.get("projectIds", [])
            friend_ids = request.data.get("friendIds", [])
            match_ids = request.data.get("matchIds", [])
            distance_from = request.data.get("distanceFrom", 0)
            distance_to = request.data.get("distanceUpTo", 50)
            ratings = request.data.get("ratings", "")
            lat = float(request.data.get("lat", 0))
            long = float(request.data.get("long", 0))

            params = ['lat', 'long']
            err = UtilsObj.check_req_params(request.data, params)
            if err: return err

            token = UtilsObj.get_auth_token(request)
            # user_id = UtilsObj.get_userId(request)

            # queries
            store_category_filter = {"status": 1}
            project_filter = {"status": "ACTIVE"}
            store_ratings_filter = {"status": 1}

            # store category filter
            store_categories_filtered_id = []
            if len(store_category_ids):
                store_category_filter['categoryId'] = {"$in": [ObjectId(x) for x in store_category_ids]}
            avail_store_cat = db.stores.find(store_category_filter).sort("_id", -1)
            for store in avail_store_cat:
                store_categories_filtered_id.append(str(store['storeId']))

            # projects filter
            mutual_projects_filtered_id = []
            if len(project_ids):
                project_filter["_id"] = {"$in": [ObjectId(x) for x in project_ids]}
            if len(friend_ids) or len(match_ids):
                friend_and_match = friend_ids + match_ids
                project_filter["$or"] = [{"userFav": {"$in": friend_and_match}}]
            avail_projects = db.project.find(project_filter).sort("_id", -1)
            for pro in avail_projects:
                mutual_projects_filtered_id.append(str(pro['storeId']))

            # friends checkins filter
            friend_filtered_id = []
            if not len(friend_ids):
                friends_api_url = SWAGGER_URL + "v1/friends"

                payload = {}
                headers = {
                    'lang': language,
                    'Authorization': token
                }
                friend_response = requests.request("GET", friends_api_url, headers=headers, data=payload)
                if friend_response.status_code == 200:
                    friend_filtered_id.extend([x["_id"] for x in friend_response.json().get("data", [])])
                else:
                    print(Ar, friend_response.text)

            # matches checkins filter
            match_filtered_id = []
            if not len(match_ids):
                matches_api_url = SWAGGER_URL + "v1/Chats?pageNo=0"

                payload = {}
                headers = {
                    'Authorization': token,
                    'lang': language,
                    'Content-Type': 'text/plain'
                }
                match_response = requests.request("GET", matches_api_url, headers=headers, data=payload)
                if match_response.status_code == 200:
                    match_filtered_id.extend([x["senderId"] for x in match_response.json().get("data", [])])
                else:
                    print(Ar, match_response.text)

            # store rating filter
            store_rating_filtered_id = []
            if ratings != "":
                store_ratings_filter["avgRating"] = {"$lte": float(ratings)}
            store_ratings = db.stores.find(store_ratings_filter)
            for store in store_ratings:
                store_rating_filtered_id.append(str(store['_id']))

            # geo location filter
            nearest_store_filtered_json = []
            nearest_store_ids = []
            combined_storeIds = store_categories_filtered_id + mutual_projects_filtered_id + store_rating_filtered_id
            for storeId in combined_storeIds:
                more_must_query = [
                    {"match": {"status": 1}},
                    {
                        "match": {
                            "_id": storeId
                        }
                    }
                ]
                more_must_query.append({
                    "geo_distance": {
                        "distance": str(distance_to) + "km",
                        "location": {
                            "lat": float(lat),
                            "lon": float(long)
                        }
                    }
                })
                geo_distance_sort = {
                    "_geo_distance": {
                        "distance_type": "plane",
                        "location": {
                            "lat": float(lat),
                            "lon": float(long)
                        },
                        "order": "asc",
                        "unit": "km"
                    }
                }
                sort_query = [geo_distance_sort]

                store_more_geo_query = {
                    "query":
                        {
                            "bool":
                                {
                                    "must": more_must_query,
                                }
                        },
                    "size": 100,
                    "from": 0,
                    "sort": sort_query
                }
                res_more = es.search(
                    index=index_store,
                    body=store_more_geo_query,
                    filter_path=[
                        "hits.hits._id",
                        "hits.hits._source"
                    ],
                )
                if len(res_more) > 0:
                    for seller in res_more['hits']['hits']:
                        store_must_query = [{"match": {"_id": str(seller['_id'])}}]
                        geo_distance_sort = {
                            "_geo_distance": {
                                "distance_type": "plane",
                                "location": {
                                    "lat": float(lat),
                                    "lon": float(long)
                                },
                                "order": "asc",
                                "unit": "km"
                            }
                        }
                        sort_query = [geo_distance_sort]
                        store_must_query.append({
                            "geo_distance": {
                                "distance": "50km",
                                "location": {
                                    "lat": float(lat),
                                    "lon": float(long)
                                }
                            }
                        })
                        store_geo_query = {
                            "query":
                                {
                                    "bool":
                                        {
                                            "must": store_must_query,
                                        }
                                },
                            "size": 1,
                            "from": 0,
                            "sort": sort_query
                        }
                        res = es.search(
                            index=index_store,
                            body=store_geo_query,
                            filter_path=[
                                "hits.total",
                                "hits.hits._id",
                                "hits.hits.sort",
                                "hits.hits._source"
                            ],
                        )
                        if res['hits']['total'] > 0:
                            if "hits" in res['hits']:
                                for seller_distance in res['hits']['hits']:
                                    distance_km = round(seller_distance['sort'][0], 2)
                                    distance_miles = round(distance_km * float(conv_fac), 2)
                            else:
                                distance_km = 0
                                distance_miles = 0
                        else:
                            distance_km = 0
                            distance_miles = 0

                        address = seller["_source"]['businessLocationAddress']['address'] if "address" in \
                                                                                             seller["_source"][
                                                                                                 'businessLocationAddress'] else ""
                        addressArea = seller["_source"]['businessLocationAddress']['addressArea'] if "addressArea" in \
                                                                                                     seller["_source"][
                                                                                                         'businessLocationAddress'] else ""
                        locality = seller["_source"]['businessLocationAddress']['locality'] if "locality" in \
                                                                                               seller["_source"][
                                                                                                   'businessLocationAddress'] else ""
                        post_code = seller["_source"]['businessLocationAddress']['postCode'] if "postCode" in \
                                                                                                seller["_source"][
                                                                                                    'businessLocationAddress'] else ""
                        state = seller["_source"]['businessLocationAddress']['state'] if "state" in seller["_source"][
                            'businessLocationAddress'] else ""
                        country = seller["_source"]['businessLocationAddress']['country'] if "country" in \
                                                                                             seller["_source"][
                                                                                                 'businessLocationAddress'] else ""
                        city = seller["_source"]['businessLocationAddress']['city'] if "city" in seller["_source"][
                            'businessLocationAddress'] else ""
                        nearest_store_ids.append(str(seller['_id']))
                        nearest_store_filtered_json.append({"address": address,
                                                            "locality": locality, "addressArea": addressArea,
                                                            "logoImages": seller['_source']['logoImages'],
                                                            "bannerImages": seller['_source']['bannerImages'],
                                                            "cityId": seller['_source']['cityId'],
                                                            "minimumOrder": seller['_source']['minimumOrder'],
                                                            "nextCloseTime": seller['_source'][
                                                                'nextCloseTime'] if "nextCloseTime" in seller[
                                                                '_source'] else "",
                                                            "nextOpenTime": seller['_source'][
                                                                'nextOpenTime'] if "nextOpenTime" in seller[
                                                                '_source'] else "",
                                                            "avgRating": seller['_source'][
                                                                'avgRating'] if "avgRating" in seller['_source'] else 0,
                                                            "storeIsOpen": seller['_source'][
                                                                'storeIsOpen'] if "storeIsOpen" in seller[
                                                                '_source'] else False,
                                                            "driverTypeId": seller['_source'][
                                                                'driverTypeId'] if "driverTypeId" in seller[
                                                                '_source'] else 0,
                                                            "driverType": seller['_source'][
                                                                'driverType'] if "driverType" in seller[
                                                                '_source'] else "",
                                                            "storeType": seller['_source'][
                                                                'storeType'] if "storeType" in seller[
                                                                '_source'] else "Food",
                                                            "location": seller['_source']['location'] if "location" in
                                                                                                         seller[
                                                                                                             '_source'] else {},
                                                            "averageDeliveryTime": str(seller["_source"][
                                                                                           'averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in
                                                                                                                                           seller[
                                                                                                                                               "_source"] else "",
                                                            "state": state, "country": country,
                                                            "city": city, "distanceKm": round(distance_km, 2),
                                                            "currencySymbol": seller['_source'][
                                                                'currencyCode'] if 'currencyCode' in seller[
                                                                '_source'] else "₹",
                                                            "currency": seller['_source'][
                                                                'currencyCode'] if 'currencyCode' in seller[
                                                                '_source'] else "INR",
                                                            "distanceMiles": distance_miles,
                                                            "storeName": seller['_source']['storeName']['en'],
                                                            "storeId": seller['_id'],
                                                            "location": seller['_source']['location'] if 'location' in
                                                                                                         seller[
                                                                                                             '_source'] else {},
                                                            "favouriteUsers": seller[
                                                                'favouriteUsers'] if 'favouriteUsers' in seller[
                                                                '_id'] else []})

            nearest_store_ids = set(nearest_store_ids)
            nearest_store_ids = list(nearest_store_ids)

            # apply filter
            mutual_checkin_stores = []
            checkins_filter = {"$and": []}
            if len(store_categories_filtered_id) or len(mutual_projects_filtered_id) or \
                    len(nearest_store_ids) or len(store_rating_filtered_id):
                stores_id = store_categories_filtered_id + mutual_projects_filtered_id + mutual_projects_filtered_id + nearest_store_ids + store_rating_filtered_id
                stores_id = [str(x) for x in stores_id]
                stores_id = list(set(stores_id))
                checkins_filter["$and"].append({"storeId": {"$in": stores_id}})
            if len(friend_filtered_id) or len(match_filtered_id):
                friend_and_match_id = friend_filtered_id + match_filtered_id
                friend_and_match_id = list(set(friend_and_match_id))
                checkins_filter["$and"].append({"userId": {"$in": friend_and_match_id}})
            if len(checkins_filter["$and"]):
                store_checkins = db.checkIns.find(checkins_filter).sort("checkInTime", -1)
                for store in store_checkins:
                    store['_id'] = str(store['_id'])
                    mutual_checkin_stores.append(store)

            if len(mutual_checkin_stores):
                data = {
                    "checkins": mutual_checkin_stores,
                    "stores": nearest_store_filtered_json,
                    "storeCount": len(nearest_store_filtered_json),
                    "checkinsCount": len(mutual_checkin_stores),
                    "message": "Data found successfully."
                }
                status = 200
            else:
                data = {
                    "checkins": mutual_checkin_stores,
                    "stores": nearest_store_filtered_json,
                    "storeCount": len(nearest_store_filtered_json),
                    "checkinsCount": len(mutual_checkin_stores),
                    "message": "Data not found."
                }
                status = 404
            return JsonResponse(data, safe=False, status=status)

        except Exception as ex:
            return UtilsObj.raise_exception(ex)


class TaggedPostDetails(APIView):
    @swagger_auto_schema(method='get', tags=["Products"],
                         operation_description="Api get all tagged post list which have product linked",
                         required=['AUTHORIZATION'],
                         manual_parameters=[
                             openapi.Parameter(
                                 name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
                                 description="authorization token"),
                             openapi.Parameter(
                                 name='language', default="en", in_=openapi.IN_HEADER, type=openapi.TYPE_STRING,
                                 required=True, description="language, for language support"),
                             openapi.Parameter(
                                 name='productId',
                                 required=True,
                                 default="612cef830579c781a1fb7902",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="product id from which product post need to fetch, from pdp page response "
                                             "need to send value from childProductId key."
                             ),
                             openapi.Parameter(
                                 name='skip',
                                 required=True,
                                 default="0",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="skip, how much data needs to skip"
                             ),
                             openapi.Parameter(
                                 name='limit',
                                 required=True,
                                 default="10",
                                 in_=openapi.IN_QUERY,
                                 type=openapi.TYPE_STRING,
                                 description="limit, how much data needs to show after skip"
                             )
                         ],
                         responses={
                             200: 'successfully.',
                             404: 'data not found.',
                             401: 'Unauthorized. token expired',
                             500: 'Internal Server Error. if server is not working that time'
                         },
                         )
    @action(detail=False, methods=['get'])
    def get(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            if token == "":
                response_data = {
                    "message": "Unauthorized"
                }
                return JsonResponse(response_data, safe=False, status=401)
            else:
                product_id = request.GET.get("productId", "")
                skip_data = int(request.GET.get("skip", "0"))
                limit_data = int(request.GET.get("limit", "10"))
                if product_id == "":
                    response_data = {
                        "message": "product id is missing"
                    }
                    return JsonResponse(response_data, safe=False, status=422)
                else:
                    mongo_query = {
                        "productData.productId": product_id,
                        "productData.status": 1,
                        "mediaType1": 1,
                        "postStatus": 1
                    }
                    mongo_query_count = {
                        "productData.productId": product_id,
                        "productData.status": 1,
                        "mediaType1": 1,
                        "postStatus": 1
                    }
                    resData = []
                    post_product_data = db.posts.find(mongo_query).sort([("_id", -1)]).skip(skip_data).limit(
                        limit_data)
                    total_count = db.posts.find(mongo_query_count).count()
                    for post in post_product_data:
                        try:
                            resData.append(
                                {
                                    "postId": str(post['_id']),
                                    "title": post['title'],
                                    "imageUrl1": post['imageUrl1'],
                                    "mediaType": post['mediaType1'] if "mediaType1" in post else 1,
                                    "thumbnailUrl1": post['thumbnailUrl1'],
                                    "userId": str(post['userId']),
                                    "firstName": post['firstName'] if "firstName" in post else "",
                                    "profilepic": post['profilepic'] if "profilepic" in post else "",
                                    "profileCoverImage": post[
                                        'profileCoverImage'] if "profileCoverImage" in post else "",
                                    "userName": post['userName'] if "userName" in post else "",
                                    "lastName": post['lastName'] if "lastName" in post else "",
                                    "fullName": post['fullName'] if "fullName" in post else "",
                                    "fullNameWithSpace": post[
                                        'fullNameWithSpace'] if "fullNameWithSpace" in post else "",
                                }
                            )
                        except Exception as ex:
                            print('Error on line {}'.format(sys.exc_info()
                                                            [-1].tb_lineno), type(ex).__name__, ex)
                    if len(resData) > 0:
                        response = {
                            "data": resData,
                            "penCount": total_count,
                            "total_count": total_count,
                            "message": "data found...!!!"
                        }
                        return JsonResponse(response, safe=False, status=200)
                    else:
                        response = {
                            "data": [],
                            "penCount": 0,
                            "total_count": 0,
                            "message": "data not found...!!!"
                        }
                        return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error_message = {
                "error": "Invalid request",
                "total_count": 0,
                "message": message
            }
            return JsonResponse(error_message, status=500)


class AddComboProducts(APIView):
    '''CU app nearest store filter.'''

    @swagger_auto_schema(
        method='post', tags=["Combo Products"],
        operation_description="API for add combo products from app side",
        required=['AUTHORIZATION'],
        manual_parameters=POST_COMBO_PRODUCTS_MANUAL_PARAM,
        request_body=POST_COMBO_PRODUCTS_REQUEST_BODY,
        responses=COMBO_PRODUCT_RESPONSES,
        operation_summary="Combo Product add")
    @action(detail=False, methods=['post'])
    def post(self, request):
        try:
            err = UtilsObj.check_authentication(request)
            if err: return err
            token = UtilsObj.get_auth_token(request)
            store_category_id = request.data.get("storeCategoryId", "")
            product_name = request.data.get("productName", "")
            linked_product_category = request.data.get("linkedProductCategory", [])
            detail_description = request.data.get("detailDescription", "")
            available_stock = request.data.get("availableStock", 0)
            price = request.data.get("price", 0)
            offer_price = request.data.get("offerPrice", 0)
            currency_symbol = request.data.get("currencySymbol", "")
            currency_code = request.data.get("currencyCode", "USD")
            image = request.data.get("image", [])
            store_id = request.data.get("storeId", "")
            combo_products = request.data.get("comboProducts", [])
            if store_category_id == "":
                response = {
                    "message": "store category id is missing"
                }
                return JsonResponse(response, safe=False, status=422)
            elif product_name == "":
                response = {
                    "message": "product name is missing"
                }
                return JsonResponse(response, safe=False, status=422)
            # elif image == "":
            #     response = {
            #         "message": "image is missing"
            #     }
            #     return JsonResponse(response, safe=False, status=422)
            elif store_id == "":
                response = {
                    "message": "store id is missing"
                }
                return JsonResponse(response, safe=False, status=422)
            elif len(combo_products) == 0:
                response = {
                    "message": "combo product array is missing"
                }
                return JsonResponse(response, safe=False, status=422)
            elif currency_symbol == "":
                response = {
                    "message": "currency symbol is missing"
                }
                return JsonResponse(response, safe=False, status=422)
            elif len(linked_product_category) == 0:
                response = {
                    "message": "product category is missing"
                }
                return JsonResponse(response, safe=False, status=422)
            else:
                image_data = []
                if type(image) == str:
                    image_data.append({
                        'small': image,
                        'medium': image,
                        'large': image,
                        'extraLarge': image,
                        'filePath': '',
                        'altText': product_name.replace(" ", "-")
                    })
                elif type(image) == list:
                    for inner_image in image:
                        image_data.append({
                            'small': inner_image,
                            'medium': inner_image,
                            'large': inner_image,
                            'extraLarge': inner_image,
                            'filePath': '',
                            'altText': product_name.replace(" ", "-")
                        })
                else:
                    pass
                product_json = {
                    'storeCategoryId': store_category_id,
                    'storeCategoryName': 'Retail Stores',
                    'storeType': '8',
                    'productType': 2,
                    'itemTaxCode': '',
                    'manufacturer': '',
                    'brand': '',
                    'tax': [],
                    'productAvailability': '1',
                    'productAvailabilityFor': '',
                    'cannabisMenuType': 1,
                    'aisleName': '',
                    'aisle': '',
                    'shelf': '',
                    'directions': '',
                    'section': '',
                    'closestAisle': '',
                    'closestAisleNo': '',
                    'numberOfPcs': 0,
                    'serversFor': 0,
                    'Weight': 0,
                    'WeightUnitName': '',
                    'countryOfOrigin': '',
                    'shippingDetails': {
                        'en': '',
                        'hi': '',
                        'am': ''
                    },
                    'term&condition': {
                        'en': '',
                        'hi': '',
                        'am': ''
                    },
                    'maxQuantity': available_stock,
                    'videoUrlType': 1,
                    'prequirementTime': 0,
                    'prescriptionRequired': 0,
                    'saleOnline': 0,
                    'uploadProductDetails': '',
                    'symptoms': [],
                    'cashOnDelivery': True,
                    'barcodeFormat': '',
                    'THC': '0',
                    'CBD': '0',
                    'units': [{
                        'colorName': '',
                        'unitSizeGroupValue': {},
                        'sizeChartId': '',
                        'unitName': {
                            'en': product_name
                        },
                        'price': {
                            'en': price
                        },
                        'productLink': '',
                        'costPrice': str(price),
                        'upc': '',
                        'barcode': '',
                        'sku': '',
                        'deliveryTime': 0,
                        'avgWeight': '',
                        'avgvolume': '',
                        'packageBoxType': '',
                        'length': '',
                        'width': '',
                        'height': '',
                        'cannabisProductType': '',
                        'THC': 0,
                        'CBD': 0,
                        'highlights': [],
                        'avgweightunitName': '',
                        'avgvolumeunitName': '',
                        'lengthunitName': '',
                        'widthunitName': '',
                        'heightunitName': '',
                        'packageBoxTypeName': '',
                        'b2cpackingNoofUnits': 0,
                        'b2cpackingPackageUnits': [],
                        'b2cpackingPackageType': [],
                        'b2cunitPackageTypes': {
                            'en': 'Pieces'
                        },
                        'b2cminimumOrderQty': 1,
                        'b2cbulkPackingEnabled': 0,
                        'b2cMultiplePriceRangeEnabled': 0,
                        'b2bpackingNoofUnits': 0,
                        'b2bpackingPackageUnits': [],
                        'b2bpackingPackageType': [],
                        'b2bunitPackageType': [],
                        'b2bminimumOrderQty': 0,
                        'b2bbulkPackingEnabled': 0,
                        'b2bMultiplePriceRangeEnabled': 0,
                        'image': image_data,
                        'mobileImage': image_data,
                        'modelImage': [],
                        'uploadIosModel': [],
                        'uploadAndroidModel': [],
                        'textureImage': [],
                        'attributes': [],
                        'unitId': str(ObjectId()),
                        'floatValue': float(price),
                        'status': 'active',
                        'availableQuantity': int(available_stock),
                        'claimedQuantity': 0,
                        'sizeAttributes': [],
                        'b2cPricing': [{
                            'b2cuptoQty': 0,
                            'b2cpriceWithTax': float(price),
                            'b2cproductSellingPrice': float(offer_price),
                            'b2cmarginType': 0,
                            'b2cpercentageMargin': 0,
                            'b2cfixedMargin': 0,
                            'b2cresellerCommission': 0,
                            'b2cpercentageCommission': 0,
                            'b2cfixedCommission': 0
                        }],
                        'b2bPricing': [{
                            'b2buptoQty': 0,
                            'b2bpriceWithTax': float(offer_price),
                            'b2bproductSellingPrice': float(offer_price),
                            'b2bmarginType': 0,
                            'b2bpercentageMargin': 0,
                            'b2bfixedMargin': 0,
                            'b2bresellerCommission': 0,
                            'b2bpercentageCommission': 0,
                            'b2bfixedCommission': 0
                        }],
                        'isPrimary': True
                    }],
                    'b2cminimumOrderQty': 1,
                    'b2cbulkPackingEnabled': 0,
                    'b2cunitPackageType': {
                        'en': 'Pieces'
                    },
                    'b2cpackingNoofUnits': 0,
                    'b2cmultipleRangeEnabled': '0',
                    'b2bminimumOrderQty': 0,
                    'b2bbulkPackingEnabled': 0,
                    'b2bpackingNoofUnits': 0,
                    'b2bmultipleRangeEnabled': '1',
                    'otherb2cpricingprimaryunitCount': '',
                    'otherb2bpricingprimaryunitCount': '',
                    'productSeo': {
                        'title': {
                            'en': product_name
                        },
                        'slug': [],
                        'metatags': [],
                        'description': []
                    },
                    'linkedAttributeCategory': [],
                    'linkedProductCategory': linked_product_category,
                    'comboProducts': combo_products,
                    'images': image_data,
                    'type': '',
                    'mpn': '',
                    'model': '',
                    'shelflifeuom': '',
                    'storageTemperature': '',
                    'storageTemperatureUOM': '',
                    'warning': '',
                    'allergyInformation': '',
                    'container': '',
                    'sizeUom': '',
                    'servingsPerContainer': '',
                    'height': '',
                    'width': '',
                    'length': '',
                    'weight': '',
                    'genre': '',
                    'label': '',
                    'artist': '',
                    'actor': '',
                    'director': '',
                    'clothingSize': '',
                    'features': '',
                    'publisher': '',
                    'author': '',
                    'brandName': 'Fresho',
                    'manufacturerName': '',
                    'needsWeighed': False,
                    'uploadARModelEnabled': False,
                    'batchDetails': False,
                    'expiryDateMandatory': False,
                    'needsIdProof': False,
                    'refrigeration': False,
                    'needsFreezer': False,
                    'allowOrderOutOfStock': False,
                    'returnPolicy': {
                        'isReturn': False,
                        'noofdays': 0
                    },
                    'exchangePolicy': {
                        'isExchange': False,
                        'noofdays': 0
                    },
                    'replacementPolicy': {
                        'isReplacement': False,
                        'noofdays': 0
                    },
                    'parentProductName': product_name,
                    'isPrimary': True,
                    'videoUrl': '',
                    'consumptionTime': {},
                    'pPName': {
                        'en': product_name
                    },
                    'pName': {
                        'en': product_name
                    },
                    'productname': {
                        'en': product_name
                    },
                    'detailDescription': {
                        'en': detail_description
                    },
                    'manufactureName': {
                        'en': ''
                    },
                    'brandTitle': {
                        'en': ''
                    },
                    'sizes': [],
                    'colors': [],
                    'detailedDesc': [
                        detail_description
                    ],
                    'b2cpackingPackageUnits': {},
                    'b2cpackingPackageType': {},
                    'b2cunitPackageTypes': {
                        'en': 'Pieces'
                    },
                    'b2bpackingPackageUnits': {},
                    'b2bpackingPackageType': {},
                    'b2bunitPackageType': {},
                    'mobileImages': image_data,
                    'modelImages': [],
                    'uploadIosModels': [],
                    'uploadAndroidModels': [],
                    'textureImages': [],
                    'color': '',
                    'currentDate': '2022-2-3 16:45:47',
                    'detailedDescription': detail_description,
                    'itemKey': product_name.replace(" ", "-"),
                    'fileName': '/var/www/html/admin/Business/application/../../../xml/.xml',
                    '_id': str(ObjectId()),
                    'flavours': [],
                    'storeId': store_id,
                    'currency': currency_code,
                    'currencySymbol': currency_symbol,
                    'status': 1
                }
                private_files = requests.post(PYTHON_PRODUCT_URL+"products/", json=product_json,
                                              headers={"Authorization": "product add", "language": "en"})
                print(private_files.json())
                if private_files.status_code == 200:
                    response = {
                        "message": "product addded successfully"
                    }
                    return JsonResponse(response, safe=False, status=200)
                else:
                    response = {
                        "message": "Opps, Error while adding product"
                    }
                    return JsonResponse(response, safe=False, status=400)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            offerdata = {"error": message, "message": "Internal server Error"}
            return JsonResponse(offerdata, status=500, safe=False)


class BevvyBestSellers(APIView):
    '''CU app nearest store filter.'''

    @swagger_auto_schema(
        method='get', tags=["Bevvy Best Sellers"],
        operation_description="API to get the best bevvy sellers",
        required=['AUTHORIZATION'],
        manual_parameters=GET_BEST_BEVVY_SELLERS,
        responses=GET_BEST_BEVVY_SELLERS_RESPONSE,
        operation_summary="Bevvy Best Sellers")
    @action(detail=False, methods=['get'])
    def get(self, request):
        try:
            err = UtilsObj.check_authentication(request)
            if err: return err
            token = UtilsObj.get_auth_token(request)
            store_category_id = request.GET.get("storeCategoryId", False)
            city_id = request.GET.get("cityId", False)

            if city_id and store_category_id:
                stores = db.stores.find({"cityId": city_id, "categoryId": store_category_id})
                if stores.count():
                    json_data = UtilsObj.serialize_cursor(stores)
                    for data in json_data:
                        try:
                            del data["lastStatusLog"]
                            del data["statusLogs"]
                        except: pass
                    return JsonResponse({"message": "data found", "data": {
                        "id": "",
                        "catName": "Best Sellers",
                        "imageUrl": "",
                        "bannerImageUrl": "",
                        "websiteImageUrl": "",
                        "websiteBannerImageUrl": "",
                        "offers": [],
                        "penCount": 0,
                        "categoryData": [json_data],
                        "type": 3,
                        "seqId": 3,
                    }}, status=200)
            return JsonResponse({"message": "data not found", "data": {
                "id": "",
                "catName": "Best Sellers",
                "imageUrl": "",
                "bannerImageUrl": "",
                "websiteImageUrl": "",
                "websiteBannerImageUrl": "",
                "offers": [],
                "penCount": 0,
                "categoryData": [],
                "type": 3,
                "seqId": 3,
            }}, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            offerdata = {"error": message, "message": "Internal server Error"}
            return JsonResponse(offerdata, status=500, safe=False)
