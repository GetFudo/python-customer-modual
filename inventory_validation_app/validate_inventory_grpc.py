from concurrent import futures
import logging
import grpc
import ast
import sys
import threading
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")

from inventory_pb_files import inventory_validation_pb2
from inventory_pb_files import inventory_validation_pb2_grpc
import time
from pymongo import MongoClient, CursorType
from bson.objectid import ObjectId
import json
import requests
from threading import Thread
import datetime
from dateutil import tz
from pymongo import MongoClient
import functools
import requests
import json
import urllib.request
from dotenv import load_dotenv

from search_api.settings import db, es, MEAT_STORE_TYPE, DINE_STORE_TYPE, CHILD_PRODUCT_INDEX, DINE_STORE_CATEGORY_ID

"""
	body - body of the inventory validation 
"""


class Greeter(inventory_validation_pb2_grpc.GreeterServicer):
    def ValidateInventoryData(self, request, context):
        try:
            print("inventory logs called")
            dc_product_details = None
            try:
                data = ast.literal_eval(request.body_data)
            except:
                data = request.body_data
            print(data)
            language = data["language"] if "language" in data else "en"
            inventory_type = data["inventoryType"] if "inventoryType" in data else 0
            login_type = 1
            time_zone = ""
            time_off_set = ""
            last_response = []
            delivery_address_id = data["deliveryAddressId"] if "deliveryAddressId" in data else ""
            delivery_address_data = data["deliveryAddress"] if "deliveryAddress" in data else ""
            if delivery_address_id != "":
                delivery_address_details = db.savedAddress.find_one(
                    {"_id": ObjectId(delivery_address_id)},
                    {"latitude": 1, "longitude": 1})
                if delivery_address_details is not None:
                    condition = {
                        "status": 1,
                        "storeId": 0,
                        "polygons": {
                            "$geoIntersects": {
                                "$geometry": {
                                    "type": "Point",
                                    "coordinates": [float(delivery_address_details["longitude"]),
                                                    float(delivery_address_details["latitude"])]
                                }
                            }
                        }
                    }
                    zone_details = db.zones.find_one(
                        condition, {"_id": 1, "title": 1, "DCStoreId": 1, "timeZone": 1, "timeOffset": 1})
                    if zone_details is not None:
                        zone_id = str(zone_details["_id"])
                        dc_store_id = zone_details["DCStoreId"] if "DCStoreId" in zone_details else ""
                        time_zone = zone_details["timeZone"] if "timeZone" in zone_details else ""
                        time_off_set = zone_details["timeOffset"] if "timeOffset" in zone_details else ""
                    else:
                        zone_id = ""
                        dc_store_id = ""
                else:
                    zone_id = ""
                    dc_store_id = ""
            elif delivery_address_data != "":
                try:
                    if delivery_address_data["latitude"] != 0 and delivery_address_data["longitude"] != 0:
                        condition = {
                            "status": 1,
                            "storeId": 0,
                            "polygons": {
                                "$geoIntersects": {
                                    "$geometry": {
                                        "type": "Point",
                                        "coordinates": [float(delivery_address_data["longitude"]),
                                                        float(delivery_address_data["latitude"])]
                                    }
                                }
                            }
                        }
                        zone_details = db.zones.find_one(
                            condition, {"_id": 1, "title": 1, "DCStoreId": 1, "timeZone": 1, "timeOffset": 1})
                        if zone_details is not None:
                            zone_id = str(zone_details["_id"])
                            dc_store_id = zone_details["DCStoreId"] if "DCStoreId" in zone_details else ""
                            time_zone = zone_details["timeZone"] if "timeZone" in zone_details else ""
                            time_off_set = zone_details["timeOffset"] if "timeOffset" in zone_details else ""
                        else:
                            zone_id = ""
                            dc_store_id = ""
                    else:
                        zone_id = ""
                        dc_store_id = ""
                except:
                    zone_id = ""
                    dc_store_id = ""
            else:
                zone_id = ""
                dc_store_id = ""
            deleted_products = []
            for pro in data["data"]:
                try:
                    qty_message = ""
                    product_data = []
                    hard_limit = 0
                    selected_quantity = pro["quantity"] if "quantity" in pro else 0
                    parent_product_id = pro["parentProductId"]
                    child_product_id = pro["childProductId"]
                    product_name = pro["productName"]
                    entity_from = int(pro["entityFrom"]) if "entityFrom" in pro else 0
                    offer_id = pro["offerId"] if "offerId" in pro else ""
                    child_product_details = db.childProducts.find_one(
                        {
                            "_id": ObjectId(child_product_id),
                            "status": 1,
                            "parentProductId": str(parent_product_id)
                        },
                        {"units": 1, "allowOrderOutOfStock": 1, "storeCategoryId": 1, "storeId": 1, "inventoryData": 1,
                         "substitute": 1})
                    if child_product_details is not None:
                        store_category_details = db.storeCategory.find_one(
                            {"_id": ObjectId(child_product_details["storeCategoryId"])})
                        if store_category_details is not None:
                            store_type = store_category_details["type"]
                        else:
                            store_type = 0
                        store_data_details = []
                        if zone_id != "":
                            store_query = {"categoryId": str(child_product_details["storeCategoryId"]),
                                           "serviceZones.zoneId": zone_id}
                            store_data = db.stores.find(store_query)
                            if store_data.count() > 0:
                                for store in store_data:
                                    store_data_details.append(str(store["_id"]))

                        if dc_store_id != "":
                            if store_type == MEAT_STORE_TYPE:
                                query = {
                                    "status": 1,
                                    "parentProductId": str(parent_product_id),
                                    "units.unitId": child_product_details["units"][0]["unitId"]
                                }
                                if dc_store_id != "":
                                    query["storeId"] = ObjectId(dc_store_id)
                                dc_product_details = db.childProducts.find_one(query, {"seller": 1, "units": 1})
                                if dc_product_details is not None:
                                    is_dc_product = True
                                    all_seller_list = []
                                    if "seller" in dc_product_details:
                                        for seller in dc_product_details["seller"]:
                                            if seller["preOrder"] == True or seller["preOrder"] == 0 and seller[
                                                "procurementTime"] != 0:
                                                all_seller_list.append(seller)
                                        if len(all_seller_list) == 0:
                                            for new_seller in dc_product_details["seller"]:
                                                if new_seller["preOrder"]:
                                                    all_seller_list.append(new_seller)
                                        if len(all_seller_list) > 0:
                                            best_buffer = min(all_seller_list, key=lambda x: x["procurementTime"])
                                        else:
                                            best_buffer = {}
                                    else:
                                        best_buffer = {}
                                else:
                                    is_dc_product = False
                                    best_buffer = {}
                            else:
                                is_dc_product = False
                                best_buffer = {}
                        else:
                            is_dc_product = False
                            best_buffer = {}
                        inventory_data = []
                        if len(best_buffer) == 0 and is_dc_product == True:
                            if "inventoryData" in child_product_details:
                                try:
                                    for id_data in child_product_details["inventoryData"]:
                                        try:
                                            if id_data["status"] != 2:
                                                if id_data["expdate"] != "":
                                                    inventory_data.append(id_data)
                                            else:
                                                pass
                                        except:
                                            pass
                                except:
                                    pass
                            else:
                                pass
                        else:
                            pass

                        if len(inventory_data) > 0:
                            newlist = sorted(inventory_data, key=lambda k: k["expdate"], reverse=False)
                            batch_id = newlist[0]["batchId"]
                        elif len(best_buffer) > 0:
                            batch_id = best_buffer["batchId"] if "batchId" in best_buffer else "DEFAULT"
                        else:
                            batch_id = "DEFAULT"

                        if offer_id == "":
                            offer_expire = True
                        else:
                            offer_details = db.offers.find({"_id": ObjectId(offer_id), "status": 1})
                            if offer_details.count() > 0:
                                for offer in offer_details:
                                    if int(offer["globalClaimCount"]) >= int(offer["globalUsageLimit"]):
                                        offer_expire = True
                                    else:
                                        offer_expire = False
                            else:
                                offer_expire = True

                        if "substitute" in child_product_details:
                            if len(child_product_details) > 0:
                                isSubstititeAdded = True
                            else:
                                isSubstititeAdded = False
                        else:
                            isSubstititeAdded = False

                        if "allowOrderOutOfStock" in child_product_details:
                            allow_order_out_of_stock = child_product_details["allowOrderOutOfStock"]
                        else:
                            allow_order_out_of_stock = False

                        pre_order = False
                        if is_dc_product:
                            if len(best_buffer) > 0:
                                hard_limit = best_buffer["hardLimit"]
                                pre_order = best_buffer["preOrder"]
                                procurementTime = best_buffer["procurementTime"]
                            else:
                                pass

                            try:
                                available_quantity = dc_product_details["units"][0]["availableQuantity"]
                            except:
                                available_quantity = 0

                            available_quantity = available_quantity  # + hard_limit
                            if available_quantity < 0:
                                if -available_quantity > hard_limit and pre_order == False:
                                    available_quantity = 0
                                    allow_order_out_of_stock = False
                            elif available_quantity == 0 and pre_order == False:
                                available_quantity = 0
                                allow_order_out_of_stock = False
                            elif available_quantity == 0 and pre_order == True:
                                available_quantity = 0
                                allow_order_out_of_stock = child_product_details[
                                    "allowOrderOutOfStock"] if "allowOrderOutOfStock" in child_product_details else False
                        else:
                            try:
                                available_quantity = child_product_details["units"][0]["availableQuantity"]
                            except:
                                available_quantity = 0

                        if store_type == DINE_STORE_TYPE:
                            out_of_stock = False
                            available_quantity_number = 1
                            qty_message = ""
                        elif child_product_details["storeId"] == "0":
                            out_of_stock = True
                            available_quantity_number = 0
                            qty_message = "Product out of stock. Please select a substitute or " \
                                          "remove this item from your " \
                                          "order. "
                        elif store_type == MEAT_STORE_TYPE:
                            if entity_from == 0:
                                if zone_id == "" and dc_store_id == "":
                                    available_quantity_number = 0
                                    out_of_stock = True
                                elif selected_quantity > available_quantity and selected_quantity > hard_limit:
                                    allow_order_out_of_stock = allow_order_out_of_stock
                                    if available_quantity < 0:
                                        available_quantity = 0
                                    if int(available_quantity) > 0:
                                        qty_message = "Product out of stock. Please select a substitute or remove this " \
                                                      "item from your order. "
                                    elif selected_quantity > available_quantity and selected_quantity > hard_limit:
                                        qty_message = "Only " + str(
                                            hard_limit) + " units of this product is in stock , " \
                                                          "so you cannot order more than " + str(
                                            hard_limit) + " units"
                                    else:
                                        qty_message = "This Product is not in stock"
                                    out_of_stock = True
                                    if child_product_details is not None:
                                        if available_quantity <= 0:
                                            available_quantity_number = available_quantity
                                        else:
                                            available_quantity_number = available_quantity
                                    else:
                                        # out_of_stock = True
                                        available_quantity_number = available_quantity
                                else:
                                    allow_order_out_of_stock = True
                                    qty_message = ""
                                    if available_quantity == 0 and pre_order == False:
                                        out_of_stock = True
                                        available_quantity_number = available_quantity
                                        qty_message = "Product out of stock. Please select a substitute or remove this " \
                                                      "item from your order. "
                                    elif available_quantity == 0 and pre_order == True:
                                        out_of_stock = True
                                        available_quantity_number = available_quantity + hard_limit
                                        qty_message = ""
                                    # elif available_quantity > 0 and pre_order == False:
                                    #     out_of_stock = True
                                    #     available_quantity_number = available_quantity
                                    #     qty_message = "Product out of stock. Please select a substitute or remove this " \
                                    #                   "item from your order. "
                                    else:
                                        out_of_stock = False
                                        available_quantity_number = available_quantity
                                        qty_message = ""
                            else:
                                if dc_store_id == "" and zone_id == "":
                                    out_of_stock = True
                                    available_quantity_number = 0
                                elif available_quantity == 0 and hard_limit == 0 and pre_order == False:
                                    qty_message = "Product out of stock. Please select a substitute or remove this item " \
                                                  "from your order. "
                                    out_of_stock = True
                                    available_quantity_number = available_quantity
                                elif available_quantity <= 0 and hard_limit > 0 and pre_order == True:
                                    qty_message = "Product out of stock. Please select a substitute or remove this item " \
                                                  "from your order. "
                                    out_of_stock = False
                                    available_quantity_number = available_quantity
                                else:
                                    if selected_quantity > available_quantity:
                                        qty_message = "Required quantity not available. We have just " + str(
                                            available_quantity) + "unit(s) So either reduce the quantity , or select " \
                                                                  "a " \
                                                                  "substitute or remove this item from your order"
                                        if child_product_details is not None:
                                            if available_quantity <= 0:
                                                out_of_stock = True
                                                available_quantity_number = available_quantity
                                            else:
                                                out_of_stock = True
                                                available_quantity_number = available_quantity
                                        else:
                                            out_of_stock = True
                                            available_quantity_number = available_quantity
                                    else:
                                        qty_message = ""
                                        if available_quantity <= 0:
                                            out_of_stock = True
                                            available_quantity_number = available_quantity
                                        else:
                                            out_of_stock = False
                                            available_quantity_number = available_quantity
                        else:
                            if entity_from == 0:
                                if selected_quantity > available_quantity:
                                    if available_quantity < 0:
                                        available_quantity = 0

                                    if int(available_quantity) > 0:
                                        qty_message = "Only " + str(
                                            available_quantity) + " units of this product is in stock , " \
                                                                  "so you cannot order more than " + str(
                                            available_quantity) + " units"
                                    else:
                                        qty_message = "This Product is not in stock"
                                    out_of_stock = True
                                    if child_product_details is not None:
                                        if available_quantity <= 0:
                                            available_quantity_number = available_quantity
                                        else:
                                            available_quantity_number = available_quantity
                                    else:
                                        # out_of_stock = True
                                        available_quantity_number = available_quantity
                                else:
                                    qty_message = ""
                                    if available_quantity == 0:
                                        out_of_stock = True
                                        available_quantity_number = available_quantity
                                    else:
                                        out_of_stock = False
                                        available_quantity_number = available_quantity
                            else:
                                if available_quantity == 0:
                                    qty_message = "Product out of stock. Please select a substitute or remove this item from your order."
                                    out_of_stock = True
                                    available_quantity_number = available_quantity
                                else:
                                    if selected_quantity > available_quantity:
                                        qty_message = "Required quantity not available. We have just " + str(
                                            available_quantity) + "unit(s) So either reduce the quantity , or select " \
                                                                  "a substitute or remove this item from your order "
                                        if child_product_details is not None:
                                            if available_quantity <= 0:
                                                out_of_stock = True
                                                available_quantity_number = available_quantity
                                            else:
                                                out_of_stock = True
                                                available_quantity_number = available_quantity
                                        else:
                                            out_of_stock = True
                                            available_quantity_number = available_quantity
                                    else:
                                        qty_message = ""
                                        if available_quantity <= 0:
                                            out_of_stock = True
                                            available_quantity_number = available_quantity
                                        else:
                                            out_of_stock = False
                                            available_quantity_number = available_quantity

                        if available_quantity_number == 0:
                            query = {
                                "query":
                                    {
                                        "bool":
                                            {
                                                "must": [
                                                    {"match": {
                                                        "_id": child_product_id}},
                                                    {"match": {"status": 1}}]}},
                                "size": 10,
                                "from": 0
                            }
                            res_popular_result = es.search(
                                index=index_products,
                                body=query,
                                filter_path=[
                                    "hits.total",
                                    "hits.hits._id",
                                    "hits.hits._source.currency",
                                    "hits.hits._source.storeId",
                                    "hits.hits._source.catName",
                                    "hits.hits._source.brandTitle",
                                    "hits.hits._source.parentId",
                                    "hits.hits._source.stores",
                                    "hits.hits._source.units",
                                    "hits.hits._source.parentProductId",
                                    "hits.hits._source.offer",
                                    "hits.hits._source.brand",
                                    "hits.hits._source.images",
                                    "hits.hits._source.subCatName",
                                    "hits.hits._source.subSubCatName",
                                    "hits.hits._source.pName",
                                    "hits.hits._source.currencySymbol",
                                    "hits.hits._source.currency",
                                ],
                            )

                            if len(res_popular_result) == 0:
                                response_search = []
                            else:
                                product_data = []
                                if "hits" in res_popular_result["hits"]:
                                    for res in res_popular_result["hits"]["hits"]:
                                        offers_details = []
                                        if "offer" in res["_source"]:
                                            for offer in res["_source"]["offer"]:
                                                if "offerFor" in offer:
                                                    if login_type == 1:
                                                        if offer["offerFor"] == 1 or offer["offerFor"] == 0:
                                                            offer_query = {"_id": ObjectId(offer["offerId"]),
                                                                           "status": 1}
                                                            offer_count = db.offers.find_one(offer_query)
                                                            if offer_count is not None:
                                                                if offer_count["startDateTime"] <= int(time.time()):
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
                                                        if offer["offerFor"] == 0 or offer["offerFor"] == 2:
                                                            if offer["status"] == 1:
                                                                offer_query = {"_id": ObjectId(offer["offerId"]),
                                                                               "status": 1}
                                                                offer_count = db.offers.find_one(offer_query)
                                                                if offer_count is not None:
                                                                    if offer_count["startDateTime"] <= int(time.time()):
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
                                                {"_id": ObjectId(best_offer["offerId"]), "status": 1}).count()
                                            if offer_details != 0:
                                                best_offer = best_offer
                                            else:
                                                best_offer = {}
                                            if len(best_offer) > 0:
                                                # ==================================offers
                                                # part=========================================
                                                discount_type = int(
                                                    best_offer["discountType"]) if "discountType" in best_offer else 0
                                                discount_value = best_offer[
                                                    "discountValue"] if "discountValue" in best_offer else 0
                                            else:
                                                best_offer = {}
                                                discount_type = 0
                                                discount_value = 0
                                        else:
                                            best_offer = {}
                                            discount_type = 0
                                            discount_value = 0

                                        # ===============================price get the
                                        # details======================================
                                        if inventory_type == 1:
                                            if discount_type == 0:
                                                discount_price = float(discount_value)
                                            elif discount_type == 1:
                                                discount_price = (
                                                                         float(
                                                                             res["_source"]["units"][0]["floatValue"]) *
                                                                         float(discount_value)) / 100
                                            else:
                                                discount_price = 0
                                        else:
                                            if discount_type == 0:
                                                discount_price = float(discount_value)
                                            elif discount_type == 1:
                                                discount_price = (
                                                                         float(
                                                                             res["_source"]["units"][0]["floatValue"]) *
                                                                         float(discount_value) / 100) * float(
                                                    discount_value) / 100
                                            else:
                                                discount_price = 0

                                        if inventory_type == 1:
                                            try:
                                                base_price = float(
                                                    res["_source"]["units"][0]["floatValue"])
                                                final_price = float(
                                                    res["_source"]["units"][0]["floatValue"]) - discount_price
                                            except:
                                                base_price = 0
                                                final_price = 0
                                        else:
                                            try:
                                                base_price = float(res["_source"]["units"][0]["floatValue"])
                                                final_price = float(res["_source"]["units"][0]
                                                                    ["floatValue"]) - discount_price
                                            except:
                                                final_price = 0
                                                base_price = 0

                                        if final_price == 0 or base_price == 0:
                                            discount_price = 0
                                        else:
                                            discount_price = discount_price

                                        # ==============================done code
                                        # here==============================================

                                        if inventory_type == 1:
                                            available_quantity_stores = res["_source"]["units"][0][
                                                "availableQuantity"] if "availableQuantity" in res[
                                                "_source"]["units"][0] else res["_source"]["units"][0]["suppliers"][0][
                                                "retailerQty"]
                                            if available_quantity_stores == "":
                                                available_quantity_stores = 0
                                            else:
                                                available_quantity_stores = available_quantity_stores
                                        else:
                                            available_quantity_stores = res["_source"]["units"][0]["distributor"][
                                                "availableQuantity"] if "distributor" in res[
                                                "_source"]["units"][0] else res["_source"]["units"][0]["suppliers"][0][
                                                "distributorQty"]
                                            if available_quantity_stores == "":
                                                available_quantity_stores = 0
                                            else:
                                                available_quantity_stores = available_quantity_stores

                                        if available_quantity_stores == 0:
                                            pass
                                        else:
                                            product_data.append({
                                                "childProductId": res["_id"],
                                                "productName": res["_source"]["pName"][language],
                                                "brandName": res["_source"]["brandTitle"][language],
                                                "unitId": res["_source"]["units"][0]["unitId"],
                                                "parentProductId": res["_source"]["parentProductId"],
                                                "colors": res["_source"]["units"][0]["colorName"],
                                                "sizes": res["_source"]["units"][0][
                                                    "unitSizeGroupList"] if "unitSizeGroupList" in
                                                                            res["_source"]["units"][
                                                                                0] else {},
                                                "availableQuantity": int(available_quantity_stores),
                                                "images": res["_source"]["images"],
                                                "discountPrice": discount_price,
                                                "discountType": discount_type,
                                                "finalPriceList": {
                                                    "basePrice": round(base_price, 2),
                                                    "finalPrice": round(final_price, 2),
                                                    "discountPrice": round(discount_price, 2),
                                                },
                                                "currencySymbol": res["_source"]["currencySymbol"],
                                                "currency": res["_source"]["currency"],
                                                "storeid": str(res["_source"]["storeId"]),
                                            })
                                else:
                                    response_search = []
                        if len(product_data) > 0:
                            message = "product substitute found"
                        else:
                            message = "There is no product available in this store or any store we will notify " \
                                      "you while product in stock"
                        last_response.append({
                            "parentProductId": parent_product_id,
                            "childProductId": child_product_id,
                            "currentQty": available_quantity_number,
                            "outOfStock": out_of_stock,
                            "similarProduct": product_data,
                            "dcStoreId": dc_store_id,
                            "isSubstititeAdded": isSubstititeAdded,
                            "isDcProduct": is_dc_product,
                            "allowOrderOutOfStock": allow_order_out_of_stock,
                            "procurementTime": best_buffer[
                                "procurementTime"] if "procurementTime" in best_buffer else 0,
                            "hardLimit": best_buffer["hardLimit"] if "hardLimit" in best_buffer else 0,
                            "batchId": batch_id,
                            "preOrder": best_buffer["preOrder"] if "preOrder" in best_buffer else False,
                            "offerExpire": offer_expire,
                            "productName": product_name,
                            "timeZone": time_zone,
                            "timeOffSet": time_off_set,
                            "qTymessage": qty_message,
                            "message": message
                        })
                    else:
                        deleted_products.append(child_product_id)
                except Exception as ex:
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    print("Error on line {}".format(sys.exc_info()
                                                    [-1].tb_lineno), type(ex).__name__, ex)
                    pass
            if len(last_response) > 0:
                response = {
                    "deletedProducts": deleted_products,
                    "data": last_response,
                    "message": "data found"
                }
                return inventory_validation_pb2.InventoryReply(message=json.dumps(response))
            elif len(deleted_products) > 0:
                response = {
                    "deletedProducts": deleted_products,
                    "data": [],
                    "message": "data found"
                }
                return inventory_validation_pb2.InventoryReply(message=json.dumps(response))
            else:
                response = {
                    "deletedProducts": deleted_products,
                    "data": [],
                    "message": "data not found"
                }
                return inventory_validation_pb2.InventoryReply(message=json.dumps(response))
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            response = {
                "data": [],
                "message": message
            }
            return inventory_validation_pb2.InventoryReply(message=str(response))


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    inventory_validation_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
    server.add_insecure_port("[::]:8099")
    server.start()
    print("hello")
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    logging.basicConfig()
    try:
        print("threading called")
        Thread(target=serve).start()
    except:
        print("except")
        Thread(target=serve).start()
