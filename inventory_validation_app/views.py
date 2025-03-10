import traceback
from search_api.settings import (
    db,
    es,
    MEAT_STORE_TYPE,
    DINE_STORE_TYPE,
    CHILD_PRODUCT_INDEX,
    DINE_STORE_CATEGORY_ID,
)
from rest_framework.views import APIView
from rest_framework.decorators import action
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from django.http import JsonResponse
from bson.objectid import ObjectId
import time
import os
import sys
from copy import copy, deepcopy
from search.views import get_linked_unit_attribute

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
index_products = CHILD_PRODUCT_INDEX

meal_timing = {
    "latenightdinner": 0,
    "breakfast": 5,
    "brunch": 10,
    "lunch": 11,
    "tea": 15,
    "dinner": 19,
}


class ValidateInventory(APIView):
    @swagger_auto_schema(
        method="post",
        tags=["Inventory"],
        operation_description="API for validate the product inventory and offer validate",
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
            openapi.Parameter(
                name="inventoryType",
                default="1",
                required=True,
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description="inventoryType for the validation value should be 1 for retailer login and 2 for distributor login",
            ),
        ],
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            required=["data"],
            properties={
                "data": openapi.Schema(
                    type=openapi.TYPE_ARRAY,
                    description="array of the data or object(parentProductId, childProductId, offerId, productName)",
                    items=openapi.Items(
                        type=openapi.TYPE_OBJECT,
                        required=["parentProductId", "childProductId", "productName", "offerId"],
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
                            "productName": openapi.Schema(
                                type=openapi.TYPE_STRING,
                                default="Apple iPhone 8",
                                description="product name of the product which need to validate",
                            ),
                            "offerId": openapi.Schema(
                                type=openapi.TYPE_STRING,
                                description="offer id of the product if offer is there else empty string",
                            ),
                            "entityFrom": openapi.Schema(
                                type=openapi.TYPE_INTEGER,
                                description="from where api is called from checkout or from cart page. if from cart value should be 0 else value should be 1",
                            ),
                            "otherSellerValue": openapi.Schema(
                                type=openapi.TYPE_BOOLEAN,
                                example=False,
                                description="this is for the other seller list if need to send other seller array in list",
                            ),
                        },
                    ),
                ),
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
            inventory_type = (
                int(request.META["HTTP_INVENTORYTYPE"])
                if "HTTP_INVENTORYTYPE" in request.META
                else 0
            )
            login_type = 1
            batch_id = "DEFAULT"
            data = request.data
            time_zone = ""
            time_off_set = ""
            last_response = []
            delivery_address_id = data["deliveryAddressId"] if "deliveryAddressId" in data else ""
            delivery_address_data = data["deliveryAddress"] if "deliveryAddress" in data else ""
            if delivery_address_id != "":
                delivery_address_details = db.savedAddress.find_one(
                    {"_id": ObjectId(delivery_address_id)}, {"latitude": 1, "longitude": 1}
                )
                if delivery_address_details is not None:
                    condition = {
                        "status": 1,
                        "storeId": 0,
                        "polygons": {
                            "$geoIntersects": {
                                "$geometry": {
                                    "type": "Point",
                                    "coordinates": [
                                        float(delivery_address_details["longitude"]),
                                        float(delivery_address_details["latitude"]),
                                    ],
                                }
                            }
                        },
                    }
                    zone_details = db.zones.find_one(
                        condition,
                        {"_id": 1, "title": 1, "DCStoreId": 1, "timeZone": 1, "timeOffset": 1},
                    )
                    if zone_details is not None:
                        zone_id = str(zone_details["_id"])
                        dc_store_id = (
                            zone_details["DCStoreId"] if "DCStoreId" in zone_details else ""
                        )
                        time_zone = zone_details["timeZone"] if "timeZone" in zone_details else ""
                        time_off_set = (
                            zone_details["timeOffset"] if "timeOffset" in zone_details else ""
                        )
                    else:
                        zone_id = ""
                        dc_store_id = ""
                else:
                    zone_id = ""
                    dc_store_id = ""
            elif delivery_address_data != "":
                # delivery_address_data['longitude'] = 77.562534
                # delivery_address_data['latitude'] = 12.980301
                try:
                    if (
                        delivery_address_data["latitude"] != 0
                        and delivery_address_data["longitude"] != 0
                    ):
                        condition = {
                            "status": 1,
                            "storeId": 0,
                            "polygons": {
                                "$geoIntersects": {
                                    "$geometry": {
                                        "type": "Point",
                                        "coordinates": [
                                            float(delivery_address_data["longitude"]),
                                            float(delivery_address_data["latitude"]),
                                        ],
                                    }
                                }
                            },
                        }
                        zone_details = db.zones.find_one(
                            condition,
                            {"_id": 1, "title": 1, "DCStoreId": 1, "timeZone": 1, "timeOffset": 1},
                        )
                        if zone_details is not None:
                            zone_id = str(zone_details["_id"])
                            dc_store_id = (
                                zone_details["DCStoreId"] if "DCStoreId" in zone_details else ""
                            )
                            time_zone = (
                                zone_details["timeZone"] if "timeZone" in zone_details else ""
                            )
                            time_off_set = (
                                zone_details["timeOffset"] if "timeOffset" in zone_details else ""
                            )
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
            print("dc_store_id", dc_store_id)
            deleted_products = []
            buy_x_get_x_product_details = []
            other_seller_data = []
            for pro in data["data"]:
                try:
                    qty_message = ""
                    product_data = []
                    hard_limit = 0
                    other_seller_value = pro['otherSellerValue'] if "otherSellerValue" in pro else False
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
                            "parentProductId": str(parent_product_id),
                        },
                        {
                            "units": 1,
                            "allowOrderOutOfStock": 1,
                            "storeCategoryId": 1,
                            "storeId": 1,
                            "inventoryData": 1,
                            "substitute": 1,
                        },
                    )
                    if child_product_details is not None:
                        store_category_details = db.storeCategory.find_one(
                            {"_id": ObjectId(child_product_details["storeCategoryId"])}
                        )
                        if store_category_details is not None:
                            store_type = store_category_details["type"]
                        else:
                            store_type = 0
                        store_data_details = []
                        if zone_id != "":
                            store_query = {
                                "categoryId": str(child_product_details["storeCategoryId"]),
                                "serviceZones.zoneId": zone_id,
                            }
                            store_data = db.stores.find(store_query)
                            if store_data.count() > 0:
                                for store in store_data:
                                    store_data_details.append(str(store["_id"]))

                        if dc_store_id != "":
                            if store_type == MEAT_STORE_TYPE:
                                query = {
                                    "status": 1,
                                    "parentProductId": str(parent_product_id),
                                    "units.unitId": child_product_details["units"][0]["unitId"],
                                }
                                if dc_store_id != "":
                                    query["storeId"] = ObjectId(dc_store_id)
                                dc_product_details = db.childProducts.find_one(
                                    query, {"seller": 1, "units": 1}
                                )
                                if dc_product_details is not None:
                                    is_dc_product = True
                                    all_seller_list = []
                                    if "seller" in dc_product_details:
                                        for seller in dc_product_details["seller"]:
                                            if (
                                                seller["preOrder"] is True
                                                or seller["preOrder"] == 0
                                                and seller["procurementTime"] != 0
                                            ):
                                                all_seller_list.append(seller)
                                        if len(all_seller_list) == 0:
                                            for new_seller in dc_product_details["seller"]:
                                                if new_seller["preOrder"]:
                                                    all_seller_list.append(new_seller)
                                        if len(all_seller_list) > 0:
                                            best_buffer = min(
                                                all_seller_list, key=lambda x: x["procurementTime"]
                                            )
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
                            newlist = sorted(
                                inventory_data, key=lambda k: k["expdate"], reverse=False
                            )
                            batch_id = newlist[0]["batchId"]
                        elif len(best_buffer) > 0:
                            batch_id = (
                                best_buffer["batchId"] if "batchId" in best_buffer else "DEFAULT"
                            )
                        else:
                            batch_id = "DEFAULT"

                        if offer_id == "":
                            offer_expire = True
                        else:
                            offer_details = db.offers.find({"_id": ObjectId(offer_id), "status": 1})
                            if offer_details.count() > 0:
                                for offer in offer_details:
                                    if int(offer["globalClaimCount"]) >= int(
                                        offer["globalUsageLimit"]
                                    ):
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
                                available_quantity = dc_product_details["units"][0][
                                    "availableQuantity"
                                ]
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
                                allow_order_out_of_stock = (
                                    child_product_details["allowOrderOutOfStock"]
                                    if "allowOrderOutOfStock" in child_product_details
                                    else False
                                )
                        else:
                            try:
                                available_quantity = child_product_details["units"][0][
                                    "availableQuantity"
                                ]
                            except:
                                available_quantity = 0

                        if store_type == DINE_STORE_TYPE:
                            out_of_stock = False
                            available_quantity_number = 1
                            qty_message = ""
                        elif child_product_details["storeId"] == "0":
                            out_of_stock = True
                            available_quantity_number = 0
                            qty_message = (
                                "Product out of stock. Please select a substitute or "
                                "remove this item from your "
                                "order. "
                            )
                        elif store_type == MEAT_STORE_TYPE:
                            if entity_from == 0:
                                if zone_id == "" and dc_store_id == "":
                                    available_quantity_number = 0
                                    out_of_stock = True
                                elif (
                                    selected_quantity > available_quantity
                                    and selected_quantity > hard_limit
                                ):
                                    allow_order_out_of_stock = allow_order_out_of_stock
                                    if available_quantity < 0:
                                        available_quantity = 0
                                    if int(available_quantity) > 0:
                                        qty_message = (
                                            "Product out of stock. Please select a substitute or remove this "
                                            "item from your order. "
                                        )
                                    elif (
                                        selected_quantity > available_quantity
                                        and selected_quantity > hard_limit
                                    ):
                                        qty_message = (
                                            "Only "
                                            + str(hard_limit)
                                            + " units of this product is in stock , "
                                            "so you cannot order more than "
                                            + str(hard_limit)
                                            + " units"
                                        )
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
                                        qty_message = (
                                            "Product out of stock. Please select a substitute or remove this "
                                            "item from your order. "
                                        )
                                    elif available_quantity == 0 and pre_order == True:
                                        out_of_stock = True
                                        available_quantity_number = available_quantity + hard_limit
                                        qty_message = ""
                                    else:
                                        out_of_stock = False
                                        available_quantity_number = available_quantity
                                        qty_message = ""
                            else:
                                if dc_store_id == "" and zone_id == "":
                                    out_of_stock = True
                                    available_quantity_number = 0
                                elif (
                                    available_quantity == 0
                                    and hard_limit == 0
                                    and pre_order == False
                                ):
                                    qty_message = (
                                        "Product out of stock. Please select a substitute or remove this item "
                                        "from your order. "
                                    )
                                    out_of_stock = True
                                    available_quantity_number = available_quantity
                                elif (
                                    available_quantity <= 0 and hard_limit > 0 and pre_order == True
                                ):
                                    qty_message = (
                                        "Product out of stock. Please select a substitute or remove this item "
                                        "from your order. "
                                    )
                                    out_of_stock = False
                                    available_quantity_number = available_quantity
                                else:
                                    if selected_quantity > available_quantity:
                                        qty_message = (
                                            "Required quantity not available. We have just "
                                            + str(available_quantity)
                                            + " unit(s) So either reduce the quantity , or select a "
                                            "substitute or remove this item from your order"
                                        )
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
                                        qty_message = (
                                            "Only "
                                            + str(available_quantity)
                                            + " units of this product is in stock , "
                                            "so you cannot order more than "
                                            + str(available_quantity)
                                            + " units"
                                        )
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
                                        qty_message = (
                                            "Required quantity not available. We have just "
                                            + str(available_quantity)
                                            + " unit(s) So either reduce the quantity , or select a substitute or remove this item from your order"
                                        )
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

                        other_product_data = []
                        if available_quantity_number <= 0:
                            query = {
                                "query": {
                                    "bool": {
                                        "must": [
                                            {"match": {"_id": child_product_id}},
                                            {"match": {"status": 1}},
                                        ]
                                    }
                                },
                                "size": 10,
                                "from": 0,
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
                                                        if (
                                                            offer["offerFor"] == 1
                                                            or offer["offerFor"] == 0
                                                        ):
                                                            offer_query = {
                                                                "_id": ObjectId(offer["offerId"]),
                                                                "status": 1,
                                                            }
                                                            offer_count = db.offers.find_one(
                                                                offer_query
                                                            )
                                                            if offer_count is not None:
                                                                if offer_count[
                                                                    "startDateTime"
                                                                ] <= int(time.time()):
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
                                                        if (
                                                            offer["offerFor"] == 0
                                                            or offer["offerFor"] == 2
                                                        ):
                                                            if offer["status"] == 1:
                                                                offer_query = {
                                                                    "_id": ObjectId(
                                                                        offer["offerId"]
                                                                    ),
                                                                    "status": 1,
                                                                }
                                                                offer_count = db.offers.find_one(
                                                                    offer_query
                                                                )
                                                                if offer_count is not None:
                                                                    if offer_count[
                                                                        "startDateTime"
                                                                    ] <= int(time.time()):
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

                                        # ===============================price get the
                                        # details======================================
                                        if inventory_type == 1:
                                            if discount_type == 0:
                                                discount_price = float(discount_value)
                                            elif discount_type == 1:
                                                discount_price = (
                                                    float(res["_source"]["units"][0]["floatValue"])
                                                    * float(discount_value)
                                                ) / 100
                                            else:
                                                discount_price = 0
                                        else:
                                            if discount_type == 0:
                                                discount_price = float(discount_value)
                                            elif discount_type == 1:
                                                discount_price = (
                                                    (
                                                        float(
                                                            res["_source"]["units"][0]["floatValue"]
                                                        )
                                                        * float(discount_value)
                                                        / 100
                                                    )
                                                    * float(discount_value)
                                                    / 100
                                                )
                                            else:
                                                discount_price = 0

                                        if inventory_type == 1:
                                            try:
                                                base_price = float(
                                                    res["_source"]["units"][0]["floatValue"]
                                                )
                                                final_price = (
                                                    float(res["_source"]["units"][0]["floatValue"])
                                                    - discount_price
                                                )
                                            except:
                                                base_price = 0
                                                final_price = 0
                                        else:
                                            try:
                                                base_price = float(
                                                    res["_source"]["units"][0]["floatValue"]
                                                )
                                                final_price = (
                                                    float(res["_source"]["units"][0]["floatValue"])
                                                    - discount_price
                                                )
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
                                            available_quantity_stores = (
                                                res["_source"]["units"][0]["availableQuantity"]
                                                if "availableQuantity" in res["_source"]["units"][0]
                                                else res["_source"]["units"][0]["suppliers"][0][
                                                    "retailerQty"
                                                ]
                                            )
                                            if available_quantity_stores == "":
                                                available_quantity_stores = 0
                                            else:
                                                available_quantity_stores = (
                                                    available_quantity_stores
                                                )
                                        else:
                                            available_quantity_stores = (
                                                res["_source"]["units"][0]["distributor"][
                                                    "availableQuantity"
                                                ]
                                                if "distributor" in res["_source"]["units"][0]
                                                else res["_source"]["units"][0]["suppliers"][0][
                                                    "distributorQty"
                                                ]
                                            )
                                            if available_quantity_stores == "":
                                                available_quantity_stores = 0
                                            else:
                                                available_quantity_stores = (
                                                    available_quantity_stores
                                                )

                                        if available_quantity_stores <= 0:
                                            pass
                                        else:
                                            product_data.append(
                                                {
                                                    "childProductId": res["_id"],
                                                    "productName": res["_source"]["pName"][
                                                        language
                                                    ],
                                                    "brandName": res["_source"]["brandTitle"][
                                                        language
                                                    ],
                                                    "unitId": res["_source"]["units"][0]["unitId"],
                                                    "parentProductId": res["_source"][
                                                        "parentProductId"
                                                    ],
                                                    "colors": res["_source"]["units"][0][
                                                        "colorName"
                                                    ],
                                                    "sizes": res["_source"]["units"][0][
                                                        "unitSizeGroupList"
                                                    ]
                                                    if "unitSizeGroupList"
                                                    in res["_source"]["units"][0]
                                                    else {},
                                                    "availableQuantity": int(
                                                        available_quantity_stores
                                                    ),
                                                    "images": res["_source"]["images"],
                                                    "discountPrice": discount_price,
                                                    "discountType": discount_type,
                                                    "finalPriceList": {
                                                        "basePrice": round(base_price, 2),
                                                        "finalPrice": round(final_price, 2),
                                                        "discountPrice": round(discount_price, 2),
                                                    },
                                                    "currencySymbol": res["_source"][
                                                        "currencySymbol"
                                                    ],
                                                    "currency": res["_source"]["currency"],
                                                    "storeid": str(res["_source"]["storeId"]),
                                                }
                                            )
                                else:
                                    response_search = []
                        if available_quantity_number <= 0 and other_seller_value is True and zone_id != "":
                            store_list = []
                            store_query = {
                                "servicesZones.zoneId": zone_id,
                                "status": 1,
                                "categoryId": child_product_details['storeCategoryId']
                            }
                            store_mongo_list = db.stores.fine(store_query)
                            for s_id in store_mongo_list:
                                store_list.append(str(s_id))
                            match_query = []
                            match_query.append({"match": {"status": 1}})
                            match_query.append({"terms": {"storeId": store_list}})
                            match_query.append({"match": {"units.unitId": child_product_details['units'][0]['unitId']}})
                            if len(store_list) > 0:
                                query = {
                                    "query": {
                                        "bool": {
                                            "must": match_query
                                        }
                                    },
                                    "size": len(store_list),
                                    "from": 0,
                                }
                                res_popular_result = es.search(
                                    index=index_products,
                                    body=query,
                                    filter_path=[
                                        "hits.total",
                                        "hits.hits._id",
                                        "hits.hits._source",
                                    ],
                                )

                                if len(res_popular_result) == 0:
                                    pass
                                else:
                                    if "hits" in res_popular_result["hits"]:
                                        for res in res_popular_result["hits"]["hits"]:
                                            offers_details = []
                                            if "offer" in res["_source"]:
                                                for offer in res["_source"]["offer"]:
                                                    if "offerFor" in offer:
                                                        if offer["offerFor"] == 1 or offer["offerFor"] == 0:
                                                            offer_query = {
                                                                "_id": ObjectId(offer["offerId"]),
                                                                "status": 1,
                                                            }
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
                                                        pass

                                            if len(offers_details) > 0:
                                                best_offer = max(
                                                    offers_details, key=lambda x: x["discountValue"]
                                                )
                                                offer_details = db.offers.find(
                                                    {
                                                        "_id": ObjectId(best_offer["offerId"]),
                                                        "status": 1,
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

                                            # ===============================price get the
                                            # details======================================
                                            if inventory_type == 1:
                                                if discount_type == 0:
                                                    discount_price = float(discount_value)
                                                elif discount_type == 1:
                                                    discount_price = (
                                                        float(res["_source"]["units"][0]["floatValue"])
                                                        * float(discount_value)
                                                    ) / 100
                                                else:
                                                    discount_price = 0
                                            else:
                                                if discount_type == 0:
                                                    discount_price = float(discount_value)
                                                elif discount_type == 1:
                                                    discount_price = (
                                                        (
                                                            float(
                                                                res["_source"]["units"][0]["floatValue"]
                                                            )
                                                            * float(discount_value)
                                                            / 100
                                                        )
                                                    )
                                                else:
                                                    discount_price = 0

                                            if inventory_type == 1:
                                                try:
                                                    base_price = float(
                                                        res["_source"]["units"][0]["floatValue"]
                                                    )
                                                    final_price = (
                                                        float(res["_source"]["units"][0]["floatValue"])
                                                        - discount_price
                                                    )
                                                except:
                                                    base_price = 0
                                                    final_price = 0
                                            else:
                                                try:
                                                    base_price = float(
                                                        res["_source"]["units"][0]["floatValue"]
                                                    )
                                                    final_price = (
                                                        float(res["_source"]["units"][0]["floatValue"])
                                                        - discount_price
                                                    )
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
                                                available_quantity_stores = (
                                                    res["_source"]["units"][0]["availableQuantity"]
                                                    if "availableQuantity" in res["_source"]["units"][0]
                                                    else res["_source"]["units"][0]["suppliers"][0][
                                                        "retailerQty"
                                                    ]
                                                )
                                                if available_quantity_stores == "":
                                                    available_quantity_stores = 0
                                                else:
                                                    available_quantity_stores = (
                                                        available_quantity_stores
                                                    )
                                            else:
                                                available_quantity_stores = (
                                                    res["_source"]["units"][0]["distributor"][
                                                        "availableQuantity"
                                                    ]
                                                    if "distributor" in res["_source"]["units"][0]
                                                    else res["_source"]["units"][0]["suppliers"][0][
                                                        "distributorQty"
                                                    ]
                                                )
                                                if available_quantity_stores == "":
                                                    available_quantity_stores = 0
                                                else:
                                                    available_quantity_stores = (
                                                        available_quantity_stores
                                                    )

                                            if available_quantity_stores <= 0:
                                                pass
                                            else:
                                                other_product_data.append(
                                                    {
                                                        "childProductId": res["_id"],
                                                        "productName": res["_source"]["pName"][language],
                                                        "brandName": res["_source"]["brandTitle"][language],
                                                        "unitId": res["_source"]["units"][0]["unitId"],
                                                        "parentProductId": res["_source"]["parentProductId"],
                                                        "colors": res["_source"]["units"][0]["colorName"],
                                                        "sizes": res["_source"]["units"][0]["unitSizeGroupList"] if "unitSizeGroupList" in res["_source"]["units"][0] else {},
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
                                                    }
                                                )
                                    else:
                                        pass

                        if len(other_product_data) > 0:
                             other_seller_data.append(
                                 {
                                     "productId": str(child_product_details['_id']),
                                     "data": other_product_data
                                  }
                             )
                        else:
                            pass
                        if len(product_data) > 0:
                            message = "product substitute found"
                        else:
                            message = (
                                "There is no product available in this store or any store we will notify "
                                "you while product in stock"
                            )

                        # ========================combo product details==========================
                        if offer_id != "":
                            best_offer_details = db.offers.find_one({"_id": ObjectId(offer_id)})
                            if best_offer_details is not None:
                                if best_offer_details["offerType"] == 2:
                                    for combo in best_offer_details["comboProducts"]:
                                        combo_product_offers_details = []
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
                                                if combo_product != None:
                                                    combo_product_price = combo_product["units"][0][
                                                        "b2cPricing"
                                                    ][0]["b2cproductSellingPrice"]
                                                else:
                                                    combo_product_price = combo_product["units"][0][
                                                        "floatValue"
                                                    ]
                                            except:
                                                combo_product_price = combo_product["units"][0][
                                                    "floatValue"
                                                ]
                                            if "offer" in combo_product:
                                                for combo_product_offer in combo_product["offer"]:
                                                    combo_product_offer_details = (
                                                        db.offers.find_one(
                                                            {
                                                                "_id": ObjectId(
                                                                    combo_product_offer["offerId"]
                                                                ),
                                                                "status": 1,
                                                            }
                                                        )
                                                    )
                                                    if combo_product_offer_details is not None:
                                                        if combo_product_offer["status"] == 1:
                                                            if combo_product_offer_details[
                                                                "startDateTime"
                                                            ] <= int(time.time()):
                                                                if (
                                                                    combo_product_offer_details
                                                                    != None
                                                                ):
                                                                    combo_product_offer[
                                                                        "termscond"
                                                                    ] = combo_product_offer_details[
                                                                        "termscond"
                                                                    ]
                                                                else:
                                                                    combo_product_offer[
                                                                        "termscond"
                                                                    ] = ""
                                                                combo_product_offer[
                                                                    "name"
                                                                ] = combo_product_offer_details[
                                                                    "name"
                                                                ][
                                                                    "en"
                                                                ]
                                                                combo_product_offer[
                                                                    "discountValue"
                                                                ] = combo_product_offer[
                                                                    "discountValue"
                                                                ]
                                                                combo_product_offer[
                                                                    "discountType"
                                                                ] = combo_product_offer[
                                                                    "discountType"
                                                                ]
                                                                combo_product_offers_details.append(
                                                                    combo_product_offer
                                                                )
                                                        else:
                                                            pass
                                                    else:
                                                        pass
                                            if len(combo_product_offers_details) > 0:
                                                combo_product_best_offer = max(
                                                    combo_product_offers_details,
                                                    key=lambda x: x["discountValue"],
                                                )
                                                com_offer_details = db.offers.find(
                                                    {
                                                        "_id": ObjectId(
                                                            combo_product_best_offer["offerId"]
                                                        ),
                                                        "status": 1,
                                                    }
                                                ).count()
                                                if com_offer_details != 0:
                                                    combo_product_best_offer = (
                                                        combo_product_best_offer
                                                    )
                                                else:
                                                    combo_product_best_offer = {}
                                            else:
                                                combo_product_best_offer = {}

                                            if len(combo_product_best_offer) == 0:
                                                combo_percentage = 0
                                                combo_discount_type = 0
                                            else:
                                                if "discountType" in combo_product_best_offer:
                                                    if (
                                                        combo_product_best_offer["discountType"]
                                                        == 0
                                                    ):
                                                        combo_percentage = 0
                                                        combo_discount_type = 0
                                                    else:
                                                        combo_percentage = int(
                                                            combo_product_best_offer[
                                                                "discountValue"
                                                            ]
                                                        )
                                                        combo_discount_type = (
                                                            combo_product_best_offer["discountType"]
                                                        )
                                                else:
                                                    combo_percentage = 0
                                                    combo_discount_type = 0

                                            if len(combo_product_best_offer) > 0:
                                                combo_discount_type = (
                                                    int(combo_product_best_offer["discountType"])
                                                    if "discountType" in combo_product_best_offer
                                                    else 0
                                                )
                                                combo_discount_value = combo_product_best_offer[
                                                    "discountValue"
                                                ]
                                            else:
                                                combo_discount_value = 0
                                                combo_discount_type = 2

                                            # ==========================unit data================================================================
                                            combo_tax_price = 0
                                            combo_tax_value = []
                                            if (
                                                combo_product["storeCategoryId"]
                                                != DINE_STORE_CATEGORY_ID
                                            ):
                                                if type(combo_product["tax"]) == list:
                                                    for combo_tax in combo_product["tax"]:
                                                        combo_tax_value.append(
                                                            {"value": combo_tax["taxValue"]}
                                                        )
                                                else:
                                                    if combo_product["tax"] != None:
                                                        if "taxValue" in combo_product["tax"]:
                                                            combo_tax_value.append(
                                                                {
                                                                    "value": combo_product["tax"][
                                                                        "taxValue"
                                                                    ]
                                                                }
                                                            )
                                                        else:
                                                            combo_tax_value.append(
                                                                {"value": combo_product["tax"]}
                                                            )
                                                    else:
                                                        pass

                                                if (
                                                    combo_product["storeCategoryId"]
                                                    != DINE_STORE_CATEGORY_ID
                                                ):
                                                    if len(combo_tax_value) == 0:
                                                        combo_tax_price = 0
                                                    else:
                                                        for amount in combo_tax_value:
                                                            combo_tax_price = combo_tax_price + (
                                                                int(amount["value"])
                                                            )
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
                                                    float(combo_product_price)
                                                    * float(combo_discount_value)
                                                ) / 100
                                            else:
                                                combo_discount_price = 0
                                            combo_final_price = (
                                                combo_product_price - combo_discount_price
                                            )
                                            combo_available_stock = combo_product["units"][0][
                                                "availableQuantity"
                                            ]
                                            if combo_available_stock == 0:
                                                combo_out_of_stock = True
                                            else:
                                                combo_out_of_stock = False

                                            try:
                                                product_name = combo_product["units"][0][
                                                    "unitName"
                                                ][lan]
                                            except:
                                                product_name = combo_product["units"][0][
                                                    "unitName"
                                                ]["en"]

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
                                                    "parentProductId": str(
                                                        combo_product["parentProductId"]
                                                    ),
                                                    "images": combo_product_images,
                                                    "mobileImage": combo_product_mobile_images,
                                                    "modelImage": combo_product_model_images,
                                                    "currency": combo_product["currency"],
                                                    "currencySymbol": combo_product[
                                                        "currencySymbol"
                                                    ],
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

                        last_response.append(
                            {
                                "parentProductId": parent_product_id,
                                "childProductId": child_product_id,
                                "currentQty": available_quantity_number,
                                "outOfStock": out_of_stock,
                                "similarProduct": product_data,
                                "dcStoreId": dc_store_id,
                                "otherSellerData": other_product_data,
                                "isSubstititeAdded": isSubstititeAdded,
                                "isDcProduct": is_dc_product,
                                "allowOrderOutOfStock": allow_order_out_of_stock,
                                "procurementTime": best_buffer["procurementTime"]
                                if "procurementTime" in best_buffer
                                else 0,
                                "hardLimit": best_buffer["hardLimit"]
                                if "hardLimit" in best_buffer
                                else 0,
                                "batchId": batch_id,
                                "preOrder": best_buffer["preOrder"]
                                if "preOrder" in best_buffer
                                else False,
                                "offerExpire": offer_expire,
                                "productName": product_name,
                                "timeZone": time_zone,
                                "offerProductDetails": buy_x_get_x_product_details,
                                "timeOffSet": time_off_set,
                                "qTymessage": qty_message,
                                "message": message,
                            }
                        )
                    else:
                        deleted_products.append(child_product_id)
                        last_response.append(
                            {
                                "parentProductId": parent_product_id,
                                "childProductId": child_product_id,
                                "currentQty": 0,
                                "outOfStock": True,
                                "similarProduct": product_data,
                                "dcStoreId": dc_store_id,
                                "otherSellerData": [],
                                "isSubstititeAdded": False,
                                "isDcProduct": False,
                                "allowOrderOutOfStock": False,
                                "procurementTime": 0,
                                "hardLimit": 0,
                                "batchId": "DEFAULT",
                                "preOrder": False,
                                "offerExpire": True,
                                "productName": product_name,
                                "timeZone": time_zone,
                                "offerProductDetails": buy_x_get_x_product_details,
                                "timeOffSet": time_off_set,
                                "qTymessage": """There is no product available in this store or any store we will notify you while product in stock""",
                                "message": """There is no product available in this store or any store we will notify you while product in stock"""
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
            if len(last_response) > 0:
                response = {
                    "deletedProducts": deleted_products,
                    "otherSellerData": other_seller_data,
                    "data": last_response,
                    "message": "data found",
                }
                return JsonResponse(response, safe=False, status=200)
            elif len(deleted_products) > 0:
                response = {
                    "deletedProducts": deleted_products,
                    "data": [],
                    "otherSellerData": [],
                    "message": "data found",
                }
                return JsonResponse(response, safe=False, status=200)
            else:
                response = {
                    "deletedProducts": deleted_products,
                    "data": [],
                    "otherSellerData": [],
                    "message": "data not found",
                }
                return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            response = {"data": [], "message": message}
            return JsonResponse(response, safe=False, status=500)


class ZoneWiseValidateInventory(APIView):
    def post(self, request):
        try:
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            data = request.data
            zone_id = data["zoneId"]
            store_category_id = data["storeCategoryId"]
            if zone_id == "":
                response = {"data": [], "message": "zone id is missing"}
                return JsonResponse(response, safe=False, status=422)

            store_find_query = {
                "status": 1,
                "serviceZones.zoneId": zone_id,
                "storeFrontTypeId": {"$ne": 5},
            }
            store_list = []
            if zone_id != "":
                if store_category_id != "":
                    store_find_query["categoryId"] = store_category_id  # 613a58378967cd5b2c7da9d5
                print("store_find_query", store_find_query)
                store_data = db.stores.find(store_find_query)
                for store_res in store_data:
                    store_list.append(str(store_res["_id"]))
            else:
                pass
            print("store list", store_list)
            unit_ids = []
            new_unit_ids = []
            parent_ids = []
            # =============get all the unit ids for the products====================
            for product in data["data"]:
                unit_ids.append(product["unitId"])
                new_unit_ids.append(product["unitId"])
                parent_ids.append(product["parentProductId"])

            # ======================find the details from the elastic search store group wise=================
            must_query = []
            if len(store_list) > 0:
                must_query.append({"terms": {"storeId": store_list}})
            else:
                pass

            must_query.append({"match": {"status": 1}})
            must_query.append({"terms": {"parentProductId": parent_ids}})
            must_query.append({"terms": {"units.unitId": unit_ids}})
            product_search_query = {
                "bool": {"must": must_query, "must_not": [{"match": {"storeId": "0"}}]}
            }
            sort_query = [
                {"isCentral": {"order": "desc"}},
                {"isInStock": {"order": "desc"}},
                {"units.discountPrice": {"order": "asc"}},
            ]
            search_item_query = {
                "size": 100,
                "query": product_search_query,
                "track_total_hits": True,
                "sort": sort_query,
                "aggs": {
                    "group_by_stores": {
                        "terms": {
                            "field": "storeId.keyword",
                            "order": {"avg_score": "desc"},
                            "size": 100,
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
                                    "size": 100,
                                }
                            },
                        },
                    }
                },
            }
            res = es.search(index=index_products, body=search_item_query)
            store_json_list = []
            if "aggregations" in res:
                if "group_by_stores" in res["aggregations"]:
                    if "buckets" in res["aggregations"]["group_by_stores"]:
                        for bucket in res["aggregations"]["group_by_stores"]["buckets"]:
                            store_id = bucket["key"]
                            unit_ids = deepcopy(new_unit_ids)
                            last_response = []
                            try:
                                for product_data in bucket["top_sales_hits"]["hits"]["hits"]:
                                    parent_product_id = product_data["_source"]["parentProductId"]
                                    child_product_id = product_data["_id"]
                                    child_product_details = db.childProducts.find_one(
                                        {
                                            "_id": ObjectId(child_product_id),
                                            "parentProductId": str(parent_product_id),
                                        },
                                    )
                                    if child_product_details["units"][0]["unitId"] in unit_ids:
                                        unit_ids.remove(child_product_details["units"][0]["unitId"])
                                    else:
                                        pass
                                    store_category_details = db.storeCategory.find_one(
                                        {"_id": ObjectId(child_product_details["storeCategoryId"])}
                                    )
                                    if store_category_details is not None:
                                        store_type = store_category_details["type"]
                                    else:
                                        store_type = 0

                                    if "substitute" in child_product_details:
                                        if len(child_product_details) > 0:
                                            isSubstititeAdded = True
                                        else:
                                            isSubstititeAdded = False
                                    else:
                                        isSubstititeAdded = False

                                    if "allowOrderOutOfStock" in child_product_details:
                                        allow_order_out_of_stock = child_product_details[
                                            "allowOrderOutOfStock"
                                        ]
                                    else:
                                        allow_order_out_of_stock = False

                                    try:
                                        available_quantity = child_product_details["units"][0][
                                            "availableQuantity"
                                        ]
                                    except:
                                        available_quantity = 0

                                    if store_type == DINE_STORE_TYPE:
                                        out_of_stock = False
                                        available_quantity_number = 100
                                    else:
                                        if available_quantity == 0:
                                            out_of_stock = True
                                            available_quantity_number = available_quantity
                                        else:
                                            out_of_stock = False
                                            available_quantity_number = available_quantity
                                    product_name = (
                                        product_data["_source"]["units"][0]["unitName"][language]
                                        if language
                                        in product_data["_source"]["units"][0]["unitName"]
                                        else product_data["_source"]["units"][0]["unitName"]["en"]
                                    )
                                    try:
                                        linked_attribute = get_linked_unit_attribute(
                                            product_data["_source"]["units"]
                                        )
                                    except:
                                        linked_attribute = []

                                    # =======================price calculation=========================
                                    offers_details = []
                                    if "offer" in child_product_details:
                                        for offer in child_product_details["offer"]:
                                            offer_detail = db.offers.find_one(
                                                {"_id": ObjectId(offer["offerId"])}
                                            )
                                            if offer_detail is not None:
                                                if offer_detail["startDateTime"] <= int(
                                                    time.time()
                                                ):
                                                    if offer["status"] == 1:
                                                        offers_details.append(offer)
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
                                                "storeId": store_id,
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

                                    tax_value = []
                                    # ===================================for tax====================================
                                    if child_product_details is not None:
                                        if type(child_product_details["tax"]) == list:
                                            for tax in child_product_details["tax"]:
                                                tax_value.append({"value": tax["taxValue"]})
                                        else:
                                            if child_product_details["tax"] is not None:
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
                                    else:
                                        tax_value = []

                                    tax_price = 0
                                    if (
                                        child_product_details["storeCategoryId"]
                                        != DINE_STORE_CATEGORY_ID
                                    ):
                                        if len(tax_value) == 0:
                                            tax_price = 0
                                        else:
                                            for amount in tax_value:
                                                tax_price = tax_price + (int(amount["value"]))
                                    else:
                                        tax_price = 0
                                    # ==========================unit data================================================================
                                    try:
                                        price = child_product_details["units"][0]["b2cPricing"][0][
                                            "b2cproductSellingPrice"
                                        ]
                                    except:
                                        price = child_product_details["units"][0]["floatValue"]

                                    # ==================================get currecny rate============================
                                    currency_rate = 0
                                    currency_symbol = child_product_details["currencySymbol"]
                                    currency = child_product_details["currency"]

                                    if price == 0 or price == "":
                                        final_price = 0
                                        discount_price = 0
                                    else:
                                        price = price + ((float(price) * tax_price) / 100)
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
                                        "basePrice": price,
                                        "finalPrice": final_price,
                                        "discountPrice": discount_price,
                                        "discountPercentage": percentage,
                                    }
                                    offer_expire = True
                                    if len(best_offer) == 0:
                                        offer_expire = True
                                    else:
                                        offer_details = db.offers.find(
                                            {"_id": ObjectId(best_offer["offerId"])}
                                        )
                                        if offer_details.count() > 0:
                                            for offer in offer_details:
                                                if int(offer["globalClaimCount"]) >= int(
                                                    offer["globalUsageLimit"]
                                                ):
                                                    offer_expire = True
                                                else:
                                                    offer_expire = False
                                        else:
                                            offer_expire = True

                                    images = (
                                        child_product_details["units"][0]["image"]
                                        if "image" in child_product_details["units"][0]
                                        else []
                                    )
                                    last_response.append(
                                        {
                                            "images": images,
                                            "finalPriceList": final_price_list,
                                            "linkedAttribute": linked_attribute,
                                            "currency": currency,
                                            "offer": best_offer,
                                            "offers": best_offer,
                                            "offerExpire": offer_expire,
                                            "currencySymbol": currency_symbol,
                                            "parentProductId": parent_product_id,
                                            "childProductId": child_product_id,
                                            "currentQty": available_quantity_number,
                                            "outOfStock": out_of_stock,
                                            "isSubstititeAdded": isSubstititeAdded,
                                            "allowOrderOutOfStock": allow_order_out_of_stock,
                                            "productName": product_name,
                                            "unitId": product_data["_source"]["units"][0]["unitId"],
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

                            if len(unit_ids) > 0:
                                for unit in data["data"]:
                                    if unit["unitId"] in unit_ids:
                                        child_product_central_details = db.childProducts.find_one(
                                            {
                                                "parentProductId": unit["parentProductId"],
                                                "units.unitId": unit["unitId"],
                                                "storeId": "0",
                                            }
                                        )
                                        if child_product_central_details is not None:
                                            last_response.append(
                                                {
                                                    "images": [],
                                                    "finalPriceList": {},
                                                    "linkedAttribute": [],
                                                    "currency": child_product_central_details[
                                                        "currency"
                                                    ],
                                                    "offer": {},
                                                    "offers": {},
                                                    "currencySymbol": child_product_central_details[
                                                        "currencySymbol"
                                                    ],
                                                    "parentProductId": child_product_central_details[
                                                        "parentProductId"
                                                    ],
                                                    "childProductId": str(
                                                        child_product_central_details["_id"]
                                                    ),
                                                    "currentQty": 0,
                                                    "outOfStock": True,
                                                    "isSubstititeAdded": False,
                                                    "allowOrderOutOfStock": False,
                                                    "productName": "",
                                                    "unitId": str(
                                                        child_product_central_details["units"][0][
                                                            "unitId"
                                                        ]
                                                    ),
                                                }
                                            )
                                    else:
                                        pass
                            else:
                                pass
                            if len(last_response) > 0:
                                store_details = db.stores.find_one({"_id": ObjectId(store_id)})
                                if store_details is not None:
                                    store_json_list.append(
                                        {
                                            "id": str(store_details["_id"]),
                                            "name": store_details["storeName"][language]
                                            if language in store_details["storeName"]
                                            else store_details["storeName"]["en"],
                                            "products": last_response,
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

            if len(store_json_list) > 0:
                response = {"data": store_json_list, "message": "data found"}
                return JsonResponse(response, safe=False, status=200)
            else:
                last_response = []
                for pro in data["data"]:
                    child_product_central_details = db.childProducts.find_one(
                        {
                            "parentProductId": pro["parentProductId"],
                            "units.unitId": pro["unitId"],
                            "storeId": "0",
                        }
                    )
                    if child_product_central_details is not None:
                        last_response.append(
                            {
                                "images": [],
                                "finalPriceList": {
                                    "basePrice": child_product_central_details["units"][0][
                                        "floatValue"
                                    ],
                                    "finalPrice": child_product_central_details["units"][0][
                                        "floatValue"
                                    ],
                                    "discountPrice": 0,
                                    "discountPercentage": 0,
                                },
                                "linkedAttribute": [],
                                "currency": child_product_central_details["currency"],
                                "offer": {},
                                "offers": {},
                                "offerExpire": True,
                                "currencySymbol": child_product_central_details["currencySymbol"],
                                "parentProductId": pro["parentProductId"],
                                "childProductId": str(child_product_central_details["_id"]),
                                "currentQty": 0,
                                "outOfStock": True,
                                "isSubstititeAdded": False,
                                "allowOrderOutOfStock": False,
                                "productName": "",
                                "unitId": pro["unitId"],
                            }
                        )
                if len(last_response) > 0:
                    store_json_list.append(
                        {"id": "0", "name": "No Seller Available", "products": last_response}
                    )
                    response = {"data": store_json_list, "message": "data found"}
                    return JsonResponse(response, safe=False, status=200)
                else:
                    response = {"data": [], "message": "data not found"}
                    return JsonResponse(response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            response = {"data": [], "message": message}
            return JsonResponse(response, safe=False, status=500)


class UnitMeasurementList(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["Units"],
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
                name="q",
                required=False,
                default="sear",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="searched text for search the units",
            ),
            openapi.Parameter(
                name="unitTypeValue",
                default="0",
                required=False,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="",
            ),
            openapi.Parameter(
                name="skip",
                default="0",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="skip the data",
            ),
            openapi.Parameter(
                name="limit",
                default="10",
                required=True,
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="limit the data",
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
            search_text = request.GET.get("q", "")
            skip_data = int(request.GET.get("skip", "0"))
            limit_data = int(request.GET.get("limit", "10"))
            unit_type_value = int(request.GET.get("unitTypeValue", 0))
            search_query = {"status": 1}
            if unit_type_value != "" and unit_type_value != 0:
                search_query["unitTypeValue"] = unit_type_value
            if search_text != "":
                search_query["name.en"] = {"$regex": search_text, "$options": "i"}
            units_data = (
                db.units.find(search_query).sort([("_id", -1)]).skip(skip_data).limit(limit_data)
            )
            units_data_count = db.units.find(search_query).count()
            if units_data.count() > 0:
                units_json = []
                for i in units_data:
                    units_json.append(
                        {
                            "text": i["name"][language]
                            if language in i["name"]
                            else i["name"]["en"],
                            "id": i["unit"][language] if language in i["unit"] else i["unit"]["en"],
                        }
                    )
                response = {
                    "items": units_json,
                    "message": "Data Found",
                    "total_count": units_data_count,
                }
                return JsonResponse(response, safe=False, status=200)
            else:
                response = {"data": [], "message": "Data Not Found", "total_count": 0}
                return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            response = {"data": [], "message": message}
            return JsonResponse(response, safe=False, status=500)
