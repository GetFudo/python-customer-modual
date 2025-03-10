from bson.objectid import ObjectId
from search_api.settings import (
    db,
    es,
    conv_fac,
    STORE_PRODUCT_INDEX as index_store,
    DINE_STORE_CATEGORY_ID,
)
import datetime
from validations.product_variant_validation import validate_variant
from validations.time_zone_validation import time_zone_converter
import sys
import pandas as pd


def food_search_data(
    buckets_list, store_id, language, lat, long, zone_id, store_category_id, timezone
):
    global distance_miles, distance_km, best_supplier
    product_details = []
    storeData = []
    for bucket in buckets_list:
        product_data = []
        new_store_id = bucket["key"]
        if store_id == "0":  # or store_id != new_store_id:
            pass
        else:
            try:
                store_details = db.stores.find_one(
                    {"_id": ObjectId(new_store_id), "status": 1, "storeFrontTypeId": {"$ne": 5}}
                )
                cusine_name = ""
                if store_details is not None:
                    # ===========================offer data==============================
                    offer_details = db.offers.find(
                        {"storeId": {"$in": [str(store_details["_id"])]}, "status": 1}
                    )
                    offer_json = []
                    for offer in offer_details:
                        offer_json.append(
                            {
                                "offerName": offer["name"][language]
                                if language in offer["name"]
                                else offer["name"]["en"],
                                "offerId": str(offer["_id"]),
                                "discountValue": offer["discountValue"],
                                "offerType": offer["offerType"],
                            }
                        )
                    if len(offer_json) > 0:
                        best_offer_store = max(offer_json, key=lambda x: x["discountValue"])
                        if best_offer_store["offerType"] == 0:
                            percentage_text = (
                                str(best_offer_store["discountValue"]) + "%" + " " + "off"
                            )
                        else:
                            percentage_text = "₹" + str(best_offer_store["discountValue"]) + " off"
                        offer_name = best_offer_store["offerName"]
                    else:
                        offer_name = ""
                        percentage_text = ""

                    try:
                        store_lat = (
                            float(store_details["businessLocationAddress"]["lat"])
                            if "businessLocationAddress" in store_details
                            else 0
                        )
                        store_long = (
                            float(store_details["businessLocationAddress"]["long"])
                            if "businessLocationAddress" in store_details
                            else 0
                        )
                        address = (
                            store_details["businessLocationAddress"]["address"]
                            if "businessLocationAddress" in store_details
                            else ""
                        )
                        addressArea = (
                            store_details["businessLocationAddress"]["addressArea"]
                            if "addressArea" in store_details["businessLocationAddress"]
                            else ""
                        )
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
                        city = (
                            store_details["businessLocationAddress"]["city"]
                            if "city" in store_details["businessLocationAddress"]
                            else ""
                        )
                    except:
                        store_lat = 0
                        store_long = 0
                        address = ""
                        addressArea = ""
                        locality = ""
                        post_code = ""
                        state = ""
                        country = ""
                        city = ""

                    if post_code is None:
                        post_code = ""
                    if state is None:
                        state = ""
                    if country is None:
                        country = ""
                    if city is None:
                        city = ""
                    # ============================query for get the distance by geo query==============
                    must_query = [
                        {"match": {"_id": str(store_details["_id"])}},
                        {"match": {"status": 1}},
                    ]
                    geo_distance_sort = {
                        "_geo_distance": {
                            "distance_type": "plane",
                            "location": {"lat": float(lat), "lon": float(long)},
                            "order": "asc",
                            "unit": "km",
                        }
                    }
                    sort_query = [geo_distance_sort]
                    must_query.append(
                        {
                            "geo_distance": {
                                "distance": "50km",
                                "location": {"lat": float(lat), "lon": float(long)},
                            }
                        }
                    )
                    query = {
                        "query": {
                            "bool": {
                                "must": must_query,
                            }
                        },
                        "size": 1,
                        "from": 0,
                        "sort": sort_query,
                    }
                    res = es.search(
                        index=index_store,
                        body=query,
                        filter_path=[
                            "hits.total",
                            "hits.hits._id",
                            "hits.hits.sort",
                            "hits.hits._source",
                        ],
                    )
                    if res["hits"]["total"]["value"] > 0:
                        if "hits" in res["hits"]:
                            for seller in res["hits"]["hits"]:
                                distance_km = round(seller["sort"][0], 2)
                                distance_miles = round(distance_km * float(conv_fac), 2)
                        else:
                            distance_km = 0
                            distance_miles = 0
                    else:
                        distance_km = 0
                        distance_miles = 0

                    store_name = (
                        store_details["storeName"][language]
                        if language in store_details["storeName"]
                        else store_details["storeName"]["en"]
                    )
                    store_id = str(store_details["_id"])
                    # ===================================for the cusines=============================================
                    if "specialities" in store_details:
                        if len(store_details["specialities"]):
                            for spec in store_details["specialities"]:
                                spec_data = db.specialities.find_one(
                                    {"_id": ObjectId(spec)}, {"specialityName": 1, "image": 1}
                                )
                                if spec_data is not None:
                                    if cusine_name == "":
                                        cusine_name = (
                                            spec_data["specialityName"][language]
                                            if language in spec_data["specialityName"]
                                            else spec_data["specialityName"]["en"]
                                        )
                                    else:
                                        cusine_name = (
                                            cusine_name
                                            + ", "
                                            + spec_data["specialityName"][language]
                                            if language in spec_data["specialityName"]
                                            else spec_data["specialityName"]["en"]
                                        )
                                else:
                                    pass
                        else:
                            pass
                    else:
                        pass

                    if "averageCostForMealForTwo" in store_details:
                        cost_for_two = store_details["averageCostForMealForTwo"]
                    else:
                        cost_for_two = 0

                    # =====================================about store tags=================================
                    if "storeIsOpen" in store_details:
                        store_is_open = store_details["storeIsOpen"]
                    else:
                        store_is_open = False

                    if "nextCloseTime" in store_details:
                        next_close_time = store_details["nextCloseTime"]
                    else:
                        next_close_time = ""

                    if "nextOpenTime" in store_details:
                        next_open_time = store_details["nextOpenTime"]
                    else:
                        next_open_time = ""

                    try:
                        if "timeZoneWorkingHour" in seller["_source"]:
                            timeZoneWorkingHour = seller["_source"]['timeZoneWorkingHour']
                        else:
                            timeZoneWorkingHour = ""
                    except:
                        timeZoneWorkingHour = ""

                    if any(
                        supplier["zoneId"] == str(zone_id)
                        for supplier in store_details["serviceZones"]
                    ):
                        is_delivery = True
                        if next_close_time == "" and next_open_time == "":
                            store_tag = "Temporarily Closed"
                        elif next_open_time != "" and store_is_open == False:
                            # next_open_time = int(next_open_time + timezone * 60)
                            next_open_time = time_zone_converter(timezone, next_open_time, timeZoneWorkingHour)
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
                            store_tag = ""
                    else:
                        is_delivery = False
                        next_open_time = ""
                        next_close_time = ""
                        store_is_open = False
                        store_tag = "Does Not Deliver To Selected Location"

                    if "shopifyStoreDetails" in store_details:
                        if "enable" in store_details["shopifyStoreDetails"]:
                            shopify_enable = store_details["shopifyStoreDetails"]["enable"]
                        else:
                            shopify_enable = False
                    else:
                        shopify_enable = False

                    avg_rating_value = 0
                    seller_rating = db.sellerReviewRatings.aggregate(
                        [
                            {
                                "$match": {
                                    "sellerId": str(store_id),
                                    "status": 1,
                                    "rating": {"$ne": 0},
                                }
                            },
                            {"$group": {"_id": "$sellerId", "avgRating": {"$avg": "$rating"}}},
                        ]
                    )
                    for avg_rating in seller_rating:
                        avg_rating_value = avg_rating["avgRating"]

                    # product count for store
                    product_count = db.childProducts.find(
                        {
                            "storeId": ObjectId(store_id),
                            "status": 1,
                            "units.isPrimary": True
                        }
                    ).count()
                    try:
                        safety_standar = store_details["safetyStandards"] if "safetyStandards" in store_details else 0
                    except:
                        safety_standar = 0
                    store_details_json = {
                        "lat": store_lat,
                        "productCount": product_count,
                        "shopifyEnable": shopify_enable,
                        "safetyStandards": safety_standar,
                        "long": store_long,
                        "address": address,
                        "locality": locality,
                        "cuisines": cusine_name,
                        "addressArea": addressArea,
                        "averageCostForMealForTwo": cost_for_two,
                        "logoImages": store_details["logoImages"],
                        "listingImage": store_details["listingImage"]
                        if "listingImage" in store_details
                        else {},
                        "bannerImages": store_details["bannerImages"],
                        "averageDeliveryTimeInMins": store_details["averageDeliveryTimeInMins"]
                        if "averageDeliveryTimeInMins" in store_details
                        else 0,
                        "avgRating": avg_rating_value,
                        "storeIsOpen": store_is_open,
                        "storeType": store_details["storeType"]
                        if "storeType" in store_details
                        else "Food",
                        "postCode": post_code,
                        "nextCloseTime": next_close_time,
                        "averageDeliveryTime": str(store_details["averageDeliveryTimeInMins"])
                        + " "
                        + "Mins"
                        if "averageDeliveryTimeInMins" in store_details
                        else "",
                        "nextOpenTime": next_open_time,
                        "city": city,
                        "driverTypeId": store_details["driverTypeId"]
                        if "driverTypeId" in store_details
                        else 0,
                        "driverType": store_details["driverType"]
                        if "driverType" in store_details
                        else "",
                        "currencySymbol": store_details["currencyCode"]
                        if "currencyCode" in store_details
                        else "INR",
                        "currency": store_details["currencyCode"]
                        if "currencyCode" in store_details
                        else "INR",
                        "supportedOrderTypes": store_details["supportedOrderTypes"]
                        if "supportedOrderTypes" in store_details
                        else 3,
                        "minimumOrder": store_details["minimumOrder"]
                        if "minimumOrder" in store_details
                        else 0,
                        "storeTag": store_tag,
                        "state": state,
                        "country": country,
                        "percentageText": percentage_text,
                        "offerName": offer_name,
                        "distanceKm": round(distance_km, 2),
                        "uniqStoreId": store_details["uniqStoreId"]
                        if "uniqStoreId" in store_details
                        else "",
                        "tableReservations": store_details["tableReservations"]
                        if "tableReservations" in store_details
                        else False,
                        "distanceMiles": distance_miles,
                        "storeName": store_name,
                        "storeId": store_id,
                        "seqId": 2,
                        "isDelivery": is_delivery,
                    }
                    for hits in bucket["top_sales_hits"]["hits"]["hits"]:
                        tax_value = []
                        attribute_data = []
                        best_supplier = {
                            "productId": hits["_id"],
                            "id": str(new_store_id),
                        }
                        # ===========================tax for the
                        # product=========================================================
                        tax_details = db.childProducts.find_one(
                            {"_id": ObjectId(best_supplier["productId"]), "status": 1}
                        )
                        if tax_details is not None:
                            if type(tax_details["tax"]) == list:
                                for tax in tax_details["tax"]:
                                    tax_value.append({"value": tax["taxValue"]})
                            else:
                                if tax_details["tax"] is not None:
                                    if "taxValue" in tax_details["tax"]:
                                        tax_value.append({"value": tax_details["tax"]["taxValue"]})
                                    else:
                                        tax_value.append({"value": tax_details["tax"]})
                                else:
                                    pass

                            # =================================price calculation===================================================================
                            try:
                                price = tax_details["units"][0]["b2cPricing"][0][
                                    "b2cproductSellingPrice"
                                ]
                            except:
                                price = tax_details["units"][0]["floatValue"]

                            tax_price = 0
                            if tax_details["storeCategoryId"] == DINE_STORE_CATEGORY_ID:
                                pass
                            else:
                                if len(tax_value) == 0:
                                    tax_price = 0
                                else:
                                    for amount in tax_value:
                                        tax_price = tax_price + (int(amount["value"]))

                            base_price = price + ((price * tax_price) / 100)

                            # =============================offer check======================================
                            offer_data = []
                            if "offer" in tax_details:
                                for offer in tax_details["offer"]:
                                    if offer["status"] == 1:
                                        offer_terms = db.offers.find_one(
                                            {
                                                "_id": ObjectId(offer["offerId"]),
                                                "storeId": store_id,
                                                "status": 1,
                                            },
                                            {"termscond": 1},
                                        )
                                        if offer_terms is not None:
                                            offer["termscond"] = offer_terms["termscond"]
                                            offer_data.append(offer)
                                        else:
                                            offer["termscond"] = ""

                                    else:
                                        pass
                            else:
                                pass

                            if len(offer_data) > 0:
                                best_offer = max(offer_data, key=lambda x: x["discountValue"])
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
                                        percentage = int(best_offer["discountValue"])
                                        discount_type = best_offer["discountType"]
                                else:
                                    percentage = 0
                                    discount_type = 0

                            if discount_type == 0:
                                discount_price = float(percentage)
                            elif discount_type == 1:
                                discount_price = (float(price) * float(percentage)) / 100
                            else:
                                discount_price = 0

                            base_price = base_price - discount_price
                            final_price_list = {
                                "basePrice": round(price, 2),
                                "finalPrice": round(base_price, 2),
                                "discountPrice": round(discount_price, 2),
                                "discountPercentage": percentage,
                            }

                            # =======================for addons=============================
                            if "addOns" in hits["_source"]["units"][0]:
                                if hits["_source"]["units"][0]["addOns"] is not None:
                                    if len(hits["_source"]["units"][0]["addOns"]) > 0:
                                        addons_count = True
                                    else:
                                        addons_count = False
                                else:
                                    addons_count = False
                            else:
                                addons_count = False

                            varinat_boolean = validate_variant(
                                hits["_id"], best_supplier["id"], zone_id, store_category_id
                            )
                            currency_symbol = tax_details["currencySymbol"]
                            currency = tax_details["currency"]

                            if tax_details["storeCategoryId"] != DINE_STORE_CATEGORY_ID:
                                if tax_details["units"][0]["availableQuantity"] > 0:
                                    out_of_stock = False
                                else:
                                    out_of_stock = True
                            else:
                                out_of_stock = False

                            additional_info = []
                            if "THC" in hits["_source"]["units"][0]:
                                additional_info.append(
                                    {
                                        "seqId": 2,
                                        "attrname": "THC",
                                        "value": str(hits["_source"]["units"][0]["THC"]) + " %",
                                    }
                                )
                            else:
                                pass
                            if "CBD" in hits["_source"]["units"][0]:
                                additional_info.append(
                                    {
                                        "seqId": 1,
                                        "attrname": "CBD",
                                        "value": str(hits["_source"]["units"][0]["CBD"]) + " %",
                                    }
                                )
                            else:
                                pass

                            # =================================================canniber product type========================
                            if "cannabisProductType" in hits["_source"]["units"][0]:
                                if hits["_source"]["units"][0]["cannabisProductType"] != "":
                                    cannabis_type_details = db.cannabisProductType.find_one(
                                        {
                                            "_id": ObjectId(
                                                hits["_source"]["units"][0]["cannabisProductType"]
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
                                                "id": hits["_source"]["units"][0][
                                                    "cannabisProductType"
                                                ],
                                            }
                                        )
                                    else:
                                        pass
                            else:
                                pass

                            if len(additional_info) > 0:
                                additional_info = sorted(
                                    additional_info, key=lambda k: k["seqId"], reverse=True
                                )
                            else:
                                additional_info = []

                            if "attributes" in tax_details["units"][0]:
                                for attr in tax_details["units"][0]["attributes"]:
                                    for att in attr["attrlist"]:
                                        if "linkedtounit" in att:
                                            if att["linkedtounit"] == 0:
                                                pass
                                            else:
                                                if att["value"] == None:
                                                    pass
                                                else:
                                                    if "measurementUnit" in att:
                                                        measurement_unit = att["measurementUnit"]
                                                    else:
                                                        measurement_unit = ""
                                                    try:
                                                        attr_value = (
                                                            str(att["value"][language])
                                                            + " "
                                                            + str(measurement_unit)
                                                        )
                                                    except:
                                                        attr_value = ""
                                                    attribute_data.append(
                                                        {
                                                            "name": att["attrname"][language]
                                                            if language in att["attrname"]
                                                            else att["attrname"]["en"],
                                                            "value": attr_value,
                                                        }
                                                    )

                            try:
                                reseller_commission = tax_details['units'][0]['b2cPricing'][0][
                                    'b2cresellerCommission']
                            except:
                                reseller_commission = 0

                            try:
                                reseller_commission_type = tax_details['units'][0]['b2cPricing']['b2cpercentageCommission']
                            except:
                                reseller_commission_type = 0

                            product_data.append(
                                {
                                    "parentProductId": str(tax_details["parentProductId"]),
                                    "resellerCommission": reseller_commission,
                                    "resellerCommissionType": reseller_commission_type,
                                    "extraAttributeDetails": additional_info,
                                    "childProductId": best_supplier["productId"],
                                    "unitId": hits["_source"]["units"][0]["unitId"],
                                    "avgRating": hits["_source"]["avgRating"]
                                    if "avgRating" in hits["_source"]
                                    else 0,
                                    "finalPriceList": final_price_list,
                                    "productName": hits["_source"]["pPName"][language]
                                    if language in hits["_source"]["pPName"]
                                    else hits["_source"]["pPName"]["en"],
                                    "images": hits["_source"]["images"],
                                    "offer": best_offer,
                                    "brandName": tax_details["brandTitle"][language]
                                    if language in tax_details["brandTitle"]
                                    else tax_details["brandTitle"]["en"],
                                    "brandTitle": tax_details["brandTitle"][language]
                                    if language in tax_details["brandTitle"]
                                    else tax_details["brandTitle"]["en"],
                                    "manufactureName": tax_details["manufactureName"][language]
                                    if language in tax_details["manufactureName"]
                                    else "",
                                    "needsIdProof": tax_details["needsIdProof"]
                                    if "needsIdProof" in tax_details
                                    else False,
                                    "variantCount": varinat_boolean,
                                    "variantData": attribute_data,
                                    "addOnsCount": addons_count,
                                    "isAddOns": addons_count,
                                    "currencySymbol": currency_symbol,
                                    "containsMeat": tax_details['containsMeat'] if 'containsMeat' in tax_details else False,
                                    "currency": currency,
                                    "outOfStock": out_of_stock,
                                    "availableQuantity": tax_details["units"][0][
                                        "availableQuantity"
                                    ],
                                }
                            )

                    if len(product_data) > 0:
                        storeData.append(store_details_json)
                        newlist = sorted(
                            product_data, key=lambda k: k["availableQuantity"], reverse=True
                        )
                        res_data_dataframe = pd.DataFrame(newlist)
                        res_data_dataframe = res_data_dataframe.drop_duplicates(
                            "parentProductId", keep="first"
                        )
                        res_data_dataframe_newlist = res_data_dataframe.to_dict(orient="records")
                        product_details.append(
                            {
                                "storeData": store_details_json,
                                "products": res_data_dataframe_newlist,
                                "seqId": 2,
                                "storeIsOpen": store_is_open,
                                "isDelivery": is_delivery,
                                "distanceKm": round(distance_km, 2),
                                "uniqStoreId": store_details["uniqStoreId"]
                                if "uniqStoreId" in store_details
                                else "",
                                "storeName": store_name,
                            }
                        )
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                print(
                    "Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex
                )
                pass

    if len(storeData) > 0:
        new_store_list = sorted(storeData, key=lambda k: k["storeIsOpen"], reverse=True)
    else:
        new_store_list = storeData
    if len(product_details) > 0:
        new_product_list = sorted(product_details, key=lambda k: k["storeIsOpen"], reverse=True)
    else:
        new_product_list = []

    return new_product_list, new_store_list
