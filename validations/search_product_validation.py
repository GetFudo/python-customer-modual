from search_api.settings import (
    db,
    currency_exchange_rate,
    ECOMMERCE_STORE_CATEGORY_ID,
    DINE_STORE_CATEGORY_ID,
    GROCERY_STORE_CATEGORY_ID,
)
from search.views import (
    validate_variant,
    get_linked_unit_attribute,
    combo_special_type_validation,
    home_units_data,
)
from bson.objectid import ObjectId
import time
import pandas as pd
import json


def search_data_validation(
    products,
    language,
    sort,
    store_category_id,
    margin_price,
    zone_id,
    currency_code,
    user_id,
    search_query,
):
    sort_type = sort
    resData = []
    filter_responseJson = []
    login_type = 1
    for child in products["products"]:
        child_product_details = db.childProducts.find_one(
            {"_id": ObjectId(child["childProductId"])}
        )
        if child_product_details is not None:
            qty = int(child_product_details["units"][0]["availableQuantity"])
            available_qty = qty
            if qty == "":
                qty = 0
            if str(child_product_details["storeId"]) == "0":
                outOfStock = True
            else:
                if qty > 0:
                    outOfStock = False
                else:
                    outOfStock = True

            if "allowOrderOutOfStock" in child_product_details:
                allow_order_out_of_stock = child_product_details["allowOrderOutOfStock"]
            else:
                allow_order_out_of_stock = False

            if "b2cpackingNoofUnits" in child_product_details:
                if type(child_product_details["b2cpackingNoofUnits"]) == int:
                    b2c_packing_no_units = child_product_details["b2cpackingNoofUnits"]
                else:
                    b2c_packing_no_units = 1
            else:
                b2c_packing_no_units = 1

            if "b2cpackingPackageUnits" in child_product_details:
                if "en" in child_product_details["b2cpackingPackageUnits"]:
                    b2c_packing_package_units = child_product_details["b2cpackingPackageUnits"][
                        "en"
                    ]
                else:
                    b2c_packing_package_units = ""
            else:
                b2c_packing_package_units = ""

            if "b2cpackingPackageType" in child_product_details:
                if "en" in child_product_details["b2cpackingPackageType"]:
                    b2c_packing_units = child_product_details["b2cpackingPackageType"]["en"]
                else:
                    b2c_packing_units = ""
            else:
                b2c_packing_units = ""

            if "containsMeat" in child_product_details:
                contains_Meat = child_product_details["containsMeat"]
            else:
                contains_Meat = False

            try:
                if b2c_packing_package_units != "" and b2c_packing_units != "":
                    mou_data = (
                        str(b2c_packing_no_units)
                        + " "
                        + b2c_packing_package_units
                        + " per "
                        + b2c_packing_units
                    )
                elif b2c_packing_package_units == "" and b2c_packing_units != "":
                    mou_data = str(b2c_packing_no_units) + " " + b2c_packing_package_units
                else:
                    mou_data = ""
            except:
                mou_data = ""

                # ========================= for the get the linked the unit data====================================
            if type(child_product_details["units"][0]) == list:
                attribute_list = child_product_details["units"][0][0]["attributes"]
            else:
                attribute_list = child_product_details["units"][0]["attributes"]

            variant_data = []
            for link_unit in attribute_list:
                if "attrlist" in link_unit:
                    for attrlist in link_unit["attrlist"]:
                        if attrlist is None:
                            pass
                        else:
                            if type(attrlist) == str:
                                pass
                            else:
                                try:
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
                else:
                    pass
            # ======================================product
            # seo======================================================
            if "productSeo" in child_product_details:
                if len(child_product_details["productSeo"]) > 0:
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
                                if language in child_product_details["productSeo"]["description"]
                                else child_product_details["productSeo"]["description"]["en"]
                            )
                        else:
                            description = ""
                    except:
                        description = ""

                    try:
                        if len(child_product_details["productSeo"]["metatags"]) > 0:
                            metatags = (
                                child_product_details["productSeo"]["metatags"][language]
                                if language in child_product_details["productSeo"]["metatags"]
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
                    product_seo = {"title": "", "description": "", "metatags": "", "slug": ""}
            else:
                product_seo = {"title": "", "description": "", "metatags": "", "slug": ""}
            product_id = child["childProductId"]

            offers_details = []
            if "offer" in child_product_details:
                for offer in child_product_details["offer"]:
                    if offer["status"] == 1 and offer["offerFor"] in [0, login_type]:
                        offer_query = {"_id": ObjectId(offer["offerId"]), "status": 1}
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

            if len(offers_details) > 0:
                best_offer = max(offers_details, key=lambda x: x["discountValue"])
                offer_details = db.offers.find({"_id": ObjectId(best_offer["offerId"])}).count()
                if offer_details == 0:
                    best_offer = min(offers_details, key=lambda x: x["discountValue"])
                    offer_details = db.offers.find({"_id": ObjectId(best_offer["offerId"])}).count()
                if offer_details != 0:
                    best_offer = best_offer
                    best_offer["endDateTimeISO"] = 0
                else:
                    best_offer = {}
            else:
                best_offer = {}

            # ===============================variant data=================================================
            best_supplier_id = str(child_product_details["storeId"])
            variant_count = validate_variant(
                str(child_product_details["parentProductId"]),
                best_supplier_id,
                zone_id,
                store_category_id,
            )
            # ======================for tax ================================================================
            tax_value = []
            if child_product_details is not None:
                if type(child_product_details["tax"]) == list:
                    for tax in child_product_details["tax"]:
                        tax_value.append({"value": tax["taxValue"]})
                else:
                    if child_product_details["tax"] is not None:
                        if "taxValue" in child_product_details["tax"]:
                            tax_value.append({"value": child_product_details["tax"]["taxValue"]})
                        else:
                            tax_value.append({"value": child_product_details["tax"]})
                    else:
                        pass
            else:
                tax_value = []

            if len(child_product_details["units"][0]["image"]) > 0:
                image_data = child_product_details["units"][0]["image"]
            else:
                image_data = [
                    {"extraLarge": "", "medium": "", "altText": "", "large": "", "small": ""}
                ]
            if "modelImage" in child_product_details["units"][0]:
                if len(child_product_details["units"][0]["modelImage"]) > 0:
                    model_data = child_product_details["units"][0]["modelImage"]
                else:
                    model_data = [
                        {"extraLarge": "", "medium": "", "altText": "", "large": "", "small": ""}
                    ]
            else:
                model_data = [
                    {"extraLarge": "", "medium": "", "altText": "", "large": "", "small": ""}
                ]
            if child_product_details["storeCategoryId"] == ECOMMERCE_STORE_CATEGORY_ID:
                isShoppingList = True
            else:
                shoppinglist_product = db.userShoppingList.find(
                    {
                        "userId": user_id,
                        "products.centralProductId": child_product_details["parentProductId"],
                        "products.childProductId": product_id,
                    }
                )

                if shoppinglist_product.count() == 0:
                    isShoppingList = False
                else:
                    isShoppingList = True

            # =========================================pharmacy details=========================================
            if "prescriptionRequired" in child_product_details:
                if child_product_details["prescriptionRequired"] == 0:
                    prescription_required = False
                else:
                    prescription_required = True
            else:
                prescription_required = False

            if "needsIdProof" in child_product_details:
                if not child_product_details["needsIdProof"]:
                    needsIdProof = False
                else:
                    needsIdProof = True
            else:
                needsIdProof = False

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
                # ==================================================================================================

                # =========================for max quantity=================================================
            if "maxQuantity" in child_product_details:
                if child_product_details["maxQuantity"] != "":
                    max_quantity = int(child_product_details["maxQuantity"])
                else:
                    max_quantity = 30
            else:
                max_quantity = 30

            # ==========================================================================================
            if "addOns" in child_product_details["units"][0]:
                if len(child_product_details["units"][0]["addOns"]) > 0:
                    addons_count = True
                else:
                    addons_count = False
            else:
                addons_count = False

            # ================================currency=================================================
            currency_symbol = child_product_details["currencySymbol"]
            currency = child_product_details["currency"]
            avg_rating = 0
            product_rating = db.reviewRatings.aggregate(
                [
                    {
                        "$match": {
                            "productId": str(child_product_details["parentProductId"]),
                            "rating": {"$ne": 0},
                        }
                    },
                    {"$group": {"_id": "$productId", "avgRating": {"$avg": "$rating"}}},
                ]
            )
            for avg_rat in product_rating:
                avg_rating = avg_rat["avgRating"]

            product_name = (
                child_product_details["units"][0]["unitName"][language]
                if language in child_product_details["units"][0]["unitName"]
                else child_product_details["units"][0]["unitName"]["en"]
            )
            try:
                linked_attribute = get_linked_unit_attribute(child_product_details["units"])
            except:
                linked_attribute = []
            # ==================================get currecny rate============================
            try:
                currency_rate = currency_exchange_rate[str(currency) + "_" + str(currency_code)]
            except:
                currency_rate = 0
            currency_details = db.currencies.find_one({"currencyCode": currency_code})
            if currency_details is not None:
                currency_symbol = currency_details["currencySymbol"]
                currency = currency_details["currencyCode"]
            product_type = combo_special_type_validation(product_id)
            addition_info = []
            if child_product_details is not None:
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
                    pass
            else:
                pass

            if child_product_details is not None:
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
                    pass
            else:
                pass

            # =================================================canniber product type========================
            if child_product_details is not None:
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
                                    "id": child_product_details["units"][0]["cannabisProductType"],
                                }
                            )
                        else:
                            pass
                else:
                    pass
            else:
                pass

            if len(addition_info) > 0:
                additional_info = sorted(addition_info, key=lambda k: k["seqId"], reverse=True)
            else:
                additional_info = []
            resData.append(
                {
                    "childProductId": product_id,
                    "score": child["score"],
                    "isShoppingList": isShoppingList,
                    "maxQuantity": max_quantity,
                    "linkedAttribute": linked_attribute,
                    "productName": product_name,
                    "brandName": child["brandName"],
                    "manufactureName": child["manufactureName"],
                    "parentProductId": child["parentProductId"],
                    "availableQty": available_qty,
                    "productSeo": product_seo,
                    "TotalStarRating": avg_rating,
                    "storeCategoryId": child_product_details["storeCategoryId"]
                    if "storeCategoryId" in child_product_details
                    else "",
                    "prescriptionRequired": prescription_required,
                    "needsIdProof": needsIdProof,
                    "modelImage": model_data,
                    "extraAttributeDetails": additional_info,
                    "productType": product_type,
                    "saleOnline": sales_online,
                    "currencyRate": currency_rate,
                    "containsMeat": contains_Meat,
                    "uploadProductDetails": upload_details,
                    "sizes": [],
                    "productTag": child["productTag"],
                    "tax": tax_value,
                    "variantData": variant_data,
                    "variantCount": variant_count,
                    "addOnsCount": addons_count,
                    "currencySymbol": currency_symbol,
                    "currency": currency,
                    "mobileImage": image_data,
                    "mouDataUnit": mou_data,
                    "units": child_product_details["units"]
                    if child_product_details is not None
                    else child_product_details["units"],
                    "unitId": child_product_details["units"][0]["unitId"],
                    "allowOrderOutOfStock": allow_order_out_of_stock,
                    "moUnit": "Pcs",
                    "offer": best_offer,
                    "popularstatus": 0,
                    "suppliers": child["supplier"],
                    "outOfStock": outOfStock,
                    "storeCount": child["storeCount"],
                }
            )
        else:
            pass
    if len(resData) > 0:
        if len(resData) > 0:
            dataframe = pd.DataFrame(resData)
            dataframe["popularstatus"] = 1
            dataframe["unitsData"] = dataframe.apply(
                home_units_data,
                lan=language,
                sort=sort,
                status=1,
                axis=1,
                logintype=1,
                store_category_id=store_category_id,
                margin_price=margin_price, city_id=""
            )
            dataframe = dataframe.drop_duplicates("childProductId", keep="first")
            details = dataframe.to_json(orient="records")
            data = json.loads(details)
            for k in data:
                if k["unitsData"]["basePrice"] == 0:
                    pass
                else:
                    try:
                        base_price = k["unitsData"]["basePrice"]
                        final_price = k["unitsData"]["finalPrice"]
                        discount_price = k["unitsData"]["discountPrice"]
                        outOfStock = k["outOfStock"]  # k['unitsData']['outOfStock']
                        availableQuantity = int(
                            k["availableQty"]
                        )  # int(k['unitsData']['availableQuantity'])
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
                    if len(k["offer"]) == 0:
                        percentage = 0
                        discount_type = 0
                    else:
                        if "discountType" in k["offer"]:
                            if k["offer"]["discountType"] == 0:
                                percentage = 0
                                discount_type = 0
                            else:
                                percentage = int(k["offer"]["discountValue"])
                                discount_type = k["offer"]["discountType"]
                        else:
                            percentage = 0
                            discount_type = 0

                    filter_responseJson.append(
                        {
                            "outOfStock": outOfStock,
                            "score": k["score"] if "score" in k else 0,
                            "childProductId": k["childProductId"],
                            "productName": k["productName"],
                            "brandName": k["brandName"],
                            "manufactureName": k["manufactureName"],
                            "isShoppingList": k["isShoppingList"],
                            "unitId": k["unitId"],
                            "storeCategoryId": k["storeCategoryId"],
                            "productType": k["productType"] if "productType" in k else 1,
                            "linkedAttribute": k["linkedAttribute"],
                            "parentProductId": k["parentProductId"],
                            "TotalStarRating": k["TotalStarRating"],
                            "extraAttributeDetails": k["extraAttributeDetails"]
                            if "extraAttributeDetails" in k
                            else [],
                            "offers": k["offer"],
                            "mouDataUnit": k["mouDataUnit"],
                            "variantCount": k["variantCount"],
                            "productTag": k["productTag"],
                            "maxQuantity": k["maxQuantity"],
                            "availableQuantity": availableQuantity,
                            "images": k["mobileImage"],
                            "addOnsCount": k["addOnsCount"],
                            "productSeo": k["productSeo"],
                            "containsMeat": k["containsMeat"],
                            "variantData": k["variantData"],
                            "allowOrderOutOfStock": k["allowOrderOutOfStock"],
                            "moUnit": "Pcs",
                            "discountPrice": discount_price,
                            "discountType": discount_type,
                            "price": final_price,
                            "finalPriceList": {
                                "basePrice": round(base_price, 2),
                                "finalPrice": round(final_price, 2),
                                "discountPrice": round(discount_price, 2),
                                "discountPercentage": percentage,
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
                                "minimumPurchaseUnit": k["unitsData"]["minimumPurchaseUnit"]
                                if "minimumPurchaseUnit" in k["unitsData"]
                                else "",
                            },
                        }
                    )
            if int(sort_type) == 0:
                newlist = sorted(filter_responseJson, key=lambda k: k["price"], reverse=False)
            elif int(sort_type) == 1:
                newlist = sorted(filter_responseJson, key=lambda k: k["price"], reverse=True)
            else:
                if search_query == "":
                    newlist = sorted(
                        filter_responseJson, key=lambda k: k["outOfStock"], reverse=False
                    )
                else:
                    newlist = sorted(
                        filter_responseJson, key=lambda k: k["outOfStock"], reverse=False
                    )

            try:
                if "value" in res["hits"]["total"]:
                    pen_count = res["hits"]["total"]["value"]
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
    return {
        "products": [],
        "penCount": 0,
        "offerBanner": [],
    }
