# -*- coding: utf-8 -*-
import json
import os
import pandas as pd
import sys
import time
import traceback

# sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from bson.objectid import ObjectId
from search.views import *
from search_api.settings import MEAT_STORE_CATEGORY_ID, RJ_DEALS, db, ECOMMERCE_STORE_CATEGORY_ID, DINE_STORE_CATEGORY_ID,session
from validations.product_city_pricing import cal_product_city_pricing

# from search.views import home_units_data
from validations.combo_special_validation import combo_special_type_validation
from search_api.settings import currency_exchange_rate
from validations.meat_availbility_validation import meat_availability_check
from validations.driver_roaster import next_availbale_driver_roaster
from rejson import Client, Path


def home_units_data(row, lan, sort, status, logintype, store_category_id, margin_price, city_id):
    try:
        currency_rate = row["currencyRate"]
    except:
        currency_rate = 0
    try:
        tax_price = 0
        best_offer = row["offer"]
        
        ### tax we are reading from units array for meola because they have tax variant wise
        try:
            if store_category_id != DINE_STORE_CATEGORY_ID:
                if "tax" in row["units"][0]:
                    if len(row["units"][0]["tax"]) == 0:
                        tax_price = 0
                    else:
                        for amount in row["units"][0]["tax"]:
                            tax_price = tax_price + (int(amount["taxValue"]))
                else:
                    if len(row["tax"]) == 0:
                        tax_price = 0
                    else:
                        for amount in row["tax"]:
                            tax_price = tax_price + (int(amount["taxValue"]))
            else:
                tax_price = 0
        except:
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

            if discount_type == 0:
                percentage = 0
            else:
                percentage = int(discount_value)

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

            base_price, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price = cal_product_city_pricing(
                logintype, city_id, row)
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

            pricedata = {
                "basePrice": round(base_price, 2),
                "finalPrice": round(final_price, 2),
                "discountType": discount_type,
                "discountPercentage": percentage,
                "outOfStock": outOfStock,
                "availableQuantity": availableQuantity,
                "discountPrice": discount_price,
                "MOQData": {
                    "minimumOrderQty": minimum_order_qty,
                    "unitPackageType": unit_package_type,
                    "unitMoqType": unit_moq_type,
                    "MOQ": str(minimum_order_qty) + " " + unit_package_type,
                },
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

            base_price_tax, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price = cal_product_city_pricing(
                logintype, city_id, row)
            if float(currency_rate) > 0:
                base_price_tax = base_price_tax * float(currency_rate)

            if base_price_tax == "":
                base_price_tax = 0

            base_price = base_price_tax + ((base_price_tax) * tax_price) / 100
            pricedata = {
                "basePrice": round(base_price_tax, 2),
                "finalPrice": round(base_price, 2),
                "discountPrice": 0,
                "availableQuantity": availableQuantity,
                "outOfStock": outOfStock,
                "discountType": 2,
                "discountPercentage": 0,
                "MOQData": {
                    "minimumOrderQty": minimum_order_qty,
                    "unitPackageType": unit_package_type,
                    "unitMoqType": unit_moq_type,
                    "MOQ": str(minimum_order_qty) + " " + unit_package_type,
                },
            }
            result = pricedata
            return result
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
        pricedata = {
            "discountType": 2,
            "discountPercentage": 0,
            "basePrice": 0,
            "availableQuantity": 0,
            "outOfStock": True,
            "finalPrice": 0,
            "discountPrice": 0,
            "MOQData": {
                "minimumOrderQty": 0,
                "unitPackageType": "",
                "unitMoqType": "",
                "MOQ": "",
            },
        }
        result = pricedata
        return result


def linked_unit_attribute(row):
    linked_units = []
    try:
        for link in row["units"][0]["attributes"]:
            try:
                for attrlist in link["attrlist"]:
                    try:
                        if attrlist["linkedtounit"] == 1:
                            linked_units.append(
                                {
                                    "attrname": attrlist["attrname"]["en"],
                                    "value": attrlist["value"]["en"],
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
    return linked_units


def linked_variant_data(row, language):
    variant_data = []
    try:
        try:
            for link_unit in row["units"][0]["attributes"]:
                try:
                    for attrlist in link_unit["attrlist"]:
                        try:
                            if attrlist is None:
                                pass
                            else:
                                if attrlist["linkedtounit"] == 1:
                                    if len(attrlist["value"]) > 0:
                                        attr_name = (
                                            attrlist["value"][language] if language in attrlist["value"] else \
                                                attrlist["value"]["en"]
                                        )
                                    else:
                                        attr_name = ""

                                    if attrlist["measurementUnit"] != "":
                                        attr_name = (
                                                str(attr_name) + " " + attrlist["measurementUnit"]
                                        )
                                    else:
                                        attr_name = str(attr_name)

                                    variant_data.append(
                                        {"value": str(attr_name), "name": attrlist["attrname"]["en"]}
                                    )
                                else:
                                    pass
                        except:
                            print("except first")
                            pass
                except:
                    print("except last")
                    pass
        except:
            pass
        try:
            if "unitSizeGroupValue" in row["units"][0]:
                if "en" in row["units"][0]["unitSizeGroupValue"]:
                    primary_child_product_size = row["units"][0]["unitSizeGroupValue"]["en"]
                    if primary_child_product_size != "":
                        variant_data.append({"value": primary_child_product_size, "name": "Size"})
                    else:
                        pass
                else:
                    pass
        except:
            pass
        try:
            if "colorName" in row["units"][0]:
                primary_child_product_colour = row["units"][0]["colorName"]
                if primary_child_product_colour == "":
                    pass
                else:
                    variant_data.append({"value": primary_child_product_colour, "name": "Color"})
            else:
                pass
        except:
            pass
        return variant_data
    except:
        traceback.print_exc()
        return variant_data


def get_avaialable_quantity(row, next_availbale_driver_time, driver_roaster, zone_id):
    ## function for fetch the available quantity of the product
    row["storeExists"] = True
    if row["storeCategoryId"] == MEAT_STORE_CATEGORY_ID:
        # if the store category type is meat
        if str(row["storeId"]) != "0":
            if "availableQuantity" in row["childProductDetails"]["units"][0]:
                if row["childProductDetails"]["units"][0]["availableQuantity"] > 0:
                    availableQuantity = row["childProductDetails"]["units"][0]["availableQuantity"]
                else:
                    availableQuantity = 0
            else:
                availableQuantity = 0
        else:
            availableQuantity = 0

        # need to call avaibility check function for get the outofstock and
        # next available time for the product for dc flow
        outOfStock, next_availbale_time = meat_availability_check(
            row["mainProductDetails"],
            availableQuantity,
            row["isDcLinked"],
            next_availbale_driver_time,
            row["hardLimit"],
            row["preOrder"],
            driver_roaster,
            row["childProductDetails"],
            zone_id,
            row["procurementTime"],
        )
        row["nextSlotTime"] = next_availbale_time
        row["availableQuantity"] = availableQuantity
        row["outOfStock"] = outOfStock
        return row
    else:
        if str(row["storeId"]) != "0":
            try:
                ## check the store is exist or not
                # store is not exist then product is not available and out of stock
                store_exists = db.stores.find_one({"_id": ObjectId(row["storeId"]), "status": 1}, {"_id": 1})
                if store_exists:
                    # check the product unit details in mongo
                    child_unit = db.childProducts.find_one({"_id": ObjectId(row["childProductId"])}, {"units": 1})
                    if child_unit:
                        availableQuant = child_unit["units"][0]["availableQuantity"]
                        availableQuantity = availableQuant if availableQuant > 0 else 0
                        outOfStock = True if not availableQuantity else False
                    else:
                        outOfStock = True
                        availableQuantity = 0
                else:
                    outOfStock = True
                    availableQuantity = 0
                    row["storeExists"] = False
            except:
                outOfStock = True
                availableQuantity = 0
                row["storeExists"] = False
        else:
            outOfStock = True
            availableQuantity = 0
        row["nextSlotTime"] = ""
        row["availableQuantity"] = availableQuantity
        row["outOfStock"] = outOfStock
        return row


def get_product_static_data(row):
    if "prescriptionRequired" in row:
        if row["prescriptionRequired"] == 0:
            prescription_required = False
        else:
            prescription_required = True
    else:
        prescription_required = False

    if "saleOnline" in row:
        if row["saleOnline"] == 0:
            sales_online = False
        else:
            sales_online = True
    else:
        sales_online = False

    if "needsIdProof" in row:
        if row["needsIdProof"] is False:
            needsIdProof = False
        else:
            needsIdProof = True
    else:
        needsIdProof = False

    if "uploadProductDetails" in row:
        upload_details = row["uploadProductDetails"]
    else:
        upload_details = ""
    
    row["needsIdProof"] = needsIdProof
    row["uploadProductDetails"] = upload_details
    row["saleOnline"] = sales_online
    row["prescriptionRequired"] = prescription_required
    return row


def get_product_cannbies_data(row):
    try:
        mobile_images = row["images"][0]
    except:
        try:
            mobile_images = row["images"]
        except:
            mobile_images = row["image"]

    addition_info = []
    try:
        if "THC" in row["units"][0]:
            addition_info.append(
                {
                    "seqId": 2,
                    "attrname": "THC",
                    "value": str(row["units"][0]["THC"]) + " %",
                    "id": "",
                }
            )
        else:
            pass
    except:
        pass

    try:
        if "CBD" in row["units"][0]:
            addition_info.append(
                {
                    "seqId": 1,
                    "attrname": "CBD",
                    "value": str(row["units"][0]["CBD"]) + " %",
                    "id": "",
                }
            )
        else:
            pass
    except:
        pass

    # =================================================canniber product type========================
    try:
        if "cannabisProductType" in row["units"][0]:
            if row["units"][0]["cannabisProductType"] != "":
                cannabis_type_details = db.cannabisProductType.find_one(
                    {
                        "_id": ObjectId(
                            row["units"][0]["cannabisProductType"]
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
                            "id": row["units"][0]["cannabisProductType"],
                        }
                    )
                else:
                    pass
        else:
            pass
    except:
        pass

    if len(addition_info) > 0:
        additional_info = sorted(addition_info, key=lambda k: k["seqId"], reverse=True)
    else:
        additional_info = []

    row["mobimages"] = mobile_images
    row["extraAttributeDetails"] = additional_info
    row["productStatus"] = ""
    row["productTag"] = ""
    row["hardLimit"] = 0
    row["preOrder"] = False
    row["procurementTime"] = 0
    row["images"] = row["images"]
    return row


def product_attribute_details(row, object_id_main_sellers):
    is_size_available = False
    try:
        if "unitSizeGroupValue" in row["units"][0]:
            if len(row["units"][0]["unitSizeGroupValue"]) > 0:
                if "en" in row["units"][0]["unitSizeGroupValue"]:
                    if row["units"][0]["unitSizeGroupValue"]["en"] != "":
                        is_size_available = True
                    else:
                        is_size_available = False
                elif "en" in row["units"][0]["unitSizeGroupValue"]:
                    if row["units"][0]["unitSizeGroupValue"]["en"] != "":
                        is_size_available = True
                    else:
                        is_size_available = False
                else:
                    is_size_available = False
            else:
                is_size_available = False
        else:
            is_size_available = False
    except:
        pass
    # ==========================highlight data=================================
    hightlight_data = []
    try:
        if "highlights" in row["units"][0]:
            if row["units"][0]["highlights"] is not None:
                for highlight in row["units"][0]["highlights"]:
                    try:
                        hightlight_data.append(
                            highlight["en"] if "en" in highlight else highlight["en"]
                        )
                    except:
                        pass
            else:
                pass
    except:
        pass
    hightlight_data = list(set(hightlight_data))
    hightlight_data = [x for x in hightlight_data if x]
    colour_details = colour_data(
        row["parentProductId"], object_id_main_sellers
    )
    try:
        if type(row["units"][0]) == list:
            attribute_list = row["units"][0][0]["attributes"]
        else:
            attribute_list = row["units"][0]["attributes"]
    except:
        attribute_list = []

    variant_data_list = []
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
                                        str(attrlist["value"]["en"])
                                        if "en" in attrlist["value"]
                                        else str(attrlist["value"]["en"])
                                    )
                                else:
                                    attr_name = (
                                        str(attrlist["value"]["en"])
                                        + " "
                                        + attrlist["measurementUnit"]
                                        if "en" in attrlist["value"]
                                        else str(attrlist["value"]["en"])
                                             + " "
                                             + attrlist["measurementUnit"]
                                    )
                                variant_data_list.append(
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

    linked_units = []
    try:
        for link in row["units"][0]["attributes"]:
            try:
                for attrlist in link["attrlist"]:
                    try:
                        if attrlist["linkedtounit"] == 1:
                            linked_units.append(
                                {
                                    "attrname": attrlist["attrname"]["en"],
                                    "value": attrlist["value"]["en"],
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
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
    except:
        pass

    try:
        if "unitSizeGroupValue" in row["units"][0]:
            if len(row["units"][0]["unitSizeGroupValue"]) > 0:
                if "en" in row["units"][0]["unitSizeGroupValue"]:
                    if row["units"][0]["unitSizeGroupValue"]["en"] != "":
                        linked_units.append(
                            {"attrname": "Size", "value": row["units"][0]["unitSizeGroupValue"]["en"]}
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
        is_size_available = False

    try:
        if "colorName" in row["units"][0]:
            if row["units"][0]["colorName"] != "":
                linked_units.append({"attrname": "Color", "value": row["units"][0]["colorName"]})
            else:
                pass
        else:
            pass
    except:
        pass

    if len(variant_data_list) == 1 and is_size_available == False:
        is_size_available = True

    tax_value = []
    if row is not None:
        if type(row["tax"]) == list:
            for tax in row["tax"]:
                if "taxValue" in tax:
                    tax_value.append({"value": tax["taxValue"]})
                else:
                    tax_value.append({"value": tax["value"]})
        else:
            if row["tax"] is not None:
                if "taxValue" in row["tax"]:
                    tax_value.append({"value": row["tax"]["taxValue"]})
                else:
                    tax_value.append({"value": row["tax"]})
            else:
                pass
    else:
        tax_value = []
    row['tax'] = tax_value
    row["isSizeAvailable"] = is_size_available
    row["hightlight"] = hightlight_data
    row["isOpenPdp"] = False
    row["colourData"] = colour_details
    row["colourCount"] = len(colour_details)
    row["linkedAttribute"] = linked_units
    row["containsMeat"] = False
    row["variantData"] = variant_data_list
    row["sizes"] = []
    row["addOnsCount"] = 0
    row["mouDataUnit"] = ""
    row["moUnit"] = "Pcs"
    row["popularstatus"] = 0
    row["storeCount"] = 0
    return row


def all_main_product_details(row, language, user_id, product_scr):
    child_product_id = str(row["_id"])
    query = {"parentProductId": str(row["parentProductId"]), "status": 1}
    try:
        if store_category_id == MEAT_STORE_CATEGORY_ID:
            query["storeId"] = {"$in": [ObjectId(row["storeId"])]}
        else:
            if str(row["storeId"]) == "0":
                query["storeId"] = row["storeId"]
            else:
                query["storeId"] = ObjectId(row["storeId"])
    except:
        query["storeId"] = row["storeId"]

    variant_count_data = db.childProducts.find(query).count()
    if variant_count_data > 1:
        variant_count = True
    else:
        variant_count = False

    try:
        detail_description = (
            row["detailDescription"][language]
            if language in row["detailDescription"]
            else row["detailDescription"]["en"]
        )
    except:
        detail_description = ""
    isShoppingList = True
    isFavourite = False

    try:
        if "modelImage" in row["units"][0]:
            if len(row["units"][0]["modelImage"]) > 0:
                model_data = row["units"][0]["modelImage"]
            else:
                model_data = [
                    {"extraLarge": "", "medium": "", "altText": "", "large": "", "small": ""}
                ]
        else:
            model_data = [
                {"extraLarge": "", "medium": "", "altText": "", "large": "", "small": ""}
            ]
    except:
        model_data = [
            {"extraLarge": "", "medium": "", "altText": "", "large": "", "small": ""}
        ]
    try:
        if len(row["units"][0]["image"]) > 0:
            image_data = row["units"][0]["image"]
        else:
            image_data = [
                {"extraLarge": "", "medium": "", "altText": "", "large": "", "small": ""}
            ]
    except:
        image_data = [
            {"extraLarge": "", "medium": "", "altText": "", "large": "", "small": ""}
        ]
    if str(child_product_id) in product_scr:
        es_scr = product_scr[str(child_product_id)]
    else:
        es_scr = 0
    try:
        if "memberPrice" in row["units"][0] and "discountPriceForNonMembers" in \
                row["units"][0]:
            pass
        else:
            if not "memberPrice" in row["units"][0]:
                row["units"][0]["memberPrice"] = row["units"][0]["floatValue"]
            if not "discountPriceForNonMembers" in row["units"][0]:
                row["units"][0]["discountPriceForNonMembers"] = row["units"][0][
                    "floatValue"]
    except:
        pass

    try:
        reseller_commission = row['units'][0]['b2cPricing'][0]['b2cresellerCommission']
    except:
        reseller_commission = 0
    try:
        product_name = row["units"][0]["unitName"][language] if language in row["units"][0]["unitName"] else row["units"][0]["unitName"]["en"]
    except:
        product_name = row["pName"][language] if language in row["pName"] else \
        row["pName"]["en"]
    reseller_commission_type = 0
    row["detailDescription"] = detail_description
    row["resellerCommission"] = reseller_commission
    row["resellerCommissionType"] = reseller_commission_type
    row["maxQuantity"] = row["maxQuantity"] if "maxQuantity" in row else 0
    row["isComboProduct"] = row["isComboProduct"] if "isComboProduct" in row else False
    row["childProductId"] = str(child_product_id)
    row["productName"] = product_name
    row["parentProductId"] = row["parentProductId"]
    row["storeCategoryId"] = row["storeCategoryId"]
    row["allowOrderOutOfStock"] = row["allowOrderOutOfStock"] if "allowOrderOutOfStock" in row else False
    row["variantCount"] = variant_count
    row["brandName"] = row["brandTitle"][language] if language in row["brandTitle"] else row["brandTitle"]["en"]
    row["manufactureName"] = row["manufactureName"][language] if language in row["manufactureName"] else ""
    try:
        row["unitId"] = row["units"][0]["unitId"]
    except:
        row["unitId"] = ""
    row["offerData"] = row["offer"] if "offer" in row else [],
    row["score"] = es_scr
    row["isShoppingList"] = isShoppingList
    row["isFavourite"] = isFavourite
    try:
        row["availableQty"] = row["units"][0]["availableQuantity"]
    except:
        row["availableQty"] = 0
    row["modelImage"] = model_data
    row["mobileImage"] = image_data
    row["isMembersOnly"] = row["isMembersOnly"] if "isMembersOnly" in row else False
    return row


def product_currency_rate(row):
    try:
        currency_rate = currency_exchange_rate[
            str(row["currency"]) + "_" + str(currency_code)
            ]
    except:
        currency_rate = 0
    currency_details = db.currencies.find_one({"currencyCode": currency_code})
    if currency_details is not None:
        currency_symbol = currency_details["currencySymbol"]
        currency = currency_details["currencyCode"]
    else:
        currency_symbol = row["currencySymbol"]
        currency = row["currency"]
    row['currency'] = currency
    row['currencySymbol'] = currency_symbol
    row['currencyRate'] = currency_rate
    return row


def product_seo_validation(row):
    if "productSeo" in row:
        try:
            if len(row["productSeo"]["title"]) > 0:
                title = (
                    row["productSeo"]["title"]["en"]
                    if "en" in row["productSeo"]["title"]
                    else row["productSeo"]["title"]["en"]
                )
            else:
                title = ""
        except:
            title = ""

        try:
            if len(row["productSeo"]["description"]) > 0:
                description = (
                    row["productSeo"]["description"]["en"]
                    if "en" in row["productSeo"]["description"]
                    else row["productSeo"]["description"]["en"]
                )
            else:
                description = ""
        except:
            description = ""

        try:
            if len(row["productSeo"]["metatags"]) > 0:
                metatags = (
                    row["productSeo"]["metatags"]["en"]
                    if "en" in row["productSeo"]["metatags"]
                    else row["productSeo"]["metatags"]["en"]
                )
            else:
                metatags = ""
        except:
            metatags = ""

        try:
            if len(row["productSeo"]["slug"]) > 0:
                slug = (
                    row["productSeo"]["slug"]["en"]
                    if "en" in row["productSeo"]["slug"]
                    else row["productSeo"]["slug"]["en"]
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
    return product_seo


def product_type_validation(row):
    if "productType" in row:
        product_type = int(row['productType'])
    else:
        product_type = 1
    return product_type


def best_supplier_function_cust(row):
    try:
        best_supplier = {
            "productId": row["childProductId"],
            "id": str(row["storeId"]),
        }
        if str(row["storeId"]) == "0":
            best_supplier["supplierName"] = "3Embed"
            best_supplier["storeAliasName"] = "3Embed"
            best_supplier["storeFrontTypeId"] = 0
            best_supplier["storeFrontType"] = "Central"
            best_supplier["storeTypeId"] = 0
            best_supplier["postCode"] = ""
            best_supplier["storeType"] = "Central"
            best_supplier["cityName"] = "Banglore"
            best_supplier["areaName"] = "Hebbal"
            best_supplier["sellerType"] = "Central"
            best_supplier["sellerTypeId"] = 1
            best_supplier["designation"] = ""
            best_supplier["profilePic"] = ""
        else:
            seller_details = db.stores.find_one(
                {
                    "_id": ObjectId(row["storeId"])
                },
                {
                    "storeName": 1,
                    "storeAliasName": 1,
                    "cityName": 1,
                    "storeFrontTypeId": 1,
                    "storeFrontType": 1,
                    "storeTypeId": 1,
                    "storeType": 1,
                    "sellerTypeId": 1,
                    "sellerType": 1
                }
            )
            if seller_details is not None:
                if "storeName" in seller_details:
                    best_supplier["supplierName"] = seller_details["storeName"]["en"]
                    best_supplier["storeAliasName"] = (
                        seller_details["storeAliasName"] if "storeAliasName" in seller_details else ""
                    )
                    best_supplier["cityName"] = seller_details["cityName"]
                    best_supplier["storeFrontTypeId"] = (
                        seller_details["storeFrontTypeId"]
                        if "storeFrontTypeId" in seller_details
                        else 0
                    )
                    best_supplier["storeFrontType"] = (
                        seller_details["storeFrontType"]
                        if "storeFrontType" in seller_details
                        else "Central"
                    )
                    best_supplier["storeTypeId"] = (
                        seller_details["storeTypeId"] if "storeTypeId" in seller_details else 0
                    )
                    best_supplier["storeType"] = (
                        seller_details["storeType"] if "storeType" in seller_details else "Central"
                    )
                    best_supplier["sellerTypeId"] = (
                        seller_details["sellerTypeId"] if "sellerTypeId" in seller_details else 0
                    )
                    best_supplier["sellerType"] = (
                        seller_details["sellerType"] if "sellerType" in seller_details else "Central "
                    )
                else:
                    best_supplier["supplierName"] = "3Embed"
                    best_supplier["storeAliasName"] = "3Embed"
                    best_supplier["storeFrontTypeId"] = 0
                    best_supplier["storeFrontType"] = "Central"
                    best_supplier["storeTypeId"] = 0
                    best_supplier["postCode"] = ""
                    best_supplier["storeType"] = "Central"
                    best_supplier["cityName"] = "Banglore"
                    best_supplier["areaName"] = "Hebbal"
                    best_supplier["sellerType"] = "Central"
                    best_supplier["sellerTypeId"] = 1
                    best_supplier["designation"] = ""
                    best_supplier["profilePic"] = ""
            else:
                best_supplier["supplierName"] = "3Embed"
                best_supplier["storeAliasName"] = "3Embed"
                best_supplier["storeFrontTypeId"] = 0
                best_supplier["storeFrontType"] = "Central"
                best_supplier["storeTypeId"] = 0
                best_supplier["postCode"] = ""
                best_supplier["storeType"] = "Central"
                best_supplier["cityName"] = "Banglore"
                best_supplier["areaName"] = "Hebbal"
                best_supplier["sellerType"] = "Central"
                best_supplier["sellerTypeId"] = 1
                best_supplier["designation"] = ""
                best_supplier["profilePic"] = ""
        return best_supplier
    except:
        return {
            "childProductId": "",
            "storeId": 0,
            "supplierName": "3Embed",
            "storeAliasName": "3Embed",
            "storeFrontTypeId": 0,
            "storeFrontType": "Central",
            "storeTypeId": 0,
            "postCode": "",
            "storeType": "Central",
            "cityName": "Banglore",
            "areaName": "Hebbal",
            "sellerType": "Central",
            "sellerTypeId": 1,
            "designation": "",
            "profilePic": ""
        }


def cal_star_rating_product(row):
    avg_product_rating_value = 0
    try:
        product_rating = db.reviewRatings.aggregate(
            [
                {
                    "$match": {
                        "productId": str(row["parentProductId"]),
                        "rating": {"$ne": 0},
                        "status": 1,
                    }
                },
                {"$group": {"_id": "$orderId", "avgRating": {"$avg": "$rating"}}},
            ]
        )
        p_ratting = 0
        rat_count = 0
        for avg_product_rating in product_rating:
            rat_count = rat_count + 1
            ratting_get = avg_product_rating['avgRating']
            p_ratting = p_ratting + ratting_get

        avg_product_rating_value = p_ratting / rat_count
        try:
            avg_product_rating_value_new = round(avg_product_rating_value, 2)
        except:
            avg_product_rating_value_new = 0
        return avg_product_rating_value_new
    except Exception as e:
        print('e--', e)
        return avg_product_rating_value


def best_offer_function_validate(row, zone_id, logintype):
    offers_details = []
    offer_data = []
    if "offerData" in row:
        if type(row["offerData"]) == list:
            if len(row["offerData"]) > 0:
                offer_data = row["offerData"][0]
                if type(offer_data) == dict:
                    offer_data = row["offerData"]
                else:
                    pass
            else:
                pass
        else:
            offer_data = row["offerData"]
        if offer_data is not None:
            for offer in offer_data:
                if offer["status"] == 1 and offer["offerFor"] in [0, logintype]:
                    offers_details.append(offer)
                else:
                    pass
        else:
            pass
    else:
        pass
    if len(offers_details) > 0:
        best_offer = max(offers_details, key=lambda x: x["discountValue"])
    else:
        best_offer = {}
    row["offer"] = best_offer
    row["offers"] = best_offer
    return row


def product_modification(
    res_res,
    language,
    next_availbale_driver_time,
    zone_id,
    currency_code,
    store_category_id,
    login_type,
    hot_deals,
    main_sellers,
    driver_roaster,
    city_id,
    user_id="",
    rjId = None,
):
    start_time = time.time()
    try:
        from validations.calculate_currency_exchange_rate_value import currency_exchange_rate
    except:
        pass
    # try:
    product_data = []
    currency_details = db.currencies.find_one({"currencyCode": currency_code})
    for main_bucket in res_res:
        product = main_bucket["top_sales_hits"]["hits"]["hits"][0]
        is_dc_linked = False
        hard_limit = 0
        pre_order = False
        procurementTime = 0
        child_product_details = product['_source']
        child_product_details['_id'] = product['_id']
        if child_product_details is not None:
            main_product_details = None
            child_product_id = str(child_product_details["_id"])

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
                        tax_value.append({"taxValue": tax["taxValue"]})
                else:
                    if child_product_details["tax"] is not None:
                        if "taxValue" in child_product_details["tax"]:
                            tax_value.append({"taxValue": child_product_details["tax"]["taxValue"]})
                        else:
                            tax_value.append({"taxValue": child_product_details["tax"]})
                    else:
                        pass
            else:
                tax_value = []
            query = {"parentProductId": str(child_product_details["parentProductId"]), "status": 1}
            try:
                if store_category_id == MEAT_STORE_CATEGORY_ID:
                    query["storeId"] = {"$in": [ObjectId(child_product_details["storeId"])]}
                else:
                    if str(child_product_details["storeId"]) == "0":
                        query["storeId"] = child_product_details["storeId"]
                    else:
                        query["storeId"] = ObjectId(child_product_details["storeId"])
            except:
                query["storeId"] = child_product_details["storeId"]
            variant_count_data = db.childProducts.find(query).count()
            if variant_count_data > 1:
                variant_count = True
            else:
                variant_count = False
            print("variant_count", variant_count)
            try:
                mobile_images = child_product_details["images"][0]
            except:
                try:
                    mobile_images = child_product_details["images"]
                except:
                    mobile_images = child_product_details["image"]

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
                pass

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
                                "id": child_product_details["units"][0]["cannabisProductType"],
                            }
                        )
                    else:
                        pass
            else:
                pass
            if len(addition_info) > 0:
                additional_info = sorted(addition_info, key=lambda k: k["seqId"], reverse=True)
            else:
                additional_info = []

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
                    "productName": child_product_details["units"][0]["unitName"][language]
                    if language in child_product_details["units"][0]["unitName"]
                    else child_product_details["units"][0]["unitName"]["en"],
                    "parentProductId": child_product_details["parentProductId"],
                    "storeCategoryId": child_product_details["storeCategoryId"],
                    "allowOrderOutOfStock": child_product_details["allowOrderOutOfStock"]
                    if "allowOrderOutOfStock" in child_product_details
                    else False,
                    "prescriptionRequired": prescription_required,
                    "saleOnline": sales_online,
                    "uploadProductDetails": upload_details,
                    "nextSlotTime": next_availbale_driver_time,
                    "productSeo": product_seo,
                    "mobimages": mobile_images,
                    "needsIdProof": needsIdProof,
                    "variantCount": variant_count,
                    "brandName": child_product_details["brandTitle"][language]
                    if language in child_product_details["brandTitle"]
                    else child_product_details["brandTitle"]["en"],
                    "manufactureName": child_product_details["manufactureName"][language]
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
        product_dataframe = product_dataframe.apply(
            get_avaialable_quantity,
            next_availbale_driver_time=next_availbale_driver_time,
            driver_roaster=driver_roaster,
            zone_id=zone_id,
            axis=1,
        )
        product_dataframe = product_dataframe.apply(
            best_offer_function_validate, zone_id=zone_id, logintype=login_type, axis=1
        )
        product_dataframe["suppliers"] = product_dataframe.apply(
            best_supplier_function_cust, axis=1
        )
        product_dataframe["productType"] = product_dataframe.apply(product_type_validation, axis=1)
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
            city_id=city_id
        )
        del product_dataframe["childProductDetails"]
        del product_dataframe["mainProductDetails"]
        details = product_dataframe.to_json(orient="records")
        data = json.loads(details)
        try:
            if rjId != "" and rjId is not None:
                RJ_DEALS.jsonset(rjId, Path.rootPath(), data)
                RJ_DEALS.expire(rjId, 3600)
        except:pass
        return data
    else:
        return []
    # except:
    #     return []
