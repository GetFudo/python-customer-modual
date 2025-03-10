import time
from search_api.settings import db, TIME_ZONE, MEAT_STORE_CATEGORY_ID
import os
import sys
from pytz import timezone
from bson.objectid import ObjectId
from validations.driver_roaster import next_availbale_driver_roaster, next_availbale_driver_shift_in_stock, next_availbale_driver_shift_out_stock, next_availbale_driver_out_stock_shift
import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
timezonename = TIME_ZONE


def validate_dc_product_data(product_list, store_id, zone_id, language, next_availbale_driver_time, user_id):
    validate_start_time = time.time()
    resData = []
    # =================get all the zone dc list=====================================================
    store_details = db.stores.find({"serviceZones.zoneId": zone_id, "storeFrontTypeId": 5, "status": 1})
    dc_seller_list = []
    for dc_seller in store_details:
        dc_seller_list.append(str(dc_seller['_id']))
    for product in product_list:
        start_time = time.time()
        is_dc_linked = False
        hard_limit = 0
        variant_data = []
        supplier_data = []
        dc_data = []
        if "units" in product['_source']:
            if "suppliers" in product['_source']['units'][0]:
                if "suppliers" in product['_source']['units'][0]:
                    for supplier in product['_source']['units'][0]['suppliers']:
                        if supplier['id'] != "0":
                            store_count = db.stores.find({"_id": ObjectId(supplier['id']), "status": 1}).count()
                            if store_count > 0:
                                if supplier['id'] in dc_seller_list:
                                    child_product_count = db.childProducts.find_one({"_id": ObjectId(supplier['productId']), "status": 1}, {"seller": 1})
                                    if child_product_count is not None:
                                        if "seller" in child_product_count:
                                            if len(child_product_count['seller']) > 0:
                                                is_dc_linked = True
                                                dc_data.append(supplier)
                                            else:
                                                pass
                                        else:
                                            pass
                                    else:
                                        pass
                                else:
                                    store_count = db.stores.find(
                                        {"_id": ObjectId(supplier['id']),
                                         "storeFrontTypeId": {"$ne": 5}}).count()
                                    if store_count > 0:
                                        child_product_count = db.childProducts.find({"_id": ObjectId(supplier['productId'])}).count()
                                        if child_product_count > 0:
                                            supplier_data.append(supplier)
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
        # ==================================for meat need to give central product===============
        if len(supplier_data) == 0:
            if "units" in product['_source']:
                if "suppliers" in product['_source']['units'][0]:
                    for supplier in product['_source']['units'][0]['suppliers']:
                        if supplier['id'] != "0":
                            pass
                        else:
                            supplier_data.append(supplier)
                else:
                    pass
        else:
            pass

        if is_dc_linked == True:
            if len(dc_data) > 0:
                best_dc = min(dc_data, key=lambda x: x['retailerPrice'])
            else:
                best_dc = {}
        else:
            best_dc = {}

        if len(supplier_data) > 0:
            best_supplier = min(supplier_data, key=lambda x: x['retailerPrice'])
            if best_supplier['retailerQty'] == 0:
                best_supplier = max(supplier_data, key=lambda x: x['retailerQty'])
            else:
                best_supplier = best_supplier
        else:
            best_supplier = {}

        if len(best_supplier) > 0:
            child_product_details = db.childProducts.find_one({"_id": ObjectId(best_supplier['productId'])})
            try:
                if child_product_details is not None:
                    if len(best_dc) > 0:
                        child_product_data = db.childProducts.find_one({"_id": ObjectId(best_dc['productId'])})
                        if "seller" in child_product_data:
                            for seller in child_product_data['seller']:
                                if seller['storeId'] == best_supplier['id']:
                                    hard_limit = seller['hardLimit']
                                    pre_order = seller['preOrder']
                                else:
                                    pass
                        else:
                            pass
                        try:
                            available_qty = child_product_data['units'][0]['availableQuantity']
                        except:
                            available_qty = 0
                    else:
                        try:
                            available_qty = child_product_details['units'][0]['availableQuantity']
                        except:
                            available_qty = 0
                    # ===============================offer data======================================
                    offers_details = []
                    offer_details_data = []
                    if 'offer' in child_product_details:
                        for offer in child_product_details['offer']:
                            offer_count = db.offers.find({"_id": ObjectId(offer["offerId"]), "status": 1}).count()
                            if offer_count > 0:
                                if offer['status'] == 1:
                                    offer_terms = db.offers.find_one({"_id": ObjectId(offer['offerId'])}, {"termscond": 1, "name": 1, "discountValue": 1, "offerType": 1})
                                    if offer_terms != None:
                                        offer['termscond'] = offer_terms['termscond']
                                    else:
                                        offer['termscond'] = ""
                                    offer['name'] = offer_terms['name']['en']
                                    offer['discountValue'] = offer_terms['discountValue']
                                    offer['discountType'] = offer_terms['offerType']
                                    offers_details.append(offer)
                                    # offers_details.append(offer)
                                    offer_details_data.append({
                                        "offerId": offer["offerId"],
                                        "offerName": offer['offerName']["en"],
                                        "webimages": offer['webimages']['image'],
                                        "mobimage": offer['images']['image'],
                                        "discountValue": offer['discountValue']
                                    })
                                else:
                                    pass
                            else:
                                pass
                    else:
                        pass

                    if len(offers_details) > 0:
                        best_offer = max(offers_details, key=lambda x: x['discountValue'])
                        currdate = datetime.datetime.now().replace(hour=23, minute=59, second=59, microsecond=59)
                        eastern = timezone(timezonename)
                        currlocal = eastern.localize(currdate)
                        best_offer['endDateTimeISO'] = (int(((currlocal).timestamp()))*1000)
                    else:
                        best_offer = {}

                    # ======================================product seo======================================================
                    if "productSeo" in child_product_details:
                        if len(child_product_details['productSeo']['title']) > 0:
                            title = child_product_details['productSeo']['title'][language] if language in child_product_details[
                                'productSeo']['title'] else child_product_details['productSeo']['title']["en"]
                        else:
                            title = ""

                        if len(child_product_details['productSeo']['description']) > 0:
                            description = child_product_details['productSeo']['description'][language] if language in \
                                                                                                          child_product_details['productSeo'][
                                                                                                              'description'] else \
                                child_product_details['productSeo']['description']["en"]
                        else:
                            description = ""

                        if len(child_product_details['productSeo']['metatags']) > 0:
                            metatags = child_product_details['productSeo']['metatags'][language] if language in \
                                                                                                    child_product_details['productSeo'][
                                                                                                        'metatags'] else \
                                child_product_details['productSeo']['metatags']["en"]
                        else:
                            metatags = ""

                        if len(child_product_details['productSeo']['slug']) > 0:
                            slug = child_product_details['productSeo']['slug'][language] if language in child_product_details[
                                'productSeo']['slug'] else child_product_details['productSeo']['slug']["en"]
                        else:
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
                    tax_value = []

                    # =========================================pharmacy details=========================================
                    if "prescriptionRequired" in child_product_details:
                        if child_product_details["prescriptionRequired"] == 0:
                            prescription_required = False
                        else:
                            prescription_required = True
                    else:
                        prescription_required = False

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

                    # ========================= for the get the linked the unit data====================================
                    for link_unit in child_product_details['units'][0]['attributes']:
                        try:
                            for attrlist in link_unit['attrlist']:
                                try:
                                    if attrlist == None:
                                        pass
                                    else:
                                        if attrlist['linkedtounit'] == 1:
                                            if attrlist['measurementUnit'] == "":
                                                attr_name = str(attrlist['value'][language]) if language in attrlist[
                                                    'value'] else str(attrlist['value']['en'])
                                            else:
                                                attr_name = str(attrlist['value'][language]) + " " + attrlist['measurementUnit'] if language in attrlist['value'] else str(
                                                    attrlist['value']['en']) + " " + attrlist['measurementUnit']
                                            variant_data.append(
                                                {
                                                    "attrname": attrlist['attrname']['en'],
                                                    "value": str(attr_name),
                                                    "name": attrlist['attrname']["en"]
                                                }
                                            )
                                        else:
                                            pass
                                except:
                                    pass
                        except:
                            pass
                    # =========================for max quantity=================================================
                    if "maxQuantity" in child_product_details:
                        if child_product_details['maxQuantity'] != "":
                            max_quantity = int(child_product_details['maxQuantity'])
                        else:
                            max_quantity = 30
                    else:
                        max_quantity = 30
                    # ==========================================================================================
                    if "allowOrderOutOfStock" in child_product_details:
                        allow_out_of_order = child_product_details['allowOrderOutOfStock']
                    else:
                        allow_out_of_order = False

                    mobile_images = []

                    if "productType" in child_product_details:
                        if child_product_details['productType'] == 2:
                            combo_product = True
                        else:
                            combo_product = False
                    else:
                        combo_product = False

                    try:
                        base_price = child_product_details['units'][0]['b2cPricing'][0]['b2cproductSellingPrice']
                    except:
                        base_price = child_product_details['units'][0]['floatValue']

                    tax_price = 0
                    if "tax" in child_product_details:
                        if len(child_product_details['tax']) == 0:
                            tax_price = 0
                        else:
                            for amount in child_product_details['tax']:
                                if "taxValue" in amount:
                                    tax_price = tax_price + (int(amount['taxValue']))
                                if "value" in amount:
                                    tax_price = tax_price + (int(amount['value']))
                                else:
                                    tax_price = tax_price + 0
                    else:
                        tax_price = 0

                    if len(best_offer) > 0:
                        discount_type = int(best_offer['discountType']) if "discountType" in best_offer else 1
                        discount_value = best_offer['discountValue'] if "discountValue" in best_offer else 0
                    else:
                        discount_type = 2
                        discount_value = 0

                    if discount_type == 0:
                        percentage = 0
                    else:
                        percentage = int(discount_value)

                    base_price = base_price + ((float(base_price) * tax_price) / 100)

                    # ==============calculate discount price =============================
                    if discount_type == 0:
                        discount_price = float(discount_value)
                    elif discount_type == 1:
                        discount_price = (float(base_price) * float(discount_value)) / 100
                    else:
                        discount_price = 0
                    final_price = base_price - discount_price

                    try:
                        product_name = child_product_details['pName']["en"] if "pName" in child_product_details else child_product_details['pPName']['en']
                    except:
                        product_name = child_product_details['units'][0]['unitName']["en"]

                    start_time1 = time.time()
                    if child_product_details['storeCategoryId'] == MEAT_STORE_CATEGORY_ID:
                        if best_supplier['id'] == "0":
                            outOfStock = True
                            next_availbale_time = ""
                        else:
                            if available_qty > 0 and is_dc_linked == True and next_availbale_driver_time != "":
                                outOfStock = False
                                next_availbale_time = next_availbale_driver_time
                            elif available_qty < 0 and hard_limit != 0 and is_dc_linked == True and next_availbale_driver_time != "":
                                if -(available_qty) < hard_limit and pre_order == True:
                                    outOfStock = False
                                    next_availbale_time = next_availbale_driver_time
                                else:
                                    outOfStock = True
                                    next_availbale_time = ""
                            elif available_qty <= 0 and is_dc_linked == True and pre_order == True:
                                if child_product_data is not None:
                                    if "seller" in child_product_data:
                                        if len(child_product_data['seller']) > 0:
                                            best_buffer = min(child_product_data['seller'], key=lambda x: x['procurementTime'])
                                            delivery_slot = next_availbale_driver_shift_out_stock(
                                                zone_id, best_buffer['procurementTime'])
                                        else:
                                            delivery_slot = next_availbale_driver_shift_out_stock(zone_id, 0)
                                        try:
                                            next_availbale_time = delivery_slot['productText']
                                        except:
                                            next_availbale_time = ""
                                        if allow_out_of_order == True and next_availbale_time != "":
                                            outOfStock = False
                                        else:
                                            outOfStock = True
                                    else:
                                        next_availbale_time = ""
                                        outOfStock = True
                                else:
                                    next_availbale_time = ""
                                    outOfStock = True
                            elif available_qty == 0 and is_dc_linked == False:
                                next_availbale_time = ""
                                outOfStock = True
                            else:
                                next_availbale_time = ""
                                outOfStock = True
                    else:
                        next_availbale_time = ""
                        if available_qty <= 0:
                            outOfStock = True
                        else:
                            outOfStock = False
                    # ===================================variant count======================================
                    seller_data = []
                    # variant_count_data = product['_source']['variantCount'] if "variantCount" in product['_source'] else 1
                    # ===================================variant count======================================
                    variant_query = {"parentProductId": product['_id'], "status": 1}
                    if best_supplier['id'] == "0":
                        variant_query['storeId'] = best_supplier['id']
                    else:
                        variant_query['storeId'] = ObjectId(best_supplier['id'])
                    variant_count_data = db.childProducts.find(variant_query).count()#product['_source']['variantCount'] if "variantCount" in product['_source'] else 1
                    if variant_count_data > 1:
                        variant_count = True
                    else:
                        variant_count = False
                    isShoppingList = False

                    if "containsMeat" in child_product_details:
                        contains_Meat = child_product_details['containsMeat']
                    else:
                        contains_Meat = False

                    parent_product_data = db.products.find_one(
                        {"_id": ObjectId(product['_id'])})  # for get the average rating of product
                    # =====================from here need to send dc supplier id for the product===============
                    if outOfStock == True and child_product_details['storeCategoryId'] == MEAT_STORE_CATEGORY_ID:
                        pass
                    else:
                        resData.append(
                            {"maxQuantity": max_quantity, "isComboProduct": combo_product,
                             "childProductId": str(best_supplier['productId']),
                             "availableQuantity": available_qty, "offerDetailsData": offer_details_data,
                             "productName": product_name, "parentProductId": product['_id'],
                             "suppliers": best_supplier, "supplier": best_supplier, "containsMeat": contains_Meat, "isShoppingList": isShoppingList,
                             "tax": tax_value, "linkedAttribute": variant_data, "allowOrderOutOfStock": allow_out_of_order,
                             "moUnit": "Pcs", "outOfStock": outOfStock, "variantData": variant_data, "addOnsCount": 0,
                             "variantCount": variant_count, "prescriptionRequired": prescription_required,
                             "saleOnline": sales_online, "uploadProductDetails": upload_details, "productSeo": product_seo,
                             "brandName": child_product_details['brandTitle'][language]
                             if language in child_product_details['brandTitle'] else child_product_details['brandTitle']
                             ['en'], "manufactureName": child_product_details['manufactureName'][language]
                            if language in child_product_details['manufactureName'] else "",
                             "TotalStarRating": parent_product_data['avgRating'] if "avgRating" in parent_product_data else 0,
                             "currencySymbol": child_product_details['currencySymbol']
                             if child_product_details['currencySymbol'] is not None else "₹", "mobileImage": [],
                             "currency": child_product_details['currency'],
                             "storeCategoryId": child_product_details['storeCategoryId']
                             if "storeCategoryId" in child_product_details else "", "images": child_product_details
                            ['images'],
                             "mobimages": mobile_images, "units": child_product_details['units'],
                             "finalPriceList":
                                 {"basePrice": round(base_price, 2),
                                  "finalPrice": round(final_price, 2),
                                  "discountPrice": round(discount_price, 2),
                                  "discountType": discount_type, "discountPercentage": percentage},
                             "price": int(final_price),
                             "isDcAvailable": is_dc_linked, "discountType": discount_type,
                             "unitId": str(child_product_details["units"][0]['unitId']),
                             "offer": best_offer,
                             #  "offers": best_offer,
                             "nextSlotTime": next_availbale_time})
                else:
                    pass
            except:
                pass

    return resData
