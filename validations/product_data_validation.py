import os
import sys
from pytz import timezone
from bson.objectid import ObjectId
import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from search_api.settings import db, TIME_ZONE, MEAT_STORE_CATEGORY_ID
from validations.supplier_validation import best_supplier_function

timezonename = TIME_ZONE
import time

'''
    Function for the get the next available time for the products
'''


def next_availbale_time(product_id):
    child_product = db.childProducts.find_one({"_id": ObjectId(product_id)},
                                              {"currencySymbol": 1, "currency": 1, "nextAvailableTime": 1})
    if child_product != None:
        currency_symbol = child_product['currencySymbol']
        currency = child_product['currency']
        # ==========================for the available product===========================
        if "nextAvailableTime" in child_product:
            if child_product['nextAvailableTime'] != "":
                product_status = True
                next_available_time = child_product["nextAvailableTime"]
                next_open_time = int(next_available_time)
                local_time = datetime.datetime.fromtimestamp(next_open_time)
                next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                next_day_midnight = next_day.replace(hour=23, minute=59, second=59)
                next_day_midnight_timestamp = int(next_day_midnight.timestamp())
                if next_day_midnight_timestamp < next_open_time:
                    open_time = local_time.strftime("%b %d %Y, %I:%M %p")
                    product_tag = "Available On " + open_time
                else:
                    open_time = local_time.strftime("%I:%M %p")
                    product_tag = "Available On Tomorrow At " + open_time
            else:
                product_status = False
                product_tag = ""
        else:
            product_status = False
            product_tag = ""
    else:
        currency_symbol = hits['_source']['currencySymbol'] if "currencySymbol" in hits['_source'] else "₹"
        currency = hits['_source']['currency'] if "currency" in hits['_source'] else "INR"
        product_status = False
        product_tag = ""
    if currency_symbol is None:
        currencySymbol = "INR"
    return currency_symbol, currency, product_status, product_tag


'''
    Function for the get the linked attribute from the product attribute
'''


def get_linked_unit_attribute(units):
    linked_units = []
    for link in units[0]['attributes']:
        try:
            for attrlist in list['attrlist']:
                try:
                    if attrlist['linkedtounit'] == 1:
                        linked_units.append(
                            {
                                "attrname": attrlist['attrname']['en'],
                                "value": attrlist['value']['en'],
                            }
                        )
                    else:
                        pass
                except:
                    pass
        except:
            pass

    return linked_units


'''
    function for the validate the products data
    :parameter
    product_list: list of the products(array of object) 
    store_id: store id from which store we need to fetch data 
    remove_central: for the remove central store products 
    zone_id: zone id from which store we need to fetch data
    store_category_id: store category id from which store we need to fetch data 
    pharma_cat_id:  pharma category id from which store we need to fetch data
    language: from which language need to fetch the data
'''


def validate_product_data(
        product_list, store_id, remove_central, zone_id, store_category_id, pharma_cat_id, language,
        next_availbale_driver_time, hot_deals):
    start_time = time.time()
    resData = []
    offer_details_data = []
    for product in product_list:
        try:
            variant_data = []
            if "units" in product['_source']:
                if "suppliers" in product['_source']['units'][0]:
                    best_supplier, product_tag = best_supplier_function(product['_id'],
                                                                        product['_source']['units'][0]['suppliers'],
                                                                        store_id, remove_central, zone_id,
                                                                        store_category_id, pharma_cat_id)
                else:
                    best_supplier = {}
                    product_tag = ""
            else:
                if "suppliers" in product['_source']:
                    best_supplier, product_tag = best_supplier_function(product['_id'], product['_source']['suppliers'],
                                                                        store_id, remove_central, zone_id,
                                                                        store_category_id, pharma_cat_id)
                else:
                    best_supplier = {}
                    product_tag = ""
            # ================================get the details from childProducts collection=====================
            if len(best_supplier) > 0:
                query = {"parentProductId": str(product['_id']), "status": 1}
                try:
                    if best_supplier['id'] == "0":
                        query['storeId'] = best_supplier['id']
                    else:
                        query['storeId'] = ObjectId(best_supplier['id'])
                except:
                    query['storeId'] = best_supplier['id']
                variant_count_data = db.childProducts.find(query).count()
                if variant_count_data > 1:
                    variant_count = True
                else:
                    variant_count = False

                child_product_query = {}
                if len(best_supplier) > 0:
                    child_product_id = best_supplier['productId']
                else:
                    child_product_id = i['_id']
                child_product_query['_id'] = ObjectId(child_product_id)
                if best_supplier['id'] == "0":
                    child_product_query['storeId'] = best_supplier['id']
                else:
                    child_product_query['storeId'] = ObjectId(best_supplier['id'])

                child_product_details = db.childProducts.find_one(child_product_query)
                if str(child_product_details['storeId']) != "0":
                    if "availableQuantity" in child_product_details['units'][0]:
                        if child_product_details['units'][0]['availableQuantity'] > 0:
                            outOfStock = False
                            availableQuantity = child_product_details['units'][0]['availableQuantity']
                        else:
                            outOfStock = True
                            availableQuantity = 0
                    else:
                        outOfStock = True
                        availableQuantity = 0
                else:
                    outOfStock = True
                    availableQuantity = 0

                # if product_tag != "":
                #     outOfStock = True
                # else:
                #     pass
                offers_details = []
                if 'offer' in child_product_details:
                    for offer in child_product_details['offer']:
                        if offer['status'] == 1:
                            offer_query = {"_id": ObjectId(offer['offerId']), "status": 1}
                            if store_id != "":
                                offer_query['storeId'] = store_id
                            elif zone_id != "" and store_category_id == MEAT_STORE_CATEGORY_ID:
                                store_json = []
                                city_details = db.zones.find_one({"_id": ObjectId(zone_id)}, {"city_ID": 1})
                                store_data = db.stores.find({"cityId": city_details['city_ID'], "status": 1})
                                for store in store_data:
                                    store_json.append(str(store['_id']))
                                offer_query['storeId'] = {"$in": store_json}
                            elif zone_id != "":
                                store_json = []
                                store_data = db.stores.find({"serviceZones.zoneId": zone_id, "status": 1})
                                for store in store_data:
                                    store_json.append(str(store['_id']))
                                offer_query['storeId'] = {"$in": store_json}
                            else:
                                pass
                            offer_count = db.offers.find(offer_query).count()
                            if offer_count > 0:
                                offers_details.append(offer)
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
                    offer_details = db.offers.find({"_id": ObjectId(best_offer['offerId'])}).count()
                    if offer_details == 0:
                        best_offer = min(offers_details, key=lambda x: x['discountValue'])
                        offer_details = db.offers.find({"_id": ObjectId(best_offer['offerId'])}).count()
                    if offer_details != 0:
                        best_offer = best_offer
                        currdate = datetime.datetime.now().replace(
                            hour=23, minute=59, second=59, microsecond=59)
                        eastern = timezone(timezonename)
                        currlocal = eastern.localize(currdate)
                        best_offer['endDateTimeISO'] = (
                                int(((currlocal).timestamp())) * 1000)
                    else:
                        best_offer = {}
                else:
                    best_offer = {}
                # ======================================product seo======================================================
                if "productSeo" in product['_source']:
                    if len(product['_source']['productSeo']['title']) > 0:
                        title = product['_source']['productSeo']['title'][language] if language in \
                                                                                       product['_source']['productSeo'][
                                                                                           'title'] else \
                        product['_source']['productSeo']['title']["en"]
                    else:
                        title = ""

                    if len(product['_source']['productSeo']['description']) > 0:
                        description = product['_source']['productSeo']['description'][language] if language in \
                                                                                                   product['_source'][
                                                                                                       'productSeo'][
                                                                                                       'description'] else \
                            product['_source']['productSeo']['description']["en"]
                    else:
                        description = ""

                    if len(product['_source']['productSeo']['metatags']) > 0:
                        metatags = product['_source']['productSeo']['metatags'][language] if language in \
                                                                                             product['_source'][
                                                                                                 'productSeo'][
                                                                                                 'metatags'] else \
                            product['_source']['productSeo']['metatags']["en"]
                    else:
                        metatags = ""

                    if len(product['_source']['productSeo']['slug']) > 0:
                        slug = product['_source']['productSeo']['slug'][language] if language in \
                                                                                     product['_source']['productSeo'][
                                                                                         'slug'] else \
                        product['_source']['productSeo']['slug']["en"]
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
                    if child_product_details["prescriptionRequired"] == 0 or child_product_details["prescriptionRequired"] == "" or \
                        child_product_details["prescriptionRequired"] == None:
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
                # ==================================================================================================

                if len(best_supplier) == 0:
                    tax_value = []
                else:
                    if child_product_details != None:
                        if type(child_product_details['tax']) == list:
                            for tax in child_product_details['tax']:
                                tax_value.append({"value": tax['taxValue']})
                        else:
                            if child_product_details['tax'] != None:
                                if "taxValue" in child_product_details['tax']:
                                    tax_value.append({"value": child_product_details['tax']['taxValue']})
                                else:
                                    tax_value.append({"value": child_product_details['tax']})
                            else:
                                pass
                    else:
                        tax_value = []

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
                                            attr_name = str(attrlist['value'][language]) + " " + attrlist[
                                                'measurementUnit'] if language in attrlist['value'] else str(
                                                attrlist['value']['en']) + " " + attrlist['measurementUnit']
                                        variant_data.append(
                                            {
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
                try:
                    mobile_images = product['_source']['images'][0]
                except:
                    try:
                        mobile_images = product['_source']['images']
                    except:
                        mobile_images = product['_source']['image']
                linked_attribute = get_linked_unit_attribute(child_product_details['units'])
                currency_symbol, currency, product_status, product_tag = next_availbale_time(child_product_id)
                if "productType" in child_product_details:
                    if child_product_details['productType'] == 2:
                        combo_product = True
                    else:
                        combo_product = False
                else:
                    combo_product = False

                if child_product_details['currencySymbol'] is None:
                    best_supplier['currencySymbol'] = "INR"
                if currency_symbol is None:
                    currency_symbol = "₹"
                if hot_deals == True and len(best_offer) == 0:
                    pass
                else:
                    resData.append({
                        "maxQuantity": max_quantity,
                        "isComboProduct": combo_product,
                        "childProductId": child_product_id,
                        "productStatus": product_status,
                        "productTag": product_tag,
                        "availableQuantity": availableQuantity,
                        "productName": child_product_details['pName'][language] if language in child_product_details[
                            'pName'] else child_product_details['pName']['en'],
                        "parentProductId": product['_id'],
                        "suppliers": best_supplier,
                        "tax": tax_value,
                        "linkedAttribute": linked_attribute,
                        "allowOrderOutOfStock": allow_out_of_order,
                        "outOfStock": outOfStock,
                        "productTag": product_tag,
                        "offerDetailsData": offer_details_data,
                        "variantData": variant_data,
                        "productTag": product_tag,
                        "variantCount": variant_count,
                        "prescriptionRequired": prescription_required,
                        "saleOnline": sales_online,
                        "uploadProductDetails": upload_details,
                        "productSeo": product_seo,
                        "brandName": child_product_details['brandTitle'][language] if language in child_product_details[
                            'brandTitle'] else child_product_details['brandTitle']['en'],
                        "manufactureName": child_product_details['manufactureName'][language] if language in
                                                                                                 child_product_details[
                                                                                                     'manufactureName'] else "",
                        "TotalStarRating": product['_source']['avgRating'] if "avgRating" in product['_source'] else 0,
                        "currencySymbol": currency_symbol,
                        "currency": currency,
                        "storeCategoryId": child_product_details[
                            'storeCategoryId'] if "storeCategoryId" in child_product_details else "",
                        "images": child_product_details['images'],
                        "mobimages": mobile_images,
                        "finalPriceList": child_product_details['units'],
                        "units": child_product_details['units'],
                        "unitId": child_product_id,
                        "offer": best_offer,
                        "offers": best_offer,
                        "nextSlotTime": next_availbale_driver_time
                    })
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
    return resData
