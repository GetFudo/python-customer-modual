import ast
import datetime
import json
import sys
import os
import traceback
import threading
import time
import grpc
import pandas as pd
from bson import ObjectId, json_util
from dateutil.tz import tz
from django.http.response import JsonResponse
from django.utils.datastructures import MultiValueDictKeyError
from validations.calculate_avg_rating import product_avg_rating
from validations.combo_special_validation import combo_special_type_validation
from notification import notification_pb2_grpc, notification_pb2
from search_api.settings import KAFKA_URL, session, CENTRAL_PRODUCT_INDEX, es, PHARMACY_STORE_CATEGORY_ID, \
    STORE_CREATE_TIME, CENRAL_STORE_NAME, currency_exchange_rate, DINE_STORE_CATEGORY_ID, conv_fac, STORE_PRODUCT_INDEX, \
    STORE_PRODUCT_DOC_TYPE, APP_NAME, DINE_STORE_CATEGORY_ID, GRPC_URL, db
from validations.driver_roaster import next_availbale_driver_roaster
from .response_helper import ResponseHelper
from .db_helper import DbHelper
from kafka_consumers.favouriteproduct import fav_product_add
from validations.time_zone_validation import time_zone_converter
from validations.product_city_pricing import cal_product_city_pricing
from validations.update_plp_page_data import *
from validations.language import language_change 
from translate import Translator as t
##### other declarations #####
index_central_product = CENTRAL_PRODUCT_INDEX
central_zero_store_creation_ts = int(STORE_CREATE_TIME)
central_store = CENRAL_STORE_NAME
index_store = STORE_PRODUCT_INDEX
doc_type_store = STORE_PRODUCT_DOC_TYPE
conv_fac = float(conv_fac)

##### creating objects of helper classes #####
DbHelper = DbHelper()
ResponseHelper = ResponseHelper()


##### other function #####
def get_linked_unit_attribute(units):
    linked_units = []
    for attr_link in units[0]['attributes']:
        try:
            for attrlist in attr_link['attrlist']:
                try:
                    if attrlist['linkedtounit'] == 1:
                        linked_units.append(
                            {
                                "attrname": attrlist['attrname']['en'],
                                "name": attrlist['attrname']['en'],
                                "value": attrlist['value']['en'],
                            }
                        )
                    else:
                        pass
                except:
                    pass
        except:
            pass
    if "unitSizeGroupValue" in units[0]:
        if len(units[0]['unitSizeGroupValue']) > 0:
            if "en" in units[0]['unitSizeGroupValue']:
                if units[0]['unitSizeGroupValue']['en'] != "":
                    linked_units.append({
                        "attrname": "Size",
                        "name": "Size",
                        "value": units[0]['unitSizeGroupValue']['en']
                    })
                else:
                    pass
            else:
                pass
        else:
            pass
    else:
        pass

    if "colorName" in units[0]:
        if units[0]['colorName'] != "":
            linked_units.append({
                "attrname": "Color",
                "name": "Color",
                "value": units[0]['colorName']
            })
        else:
            pass
    else:
        pass

    return linked_units


def home_units_data(row, lan, sort, status, logintype, store_category_id, margin_price, city_id):
    try:
        currency_rate = row["currencyRate"]
    except:
        currency_rate = 0
    try:
        tax_price = 0
        best_offer = row["offer"]
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
                        if "value" in amount:
                            tax_price = tax_price + (int(amount["value"]))
                        else:
                            tax_price = tax_price + (int(amount["taxValue"]))
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
                "basePrice": round(base_price),
                "finalPrice": round(final_price),
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
            print("base_price_tax", base_price_tax)
            if float(currency_rate) > 0:
                base_price_tax = base_price_tax * float(currency_rate)

            if base_price_tax == "":
                base_price_tax = 0

            base_price = base_price_tax + ((base_price_tax) * tax_price) / 100
            pricedata = {
                "basePrice": round(base_price_tax),
                "finalPrice": round(base_price),
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
        message = template.format(type(ex).__name__, ex.args)
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


class OperationHelper:

    def parse_json(self, data):
        return json.loads(json_util.dumps(data))

    def generate_exception(self, ex, request=None):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        traceback.print_exc()
        template = "An exception of type {0} occurred. Arguments: {1!r} in file: {2} at line: {3}"
        message = template.format(
            type(ex).__name__, ex.args, filename, exc_tb.tb_lineno)
        print(message)
        exception_response = ResponseHelper.INTERNAL_SERVER_ERROR_RESPONSE
        exception_response['error'] = message
        return exception_response

    def get_language(self, request):
        language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
        return language

    def get_auth_token(self, request):
        return request.META["HTTP_AUTHORIZATION"]

    def get_userId(self, request):
        err = self.check_authentication(request)
        if err: return err
        try:
            return json.loads(request.META["HTTP_AUTHORIZATION"]).get('userId', False)
        except:
            return False

    def check_authentication(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            if token == "":
                return JsonResponse(ResponseHelper.RESPONSE_401, safe=False, status=401)
        except Exception as ex:
            exception_resp = self.generate_exception(ex, request)
            return JsonResponse(exception_resp, safe=False, status=500)

    def check_req_params(self, data, params):
        try:
            non_nullbale_param = []
            for param_name in params:
                try:
                    value = data[param_name]
                    if value == '' or value == None:
                        non_nullbale_param.append(param_name)
                except MultiValueDictKeyError as e:
                    non_nullbale_param.append(param_name)
                except KeyError as e:
                    non_nullbale_param.append(param_name)

            if len(non_nullbale_param):
                msg = ResponseHelper.gen_422_msg(", ".join(non_nullbale_param))
                req_param_resp = ResponseHelper.JSON_RESPONSE_422
                req_param_resp['message'] = msg
                return JsonResponse(req_param_resp, safe=False, status=422)
        except Exception as ex:
            exception_resp = self.generate_exception(ex)
            return JsonResponse(exception_resp, safe=False, status=500)

    def process_category_list_data(self, store_category_id, store_id, zone_id, category_id, from_data, to_data,
                                   request_from, lan, token, integration_type):
        """
        This method returns category list data for category list api
        :return:
        """
        ##### query category collection for data #####
        store_data_details = []
        store_data_json = []

        category_query = {"status": 1, "storeCategory.storeCategoryId": str(
            store_category_id), "parentId": {"$exists": False}, "productCount": {"$gt": 0}}

        if store_id == "" and zone_id == "":
            category_query['storeId'] = "0"

        elif store_id != "" and zone_id == "":
            category_query['$or'] = [{"storeid": {"$in": [store_id]}}, {"storeId": {"$in": [ObjectId(store_id)]}}]

        elif store_id == "" and zone_id != "":
            store_data = DbHelper.get_stores({"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id})
            if store_data.count() > 0:
                for store in store_data:
                    store_data_details.append(str(store['_id']))
            category_query['$or'] = [
                {"storeid": {"$in": store_data_details}},
                {"storeId": {"$in": [ObjectId(x) for x in store_data_details]}}]

        elif store_id != "" and zone_id != "":
            category_query['storeid'] = {"$in": [store_id]}
            category_query['storeId'] = {"$in": [ObjectId(store_id)]}

        else:
            category_query['storeId'] = "0"

        ##### if category id not given then return all parents category, sub category and sub sub category #####
        if category_id == "":
            project_query = {
                "_id": 1, "categoryName": 1, "imageUrl": 1, "bannerImageUrl": 1, "websiteImageUrl": 1,
                "storeid": 1, "storeId": 1, "websiteBannerImageUrl": 1, "mobileIcon": 1, "mobileImage": 1,
                "websiteIcon": 1, "websiteImage": 1
            }
            sort_query = [("seqId", -1)]

            if int(integration_type) == 0:
                pass
            elif int(integration_type) == 1:
                category_query['magentoId'] = {"$ne": 0}
            elif int(integration_type) == 2:
                category_query['shopify_id'] = {"$ne": ""}
            elif int(integration_type) == 3:
                or_query = [{
                    "magentoId": 0
                }, {
                    "shopify_id": ""
                }]
                category_query['$and'] = or_query

            ##### query database #####
            # print("first level-----", category_query)
            category_data = DbHelper.get_categories(category_query, project_query, from_data, to_data, sort_query)
            category_data_count = DbHelper.get_categories_count(category_query, project_query)

            ##### for each category in result, find its sub categories etc #####
            category_details = []
            if category_data.count() > 0:
                for i in category_data:
                    cat_list = []
                    sub_category_query = {"status": 1, "parentId": i["_id"]}

                    if store_id == "" and zone_id == "":
                        sub_category_query["storeId"] = "0"

                    elif store_id != "" and zone_id == "":
                        # sub_category_query["storeId"] = "0"
                        sub_category_query['$or'] = [
                            {"storeid": {"$in": [store_id, "0"]}},
                            {"storeId": {"$in": [ObjectId(store_id)]}}]

                    elif store_id == "" and zone_id != "":
                        store_data = DbHelper.get_stores({"categoryId": str(store_category_id),
                                                          "serviceZones.zoneId": zone_id})
                        if store_data.count() > 0:
                            for store in store_data:
                                store_data_details.append(str(store['_id']))
                        sub_category_query['$or'] = [
                            {"storeid": {"$in": store_data_details}},
                            {"storeId": {"$in": [ObjectId(x) for x in store_data_details]}}]

                    elif store_id != "" and zone_id != "":
                        sub_category_query['$or'] = [
                            {"storeid": {"$in": [store_id]}},
                            {"storeId": {"$in": [ObjectId(x) for x in store_data_details]}}]

                    else:
                        sub_category_query["storeId"] = "0"

                    if int(integration_type) == 0:
                        pass
                    elif int(integration_type) == 1:
                        sub_category_query['magentoId'] = {"$ne": 0}
                    elif int(integration_type) == 2:
                        sub_category_query['shopify_id'] = {"$ne": ""}
                    elif int(integration_type) == 3:
                        or_query = [{
                            "magentoId": 0
                        }, {
                            "shopify_id": ""
                        }]
                        sub_category_query['$and'] = or_query

                    project_query = {
                        "_id": 1, 'categoryName': 1, "imageUrl": 1, "mobileIcon": 1, "mobileImage": 1, "websiteIcon": 1,
                        "websiteImage": 1
                    }
                    # print("second level ------", sub_category_query)
                    sub_category = DbHelper.get_categories(sub_category_query, project_query, from_data, to_data,
                                                           sort_query=sort_query)

                    ##### for each sub category, find its sub sub category #####
                    for j in sub_category:
                        sub_sub_cat_list = []
                        if store_id != "":
                            query = {"status": 1, "parentId": j['_id'], "storeid": str(store_id)
                                     }
                            if int(integration_type) == 0:
                                pass
                            elif int(integration_type) == 1:
                                query['magentoId'] = {"$ne": 0}
                            elif int(integration_type) == 2:
                                query['shopify_id'] = {"$ne": ""}
                            elif int(integration_type) == 3:
                                query_or_query = [{
                                    "magentoId": 0
                                }, {
                                    "shopify_id": ""
                                }]
                                query['$and'] = query_or_query
                            if request_from == 0:
                                sub_sub_category = DbHelper.get_categories(query, project_query, from_data, to_data,
                                                                           sort_query)
                            sub_sub_category_count = DbHelper.get_categories_count(query)

                        else:
                            query = {"status": 1, "parentId": ObjectId(j['_id'])}
                            if int(integration_type) == 0:
                                pass
                            elif int(integration_type) == 1:
                                query['magentoId'] = {"$ne": 0}
                            elif int(integration_type) == 2:
                                query['shopify_id'] = {"$ne": ""}
                            elif int(integration_type) == 3:
                                query_or_query = [{
                                    "magentoId": 0
                                }, {
                                    "shopify_id": ""
                                }]
                                query['$and'] = query_or_query
                            if request_from == 0:
                                sub_sub_category = DbHelper.get_categories(query, project_query, from_data, to_data,
                                                                           sort_query=sort_query)
                            sub_sub_category_count = DbHelper.get_categories_count(query)

                        if request_from == 0:
                            for k in sub_sub_category:
                                if zone_id == "":
                                    sub_category_count = DbHelper.get_categories_count(
                                        {"status": 1, "storeId": str(store_id),
                                         "parentId": ObjectId(k["_id"])})
                                    if sub_category_count == 0:
                                        query = {"status": 1,
                                                 "storeid": {"$in": [str(store_id)]},
                                                 "parentId": ObjectId(k["_id"])}
                                        if int(integration_type) == 0:
                                            pass
                                        elif int(integration_type) == 1:
                                            query['magentoId'] = {"$ne": 0}
                                        elif int(integration_type) == 2:
                                            query['shopify_id'] = {"$ne": ""}
                                        elif int(integration_type) == 3:
                                            query_or_query = [{
                                                "magentoId": 0
                                            }, {
                                                "shopify_id": ""
                                            }]
                                            query['$and'] = query_or_query
                                        sub_category_count = DbHelper.get_categories_count(query)
                                else:
                                    query = {
                                        "categoryId": str(store_category_id),
                                        "serviceZones.zoneId": zone_id
                                    }
                                    store_data = DbHelper.get_stores(query)

                                    if store_data.count() > 0:
                                        for store in store_data:
                                            store_data_details.append(str(store['_id']))

                                    sub_category_count = DbHelper.get_categories_count(
                                        {"status": 1,
                                         "storeId": {"$in": store_data_details},
                                         "parentId": ObjectId(k["_id"])})

                                    if sub_category_count == 0:
                                        sub_category_count = DbHelper.get_categories_count(
                                            {"status": 1,
                                             "storeid": {"$in": store_data_details},
                                             "parentId": ObjectId(k["_id"])})

                                ##### compiling query results #####
                                sub_sub_cat_list.append({
                                    "id": str(k['_id']),
                                    "subSubCategoryName": k['categoryName'][lan]
                                    if lan in k['categoryName'] else k['categoryName']["en"],
                                    "imageUrl": k['mobileImage'] if "mobileImage" in k else "",
                                    "childCount": sub_category_count
                                })

                            ##### dropping duplicates #####
                            if len(sub_sub_cat_list) > 0:
                                dataframe = pd.DataFrame(sub_sub_cat_list)
                                dataframe = dataframe.drop_duplicates(subset='subSubCategoryName', keep="last")
                                dataframe = dataframe.sort_values("subSubCategoryName")
                                cat_details = dataframe.to_json(orient='records')
                                sub_cat_data = json.loads(cat_details)
                            else:
                                sub_cat_data = []
                        else:
                            sub_cat_data = []
                        ##### compiling category list data #####
                        product_query = {
                            "categoryList.parentCategory.childCategory.categoryId": str(j['_id']),
                            "status": 1
                        }
                        if store_id != "" and store_id != "0":
                            product_query['storeId'] = ObjectId(store_id)
                        product_count = db.childProducts.find(product_query).count()
                        if product_count >= 1:
                            cat_list.append(
                                {
                                    "id": str(j['_id']),
                                    "subCategoryName": j['categoryName'][lan] if lan in j['categoryName'] else
                                    j['categoryName']['en'],
                                    "imageUrl": j['mobileImage'] if "mobileImage" in j else "",
                                    "subSubCategory": sub_cat_data,
                                    "subSubCategoryCount": sub_sub_category_count,
                                    "subCatCount": sub_sub_category_count,
                                }
                            )
                    if lan in i['categoryName']:
                        cat_name = i['categoryName'][lan]
                        if cat_name == "":
                            cat_name = i['categoryName']['en']
                        else:
                            pass
                    else:
                        cat_name = i['categoryName']['en']
                    if len(cat_list) > 0:
                        dataframe = pd.DataFrame(cat_list)
                        dataframe = dataframe.drop_duplicates(subset='subCategoryName', keep="last")
                        dataframe = dataframe.sort_values("subCategoryName")
                        details = dataframe.to_json(orient='records')
                        cat_list_data = json.loads(details)
                    else:
                        cat_list_data = []

                    category_details.append({
                        "id": str(i['_id']),
                        "catName": cat_name,
                        "imageUrl": i['mobileImage'] if "mobileImage" in i else "",
                        "bannerImageUrl": i['mobileImage'] if "mobileImage" in i else "",
                        "websiteImageUrl": i['websiteImage'] if 'websiteImage' in i else "",
                        "websiteBannerImageUrl": i['websiteImageUrl'] if 'websiteImageUrl' in i else "",
                        "subCategory": cat_list_data,
                        "penCount": len(cat_list_data)  # category_data_count
                    })
            if len(cat_list) > 0:
                dataframe = pd.DataFrame(category_details)
                dataframe = dataframe.drop_duplicates(subset='catName', keep="last")
                dataframe = dataframe.sort_values("catName")
                details = dataframe.to_json(orient='records')
                data = json.loads(details)
                response = {
                "data": data,
                "message": "category found....!!!!"
                }
                return ResponseHelper.get_status_200(response)

            else:
                response = {
                    "data": [],
                    "message": "category found....!!!!"
                }
                return ResponseHelper.get_status_404(response)
        else:
            ##### if category id is given then find its sub categories and sub sub categories etc return result #####
            cat_list = []
            category_list_json = []
            query = {
                "status": 1,
                "parentId": ObjectId(category_id)
            }
            if store_id != "":
                query['storeid'] = store_id
            if int(integration_type) == 0:
                pass
            elif int(integration_type) == 1:
                query['magentoId'] = {"$ne": 0}
            elif int(integration_type) == 2:
                query['shopify_id'] = {"$ne": ""}
            elif int(integration_type) == 3:
                query_or_query = [{
                    "magentoId": 0
                }, {
                    "shopify_id": ""
                }]
                query['$and'] = query_or_query

            # query['storeid'] = "0"
            project_query = {"_id": 1, "categoryName": 1, "imageUrl": 1, "bannerImageUrl": 1, "websiteImageUrl": 1,
                             "websiteBannerImageUrl": 1, "mobileIcon": 1, "mobileImage": 1, "websiteIcon": 1,
                             "websiteImage": 1}
            sort_query = [("seqId", -1)]

            ##### query database #####
            category_data = DbHelper.get_categories(query, project_query, from_data, to_data, sort_query)
            category_data_count = DbHelper.get_categories_count(query)
            ##### find sub categories #####
            if category_data.count() > 0:
                for j in category_data:
                    product_query = {
                            "categoryList.parentCategory.childCategory.categoryId": str(j['_id']),
                            "status": 1
                    }
                    if store_id != "" and store_id != "0":
                        product_query['storeId'] = ObjectId(store_id)
                    # else:
                    #     product_query['storeId'] = {"$ne": "0"}
                    if store_id == "" and zone_id != "":
                        store_data = DbHelper.get_stores({"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id})
                        if store_data.count() > 0:
                            for store in store_data:
                                store_data_details.append(str(store['_id']))
                        product_query['$or'] = [
                            {"storeid": {"$in": store_data_details}},
                            {"storeId": {"$in": [ObjectId(x) for x in store_data_details]}}]
                    print('product_query--',product_query)
                    product_count = db.childProducts.find(product_query).count()
                    child_category_query = {
                        "status": 1, "parentId": ObjectId(j['_id'])
                    }
                    # if store_id != "":
                    #     child_category_query['storeid'] = store_id
                    child_category = DbHelper.get_categories_count(child_category_query)
                    if product_count >= 1:
                        print('storeId----------',store_data_details)
                        print(str(j['_id']))
                        cat_list.append(
                            {
                                "id": str(j['_id']),
                                "subCategoryName": j['categoryName'][lan]
                                if lan in j['categoryName'] else j['categoryName']['en'],
                                "imageUrl": j['mobileImage'] if "mobileImage" in j else "",
                                "subCatCount": child_category
                            }
                        )
                if len(cat_list) > 0:
                    dataframe = pd.DataFrame(cat_list)
                    dataframe = dataframe.drop_duplicates(subset='subCategoryName', keep="last")
                    # dataframe = dataframe.drop_duplicates(subset='id', keep="last")
                    dataframe = dataframe.sort_values("subCategoryName")
                    details = dataframe.to_json(orient='records')
                    data = json.loads(details)
                else:
                    data = []
                category_list_json.append({
                    "id": "",
                    "catName": "",
                    "imageUrl": "",
                    "bannerImageUrl": "",
                    "websiteImageUrl": "",
                    "websiteBannerImageUrl": "",
                    "subCategory": data,
                    "penCount": len(data)
                })
                response = {
                    "data": category_list_json,
                    "message": "category found....!!!!"
                }
                return ResponseHelper.get_status_200(response)
            else:
                response = {
                    "data": [],
                    "message": "category found....!!!!"
                }
                return ResponseHelper.get_status_404(response)

    def process_favourite_product_post_api(self, token, data):
        """
        This method stores favourite product data in cassandra
        :return:
        """
        user_id = json.loads(token)['userId']
        try:
            session_id = json.loads(token)['sessionId']
        except:
            session_id = ""
        # user_id = "5d92f959fc2045620ce36c92"

        ##### --------------------- query database and process data -------------------------- #####
        time_stamp = int(datetime.datetime.now().timestamp()) * 1000

        ##### get child product #####
        search_item_query = {"_id": ObjectId(data['productid'])}
        res = DbHelper.get_child_product(search_item_query)
        if res is None:
            response = {
                "message": "product not found"
            }
            return ResponseHelper.get_status_404(response)

        ##### get parent product #####
        query = {"_id": ObjectId(res['parentProductId'])}
        project_query = {"storeCategoryId": 1}
        central_product = DbHelper.get_product(query, project_query)

        ##### processing data for logs #####
        parentProductId = res['parentProductId']
        product_id = data['productid']
        child_product_details = res
        try:
            if child_product_details is not None:
                store_id = str(child_product_details['storeId'])
            else:
                store_id = "0"
        except:
            store_id = "0"
        try:
            product_name = res['units'][0]['unitName']['en']
        except:
            product_name = "N/A"

        latitude = data['latitude'] if data['latitude'] != "" else 0
        longitude = data['longitude'] if data['longitude'] != "" else 0
        #check user is influencer or not 
        if "isInfluencer" in data:
            isInfluencer = data['isInfluencer']
        else:
            isInfluencer= False

        data = {
            'parentproductid': str(parentProductId), 'childproductid': str(product_id),
            "storeid": store_id,
            'productname': str(product_name), 'userid': str(user_id),
            "affiliateid": "0",
            "createdtimestamp": time_stamp,
            'ipaddress': str(data['ipaddress']), 'latitude': latitude,
            'longitude': longitude,
            'cityid': str(data['cityid']) if "cityid" in data else "",
            "session_id": session_id,
            'countryid': str(data['countryid']),
            "isInfluencer":isInfluencer,
            "storeCategoryId": str(central_product['storeCategoryId']) if "storeCategoryId" in central_product else ""
        }
        ##### send data to kafka consumer to store in database #####
        fav_product_add(data)
        response = {
            "message": "Saved to favourite list"
        }
        return ResponseHelper.get_status_200(response)

    def process_favourite_product_patch_api(self, data, user_id):
        """
        This method deletes favourite product entery for given user from database
        :return:
        """
        search_item_query = {"_id": ObjectId(data['productid'])}
        res = DbHelper.get_child_product(search_item_query)
        if res is None:
            response = {
                "message": "product not found"
            }
            return ResponseHelper.get_status_404(response)
        else:
            product_id = data['productid']
            session.execute(
                """DELETE FROM favouriteproductsuserwise WHERE userid=%(userid)s AND 
                childproductid=%(childproductid)s""",
                {"userid": user_id, "childproductid": str(product_id)}
            )
            fav_data = {"userid": user_id, "childproductid": str(product_id)}
            if APP_NAME == "GetFudo":
                result = db.likesProducts.delete_one(fav_data)
                if result.deleted_count == 1:
                    print("Document deleted successfully.")
            response = {
                "message": "Removed from to favourite list"
            }
            return ResponseHelper.get_status_200(response)

    def process_wishlist_get_api(self, token, store_category_id, zone_id, from_data, to_data, language, currency_code,
                                 login_type, search_text, sort_type, city_id):
        """
        This method returns response for wish list get api
        :return:
        """
        user_id = json.loads(token)['userId']
        # user_id = "5fe08d32164e6e2a2aa1a7d3"

        ##### get fav product data and count from cassandra #####
        if store_category_id != "":
            response_casandra, response_casandra_count = DbHelper.get_favourite_product(user_id, store_category_id)
        else:
            response_casandra, response_casandra_count = DbHelper.get_favourite_product(user_id)

        ##### if no data found then return error #####
        if not response_casandra:
            response = {
                "message": "data not found",
                "data": [],
                "penCount": 0
            }
            return_response = {"data": response}
            return ResponseHelper.get_status_404(return_response)
        else:
            ##### process data #####
            fav_products = []
            recent_data = []
            resData = []

            ##### for meamo get next delivery slot #####
            try:
                if zone_id != "":
                    driver_roaster = next_availbale_driver_roaster(zone_id)
                    next_availbale_driver_time = driver_roaster['productText']
                else:
                    next_availbale_driver_time = ""
            except:
                next_availbale_driver_time = ""

            for fav in response_casandra:
                fav_products.append({
                    "parentProductId": fav.parentproductid,
                    "childProductId": fav.childproductid,
                    "createdtimestamp": fav.createdtimestamp
                })
            parent_stores_sorted = sorted(fav_products, key=lambda k: k['createdtimestamp'], reverse=True)
            dataframe = pd.DataFrame(parent_stores_sorted)
            dataframe = dataframe.drop_duplicates(subset='childProductId', keep="last")
            details = dataframe.to_json(orient='records')
            data = json.loads(details)

            ##### for each product, get chaild products for more details #####
            for product in data[from_data:to_data]:
                try:
                    query = {"_id": ObjectId(product['childProductId'])}
                    res_varient_parameters = DbHelper.get_child_product(query)
                    if res_varient_parameters is not None:
                        variant_data = []
                        units_data_json = res_varient_parameters['units']
                        # ========================= for the get the linked the unit data====================================
                        try:
                            for link_unit in res_varient_parameters['units'][0]['attributes']:
                                if "attrlist" in link_unit:
                                    for attrlist in link_unit['attrlist']:
                                        try:
                                            if attrlist is None:
                                                pass
                                            else:
                                                if type(attrlist) == str:
                                                    pass
                                                else:
                                                    if attrlist['linkedtounit'] == 1:
                                                        if "value" in attrlist:
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
                        except:
                            pass
                        central_query = {
                            "size": 1,
                            "from": 0,
                            "query": {
                                "bool": {
                                    "must": [
                                        {"match": {"status": 1}},
                                        {"match": {
                                            "_id": res_varient_parameters['parentProductId']}},
                                    ]
                                }
                            },
                        }
                        res_filter_parameters = es.search(
                            index=index_central_product,
                            # # doc_type=doc_central_product,
                            body=central_query,
                            filter_path=[
                                "hits.hits._id",
                                "hits.hits._source.pPName",
                                "hits.hits._source.offer",
                                "hits.hits._source.images",
                                "hits.hits._source.images",
                                "hits.hits._source.brandTitle",
                                "hits.hits._source.manufactureName",
                                "hits.hits._source.currencySymbol",
                                "hits.hits._source.currency",
                                "hits.hits._source.storeCategoryId",
                                "hits.hits._source.tax",
                                "hits.hits._source.prescriptionRequired",
                                "hits.hits._source.needsIdProof",
                                "hits.hits._source.saleOnline",
                                "hits.hits._source.uploadProductDetails",
                                "hits.hits._source.allowOrderOutOfStock",
                                "hits.hits._source.maxQuantity",
                                "hits.hits._source.avgRating"
                            ],
                        )
                        store_list_json = []
                        # ===========================================get the store data========================================
                        if zone_id != "" and store_category_id == PHARMACY_STORE_CATEGORY_ID:
                            stores_list = DbHelper.get_stores({"status": 1, "serviceZones.zoneId": zone_id})
                            for s in stores_list:
                                store_list_json.append(str(s['_id']))
                        else:
                            pass
                        # ==================================================end================================================
                        product_tag = ""
                        if len(res_filter_parameters) > 0:
                            for central_data in res_filter_parameters['hits']['hits']:
                                if "allowOrderOutOfStock" in central_data['_source']:
                                    allow_order_of_stock = central_data['_source']['allowOrderOutOfStock']
                                else:
                                    allow_order_of_stock = False

                                # =========================for max quantity=================================================
                                if "maxQuantity" in central_data['_source']:
                                    if central_data['_source']['maxQuantity'] != "":
                                        max_quantity = int(central_data['_source']['maxQuantity'])
                                    else:
                                        max_quantity = 30
                                else:
                                    max_quantity = 30
                                # ==========================================================================================

                                # =========================================pharmacy details========================
                                if "prescriptionRequired" in central_data['_source']:
                                    if central_data['_source']["prescriptionRequired"] == 0:
                                        prescription_required = False
                                    else:
                                        prescription_required = True
                                else:
                                    prescription_required = False

                                if "needsIdProof" in central_data['_source']:
                                    if not central_data['_source']["needsIdProof"]:
                                        needsIdProof = False
                                    else:
                                        needsIdProof = True
                                else:
                                    needsIdProof = False

                                if "saleOnline" in central_data['_source']:
                                    if central_data['_source']["saleOnline"] == 0:
                                        sales_online = False
                                    else:
                                        sales_online = True
                                else:
                                    sales_online = False
                                avg_rating = product_avg_rating(str(central_data['_id']))
                                if "uploadProductDetails" in central_data['_source']:
                                    upload_details = central_data['_source']["uploadProductDetails"]
                                else:
                                    upload_details = ""
                                # =============================================================================
                                try:
                                    best_supplier = {
                                        "productId": str(res_varient_parameters['_id']),
                                        "id": str(res_varient_parameters['storeId']),
                                        "retailerQty": res_varient_parameters['units'][0]['availableQuantity']
                                    }

                                    if len(best_supplier) != 0:
                                        if best_supplier['retailerQty'] > 0:
                                            outOfStock = False
                                            availableQuantity = best_supplier['retailerQty']
                                        else:
                                            outOfStock = True
                                            availableQuantity = 0
                                    else:
                                        outOfStock = True
                                        availableQuantity = 0

                                    offers_details = []
                                    if 'offer' in res_varient_parameters:
                                        for offer in res_varient_parameters['offer']:
                                            offer_details = db.offers.find_one({"_id": ObjectId(offer["offerId"])})
                                            if offer_details is not None:
                                                if offer_details['startDateTime'] <= int(time.time()):
                                                    if offer['status'] == 1:
                                                        offers_details.append(offer)
                                                    else:
                                                        pass

                                    if len(offers_details) > 0:
                                        best_offer = max(offers_details, key=lambda x: x['discountValue'])
                                    else:
                                        best_offer = {}

                                    if len(best_supplier) > 0:
                                        if "productId" in best_supplier:
                                            product_id = best_supplier['productId']
                                        else:
                                            product_id = str(res_varient_parameters["_id"])
                                    else:
                                        product_id = str(res_varient_parameters["_id"])

                                    rating_count = DbHelper.get_review_ratings_count(
                                        {"productId": central_data['_id'], "rating": {"$ne": 0}})
                                    tax_value = []

                                    # =======for tax====================================
                                    tax_details = DbHelper.get_child_product({"_id": ObjectId(product_id)}, {"tax": 1})

                                    if tax_details is not None:
                                        if type(tax_details['tax']) == list:
                                            for tax in tax_details['tax']:
                                                tax_value.append(
                                                    {"value": tax['taxValue']})
                                        else:
                                            if tax_details['tax'] is not None:
                                                if "taxValue" in tax_details['tax']:
                                                    tax_value.append(
                                                        {"value": tax_details['tax']['taxValue']})
                                                else:
                                                    tax_value.append(
                                                        {"value": tax_details['tax']})
                                            else:
                                                tax_value = []
                                    else:
                                        tax_value = []

                                    # =========================best seller data(seller info)========================
                                    if len(best_supplier) > 0:
                                        if best_supplier['id'] == "0":
                                            dt_object = datetime.datetime.fromtimestamp(
                                                central_zero_store_creation_ts)
                                            day_s = datetime.datetime.now() - dt_object
                                            if day_s.days == 0:
                                                if int(day_s.seconds) > 59:
                                                    sec = datetime.timedelta(
                                                        seconds=day_s.seconds)
                                                    if int(sec.seconds / 60) > 59:
                                                        time_create = str(
                                                            int(sec.seconds / 3600)) + " hours"
                                                    else:
                                                        time_create = str(
                                                            int(sec.seconds / 60)) + " minutes"
                                                else:
                                                    time_create = str(
                                                        day_s.seconds) + " seconds"

                                            else:
                                                time_create = str(day_s.days) + " days"

                                            best_supplier['supplierName'] = central_store
                                            best_supplier['sellerSince'] = time_create
                                            best_supplier['storeFrontTypeId'] = 0
                                            best_supplier['storeFrontType'] = "Central"
                                            best_supplier['storeTypeId'] = 0
                                            best_supplier['storeType'] = "Central"
                                            best_supplier['cityName'] = "Banglore"
                                            best_supplier['areaName'] = "Hebbal"
                                            best_supplier['sellerType'] = "Central"
                                            best_supplier['sellerTypeId'] = 1
                                        else:
                                            seller_details = DbHelper.get_single_store(
                                                {"_id": ObjectId(best_supplier['id'])},
                                                {"storeName": 1, "registrationDateTimeStamp": 1, "cityName": 1,
                                                 "logoImages": 1, "bannerImages": 1, "storeTypeId": 1, "storeType": 1,
                                                 "storeFrontTypeId": 1, "storeFrontType": 1, "sellerType": 1,
                                                 "sellerTypeId": 1, "billingAddress": 1})
                                            if seller_details is not None or "storeName" in seller_details:
                                                dt_object = datetime.datetime.fromtimestamp(
                                                    seller_details['registrationDateTimeStamp'])
                                                day_s = datetime.datetime.now() - dt_object
                                                if day_s.days == 0:
                                                    if int(day_s.seconds) > 59:
                                                        sec = datetime.timedelta(
                                                            seconds=day_s.seconds)
                                                        if int(sec.seconds / 60) > 59:
                                                            time_create = str(
                                                                int(sec.seconds / 3600)) + " hours"
                                                        else:
                                                            time_create = str(
                                                                int(sec.seconds / 60)) + " minutes"
                                                    else:
                                                        time_create = str(
                                                            day_s.seconds) + " seconds"

                                                else:
                                                    time_create = str(day_s.days) + " days"

                                                best_supplier['supplierName'] = seller_details['storeName'][language]
                                                best_supplier['cityName'] = seller_details['cityName']
                                                best_supplier['logoImages'] = seller_details['logoImages']
                                                best_supplier['bannerImages'] = seller_details['bannerImages']
                                                best_supplier['sellerSince'] = time_create
                                                best_supplier['storeFrontTypeId'] = seller_details[
                                                    'storeFrontTypeId'] if "storeFrontTypeId" in seller_details else 0
                                                best_supplier['storeFrontType'] = seller_details[
                                                    'storeFrontType'] if "storeFrontType" in seller_details else "Central"
                                                best_supplier['storeTypeId'] = seller_details[
                                                    'storeTypeId'] if "storeTypeId" in seller_details else 0
                                                best_supplier['storeType'] = seller_details[
                                                    'storeType'] if "storeType" in seller_details else "Central"
                                                best_supplier['sellerTypeId'] = seller_details[
                                                    'sellerTypeId'] if "sellerTypeId" in seller_details else 0
                                                best_supplier['sellerType'] = seller_details[
                                                    'sellerType'] if "sellerType" in seller_details else "Central"
                                                best_supplier['areaName'] = seller_details['billingAddress'][
                                                    'areaOrDistrict'] if "areaOrDistrict" in seller_details[
                                                    'billingAddress'] else "Hebbal"
                                            else:
                                                dt_object = datetime.datetime.fromtimestamp(
                                                    central_zero_store_creation_ts)
                                                day_s = datetime.datetime.now() - dt_object
                                                if day_s.days == 0:
                                                    if int(day_s.seconds) > 59:
                                                        sec = datetime.timedelta(
                                                            seconds=day_s.seconds)
                                                        if int(sec.seconds / 60) > 59:
                                                            time_create = str(
                                                                int(sec.seconds / 3600)) + " hours"
                                                        else:
                                                            time_create = str(
                                                                int(sec.seconds / 60)) + " minutes"
                                                    else:
                                                        time_create = str(
                                                            day_s.seconds) + " seconds"

                                                else:
                                                    time_create = str(day_s.days) + " days"

                                                best_supplier['supplierName'] = central_store
                                                best_supplier['sellerSince'] = time_create
                                                best_supplier['storeFrontTypeId'] = 0
                                                best_supplier['storeFrontType'] = "Central"
                                                best_supplier['storeTypeId'] = 0
                                                best_supplier['storeType'] = "Central"
                                                best_supplier['cityName'] = "Banglore"
                                                best_supplier['areaName'] = "Hebbal"
                                                best_supplier['sellerType'] = "Central"
                                                best_supplier['sellerTypeId'] = 1
                                    else:
                                        dt_object = datetime.datetime.fromtimestamp(
                                            central_zero_store_creation_ts)
                                        day_s = datetime.datetime.now() - dt_object
                                        if day_s.days == 0:
                                            if int(day_s.seconds) > 59:
                                                sec = datetime.timedelta(seconds=day_s.seconds)
                                                if int(sec.seconds / 60) > 59:
                                                    time_create = str(
                                                        int(sec.seconds / 3600)) + " hours"
                                                else:
                                                    time_create = str(
                                                        int(sec.seconds / 60)) + " minutes"
                                            else:
                                                time_create = str(day_s.seconds) + " seconds"

                                        else:
                                            time_create = str(day_s.days) + " days"
                                        best_supplier['supplierName'] = central_store
                                        best_supplier['sellerSince'] = time_create
                                        best_supplier['storeFrontTypeId'] = 0
                                        best_supplier['storeFrontType'] = "Central"
                                        best_supplier['storeTypeId'] = 0
                                        best_supplier['storeType'] = "Central"
                                        best_supplier['cityName'] = "Banglore"
                                        best_supplier['areaName'] = "Hebbal"
                                        best_supplier['sellerType'] = "Central"
                                        best_supplier['sellerTypeId'] = 1
                                    # ========================end of the supplier===================================
                                    # ================================currency=================================================
                                    child_product = DbHelper.get_child_product(
                                        {"_id": ObjectId(product_id)},
                                        {"currencySymbol": 1, "currency": 1, "units": 1})
                                    if child_product is not None:
                                        currency_symbol = child_product['currencySymbol']
                                        currency = child_product['currency']
                                        # ==================================get currecny rate============================
                                        try:
                                            currency_rate = currency_exchange_rate[str(currency) + "_" + str(currency_code)]
                                        except:
                                            currency_rate = 0
                                        currency_details = DbHelper.get_currency({"currencyCode": currency_code})
                                        if currency_details is not None:
                                            currency_symbol = currency_details['currencySymbol']
                                            currency = currency_details['currencyCode']

                                        linked_attribute = get_linked_unit_attribute(child_product['units'])
                                        if product_tag != "":
                                            outOfStock = True

                                        if "modelImage" in res_varient_parameters["units"][0]:
                                            if len(res_varient_parameters["units"][0]['modelImage']) > 0:
                                                model_data = res_varient_parameters["units"][0]['modelImage']
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
                                        product_type = combo_special_type_validation(str(product_id))
                                        base_price, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price = cal_product_city_pricing(login_type, city_id, res_varient_parameters)
                                        resData.append(
                                            {"childProductId": product_id, "currencyRate": currency_rate,
                                             "availableQuantity": availableQuantity,
                                             "productType": product_type,
                                             "productName": res_varient_parameters["units"][0]['unitName']
                                             [language]
                                             if language in res_varient_parameters["units"][0]['unitName'] else
                                             res_varient_parameters["units"][0]['unitName']['en'],
                                             "parentProductId": central_data['_id'],
                                             "brandName": central_data['_source']['brandTitle'][language],
                                             "storeCategoryId": central_data['_source']['storeCategoryId'],
                                             "manufactureName": central_data['_source']['manufactureName']
                                             [language]
                                             if language in central_data['_source']['manufactureName'] else "",
                                             "outOfStock": outOfStock, "maxQuantity": max_quantity,
                                             "linkedAttribute": linked_attribute,
                                             "allowOrderOutOfStock": allow_order_of_stock,
                                             "MOQData": {
                                                "minimumOrderQty": minimum_order_qty,
                                                "unitPackageType": unit_package_type,
                                                "unitMoqType": unit_moq_type,
                                                "MOQ": moq_data #str(minimum_order_qty) + " " + unit_package_type,
                                             },
                                             "TotalStarRating": round(avg_rating, 2),
                                             "timestamp": product['createdtimestamp'],
                                             "isFavourite": True,
                                             "prescriptionRequired": prescription_required,
                                             "needsIdProof": needsIdProof, "saleOnline": sales_online,
                                             "uploadProductDetails": upload_details, "suppliers": best_supplier,
                                             "tax": tax_value, "variantData": linked_attribute, "currency": currency,
                                             "currencySymbol": currency_symbol,
                                             "images": res_varient_parameters["units"][0]['image']
                                             if "image" in res_varient_parameters["units"][0] else
                                             central_data['_source']['images'],
                                             "modelImage": model_data, "finalPriceList": units_data_json,
                                             "units": units_data_json, "nextSlotTime": next_availbale_driver_time,
                                             "unitId": res_varient_parameters["units"][0]['unitId'],
                                             "offer": best_offer, "avgRating": central_data['_source']['avgRating']
                                            if "avgRating" in central_data['_source'] else 0,
                                             "totalRatingCount": rating_count
                                             }
                                             )
                                except Exception as ex:
                                    template = "an exception of type {0} occurred. arguments:\n{1!r}"
                                    message = template.format(type(ex).__name__, ex.args)
                                    print('Error on line {}'.format(sys.exc_info()
                                                                    [-1].tb_lineno), type(ex).__name__, ex)
                                    pass
                        else:
                            pass
                except Exception as ex:
                    template = "an exception of type {0} occurred. arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    print('Error on line {}'.format(sys.exc_info()
                                                    [-1].tb_lineno), type(ex).__name__, ex)
                    pass
            if len(resData) > 0:
                dataframe = pd.DataFrame(resData)
                dataframe["unitsData"] = dataframe.apply(
                    home_units_data, lan=language, sort=0, status=0, logintype=login_type,
                    store_category_id=store_category_id, axis=1, margin_price=True, city_id=city_id)
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
                        "TotalStarRating": k['TotalStarRating'],
                        "availableQuantity": k['availableQuantity'],
                        "images": k['images'],
                        "maxQuantity": k['maxQuantity'],
                        "allowOrderOutOfStock": k['allowOrderOutOfStock'],
                        "timestamp": k['timestamp'],
                        "supplier": k['suppliers'],
                        "brandName": k['brandName'],
                        "linkedAttribute": k['linkedAttribute'],
                        "manufactureName": k['manufactureName'],
                        "variantData": k['variantData'],
                        "prescriptionRequired": k['prescriptionRequired'],
                        "needsIdProof": k['needsIdProof'] if "needsIdProof" in k else False,
                        "productType": k['productType'] if "productType" in k else 1,
                        "nextSlotTime": k['nextSlotTime'] if "nextSlotTime" in k else "",
                        "modelImage": k['modelImage'] if "modelImage" in k else [],
                        "MOQData": k['MOQData'] if "MOQData" in k else {},
                        "saleOnline": k['saleOnline'],
                        "isFavourite": k['isFavourite'] if "isFavourite" in k else True,
                        "uploadProductDetails": k['uploadProductDetails'],
                        "discountType": k['offer']['discountType'] if "discountType" in k['offer'] else 0,
                        "finalPrice": round(k['unitsData']['finalPrice'], 2),
                        "finalPriceList": {
                            "basePrice": round(k['unitsData']['basePrice'], 2),
                            "finalPrice": round(k['unitsData']['finalPrice'], 2),
                            "discountPrice": round(k['unitsData']['discountPrice'], 2),
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

                if search_text != "":
                    final_list = []
                    for r_d in recent_data:
                        if search_text.upper() in r_d['productName'] or search_text.lower() in r_d[
                            'productName'] or search_text in r_d['productName'] or search_text.capitalize() in r_d[
                            'productName']:
                            final_list.append(r_d)
                    sorted_data = final_list
                elif int(sort_type) == 0:
                    sorted_data = sorted(recent_data, key=lambda k: k['timestamp'], reverse=True)
                elif int(sort_type) == 1:
                    sorted_data = sorted(recent_data, key=lambda k: k['finalPrice'], reverse=False)
                elif int(sort_type) == 2:
                    sorted_data = sorted(recent_data, key=lambda k: k['finalPrice'], reverse=True)

                for count in response_casandra_count:
                    for s_c in count:
                        total_count = s_c
                response = {
                    "data": sorted_data,
                    "penCount": total_count,
                    "message": "data found...!!!"
                }
                return_response = {"data": response}
                return ResponseHelper.get_status_200(return_response)
            else:
                response = {
                    "data": [],
                    "penCount": 0,
                    "message": "data not found...!!!"
                }
                return_response = {"data": response}
                return ResponseHelper.get_status_404(return_response)

    def process_brandlist_get_api(self, store_category_id, search_text, from_page, to_page, lan, s_id):
        """
        this method gets brand data and return rsponse
        :return:
        """
        ##### create query for brand data and get data #####
        brand_query = {"status": 1, "storeCategoryId": str(store_category_id)}
        if search_text != "":
            brand_query['name.en'] = {'$regex': (search_text).replace("%20", " "), '$options': 'i'}
        sort_query = [("_id", -1)]
        brand_details = DbHelper.get_brands(brand_query, from_data=from_page, to_data=to_page, sort_query=sort_query)
        brand_count = DbHelper.get_brands_count(brand_query)

        ##### ------------------------------ process data ----------------------------------------- #####
        brand_data = []
        banner_deatils_data = []
        for brand in brand_details:
            try:
                ##### get products count for brand #####
                if s_id != "" and s_id != "0":
                    product_count = DbHelper.get_main_product_count(
                        {"brand": str(brand['_id']), "status": 1, "storeId": ObjectId(s_id)})
                else:
                    product_count = DbHelper.get_main_product_count({"brand": str(brand['_id']), "status": 1})
                if s_id != "" and s_id != "0":
                    if product_count != 0:
                        brand_data.append({
                            "id": str(brand['_id']),
                            "name": brand['name'][lan],
                            "bannerImage": brand['bannerImage'],
                            "imageWeb": brand['websiteBannerImage'],
                            "logo": brand['logoImage'],
                            "productCount": product_count
                        })
                    else:
                        pass
                else:
                    brand_data.append({
                        "id": str(brand['_id']),
                        "name": brand['name'][lan],
                        "bannerImage": brand['bannerImage'],
                        "imageWeb": brand['websiteBannerImage'],
                        "logo": brand['logoImage'],
                        "productCount": product_count
                    })
                banner_deatils_data.append(
                    {
                        "type": 2,
                        "bannerTypeMsg": "brands",
                        "name": brand['name'][lan],
                        "imageWeb": brand['websiteBannerImage'],
                        "logo": brand['logoImage'],
                    }
                )
            except:
                pass

        ##### drop duplicates and return data #####
        df = pd.DataFrame(brand_data)
        df = df.drop_duplicates(subset="name", keep="last")
        df1 = pd.DataFrame(banner_deatils_data)
        df1 = df.drop_duplicates(subset="name", keep="last")
        response = {
            "message": "data found",
            "penCount": brand_count,
            "data": df.to_dict(orient="records"),
            "bannerData": df1.to_dict(orient="records")
        }
        return ResponseHelper.get_status_200(response)

    def process_symptoms_get_api(self, search_text, from_page, to_page, lan):
        """
        This method process symptoms get api and return data
        :return:
        """
        ##### create query and get data from mongodb #####
        symptoms_query = {"status": 1}
        if search_text != "":
            symptoms_query['name.en'] = {'$regex': (search_text).replace("%20", " "), '$options': 'i'}
        sort_query = [("_id", -1)]
        symptom_details = DbHelper.get_symptoms(symptoms_query, from_data=from_page, to_data=to_page)
        symptom_count = DbHelper.get_symptoms_count(symptoms_query)

        ##### processing data #####
        symptom_data = []
        for sym in symptom_details:

            try:
                symptuery = {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "storeCategoryId": PHARMACY_STORE_CATEGORY_ID
                                    }
                                },
                                {
                                    "match_phrase_prefix": {
                                        "symptoms.symptomId": str(sym['_id'])
                                    }
                                },
                                {
                                    "match": {
                                        "status": 1
                                    }
                                }
                            ]
                        }
                    }
                }

                res_varient_parameters = es.search(
                    index=index_central_product,
                    # doc_type=doc_central_product,
                    body=symptuery,
                    filter_path=[
                        "hits.total",
                        "hits.hits._id",
                        "hits.hits._source.pPName",
                    ],
                )
                try:
                    product_count = res_varient_parameters['hits']['total']['value']
                except:
                    product_count = 0
                # product_count = db.products.find(
                #     {"symptoms.symptomId": str(sym['_id']), "status": 1}).count()
                symptom_data.append({
                    "id": str(sym['_id']),
                    "name": sym['name'][lan],
                    "webImage": sym['webImage'],
                    "mobileImage": sym['mobileImage'],
                    "logo": sym['mobileImage'],
                    "productCount": product_count
                })
            except:
                pass

        df = pd.DataFrame(symptom_data)
        df = df.drop_duplicates(subset="name", keep="last")
        response = {
            "message": "data found",
            "penCount": symptom_count,
            "data": df.to_dict(orient="records"),
        }
        return ResponseHelper.get_status_200(response)

    def process_get_cuisines_api(self, store_category_id, search_text, zone_id, language, from_data, to_data,user_id):
        """
        This api gets cuisines data and return response
        :return:
        """
        store_data_details = []

        ##### compile query and get data from database #####
        query = {'catID': str(store_category_id)}
        if search_text != "":
            query['specialityName.en'] = {'$regex': '^'+search_text.replace("%20", " "), '$options': 'i'}

        print("zone_id",zone_id,type(zone_id))
        print("userid",user_id)
        zone_ids = [ids for n in zone_id.split() for ids in n.split(",")]
        pipeline = [
                {"$match": {
                    "serviceZones.zoneId": {"$in": zone_ids},
                    "status": 1,
                    "categoryId": store_category_id
                }},
                {"$unwind": "$specialities"},
                {"$group": {"_id": "$specialities"}},
                {"$facet": {
                    "totalCount": [{"$count": "total"}],
                    "limitedSpecialityIDs": [
                        {"$group": {
                            "_id": None,
                            "speciality_ids": {"$addToSet": {"$convert": {"input": "$_id", "to": "objectId"}}}
                        }},
                        {'$sort': {'_id': -1}}
                    ]
                }}
            ]
        if zone_id == "0":
            del pipeline[0]["$match"]["serviceZones.zoneId"]
        distinct_specialities_result = list(db.stores.aggregate(pipeline))

        # Extracting the result
        if distinct_specialities_result:
            speciality_ids = distinct_specialities_result[0]['limitedSpecialityIDs'][0]['speciality_ids']
            query["_id"] = {"$in": speciality_ids}
            print("query1",query)
            store_specility_data = db.specialities.find(query).sort([("specialityName", 1)]).skip(from_data).limit(to_data)
            if search_text != "":
                store_data_count = DbHelper.get_specialities_count(query)
            else:
                store_data_count = DbHelper.get_specialities_count(query)
            user_selected_specialities = db.customer.find_one({"userId": str(user_id)}, {"specialities": 1})
            if user_selected_specialities and 'specialities' in user_selected_specialities:
                selected_speciality_ids = set(user_selected_specialities['specialities'])
            else:
                # Handle the case when 'specialities' field is not found or document not found
                selected_speciality_ids = set()
            for spe in store_specility_data:
                try:
                    spe_id = str(spe['_id'])
                    is_selected = spe_id in selected_speciality_ids            
                    store_data_details.append({"id": str(spe['_id']),
                                            "selected":is_selected,
                                            "image": spe.get('image', ""),
                                            "name": spe.get('specialityName', {}).get(language, spe['specialityName'].get("en", ""))
                                            })
                except:
                    traceback.print_exc()
        if len(store_data_details) > 0:
            dataframe = pd.DataFrame(store_data_details)
            dataframe = dataframe.drop_duplicates(subset="name", keep="last")
            data_list = dataframe.to_dict(orient="records")
            response = {
                "data": data_list,
                "penCount": store_data_count,
                "message": "data found"
            }
            return ResponseHelper.get_status_200(response)
        else:
            response = {
                "data": [],
                "message": "data not found"
            }
            return ResponseHelper.get_status_404(response)

    def process_get_services_api(self, token, language, lat, long, search, timezone, request_from, user_id):
        """"
        This method gets details from databases and returns final response
        """
        ##### ---------------------- get cities from database -------------------- #####
        if search == "":
            ##### query based on lat long of user
            condition = {
                "isDeleted": False,
                "polygons": {
                    "$geoIntersects": {
                        "$geometry": {
                            "type": "Point",
                            "coordinates": [float(long), float(lat)]
                        }
                    }
                }
            }
        else:
            ##### if any search keyword on category name then query based on search and lat long #####
            condition = {
                "storeCategory.storeCategoryName.en": {'$regex': (search).replace("%20", " "), '$options': 'i'},
                "isDeleted": False,
                "polygons": {
                    "$geoIntersects": {
                        "$geometry": {
                            "type": "Point",
                            "coordinates": [float(long), float(lat)]
                        }
                    }
                }
            }
        zone_details = DbHelper.get_city_data(condition)

        ##### -------------------- get zone data from database ------------------ #####
        zone_condition = {
            "status": 1,
            "polygons": {
                "$geoIntersects": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [float(long), float(lat)]
                    }
                }
            }
        }
        zone_data = DbHelper.get_zone_data(zone_condition)

        ##### ------------------- processing database results --------------------- #####
        store_category_details = []
        store_category_wise_details = []
        hyper_local_data = []

        if zone_details is not None:
            ##### if storeCategory array in city details #####
            if "storeCategory" in zone_details:
                ##### for each store category in cities data, process data #####
                for cat in zone_details['storeCategory']:
                    if search == "":
                        ##### get store category data based on id of store category #####
                        details = DbHelper.get_store_category({"_id": ObjectId(cat['storeCategoryId'])})

                        ##### get list of hyperlocal store category #####
                        hyperlocal = cat['hyperlocal'] if "hyperlocal" in cat else False
                        storeListing = cat['storeListing'] if "storeListing" in cat else 0
                        if hyperlocal == True and storeListing == int(1):
                            hyper_local_data.append(str(cat['storeCategoryId']))
                        else:
                            pass

                        ##### prepare response for store categories #####
                        if details is not None:
                            store_category_json = {
                                "_id": str(cat['storeCategoryId']),
                                "categoryName": cat['storeCategoryName']['en'] if "en" in cat[
                                    'storeCategoryName'] else "",
                                "description": details['storeCategoryDescription']['en'] if "en" in details[
                                    'storeCategoryDescription'] else "",
                                "featured": cat['featured'] if "featured" in cat else False,
                                "bannerImage": cat['bannerImage'] if "bannerImage" in cat else details['bannerImage'],
                                "mobileGIF": cat['mobileGIF'] if "mobileGIF" in cat else "",
                                "hyperlocal": cat['hyperlocal'] if "hyperlocal" in cat else False,
                                "linkFromId": cat['linkFromId'] if "linkFromId" in cat else 0,
                                "extraASAPDeliveryFee": cat[
                                    'extraASAPDeliveryFee'] if "extraASAPDeliveryFee" in cat else 0,
                                "storeListing": storeListing,
                                "deliveryTime": int(cat['deliveryTime']) if "deliveryTime" in cat else 0,
                                "nowBooking": int(cat['nowBooking']) if "nowBooking" in cat else 0,
                                "scheduleBooking": int(cat['scheduleBooking']) if "scheduleBooking" in cat else 0,
                                "deliverTo": details['deliverTo'] if "deliverTo" in details else 0,
                                "shiftSelection": int(cat['shiftSelection']) if "shiftSelection" in cat else 0,
                                "logoImage": cat['logoImage'] if "logoImage" in cat else details['logoImage'],
                                "iconlogoimg": cat['iconlogoimg'] if "iconlogoimg" in cat else details['iconlogoimg'],
                                "type": details['type'] if "type" in details else 1,
                                "typeMsg": details['typeMsg'] if "typeMsg" in details else "",
                                "colorCode": details['colorCode'] if "colorCode" in details else "29297b"
                            }
                        else:
                            ##### if store category not found then prepare response without few keys #####
                            store_category_json = {
                                "_id": str(cat['storeCategoryId']),
                                "categoryName": cat['storeCategoryName']['en'] if "en" in cat[
                                    'storeCategoryName'] else "",
                                "description": "",
                                "featured": cat['featured'] if "featured" in cat else False,
                                "bannerImage": cat['bannerImage'] if "bannerImage" in cat else details['bannerImage'],
                                "mobileGIF": cat['mobileGIF'] if "mobileGIF" in cat else "",
                                "hyperlocal": cat['hyperlocal'] if "hyperlocal" in cat else False,
                                "linkFromId": cat['linkFromId'] if "linkFromId" in cat else 0,
                                "extraASAPDeliveryFee": cat[
                                    'extraASAPDeliveryFee'] if "extraASAPDeliveryFee" in cat else 0,
                                "storeListing": storeListing,
                                "deliveryTime": int(cat['deliveryTime']) if "deliveryTime" in cat else 0,
                                "nowBooking": int(cat['nowBooking']) if "nowBooking" in cat else 0,
                                "scheduleBooking": int(cat['scheduleBooking']) if "scheduleBooking" in cat else 0,
                                "deliverTo": 0,
                                "shiftSelection": int(cat['shiftSelection']) if "shiftSelection" in cat else 0,
                                "logoImage": cat['logoImage'] if "logoImage" in cat else details['logoImage'],
                                "iconlogoimg": cat['iconlogoimg'] if "iconlogoimg" in cat else details['iconlogoimg'],
                                "type": 1,
                                "typeMsg": "",
                                "colorCode": "29297b"
                            }

                        ##### append above response into list ####
                        if "activeMobile" in cat and "activeWeb" in cat:
                            if request_from == 1 and cat['activeWeb'] == True:
                                store_category_details.append(store_category_json)
                            elif request_from == 2 and cat['activeMobile'] == True:
                                store_category_details.append(store_category_json)
                            elif request_from == 0:
                                store_category_details.append(store_category_json)
                            else:
                                pass
                        else:
                            store_category_details.append(store_category_json)

                    ##### if user has given any search keyword for store category #####
                    else:
                        if search.lower() in cat['storeCategoryName']['en'].lower():
                            ##### fetch store category from database #####
                            details = DbHelper.get_store_category({"_id": ObjectId(cat['storeCategoryId'])})

                            ##### get list of hyperlocal store category #####
                            hyperlocal = cat['hyperlocal'] if "hyperlocal" in cat else False
                            storeListing = cat['storeListing'] if "storeListing" in cat else 0
                            if hyperlocal == True and storeListing == int(1):
                                hyper_local_data.append(str(cat['storeCategoryId']))
                            else:
                                pass

                            ##### if details fount then compile response #####
                            if details is not None:
                                store_category_details.append({
                                    "_id": str(cat['storeCategoryId']),
                                    "categoryName": cat['storeCategoryName']['en'] if "en" in details[
                                        'storeCategoryName'] else "",
                                    "description": details['storeCategoryDescription']['en'] if "en" in details[
                                        'storeCategoryDescription'] else "",
                                    "featured": cat['featured'] if "featured" in cat else False,
                                    "bannerImage": cat['bannerImage'] if "bannerImage" in cat else details[
                                        'bannerImage'],
                                    "mobileGIF": cat['mobileGIF'] if "mobileGIF" in cat else "",
                                    "hyperlocal": cat['hyperlocal'] if "hyperlocal" in cat else False,
                                    "linkFromId": cat['linkFromId'] if "linkFromId" in cat else 0,
                                    "extraASAPDeliveryFee": cat[
                                        'extraASAPDeliveryFee'] if "extraASAPDeliveryFee" in cat else 0,
                                    "storeListing": storeListing,
                                    "deliveryTime": int(cat['deliveryTime']) if "deliveryTime" in cat else 0,
                                    "nowBooking": int(cat['nowBooking']) if "nowBooking" in cat else 0,
                                    "scheduleBooking": int(cat['scheduleBooking']) if "scheduleBooking" in cat else 0,
                                    "deliverTo": int(details['deliverTo']) if "deliverTo" in details else 0,
                                    "shiftSelection": cat['shiftSelection'] if "shiftSelection" in cat else 0,
                                    "logoImage": cat['logoImage'] if "logoImage" in cat else details['logoImage'],
                                    "iconlogoimg": cat['iconlogoimg'] if "iconlogoimg" in cat else details[
                                        'iconlogoimg'],
                                    "type": details['type'] if "type" in details else 1,
                                    "typeMsg": details['typeMsg'] if "typeMsg" in details else "",
                                    "colorCode": details['colorCode'] if "colorCode" in details else "29297b"
                                })
                            else:
                                pass
            else:
                ##### if storeCategory array not in city details #####
                store_category_details = []

            ##### ------------------------ fetching data for stores for every store category ----------------- #####
            if len(hyper_local_data) > 0 and zone_data is not None:
                for cat_id in hyper_local_data:
                    seller_data = []
                    all_store_details = []

                    ##### query on elastic search for stores under storeCategory and near to user location #####
                    store_query = {
                        "sort": [
                            {
                                "storeIsOpen": {
                                    "order": "desc"
                                }
                            },
                            {
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
                        ],
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "match_phrase_prefix": {
                                            "categoryId": cat_id
                                        }
                                    },
                                    {
                                        "match": {
                                            "status": 1
                                        }
                                    },
                                    {
                                        "match": {
                                            "serviceZones.zoneId": str(zone_data['_id'])
                                        }
                                    },
                                    {
                                        "geo_distance": {
                                            "distance": "50km",
                                            "location": {
                                                "lat": float(lat),
                                                "lon": float(long),
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        "size": 500,
                        "from": 0
                    }
                    filter_path_keys = [
                        "hits.hits._id",
                        "hits.hits.sort",
                        "hits.hits._source.storeName",
                        "hits.hits._source.parentSellerIdOrSupplierId",
                        "hits.hits._source.uniqStoreId"
                    ]
                    res_store = DbHelper.get_stores_from_elastic(store_query, index_store, filter_path_keys)

                    ##### if query returns result then store in list #####
                    if len(res_store) > 0:
                        for r_s in res_store['hits']['hits']:
                            distance_km = round(r_s['sort'][1], 2)
                            distance_miles = round(distance_km * conv_fac, 2)
                            all_store_details.append({
                                "id": str(r_s['_id']),
                                "distance": distance_km,
                                "storeName": r_s['_source']['storeName']['en'],
                                "parentSellerIdOrSupplierId": r_s['_source']['parentSellerIdOrSupplierId'],
                                "uniqStoreId": r_s['_source']['uniqStoreId'] if "uniqStoreId" in r_s[
                                    '_source'] else "0",
                            })
                    # unique_stores = []
                    unique_stores = sorted(all_store_details, key=lambda k: k['distance'], reverse=False)

                    ##### keep only one store which is nearest to user if we found multiple stores of same brand #####
                    if len(unique_stores) > 0:
                        dataframe_details = pd.DataFrame(unique_stores)
                        dataframe_details = dataframe_details.drop_duplicates("uniqStoreId", keep="first")
                        spec_data = dataframe_details.to_json(orient='records')
                        spec_data = json.loads(spec_data)
                    else:
                        spec_data = []

                    final_stores = []
                    # for store in store_non_unique:
                    for store in spec_data:
                        final_stores.append(store['id'])

                    ##### query for elastic search for stores after excluding duplicate stores #####
                    query = {
                        "sort": [
                            {
                                "storeIsOpen": {
                                    "order": "desc"
                                }
                            },
                            {
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
                        ],
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "terms": {
                                            "_id": final_stores
                                        }
                                    },
                                    {
                                        "match": {
                                            "status": 1
                                        }
                                    },
                                    {
                                        "match": {
                                            "serviceZones.zoneId": str(zone_data['_id'])
                                        }
                                    }
                                ]
                            }
                        },
                        "size": 6,
                        "from": 0
                    }
                    filter_path = [
                        "hits.total",
                        "hits.hits._id",
                        "hits.hits.sort",
                        "hits.hits._source"
                    ]
                    res = DbHelper.get_stores_from_elastic(query, index_store, filter_path)

                    ##### getting required data from elastic query result #####
                    if len(res) > 0:
                        if "hits" in res:
                            if "hits" in res['hits']:
                                try:
                                    for seller in res['hits']['hits']:
                                        ##### for each store prepare response and add to stpre_data list #####
                                        try:
                                            store_category_id = cat_id
                                            doc_count = res['hits']['total']['value']
                                            try:
                                                store_is_open = seller["_source"]['storeIsOpen']
                                            except:
                                                store_is_open = False

                                            try:
                                                next_open_time = int(seller["_source"]['nextOpenTime']) + timezone * 60
                                            except:
                                                next_open_time = ""
                                            try:
                                                next_close_time = int(
                                                    seller["_source"]['nextCloseTime']) + timezone * 60
                                            except:
                                                next_close_time = ""

                                            if next_close_time == "" and next_open_time == "":
                                                is_temp_close = True
                                            elif next_open_time != "" and store_is_open == False:
                                                is_temp_close = False
                                            else:
                                                is_temp_close = False

                                            ##### append collected data to store_data list #####
                                            seller_data.append(
                                                {
                                                    "_id": str(seller['_id']),
                                                    "logoImages": seller["_source"]['logoImages'],
                                                    "nextCloseTime": next_close_time,
                                                    "isTempClose": is_temp_close,
                                                    "nextOpenTime": next_open_time,
                                                    "currencyCode": seller["_source"][
                                                        'currencyCode'] if "currencyCode" in seller["_source"] else "₹",
                                                    "currencySymbol": seller["_source"][
                                                        'currencyCode'] if "currencyCode" in seller["_source"] else "₹",
                                                    "currency": seller["_source"]['currencyCode'] if "currencyCode" in
                                                                                                     seller[
                                                                                                         "_source"] else "INR",
                                                    "averageDeliveryTime": str(seller["_source"][
                                                                                   'averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in
                                                                                                                                   seller[
                                                                                                                                       "_source"] else "",
                                                    "bannerImages": seller["_source"]['bannerImages'],
                                                    "galleryImages": seller["_source"]['galleryImages'],
                                                    "storeName": seller["_source"]['storeName'][language] if language in
                                                                                                             seller[
                                                                                                                 "_source"][
                                                                                                                 'storeName'] else
                                                    seller["_source"]['storeName']['en'],
                                                    "driverTypeId": seller["_source"][
                                                        'driverTypeId'] if "driverTypeId" in seller["_source"] else 0,
                                                    "driverType": seller["_source"]['driverType'] if "driverType" in
                                                                                                     seller[
                                                                                                         "_source"] else 0,
                                                    "storeType": seller["_source"]['storeType'] if "storeType" in
                                                                                                   seller[
                                                                                                       "_source"] else 0,
                                                    "storeIsOpen": seller["_source"]['storeIsOpen'] if "storeIsOpen" in
                                                                                                       seller[
                                                                                                           "_source"] else False,
                                                }
                                            )

                                            ##### compile store category data for response #####
                                            category_details = [sub_cat_id for sub_cat_id in store_category_details if
                                                                store_category_id == sub_cat_id['_id']]
                                            if len(category_details) > 0:
                                                category_name = category_details[0]['categoryName']
                                                description = category_details[0]['description']
                                                type = category_details[0]['type']
                                                typeMsg = category_details[0]['typeMsg']
                                            else:
                                                category_name = ""
                                        except:
                                            pass

                                    ##### compile final response of store category details and all stores under category
                                    if category_name != "":
                                        store_category_wise_details.append({
                                            "categoryId": store_category_id,
                                            "penCount": doc_count,
                                            "description": description,
                                            "type": type,
                                            "typeMsg": typeMsg,
                                            "categoryName": category_name,
                                            "storeData": seller_data
                                        })
                                except:
                                    pass
                            else:
                                pass
                        else:
                            pass

            ##### sort store categories to show featured categories first #####
            if len(store_category_details) > 0:
                new_product_list = sorted(store_category_details, key=lambda k: k['featured'], reverse=True)
            else:
                new_product_list = []

            ##### compile final response for all store categories to show under categories section on web page #####
            if len(new_product_list) > 0:
                zone_data = {
                    "city": zone_details['cityName'],
                    "cityId": str(zone_details['cityId']),
                    "countryId": zone_details['countryId'],
                    "currency": zone_details['currency'],
                    "state": zone_details['state'],
                    "currencySymbol": zone_details['currencySymbol'],
                    "title": zone_details['cityName'],
                    "storeCategory": new_product_list,
                    "laundry": zone_details['laundry'] if "laundry" in zone_details else {},
                    "_id": str(zone_details['_id']),
                }

                ##### get unseen notification_count for user #####
                notification_count_query = {"app_name": APP_NAME, "userid": user_id, "isSeen": False}
                notification_count = DbHelper.get_notification_count(notification_count_query)

                ##### compiling final response #####
                response = {
                    "data": zone_data,
                    "storeData": store_category_wise_details,
                    "notificationCount": notification_count,
                    "message": "In operational city."
                }

                return ResponseHelper.get_status_200(response)
            else:
                response = {
                    "message": "data not found"
                }
                return ResponseHelper.get_status_404(response)
        else:
            response = {
                "message": "data not found"
            }
            return ResponseHelper.get_status_404(response)

    def favourite_store_user(self, category_id, user_id, zone_id, user_latitude, user_longtitude, language, skip_data,
                             limit_data, sort_data, cuisines_data, avg_cost_max, avg_cost_min, timezone,
                             min_ratings_data, max_ratings_data, order_type,hygiene_rating_data,dietary_preferences,
                             food_Prefereces,allergen,creator_product_type,delivery_filter_data):
        """
        function for the get the favourite stores for the user
            1 Popularity, 2 for rating: high to low, 3 cost: low to high, 4 for cost: high to low
        :param self:
        :param category_id:
        :param user_id:
        :param zone_id:
        :param user_latitude:
        :param user_longtitude:
        :param language:
        :param skip_data:
        :param limit_data:
        :param sort_data:
        :param cuisines_data:
        :param avg_cost_max:
        :param avg_cost_min:
        :param timezone:
        :param min_ratings_data:
        :param max_ratings_data:
        :param order_type:
        :param hygiene_rating_data:
        :param dietary_preferences:
        :param food_Prefereces:
        :return:
        """
        is_rating_sort = False
        must_not_query = []
        translator = t(to_lang=language)
        # must_not_query.append({"terms": {"storeFrontTypeId": [2]}})
        start_fq_time = time.time()
        store_query, must_query, range_query, sort_query = self.get_favourite_store_query(category_id, user_id, zone_id,
                                                                                          user_latitude,
                                                                                          user_longtitude, language,
                                                                                          skip_data,
                                                                                          limit_data, sort_data,
                                                                                          cuisines_data, avg_cost_max,
                                                                                          avg_cost_min, timezone,
                                                                                          min_ratings_data,
                                                                                          max_ratings_data, order_type,hygiene_rating_data,dietary_preferences,food_Prefereces,
                                                                                          allergen,creator_product_type,
                                                                                          delivery_filter_data)

        ##### query on elastic search #####
        filter_path = [
            "hits.hits._id",
            "hits.hits.sort",
            "hits.hits._source.storeName",
            "hits.hits._source.uniqStoreId",
            "hits.hits._source.parentSellerIdOrSupplierId"
        ]
        print('set query :', time.time() - start_fq_time)
        print('query----',store_query)
        query_time = time.time()
        res_store = DbHelper.get_stores_from_elastic(store_query, index_store, filter_path)
        
        ##### ------------------- removing duplicate stoes and compiling query again ------------- #####
        all_store_details = []
        if len(res_store) > 0:
            for r_s in res_store['hits']['hits']:
                if int(sort_data) == 0 or int(sort_data) == 1:
                    distance_km = round(r_s['sort'][1], 2)
                else:
                    try:
                        distance_km = round(r_s['sort'][2], 2)
                    except:
                        try:
                            distance_km = round(r_s['sort'][1], 2)
                        except:
                            distance_km = round(r_s['sort'][0], 2)
                all_store_details.append({
                    "id": str(r_s['_id']),
                    "distance": distance_km,
                    "storeName": r_s['_source']['storeName']['en'],
                    "parentSellerIdOrSupplierId": r_s['_source']['parentSellerIdOrSupplierId'],
                    "uniqStoreId": r_s['_source']['uniqStoreId'] if "uniqStoreId" in r_s['_source'] else "",
                })

        # unique_stores = []
        unique_stores = sorted(all_store_details, key=lambda k: k['distance'], reverse=False)

        if len(unique_stores) > 0 and category_id == DINE_STORE_CATEGORY_ID:
            dataframe_details = pd.DataFrame(unique_stores)
            dataframe_details = dataframe_details.drop_duplicates("uniqStoreId", keep="first")
            spec_data = dataframe_details.to_json(orient='records')
            spec_data = json.loads(spec_data)
        elif len(unique_stores) > 0:
            dataframe_details = pd.DataFrame(unique_stores)
            spec_data = dataframe_details.to_json(orient='records')
            spec_data = json.loads(spec_data)
        else:
            spec_data = []
        final_stores = []
        # for store in store_non_unique:
        for store in spec_data:
            final_stores.append(store['id'])

        ##### query es again with new query #####
        if len(final_stores) > 0:
            must_query.append({"terms": {"_id": final_stores}})
        query = {
            "query":
                {
                    "bool":
                        {
                            "must": must_query,
                            "must_not": must_not_query,
                            "filter": range_query,
                        }
                },
            "size": int(limit_data),
            "from": int(skip_data),
            "sort": sort_query
        }
        filter_path = [
            "hits.total",
            "hits.hits._id",
            "hits.hits.sort",
            "hits.hits._source"
        ]
        res = DbHelper.get_stores_from_elastic(query, index_store, filter_path)
        print('query exicute time :', time.time()-query_time)
        ##### ----------------- processing fetched data ------------------- #####
        store_data_json = []
        close_data_json = []
        specialities_data = []
     
        fav_store_data_count = 0
        if res['hits']['total']['value'] > 0:
            fav_store_data_count = res['hits']['total']['value']
            if "hits" in res['hits']:
                all_fav_store = time.time()
                for seller in res['hits']['hits']:
                    cusine_name = ""
                    avg_rating_value = 0

                    ##### get seller review ratings #####
                    if "listingImageWeb" in seller["_source"]:
                        listing_image_web = seller["_source"]['listingImageWeb']
                    else:
                        if "listingImage" in seller["_source"]:
                            listing_image_web = {
                                "listingImageExtraLarge": seller["_source"]['listingImage']['listingImageweb'],
                                "listingImageLarge": seller["_source"]['listingImage']['listingImageweb'],
                                "listingImageMedium": seller["_source"]['listingImage']['listingImageweb'],
                                "listingImageSmall": seller["_source"]['listingImage']['listingImageweb']
                            }
                        else:
                            listing_image_web = {}

                    # avg_rating_value = 0
                    # seller_rating = db.sellerReviewRatings.aggregate(
                    #     [
                    #         {"$match": {
                    #             "sellerId": str(seller['_id']),
                    #             "rating": {"$ne": 0},
                    #             "status": 1}},
                    #         {
                    #             "$group":
                    #                 {
                    #                     "_id": "$sellerId",
                    #                     "avgRating": {"$avg": "$rating"}
                    #                 }
                    #         }
                    #     ]
                    # )
                    # for avg_rating in seller_rating:
                    #     avg_rating_value = avg_rating['avgRating']

                    # if int(sort_data) == 0 or int(sort_data) == 1:
                    #     distance_km = round(seller['sort'][1], 2)
                    #     distance_miles = round(distance_km * conv_fac, 2)
                    # else:
                    #     try:
                    #         distance_km = round(seller['sort'][2], 2)
                    #         distance_miles = round(distance_km * conv_fac, 2)
                    #     except:
                    #         try:
                    #             distance_km = round(seller['sort'][1], 2)
                    #             distance_miles = round(distance_km * conv_fac, 2)
                    #         except:
                    #             distance_km = round(seller['sort'][0], 2)
                    #             distance_miles = round(distance_km * conv_fac, 2)
                    try:
                        distance_km = round(seller['sort'][2], 2)
                    except IndexError:
                        try:
                            distance_km = round(seller['sort'][1], 2)
                        except IndexError:
                            distance_km = round(seller['sort'][0], 2)
                    distance_miles = round(distance_km * conv_fac, 2)


                    # if "averageCostForMealForTwo" in seller["_source"]:
                    #     cost_for_two = seller["_source"]['averageCostForMealForTwo']
                    # else:
                    #     cost_for_two = 0

                    ##### get seller offer data ######
                    offer_time = time.time()
                    offer_details = DbHelper.get_offer_details(str(seller['_id']))
                    offer_json = []
                    for offer in offer_details:
                        offer_json.append({
                            "offerName": offer['name'][language],
                            "offerId": str(offer['_id']),
                            "offerType": offer['offerType'],
                            "discountValue": offer['discountValue']
                        })
                    if len(offer_json) > 0:
                        best_offer_store = max(offer_json, key=lambda x: x['discountValue'])
                        if best_offer_store['offerType'] == 1:
                            percentage_text = str(best_offer_store['discountValue']) + "%" + " " + "off"
                        else:
                            try:
                                percentage_text = seller['_source']['currencySymbol'] + str(best_offer_store['discountValue']) + " off"
                            except:
                                percentage_text = "₹" + str(best_offer_store['discountValue']) + " off"
                        offer_name = best_offer_store['offerName']
                    else:
                        offer_name = ""
                        percentage_text = ""
                    # if offer_json:
                    #     best_offer_store = max(offer_json, key=lambda x: x['discountValue'])
                    #
                    #     discount_value = best_offer_store['discountValue']
                    #     offer_type = best_offer_store['offerType']
                    #
                    #     if offer_type == 1:
                    #         percentage_text = f"{discount_value}% off"
                    #     else:
                    #         currency_symbol = seller.get('_source', {}).get('currencySymbol', '₹')
                    #         percentage_text = f"{currency_symbol} {discount_value} off"
                    #
                    #     offer_name = best_offer_store['offerName']
                    # else:
                    #     offer_name = ""
                    #     percentage_text = ""


                    # address = seller["_source"]['businessLocationAddress']['address'] if "address" in seller["_source"][
                    #     'businessLocationAddress'] else ""
                    # addressArea = seller["_source"]['businessLocationAddress']['addressArea'] if "addressArea" in \
                    #                                                                              seller["_source"][
                    #                                                                                  'businessLocationAddress'] else ""
                    # locality = seller["_source"]['businessLocationAddress']['locality'] if "locality" in \
                    #                                                                        seller["_source"][
                    #                                                                            'businessLocationAddress'] else ""
                    # post_code = seller["_source"]['businessLocationAddress']['postCode'] if "postCode" in \
                    #                                                                         seller["_source"][
                    #                                                                             'businessLocationAddress'] else ""
                    # state = seller["_source"]['businessLocationAddress']['state'] if "state" in seller["_source"][
                    #     'businessLocationAddress'] else ""
                    # country = seller["_source"]['businessLocationAddress']['country'] if "country" in seller["_source"][
                    #     'businessLocationAddress'] else ""
                    # city = seller["_source"]['businessLocationAddress']['city'] if "city" in seller["_source"][
                    #     'businessLocationAddress'] else ""

                    # ===================================for the cusines=============================================
                    # if "specialities" in seller["_source"]:
                    #     if len(seller["_source"]['specialities']):
                    #         for spec in seller["_source"]['specialities']:
                    #             spec_data = DbHelper.get_single_speciality({"_id": ObjectId(spec)},
                    #                                                        {"specialityName": 1, "image": 1})
                    #             if spec_data != None:
                    #                 specialities_data.append({
                    #                     "id": str(spec),
                    #                     "image": spec_data['image'] if "image" in spec_data else "",
                    #                     "name": spec_data['specialityName'][language] if language in spec_data[
                    #                         'specialityName'] else spec_data['specialityName']["en"],
                    #                 })
                    #                 if cusine_name == "":
                    #                     cusine_name = spec_data['specialityName'][language] if language in spec_data[
                    #                         'specialityName'] else spec_data['specialityName']["en"]
                    #                 else:
                    #                     cusine_name = cusine_name + ", " + spec_data['specialityName'][
                    #                         language] if language in spec_data['specialityName'] else \
                    #                         spec_data['specialityName']["en"]
                    #             else:
                    #                 pass
                    #     else:
                    #         pass
                    # else:
                    #     pass
                    if "specialities" in seller["_source"]:
                            for spec in seller["_source"].get("specialities", []):
                                spec_data = DbHelper.get_single_speciality(
                                    {"_id": ObjectId(spec)},
                                    {"specialityName." + language: 1, "image": 1}
                                )
                                
                                if spec_data:
                                    specialities_data.append({
                                        "id": str(spec),
                                        "image": spec_data.get("image", ""),
                                        "name": spec_data['specialityName'].get(language, spec_data['specialityName'].get("en", "")),
                                    })
                                    
                                    if not cusine_name:
                                        cusine_name = spec_data['specialityName'].get(language, spec_data['specialityName'].get("en", ""))
                                    else:
                                        cusine_name += ", " + spec_data['specialityName'].get(language, spec_data['specialityName'].get("en", ""))

                    if "storeIsOpen" in seller["_source"]:
                        store_is_open = seller["_source"]['storeIsOpen']
                    else:
                        store_is_open = False

                    if "nextCloseTime" in seller["_source"]:
                        next_close_time = seller["_source"]['nextCloseTime']
                    else:
                        next_close_time = ""

                    if "nextOpenTime" in seller["_source"]:
                        next_open_time = seller["_source"]['nextOpenTime']
                    else:
                        next_open_time = ""

                    if "timeZoneWorkingHour" in seller["_source"]:
                        timeZoneWorkingHour = seller["_source"]['timeZoneWorkingHour']
                    else:
                        timeZoneWorkingHour = ""

                    if next_close_time == "" and next_open_time == "":
                        is_temp_close = True
                        store_tag = language_change("Temporarily Closed", language)
                    elif next_open_time != "" and store_is_open == False:
                        is_temp_close = False
                        # next_open_time = int(next_open_time + timezone * 60)
                        next_open_time = time_zone_converter(timezone, next_open_time, timeZoneWorkingHour)
                        local_time = datetime.datetime.fromtimestamp(next_open_time)
                        next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                        next_day_midnight = next_day.replace(hour=0, minute=0, second=0)
                        next_day_midnight_timestamp = int(next_day_midnight.timestamp())
                        if next_day_midnight_timestamp > next_open_time:
                            open_time = local_time.strftime("%I:%M %p")
                            check_language_tag = language_change("Opens Next At", language)
                            store_tag = check_language_tag + " " + str(open_time)
                        else:
                            open_time = local_time.strftime("%I:%M %p")
                            check_language_tag = language_change("Opens Tomorrow At", language)
                            store_tag = check_language_tag + " " + str(open_time)
                    else:
                        is_temp_close = False
                        store_tag = ""

                    # # =======================================safetyStandards=============================================
                    # if "safetyStandards" in seller["_source"]:
                    #     if int(seller["_source"]['safetyStandards']) == 0:
                    #         safety_standards = False
                    #     else:
                    #         safety_standards = True
                    # else:
                    #     safety_standards = False

                    # =======================================safetyStandards description================================
                    # if "safetyStandardsDynamicContent" in seller["_source"]:
                    #     if seller["_source"]['safetyStandardsDynamicContent'] == "":
                    #         safety_standards_dynamic_content = seller["_source"]['safetyStandardsDynamicContent']
                    #     else:
                    #         safety_standards_dynamic_content = ""
                    # else:
                    #     safety_standards_dynamic_content = ""

                    # if "shopifyStoreDetails" in seller['_source']:
                    #     if "enable" in seller["_source"]["shopifyStoreDetails"]:
                    #         shopify_enable = seller["_source"]["shopifyStoreDetails"]['enable']
                    #     else:
                    #         shopify_enable = False
                    # else:
                    #     shopify_enable = False

                    # if "shopifyId" in seller['_source']:
                    #     shopify_id = seller['_source']['shopifyId']
                    # else:
                    #     shopify_id = ""

                    # if "favouriteUsers" in seller["_source"]:
                    #     if user_id in seller["_source"]['favouriteUsers']:
                    #         favourite_store = True
                    #     else:
                    #         favourite_store = False
                    # else:
                    #     favourite_store = False
                    avg_rating_value = 0
                    seller_rating = db.sellerReviewRatings.aggregate(
                            [
                                {
                                    "$match": {
                                        "sellerId": str(seller["_id"]),
                                        "status": 1,
                                        "rating": {"$ne": 0},
                                    }
                                },
                                {"$group": {"_id": "$sellerId", "avgRating": {"$avg": "$rating"}}},
                            ]
                        )
                    for avg_rating in seller_rating:
                        avg_rating_value = avg_rating["avgRating"]
                    if float(min_ratings_data) != 0.0 and float(max_ratings_data) != 0.0:
                        if round(avg_rating_value, 2) >= float(min_ratings_data) and round(avg_rating_value,
                                                                                           2) <= float(
                            max_ratings_data):
                            is_rating_sort = False
                        else:
                            is_rating_sort = True
                    elif float(max_ratings_data) != 0.0:
                        if round(avg_rating_value, 2) <= float(max_ratings_data):
                            is_rating_sort = False
                        else:
                            is_rating_sort = True
                    else:
                        pass
                    try:
                        store_user_count = seller["_source"]['favouriteUsers']
                        fav_store_user_count = len(store_user_count)
                    except Exception as e:
                        print(e)
                        fav_store_user_count = 0
                    if store_is_open == True and is_rating_sort == False:
                        store_data_json.append({
                            "safetyStandardsDynamicContent": seller["_source"]['safetyStandardsDynamicContent'] if "safetyStandardsDynamicContent" in seller["_source"] and seller["_source"]['safetyStandardsDynamicContent'] != "" else "",
                                "_id": str(seller['_id']),
                                "avgRating": avg_rating_value,
                                "averageCostForMealForTwo": seller['_source']['averageCostForMealForTwo'] if "averageCostForMealForTwo" in seller["_source"] else 0,
                                "isFavourite": True if "favouriteUsers" in seller["_source"] and user_id in seller["_source"]['favouriteUsers'] else False,
                                "shopifyId": seller['_source']['shopifyId'] if 'shopifyId' in seller['_source'] else "",
                                "shopifyEnable":  seller["_source"]["shopifyStoreDetails"]['enable'] if "shopifyStoreDetails" in seller['_source'] and  "enable" in seller["_source"]["shopifyStoreDetails"] else False,
                                "isTempClose": is_temp_close,
                                "currencyCode": seller["_source"]['currencyCode'] if "currencyCode" in seller[
                                    "_source"] else "INR",
                                "businessLocationAddress": seller["_source"]['businessLocationAddress'],
                                "billingAddress": seller["_source"]['billingAddress'] if "billingAddress" in seller[
                                    "_source"] else {},
                                "headOffice": seller["_source"]['headOffice'] if "headOffice" in seller["_source"] else {},
                                "logoImages": seller["_source"]['logoImages'],
                                "listingImage": seller["_source"]['listingImage'],
                                "listingImageWeb": listing_image_web,
                                "distanceKm": round(distance_km, 2),
                                "freeDeliveryAbove": seller["_source"]['freeDeliveryAbove'] if "freeDeliveryAbove" in seller[
                                    "_source"] else 0,
                                "currencySymbol": seller["_source"]['currencySymbol'] if "currencySymbol" in seller[
                                    "_source"] else "₹",
                                "currency": seller["_source"]['currencySymbol'] if "currencySymbol" in seller["_source"] else "₹",
                                "priceForBookingTable": seller["_source"]['priceForBookingTable'] if "priceForBookingTable" in seller["_source"] else 0,
                                "tableReservations": seller["_source"]['tableReservations'] if "tableReservations" in seller["_source"] else False,
                                "storeTag": store_tag,
                                "offerName": offer_name,
                                "safetyStandards": True if 'safetyStandards' in seller["_source"] and seller["_source"]['safetyStandards'] and seller["_source"]['safetyStandards'] != "" and seller["_source"]['safetyStandards'] == 1 else False,
                                "cuisines": cusine_name,
                                "address": seller["_source"]['businessLocationAddress']['address'] if "address" in seller["_source"]['businessLocationAddress'] else "",
                                "locality": seller["_source"]['businessLocationAddress']['locality'] if "locality" in seller["_source"]['businessLocationAddress'] else "",
                                "postCode": seller["_source"]['businessLocationAddress']['postCode'] if "postCode" in seller["_source"]['businessLocationAddress'] else "",
                                "addressArea": seller["_source"]['businessLocationAddress']['addressArea'] if "addressArea" in seller["_source"]['businessLocationAddress'] else "",
                                "state": seller["_source"]['businessLocationAddress']['state'] if "state" in seller["_source"]['businessLocationAddress'] else "",
                                "country": seller["_source"]['businessLocationAddress']['country'] if "country" in seller["_source"]['businessLocationAddress'] else "",
                                "city": seller["_source"]['businessLocationAddress']['city'] if "city" in seller["_source"]['businessLocationAddress'] else "",
                                "paymentMethods": {
                                    "cashOnPickUp": seller["_source"]['cashOnPickUp'] if "cashOnPickUp" in seller[
                                        "_source"] else False,
                                    "pickUpPrePaymentCard": seller["_source"][
                                        'pickUpPrePaymentCard'] if "pickUpPrePaymentCard" in seller["_source"] else False,
                                    "cashOnDelivery": seller["_source"]['cashOnDelivery'] if "cashOnDelivery" in seller[
                                        "_source"] else False,
                                    "deliveryPrePaymentCard": seller["_source"][
                                        'deliveryPrePaymentCard'] if "deliveryPrePaymentCard" in seller["_source"] else False,
                                    "cardOnDelivery": seller["_source"]['cardOnDelivery'] if "cardOnDelivery" in seller[
                                        "_source"] else False,
                                    "acceptsCashOnDelivery": seller["_source"][
                                        'acceptsCashOnDelivery'] if "acceptsCashOnDelivery" in seller["_source"] else False,
                                    "acceptsCard": seller["_source"]['acceptsCard'] if "acceptsCard" in seller[
                                        "_source"] else False,
                                    "acceptsWallet": seller["_source"]['acceptsWallet'] if "acceptsWallet" in seller[
                                        "_source"] else False,
                                },
                                "distanceMiles": distance_miles,
                                "bannerImages": seller["_source"]['bannerImages'],
                                "minimumOrder": seller["_source"]['minimumOrder'],
                                "minimumOrderValue": str(seller["_source"]['minimumOrder']) if seller["_source"]['minimumOrder'] != 0 else "No Minimum" ,
                                "galleryImages": seller["_source"]['galleryImages'],
                                "supportedOrderTypes": seller["_source"]['supportedOrderTypes'] if "supportedOrderTypes" in
                                                                                                seller['_source'] else 3,
                                "cityId": seller["_source"]['cityId'],
                                "citiesOfOperation": seller["_source"]['citiesOfOperation'],
                                "averageDeliveryTime": str(seller["_source"][
                                                            'averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in
                                                                                                            seller[
                                                                                                                "_source"] else "",
                                "isExpressDelivery": int(seller["_source"]['isExpressDelivery']) if "isExpressDelivery" in
                                                                                                    seller["_source"] else 0,
                                "parentSellerIdOrSupplierId": seller["_source"]['parentSellerIdOrSupplierId'],
                                "storeName": seller["_source"]['storeName'][language] if language in seller["_source"]['storeName'] else seller["_source"]['storeName']['en'],
                                "address": seller["_source"]['businessLocationAddress']['address'] if "address" in seller["_source"]['businessLocationAddress'] else "",
                                "sellerTypeId": seller["_source"]['sellerTypeId'],
                                "sellerType": seller["_source"]['sellerType'],
                                "averageDeliveryTimeInMins": seller["_source"][
                                    'averageDeliveryTimeInMins'] if "averageDeliveryTimeInMins" in seller['_source'] else 0,
                                "storeFrontTypeId": seller["_source"]['storeFrontTypeId'],
                                "storeFrontType": seller["_source"]['storeFrontType'],
                                "driverTypeId": seller["_source"]['driverTypeId'] if "driverTypeId" in seller["_source"] else 0,
                                "driverType": seller["_source"]['driverType'] if "driverType" in seller["_source"] else 0,
                                "nextCloseTime": next_close_time,
                                "nextOpenTime": next_open_time,
                                "storeIsOpen": store_is_open,
                                "status": seller["_source"]['status'],
                                "percentageText": percentage_text,
                                "userLikeCount": fav_store_user_count,
                                "bookATable": seller["_source"]['bookATable'] if 'bookATable' in seller["_source"] else "",
                                "openTable": seller["_source"]['openTable'] if 'openTable' in seller["_source"] else "",
                                "hygieneRating": seller["_source"]['hygieneRating'] if 'hygieneRating' in seller["_source"] else 0,
                                "businessType": seller["_source"]['businessType'] if 'businessType' in seller["_source"] else [],
                                "hygieneRatingInspectionDate": seller["_source"]['hygieneRatingInspectionDate'] if 'hygieneRatingInspectionDate' in seller["_source"] else "",


                                })
                    print('fav store collect time :', time.time()-all_fav_store)
            if int(sort_data) == 2:
                store_data_json = sorted(store_data_json, key=lambda k: k['avgRating'], reverse=True)
            else:
                pass
            return store_data_json, fav_store_data_count, specialities_data
        else:
            return store_data_json, fav_store_data_count, specialities_data

    def normal_store_user(self, category_id, user_id, zone_id, user_latitude, user_longtitude, language, skip_data,
                          limit_data, sort_data, cuisines_data, avg_cost_max, avg_cost_min, timezone, min_ratings_data,
                          max_ratings_data, order_type,hygiene_rating_data,dietary_preferences,food_Prefereces,allergen,
                          creator_product_type,delivery_filter_data):
        is_rating_sort = False
        start_time = time.time()
        translator = t(to_lang=language)
        start_q_time = time.time()
        store_query, must_query, range_query, must_not_query, sort_query = self.get_normal_store_query(category_id,
                                                                                                       user_id, zone_id,
                                                                                                       user_latitude,
                                                                                                       user_longtitude,
                                                                                                       language,
                                                                                                       skip_data,
                                                                                                       limit_data,
                                                                                                       sort_data,
                                                                                                       cuisines_data,
                                                                                                       avg_cost_max,
                                                                                                       avg_cost_min,
                                                                                                       timezone,
                                                                                                       min_ratings_data,
                                                                                                       max_ratings_data,
                                                                                                       order_type,
                                                                                                       hygiene_rating_data,
                                                                                                       dietary_preferences,
                                                                                                       food_Prefereces,
                                                                                                       allergen,
                                                                                                       creator_product_type,
                                                                                                       delivery_filter_data)
        ##### query es #####
        filter_path = [
            "hits.hits._id",
            "hits.hits.sort",
            "hits.hits._source.storeName",
            "hits.hits._source.parentSellerIdOrSupplierId",
            "hits.hits._source.uniqStoreId"
        ]
        res_store = DbHelper.get_stores_from_elastic(store_query, index_store, filter_path)
        ##### -------------------- process result, drop duplicate stores and query again ---------------- #####
        all_store_details = []
        if len(res_store) > 0:
            for r_s in res_store['hits']['hits']:
                if int(sort_data) == 0 or int(sort_data) == 1:
                    distance_km = round(r_s['sort'][1], 2)
                    distance_miles = round(distance_km * conv_fac, 2)
                else:
                    try:
                        distance_km = round(r_s['sort'][2], 2)
                        distance_miles = round(distance_km * conv_fac, 2)
                    except:
                        try:
                            distance_km = round(r_s['sort'][1], 2)
                            distance_miles = round(distance_km * conv_fac, 2)
                        except:
                            distance_km = round(r_s['sort'][0], 2)
                            distance_miles = round(distance_km * conv_fac, 2)
                all_store_details.append({
                    "id": str(r_s['_id']),
                    "distance": distance_km,
                    "storeName": r_s['_source']['storeName']['en'],
                    "parentSellerIdOrSupplierId": r_s['_source']['parentSellerIdOrSupplierId'],
                    "uniqStoreId": r_s['_source']['uniqStoreId'] if "uniqStoreId" in r_s['_source'] else "0",
                })

        # unique_stores = []
        unique_stores = sorted(all_store_details, key=lambda k: k['distance'], reverse=False)

        if len(unique_stores) > 0 and category_id == DINE_STORE_CATEGORY_ID:
            dataframe_details = pd.DataFrame(unique_stores)
            dataframe_details = dataframe_details.drop_duplicates("uniqStoreId", keep="first")
            spec_data = dataframe_details.to_json(orient='records')
            spec_data = json.loads(spec_data)
        elif len(unique_stores) > 0:
            dataframe_details = pd.DataFrame(unique_stores)
            spec_data = dataframe_details.to_json(orient='records')
            spec_data = json.loads(spec_data)
        else:
            spec_data = []

        final_stores = []
        # for store in store_non_unique:
        for store in spec_data:
            final_stores.append(store['id'])
        must_query.append({"terms": {"_id": final_stores}})

        ##### query es again #####
        query = {
            "query":
                {
                    "bool":
                        {
                            "must": must_query,
                            "must_not": must_not_query,
                            "filter": range_query,
                        }
                },
            "size": int(limit_data),
            "from": int(skip_data),
            "sort": sort_query
        }
        filter_path = [
            "hits.total",
            "hits.hits._id",
            "hits.hits.sort",
            "hits.hits._source"
        ]
        res = DbHelper.get_stores_from_elastic(query, index_store, filter_path)
        ##### -------------------------- processing fetched data -------------------------- #####
        store_data_json = []
        close_data_json = []
        specialities_data = []
        popular_store_data_json =[]
        try:
            store_data_count = res['hits']['total']['value']
        except Exception as e:
            print('e--',res)
            store_data_count = 0
        print("res",res)
        print("query",query)
        if res['hits']['total']['value'] > 0:
            if "hits" in res['hits']:
                for seller in res['hits']['hits']:
                    cusine_name = ""
                    avg_rating_value = 0
                    seller_rating = db.sellerReviewRatings.aggregate(
                        [
                            {"$match": {
                                "sellerId": str(seller['_id']),
                                "rating": {"$ne": 0},
                                "status": 1}},
                            {
                                "$group":
                                    {
                                        "_id": "$sellerId",
                                        "avgRating": {"$avg": "$rating"}
                                    }
                            }
                        ]
                    )
                    for avg_rating in seller_rating:
                        avg_rating_value = avg_rating['avgRating']

                    if int(sort_data) == 0 or int(sort_data) == 1:
                        distance_km = round(seller['sort'][1], 2)
                        distance_miles = round(distance_km * conv_fac, 2)
                    else:
                        try:
                            distance_km = round(seller['sort'][2], 2)
                            distance_miles = round(distance_km * conv_fac, 2)
                        except:
                            try:
                                distance_km = round(seller['sort'][1], 2)
                                distance_miles = round(distance_km * conv_fac, 2)
                            except:
                                distance_km = round(seller['sort'][0], 2)
                                distance_miles = round(distance_km * conv_fac, 2)
                    if "averageCostForMealForTwo" in seller["_source"]:
                        cost_for_two = seller["_source"]['averageCostForMealForTwo']
                    else:
                        cost_for_two = 0

                    offer_details = DbHelper.get_offer_details(str(seller['_id']))
                    offer_json = []
                    for offer in offer_details:
                        offer_json.append({
                            "offerName": offer['name'][language] if language in offer['name'] else offer['name']['en'],
                            "offerId": str(offer['_id']),
                            "offerType": offer['offerType'],
                            "discountValue": offer['discountValue']
                        })

                    if len(offer_json) > 0:
                        best_offer_store = max(offer_json, key=lambda x: x['discountValue'])
                        if best_offer_store['offerType'] == 1:
                            percentage_text = str(best_offer_store['discountValue']) + " %" + " " + "off"
                        else:
                            try:
                                percentage_text = seller['_source']['currencySymbol']+str(
                                    best_offer_store['discountValue']) + " off"
                            except:
                                percentage_text = "₹" +" "+str(best_offer_store['discountValue']) + "  off"
                        offer_name = best_offer_store['offerName']
                    else:
                        offer_name = ""
                        percentage_text = ""

                    # address = seller["_source"]['businessLocationAddress']['address'] if "address" in seller["_source"][
                    #     'businessLocationAddress'] else ""
                    # addressArea = seller["_source"]['businessLocationAddress']['addressArea'] if "addressArea" in \
                    #                                                                              seller["_source"][
                    #                                                                                  'businessLocationAddress'] else ""
                    # locality = seller["_source"]['businessLocationAddress']['locality'] if "locality" in \
                    #                                                                        seller["_source"][
                    #                                                                            'businessLocationAddress'] else ""
                    # post_code = seller["_source"]['businessLocationAddress']['postCode'] if "postCode" in \
                    #                                                                         seller["_source"][
                    #                                                                             'businessLocationAddress'] else ""
                    # state = seller["_source"]['businessLocationAddress']['state'] if "state" in seller["_source"][
                    #     'businessLocationAddress'] else ""
                    # country = seller["_source"]['businessLocationAddress']['country'] if "country" in seller["_source"][
                    #     'businessLocationAddress'] else ""
                    # city = seller["_source"]['businessLocationAddress']['city'] if "city" in seller["_source"][
                    #     'businessLocationAddress'] else ""

                    # ===================================for the cusines=============================================
                    # if "specialities" in seller["_source"]:
                    #     if len(seller["_source"]['specialities']):
                    #         for spec in seller["_source"]['specialities']:
                    #             spec_data = DbHelper.get_single_speciality({"_id": ObjectId(spec)},
                    #                                                        {"specialityName": 1, "image": 1})
                    #             if spec_data != None:
                    #                 specialities_data.append({
                    #                     "id": str(spec),
                    #                     "image": spec_data['image'] if "image" in spec_data else "",
                    #                     "name": spec_data['specialityName'][language] if language in spec_data[
                    #                         'specialityName'] else spec_data['specialityName']["en"],
                    #                 })
                    #                 if cusine_name == "":
                    #                     cusine_name = spec_data['specialityName'][language] if language in spec_data[
                    #                         'specialityName'] else spec_data['specialityName']["en"]
                    #                 else:
                    #                     cusine_name = cusine_name + ", " + spec_data['specialityName'][
                    #                         language] if language in spec_data['specialityName'] else \
                    #                         spec_data['specialityName']["en"]
                    #             else:
                    #                 pass
                    #     else:
                    #         pass
                    # else:
                    #     pass
                    if "specialities" in seller["_source"]:
                        for spec in seller["_source"].get("specialities", []):
                            spec_data = DbHelper.get_single_speciality(
                                {"_id": ObjectId(spec)},
                                {"specialityName." + language: 1, "image": 1}
                            )
                            
                            if spec_data:
                                specialities_data.append({
                                    "id": str(spec),
                                    "image": spec_data.get("image", ""),
                                    "name": spec_data['specialityName'].get(language, spec_data['specialityName'].get("en", "")),
                                })
                                
                                if not cusine_name:
                                    cusine_name = spec_data['specialityName'].get(language, spec_data['specialityName'].get("en", ""))
                                else:
                                    cusine_name += ", " + spec_data['specialityName'].get(language, spec_data['specialityName'].get("en", ""))

                    try:
                        if "storeIsOpen" in seller["_source"]:
                            store_is_open = seller["_source"]['storeIsOpen']
                        else:
                            store_is_open = False
                    except:
                        store_is_open = False

                    try:
                        if "nextCloseTime" in seller["_source"]:
                            next_close_time = seller["_source"]['nextCloseTime']
                        else:
                            next_close_time = ""
                    except:
                        next_close_time = ""

                    try:
                        if "timeZoneWorkingHour" in seller["_source"]:
                            timeZoneWorkingHour = seller["_source"]['timeZoneWorkingHour']
                        else:
                            timeZoneWorkingHour = ""
                    except:
                        timeZoneWorkingHour = ""

                    try:
                        if "nextOpenTime" in seller["_source"]:
                            next_open_time = seller["_source"]['nextOpenTime']
                        else:
                            next_open_time = ""
                    except:
                        next_open_time = ""

                    if next_close_time == "" and next_open_time == "":
                        is_temp_close = True
                        store_tag = language_change("Temporarily Closed", language)
                        seq_id = 1
                    elif next_open_time != "" and store_is_open == False:
                        is_temp_close = False
                        # next_open_time = int(next_open_time + timezone * 60)
                        next_open_time = time_zone_converter(timezone, next_open_time, timeZoneWorkingHour)
                        local_time = datetime.datetime.fromtimestamp(next_open_time)
                        next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                        next_day_midnight = next_day.replace(hour=0, minute=0, second=0)
                        next_day_midnight_timestamp = int(next_day_midnight.timestamp())
                        seq_id = 0
                        if next_day_midnight_timestamp > next_open_time:
                            open_time = local_time.strftime("%I:%M %p")
                            check_language_tag = language_change("Opens Next At", language)
                            store_tag = check_language_tag + str(open_time)
                        else:
                            open_time = local_time.strftime("%I:%M %p")
                            # store_tag = translator.translate("Opens Tomorrow At" + open_time)
                            check_language_tag = language_change("Opens Tomorrow At", language)
                            store_tag = check_language_tag + " " + str(open_time)
                    else:
                        seq_id = 0
                        is_temp_close = False
                        store_tag = ""

                    # =======================================safetyStandards=============================================
                    # if "safetyStandards" in seller["_source"]:
                    #     if int(seller["_source"]['safetyStandards']) == 0:
                    #         safety_standards = False
                    #     else:
                    #         safety_standards = True
                    # else:
                    #     safety_standards = False

                    ##### get seller review ratings #####
                    if "listingImageWeb" in seller["_source"]:
                        listing_image_web = seller["_source"]['listingImageWeb']
                    else:
                        if "listingImage" in seller["_source"]:
                            listing_image_web = {
                                "listingImageExtraLarge": seller["_source"]['listingImage']['listingImageweb'],
                                "listingImageLarge": seller["_source"]['listingImage']['listingImageweb'],
                                "listingImageMedium": seller["_source"]['listingImage']['listingImageweb'],
                                "listingImageSmall": seller["_source"]['listingImage']['listingImageweb']
                            }
                        else:
                            listing_image_web = {}

                    # =======================================safetyStandards description================================
                    # if "safetyStandardsDynamicContent" in seller["_source"]:
                    #     if seller["_source"]['safetyStandardsDynamicContent'] == "":
                    #         safety_standards_dynamic_content = seller["_source"]['safetyStandardsDynamicContent']
                    #     else:
                    #         safety_standards_dynamic_content = ""
                    # else:
                    #     safety_standards_dynamic_content = ""

                    if float(min_ratings_data) != 0.0 and float(max_ratings_data) != 0.0:
                        if round(avg_rating_value, 2) >= float(min_ratings_data) and round(avg_rating_value,
                                                                                           2) <= float(
                            max_ratings_data):
                            is_rating_sort = False
                        else:
                            is_rating_sort = True
                    elif float(max_ratings_data) != 0.0:
                        if round(avg_rating_value, 2) <= float(max_ratings_data):
                            is_rating_sort = False
                        else:
                            is_rating_sort = True
                    else:
                        pass

                    # if "shopifyStoreDetails" in seller['_source']:
                    #     if "enable" in seller["_source"]["shopifyStoreDetails"]:
                    #         shopify_enable = seller["_source"]["shopifyStoreDetails"]['enable']
                    #     else:
                    #         shopify_enable = False
                    # else:
                    #     shopify_enable = False

                    # if "shopifyId" in seller['_source']:
                    #     shopify_id = seller['_source']['shopifyId']
                    # else:
                    #     shopify_id = ""

                    # if "favouriteUsers" in seller["_source"]:
                    #     if user_id in seller["_source"]['favouriteUsers']:
                    #         favourite_store = True
                    #     else:
                    #         favourite_store = False
                    # else:
                    #     favourite_store = False
                    avg_rating_value = 0
                    seller_rating = db.sellerReviewRatings.aggregate(
                            [
                                {
                                    "$match": {
                                        "sellerId": str(seller["_id"]),
                                        "status": 1,
                                        "rating": {"$ne": 0},
                                    }
                                },
                                {"$group": {"_id": "$sellerId", "avgRating": {"$avg": "$rating"}}},
                            ]
                        )
                    for avg_rating in seller_rating:
                        avg_rating_value = avg_rating["avgRating"]
                    
                    try:
                        store_user_count = seller["_source"]['favouriteUsers']
                        fav_store_user_count = len(store_user_count)
                    except Exception as e:
                        print(e)
                        fav_store_user_count = 0
                    
                    # ======================================open stores=============================================
                    if store_is_open == True and is_rating_sort == False:
                        store_data_json.append({
                             "safetyStandardsDynamicContent": seller["_source"]['safetyStandardsDynamicContent'] if "safetyStandardsDynamicContent" in seller["_source"] and seller["_source"]['safetyStandardsDynamicContent'] != "" else "",
                            "_id": str(seller['_id']),
                            "avgRating": avg_rating_value,
                            "averageCostForMealForTwo": seller['_source']['averageCostForMealForTwo'] if "averageCostForMealForTwo" in seller["_source"] else 0,
                            "isFavourite": True if "favouriteUsers" in seller["_source"] and user_id in seller["_source"]['favouriteUsers'] else False,
                            "shopifyId": seller['_source']['shopifyId'] if 'shopifyId' in seller['_source'] else "",
                            "shopifyEnable":  seller["_source"]["shopifyStoreDetails"]['enable'] if "shopifyStoreDetails" in seller['_source'] and  "enable" in seller["_source"]["shopifyStoreDetails"] else False,
                            "isTempClose": is_temp_close,
                            "currencyCode": seller["_source"]['currencyCode'] if "currencyCode" in seller[
                                "_source"] else "INR",
                            "businessLocationAddress": seller["_source"]['businessLocationAddress'],
                            "billingAddress": seller["_source"]['billingAddress'] if "billingAddress" in seller[
                                "_source"] else {},
                            "headOffice": seller["_source"]['headOffice'] if "headOffice" in seller["_source"] else {},
                            "logoImages": seller["_source"]['logoImages'],
                            "listingImage": seller["_source"]['listingImage'],
                            "listingImageWeb": listing_image_web,
                            "distanceKm": round(distance_km, 2),
                            "freeDeliveryAbove": seller["_source"]['freeDeliveryAbove'] if "freeDeliveryAbove" in seller[
                                "_source"] else 0,
                            "currencySymbol": seller["_source"]['currencySymbol'] if "currencySymbol" in seller[
                                "_source"] else "₹",
                            "currency": seller["_source"]['currencySymbol'] if "currencySymbol" in seller["_source"] else "₹",
                            "priceForBookingTable": seller["_source"]['priceForBookingTable'] if "priceForBookingTable" in seller["_source"] else 0,
                            "tableReservations": seller["_source"]['tableReservations'] if "tableReservations" in seller["_source"] else False,
                            "storeTag": store_tag,
                            "offerName": offer_name,
                            "safetyStandards": True if 'safetyStandards' in seller["_source"] and seller["_source"]['safetyStandards'] and seller["_source"]['safetyStandards'] != "" and seller["_source"]['safetyStandards'] == 1 else False,
                            "cuisines": cusine_name,
                            "address": seller["_source"]['businessLocationAddress']['address'] if "address" in seller["_source"]['businessLocationAddress'] else "",
                            "locality": seller["_source"]['businessLocationAddress']['locality'] if "locality" in seller["_source"]['businessLocationAddress'] else "",
                            "postCode": seller["_source"]['businessLocationAddress']['postCode'] if "postCode" in seller["_source"]['businessLocationAddress'] else "",
                            "addressArea": seller["_source"]['businessLocationAddress']['addressArea'] if "addressArea" in seller["_source"]['businessLocationAddress'] else "",
                            "state": seller["_source"]['businessLocationAddress']['state'] if "state" in seller["_source"]['businessLocationAddress'] else "",
                            "country": seller["_source"]['businessLocationAddress']['country'] if "country" in seller["_source"]['businessLocationAddress'] else "",
                            "city": seller["_source"]['businessLocationAddress']['city'] if "city" in seller["_source"]['businessLocationAddress'] else "",
                            "paymentMethods": {
                                "cashOnPickUp": seller["_source"]['cashOnPickUp'] if "cashOnPickUp" in seller[
                                    "_source"] else False,
                                "pickUpPrePaymentCard": seller["_source"][
                                    'pickUpPrePaymentCard'] if "pickUpPrePaymentCard" in seller["_source"] else False,
                                "cashOnDelivery": seller["_source"]['cashOnDelivery'] if "cashOnDelivery" in seller[
                                    "_source"] else False,
                                "deliveryPrePaymentCard": seller["_source"][
                                    'deliveryPrePaymentCard'] if "deliveryPrePaymentCard" in seller["_source"] else False,
                                "cardOnDelivery": seller["_source"]['cardOnDelivery'] if "cardOnDelivery" in seller[
                                    "_source"] else False,
                                "acceptsCashOnDelivery": seller["_source"][
                                    'acceptsCashOnDelivery'] if "acceptsCashOnDelivery" in seller["_source"] else False,
                                "acceptsCard": seller["_source"]['acceptsCard'] if "acceptsCard" in seller[
                                    "_source"] else False,
                                "acceptsWallet": seller["_source"]['acceptsWallet'] if "acceptsWallet" in seller[
                                    "_source"] else False,
                            },
                            "distanceMiles": distance_miles,
                            "bannerImages": seller["_source"]['bannerImages'],
                            "minimumOrder": seller["_source"]['minimumOrder'],
                            "minimumOrderValue": str(seller["_source"]['minimumOrder']) if seller["_source"]['minimumOrder'] != 0 else "No Minimum" ,
                            "galleryImages": seller["_source"]['galleryImages'],
                            "supportedOrderTypes": seller["_source"]['supportedOrderTypes'] if "supportedOrderTypes" in
                                                                                            seller['_source'] else 3,
                            "cityId": seller["_source"]['cityId'],
                            "citiesOfOperation": seller["_source"]['citiesOfOperation'],
                            "averageDeliveryTime": str(seller["_source"][
                                                        'averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in
                                                                                                        seller[
                                                                                                            "_source"] else "",
                            "isExpressDelivery": int(seller["_source"]['isExpressDelivery']) if "isExpressDelivery" in
                                                                                                seller["_source"] else 0,
                            "parentSellerIdOrSupplierId": seller["_source"]['parentSellerIdOrSupplierId'],
                            "storeName": seller["_source"]['storeName'][language] if language in seller["_source"]['storeName'] else seller["_source"]['storeName']['en'],
                            "address": seller["_source"]['businessLocationAddress']['address'] if "address" in seller["_source"]['businessLocationAddress'] else "",
                            "sellerTypeId": seller["_source"]['sellerTypeId'],
                            "sellerType": seller["_source"]['sellerType'],
                            "averageDeliveryTimeInMins": seller["_source"][
                                'averageDeliveryTimeInMins'] if "averageDeliveryTimeInMins" in seller['_source'] else 0,
                            "storeFrontTypeId": seller["_source"]['storeFrontTypeId'],
                            "storeFrontType": seller["_source"]['storeFrontType'],
                            "driverTypeId": seller["_source"]['driverTypeId'] if "driverTypeId" in seller["_source"] else 0,
                            "driverType": seller["_source"]['driverType'] if "driverType" in seller["_source"] else 0,
                            "nextCloseTime": next_close_time,
                            "nextOpenTime": next_open_time,
                            "storeIsOpen": store_is_open,
                            "status": seller["_source"]['status'],
                            "percentageText": percentage_text,
                            "userLikeCount": fav_store_user_count,
                            "bookATable": seller["_source"]['bookATable'] if 'bookATable' in seller["_source"] else "",
                            "openTable": seller["_source"]['openTable'] if 'openTable' in seller["_source"] else "",
                            "hygieneRating": seller["_source"]['hygieneRating'] if 'hygieneRating' in seller["_source"] else 0,
                            "businessType": seller["_source"]['businessType'] if 'businessType' in seller["_source"] else [],
                            "hygieneRatingInspectionDate": seller["_source"]['hygieneRatingInspectionDate'] if 'hygieneRatingInspectionDate' in seller["_source"] else "",

                        })
                    elif store_is_open == False and is_rating_sort == False:
                        # ========================================close stores========================================
                        close_data_json.append({
                             "safetyStandardsDynamicContent": seller["_source"]['safetyStandardsDynamicContent'] if "safetyStandardsDynamicContent" in seller["_source"] and seller["_source"]['safetyStandardsDynamicContent'] != "" else "",
                            "_id": str(seller['_id']),
                            "avgRating": avg_rating_value,
                            "averageCostForMealForTwo": seller['_source']['averageCostForMealForTwo'] if "averageCostForMealForTwo" in seller["_source"] else 0,
                            "isFavourite": True if "favouriteUsers" in seller["_source"] and user_id in seller["_source"]['favouriteUsers'] else False,
                            "shopifyId": seller['_source']['shopifyId'] if 'shopifyId' in seller['_source'] else "",
                            "shopifyEnable":  seller["_source"]["shopifyStoreDetails"]['enable'] if "shopifyStoreDetails" in seller['_source'] and  "enable" in seller["_source"]["shopifyStoreDetails"] else False,
                            "isTempClose": is_temp_close,
                            "currencyCode": seller["_source"]['currencyCode'] if "currencyCode" in seller[
                                "_source"] else "INR",
                            "businessLocationAddress": seller["_source"]['businessLocationAddress'],
                            "billingAddress": seller["_source"]['billingAddress'] if "billingAddress" in seller[
                                "_source"] else {},
                            "headOffice": seller["_source"]['headOffice'] if "headOffice" in seller["_source"] else {},
                            "logoImages": seller["_source"]['logoImages'],
                            "listingImage": seller["_source"]['listingImage'],
                            "listingImageWeb": listing_image_web,
                            "distanceKm": round(distance_km, 2),
                            "freeDeliveryAbove": seller["_source"]['freeDeliveryAbove'] if "freeDeliveryAbove" in seller[
                                "_source"] else 0,
                            "currencySymbol": seller["_source"]['currencySymbol'] if "currencySymbol" in seller[
                                "_source"] else "₹",
                            "currency": seller["_source"]['currencySymbol'] if "currencySymbol" in seller["_source"] else "₹",
                            "priceForBookingTable": seller["_source"]['priceForBookingTable'] if "priceForBookingTable" in seller["_source"] else 0,
                            "tableReservations": seller["_source"]['tableReservations'] if "tableReservations" in seller["_source"] else False,
                            "storeTag": store_tag,
                            "offerName": offer_name,
                            "safetyStandards": True if 'safetyStandards' in seller["_source"] and seller["_source"]['safetyStandards'] and seller["_source"]['safetyStandards'] != "" and seller["_source"]['safetyStandards'] == 1 else False,
                            "cuisines": cusine_name,
                            "address": seller["_source"]['businessLocationAddress']['address'] if "address" in seller["_source"]['businessLocationAddress'] else "",
                            "locality": seller["_source"]['businessLocationAddress']['locality'] if "locality" in seller["_source"]['businessLocationAddress'] else "",
                            "postCode": seller["_source"]['businessLocationAddress']['postCode'] if "postCode" in seller["_source"]['businessLocationAddress'] else "",
                            "addressArea": seller["_source"]['businessLocationAddress']['addressArea'] if "addressArea" in seller["_source"]['businessLocationAddress'] else "",
                            "state": seller["_source"]['businessLocationAddress']['state'] if "state" in seller["_source"]['businessLocationAddress'] else "",
                            "country": seller["_source"]['businessLocationAddress']['country'] if "country" in seller["_source"]['businessLocationAddress'] else "",
                            "city": seller["_source"]['businessLocationAddress']['city'] if "city" in seller["_source"]['businessLocationAddress'] else "",
                            "paymentMethods": {
                                "cashOnPickUp": seller["_source"]['cashOnPickUp'] if "cashOnPickUp" in seller[
                                    "_source"] else False,
                                "pickUpPrePaymentCard": seller["_source"][
                                    'pickUpPrePaymentCard'] if "pickUpPrePaymentCard" in seller["_source"] else False,
                                "cashOnDelivery": seller["_source"]['cashOnDelivery'] if "cashOnDelivery" in seller[
                                    "_source"] else False,
                                "deliveryPrePaymentCard": seller["_source"][
                                    'deliveryPrePaymentCard'] if "deliveryPrePaymentCard" in seller["_source"] else False,
                                "cardOnDelivery": seller["_source"]['cardOnDelivery'] if "cardOnDelivery" in seller[
                                    "_source"] else False,
                                "acceptsCashOnDelivery": seller["_source"][
                                    'acceptsCashOnDelivery'] if "acceptsCashOnDelivery" in seller["_source"] else False,
                                "acceptsCard": seller["_source"]['acceptsCard'] if "acceptsCard" in seller[
                                    "_source"] else False,
                                "acceptsWallet": seller["_source"]['acceptsWallet'] if "acceptsWallet" in seller[
                                    "_source"] else False,
                            },
                            "distanceMiles": distance_miles,
                            "bannerImages": seller["_source"]['bannerImages'],
                            "minimumOrder": seller["_source"]['minimumOrder'],
                            "minimumOrderValue": str(seller["_source"]['minimumOrder']) if seller["_source"]['minimumOrder'] != 0 else "No Minimum" ,
                            "galleryImages": seller["_source"]['galleryImages'],
                            "supportedOrderTypes": seller["_source"]['supportedOrderTypes'] if "supportedOrderTypes" in
                                                                                            seller['_source'] else 3,
                            "cityId": seller["_source"]['cityId'],
                            "citiesOfOperation": seller["_source"]['citiesOfOperation'],
                            "averageDeliveryTime": str(seller["_source"][
                                                        'averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in
                                                                                                        seller[
                                                                                                            "_source"] else "",
                            "isExpressDelivery": int(seller["_source"]['isExpressDelivery']) if "isExpressDelivery" in
                                                                                                seller["_source"] else 0,
                            "parentSellerIdOrSupplierId": seller["_source"]['parentSellerIdOrSupplierId'],
                            "storeName": seller["_source"]['storeName'][language] if language in seller["_source"]['storeName'] else seller["_source"]['storeName']['en'],
                            "address": seller["_source"]['businessLocationAddress']['address'] if "address" in seller["_source"]['businessLocationAddress'] else "",
                            "sellerTypeId": seller["_source"]['sellerTypeId'],
                            "sellerType": seller["_source"]['sellerType'],
                            "averageDeliveryTimeInMins": seller["_source"][
                                'averageDeliveryTimeInMins'] if "averageDeliveryTimeInMins" in seller['_source'] else 0,
                            "storeFrontTypeId": seller["_source"]['storeFrontTypeId'],
                            "storeFrontType": seller["_source"]['storeFrontType'],
                            "driverTypeId": seller["_source"]['driverTypeId'] if "driverTypeId" in seller["_source"] else 0,
                            "driverType": seller["_source"]['driverType'] if "driverType" in seller["_source"] else 0,
                            "nextCloseTime": next_close_time,
                            "nextOpenTime": next_open_time,
                            "storeIsOpen": store_is_open,
                            "status": seller["_source"]['status'],
                            "percentageText": percentage_text,
                            "seqId": seller["_source"]['seqId'],
                            "userLikeCount": fav_store_user_count,
                            "bookATable": seller["_source"]['bookATable'] if 'bookATable' in seller["_source"] else "",
                            "openTable": seller["_source"]['openTable'] if 'openTable' in seller["_source"] else "",
                            "hygieneRating": seller["_source"]['hygieneRating'] if 'hygieneRating' in seller["_source"] else 0,
                            "businessType": seller["_source"]['businessType'] if 'businessType' in seller["_source"] else [],
                            "hygieneRatingInspectionDate": seller["_source"]['hygieneRatingInspectionDate'] if 'hygieneRatingInspectionDate' in seller["_source"] else "",

                        })
                    else:
                        pass
            if int(sort_data) == 2:
                store_data_json = sorted(store_data_json, key=lambda k: k['avgRating'], reverse=True)
                close_data_json = sorted(close_data_json, key=lambda k: k['avgRating'], reverse=True)
            else:
                pass
            # close_data_json = sorted(close_data_json, key=lambda k: k['seqId'], reverse=False)
            popular_store_data_json = sorted(store_data_json, key=lambda k: k['avgRating'], reverse=True)
            return store_data_json, store_data_count, specialities_data, close_data_json,popular_store_data_json
        else:
            return store_data_json, store_data_count, specialities_data, close_data_json,popular_store_data_json

    
    
    def user_preferences_store(self, category_id, user_id, zone_id, user_latitude, user_longtitude, language, skip_data,
                          limit_data, sort_data, cuisines_data, avg_cost_max, avg_cost_min, timezone, min_ratings_data,
                          max_ratings_data, order_type,hygiene_rating_data,dietary_preferences,food_Prefereces,allergen,
                          creator_product_type,delivery_filter_data):
        is_rating_sort = False
        start_time = time.time()
        store_data_json = []
        store_data_count = 0
        translator = t(to_lang=language)
        start_q_time = time.time()
        store_query, must_query, range_query, must_not_query, sort_query = self.get_quick_picks_store_query(category_id,
                                                                                                       user_id, zone_id,
                                                                                                       user_latitude,
                                                                                                       user_longtitude,
                                                                                                       language,
                                                                                                       skip_data,
                                                                                                       limit_data,
                                                                                                       sort_data,
                                                                                                       cuisines_data,
                                                                                                       avg_cost_max,
                                                                                                       avg_cost_min,
                                                                                                       timezone,
                                                                                                       min_ratings_data,
                                                                                                       max_ratings_data,
                                                                                                       order_type,
                                                                                                       hygiene_rating_data,
                                                                                                       dietary_preferences,
                                                                                                       food_Prefereces,
                                                                                                       allergen,
                                                                                                       creator_product_type,
                                                                                                       delivery_filter_data)
        ##### query es #####
        print("store query for prefrence store ",store_query)
        filter_path = [
            "hits.hits._id",
            "hits.hits.sort",
            "hits.hits._source.storeName",
            "hits.hits._source.parentSellerIdOrSupplierId",
            "hits.hits._source.uniqStoreId"
        ]
        if store_query == []:
            return store_data_json,store_data_count
        res_store = DbHelper.get_stores_from_elastic(store_query, index_store, filter_path)
        ##### -------------------- process result, drop duplicate stores and query again ---------------- #####
        all_store_details = []
        if len(res_store) > 0:
            for r_s in res_store['hits']['hits']:
                if int(sort_data) == 0 or int(sort_data) == 1:
                    distance_km = round(r_s['sort'][1], 2)
                    distance_miles = round(distance_km * conv_fac, 2)
                else:
                    try:
                        distance_km = round(r_s['sort'][2], 2)
                        distance_miles = round(distance_km * conv_fac, 2)
                    except:
                        try:
                            distance_km = round(r_s['sort'][1], 2)
                            distance_miles = round(distance_km * conv_fac, 2)
                        except:
                            distance_km = round(r_s['sort'][0], 2)
                            distance_miles = round(distance_km * conv_fac, 2)
                all_store_details.append({
                    "id": str(r_s['_id']),
                    "distance": distance_km,
                    "storeName": r_s['_source']['storeName']['en'],
                    "parentSellerIdOrSupplierId": r_s['_source']['parentSellerIdOrSupplierId'],
                    "uniqStoreId": r_s['_source']['uniqStoreId'] if "uniqStoreId" in r_s['_source'] else "0",
                })

        # unique_stores = []
        unique_stores = sorted(all_store_details, key=lambda k: k['distance'], reverse=False)

        if len(unique_stores) > 0 and category_id == DINE_STORE_CATEGORY_ID:
            dataframe_details = pd.DataFrame(unique_stores)
            dataframe_details = dataframe_details.drop_duplicates("uniqStoreId", keep="first")
            spec_data = dataframe_details.to_json(orient='records')
            spec_data = json.loads(spec_data)
        elif len(unique_stores) > 0:
            dataframe_details = pd.DataFrame(unique_stores)
            spec_data = dataframe_details.to_json(orient='records')
            spec_data = json.loads(spec_data)
        else:
            spec_data = []

        final_stores = []
        # for store in store_non_unique:
        for store in spec_data:
            final_stores.append(store['id'])
        must_query.append({"terms": {"_id": final_stores}})
        

        ##### query es again #####
        query = {
            "query":
                {
                    "bool":
                        {
                            "must": must_query,
                            # "must_not": must_not_query,
                            # "filter": range_query,
                        }
                },
            "size": int(limit_data),
            "from": int(skip_data),
            "sort": sort_query
        }
        filter_path = [
            "hits.total",
            "hits.hits._id",
            "hits.hits.sort",
            "hits.hits._source"
        ]
        res = DbHelper.get_stores_from_elastic(query, index_store, filter_path)
        ##### -------------------------- processing fetched data -------------------------- #####
        store_data_json = []
        close_data_json = []
        specialities_data = []
        popular_store_data_json =[]
        try:
            store_data_count = res['hits']['total']['value']
        except Exception as e:
            print('e--',res)
            store_data_count = 0
        if res['hits']['total']['value'] > 0:
            if "hits" in res['hits']:
                for seller in res['hits']['hits']:
                    cusine_name = ""
                    avg_rating_value = 0
                    seller_rating = db.sellerReviewRatings.aggregate(
                        [
                            {"$match": {
                                "sellerId": str(seller['_id']),
                                "rating": {"$ne": 0},
                                "status": 1}},
                            {
                                "$group":
                                    {
                                        "_id": "$sellerId",
                                        "avgRating": {"$avg": "$rating"}
                                    }
                            }
                        ]
                    )
                    for avg_rating in seller_rating:
                        avg_rating_value = avg_rating['avgRating']

                    if int(sort_data) == 0 or int(sort_data) == 1:
                        distance_km = round(seller['sort'][1], 2)
                        distance_miles = round(distance_km * conv_fac, 2)
                    else:
                        try:
                            distance_km = round(seller['sort'][2], 2)
                            distance_miles = round(distance_km * conv_fac, 2)
                        except:
                            try:
                                distance_km = round(seller['sort'][1], 2)
                                distance_miles = round(distance_km * conv_fac, 2)
                            except:
                                distance_km = round(seller['sort'][0], 2)
                                distance_miles = round(distance_km * conv_fac, 2)
                    if "averageCostForMealForTwo" in seller["_source"]:
                        cost_for_two = seller["_source"]['averageCostForMealForTwo']
                    else:
                        cost_for_two = 0

                    offer_details = DbHelper.get_offer_details(str(seller['_id']))
                    offer_json = []
                    for offer in offer_details:
                        offer_json.append({
                            "offerName": offer['name'][language] if language in offer['name'] else offer['name']['en'],
                            "offerId": str(offer['_id']),
                            "offerType": offer['offerType'],
                            "discountValue": offer['discountValue']
                        })

                    if len(offer_json) > 0:
                        best_offer_store = max(offer_json, key=lambda x: x['discountValue'])
                        if best_offer_store['offerType'] == 1:
                            percentage_text = str(best_offer_store['discountValue']) + " %" + " " + "off"
                        else:
                            try:
                                percentage_text = seller['_source']['currencySymbol']+str(
                                    best_offer_store['discountValue']) + " off"
                            except:
                                percentage_text = "₹" +" "+str(best_offer_store['discountValue']) + "  off"
                        offer_name = best_offer_store['offerName']
                    else:
                        offer_name = ""
                        percentage_text = ""
                   
                    if "specialities" in seller["_source"]:
                        for spec in seller["_source"].get("specialities", []):
                            spec_data = DbHelper.get_single_speciality(
                                {"_id": ObjectId(spec)},
                                {"specialityName." + language: 1, "image": 1}
                            )
                            
                            if spec_data:
                                specialities_data.append({
                                    "id": str(spec),
                                    "image": spec_data.get("image", ""),
                                    "name": spec_data['specialityName'].get(language, spec_data['specialityName'].get("en", "")),
                                })
                                
                                if not cusine_name:
                                    cusine_name = spec_data['specialityName'].get(language, spec_data['specialityName'].get("en", ""))
                                else:
                                    cusine_name += ", " + spec_data['specialityName'].get(language, spec_data['specialityName'].get("en", ""))

                    try:
                        if "storeIsOpen" in seller["_source"]:
                            store_is_open = seller["_source"]['storeIsOpen']
                        else:
                            store_is_open = False
                    except:
                        store_is_open = False

                    try:
                        if "nextCloseTime" in seller["_source"]:
                            next_close_time = seller["_source"]['nextCloseTime']
                        else:
                            next_close_time = ""
                    except:
                        next_close_time = ""

                    try:
                        if "timeZoneWorkingHour" in seller["_source"]:
                            timeZoneWorkingHour = seller["_source"]['timeZoneWorkingHour']
                        else:
                            timeZoneWorkingHour = ""
                    except:
                        timeZoneWorkingHour = ""

                    try:
                        if "nextOpenTime" in seller["_source"]:
                            next_open_time = seller["_source"]['nextOpenTime']
                        else:
                            next_open_time = ""
                    except:
                        next_open_time = ""

                    if next_close_time == "" and next_open_time == "":
                        is_temp_close = True
                        store_tag = language_change("Temporarily Closed", language)
                        seq_id = 1
                    elif next_open_time != "" and store_is_open == False:
                        is_temp_close = False
                        # next_open_time = int(next_open_time + timezone * 60)
                        next_open_time = time_zone_converter(timezone, next_open_time, timeZoneWorkingHour)
                        local_time = datetime.datetime.fromtimestamp(next_open_time)
                        next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                        next_day_midnight = next_day.replace(hour=0, minute=0, second=0)
                        next_day_midnight_timestamp = int(next_day_midnight.timestamp())
                        seq_id = 0
                        if next_day_midnight_timestamp > next_open_time:
                            open_time = local_time.strftime("%I:%M %p")
                            check_language_tag = language_change("Opens Next At", language)
                            store_tag = check_language_tag + str(open_time)
                        else:
                            open_time = local_time.strftime("%I:%M %p")
                            # store_tag = translator.translate("Opens Tomorrow At" + open_time)
                            check_language_tag = language_change("Opens Tomorrow At", language)
                            store_tag = check_language_tag + " " + str(open_time)
                    else:
                        seq_id = 0
                        is_temp_close = False
                        store_tag = ""

                    ##### get seller review ratings #####
                    if "listingImageWeb" in seller["_source"]:
                        listing_image_web = seller["_source"]['listingImageWeb']
                    else:
                        if "listingImage" in seller["_source"]:
                            listing_image_web = {
                                "listingImageExtraLarge": seller["_source"]['listingImage']['listingImageweb'],
                                "listingImageLarge": seller["_source"]['listingImage']['listingImageweb'],
                                "listingImageMedium": seller["_source"]['listingImage']['listingImageweb'],
                                "listingImageSmall": seller["_source"]['listingImage']['listingImageweb']
                            }
                        else:
                            listing_image_web = {}


                    if float(min_ratings_data) != 0.0 and float(max_ratings_data) != 0.0:
                        if round(avg_rating_value, 2) >= float(min_ratings_data) and round(avg_rating_value,
                                                                                           2) <= float(
                            max_ratings_data):
                            is_rating_sort = False
                        else:
                            is_rating_sort = True
                    elif float(max_ratings_data) != 0.0:
                        if round(avg_rating_value, 2) <= float(max_ratings_data):
                            is_rating_sort = False
                        else:
                            is_rating_sort = True
                    else:
                        pass

                    avg_rating_value = 0
                    seller_rating = db.sellerReviewRatings.aggregate(
                            [
                                {
                                    "$match": {
                                        "sellerId": str(seller["_id"]),
                                        "status": 1,
                                        "rating": {"$ne": 0},
                                    }
                                },
                                {"$group": {"_id": "$sellerId", "avgRating": {"$avg": "$rating"}}},
                            ]
                        )
                    for avg_rating in seller_rating:
                        avg_rating_value = avg_rating["avgRating"]
                    
                    try:
                        store_user_count = seller["_source"]['favouriteUsers']
                        fav_store_user_count = len(store_user_count)
                    except Exception as e:
                        print(e)
                        fav_store_user_count = 0
                    
                    # ======================================open stores=============================================
                    if store_is_open == True and is_rating_sort == False:
                        store_data_json.append({
                             "safetyStandardsDynamicContent": seller["_source"]['safetyStandardsDynamicContent'] if "safetyStandardsDynamicContent" in seller["_source"] and seller["_source"]['safetyStandardsDynamicContent'] != "" else "",
                            "_id": str(seller['_id']),
                            "avgRating": avg_rating_value,
                            "averageCostForMealForTwo": seller['_source']['averageCostForMealForTwo'] if "averageCostForMealForTwo" in seller["_source"] else 0,
                            "isFavourite": True if "favouriteUsers" in seller["_source"] and user_id in seller["_source"]['favouriteUsers'] else False,
                            "shopifyId": seller['_source']['shopifyId'] if 'shopifyId' in seller['_source'] else "",
                            "shopifyEnable":  seller["_source"]["shopifyStoreDetails"]['enable'] if "shopifyStoreDetails" in seller['_source'] and  "enable" in seller["_source"]["shopifyStoreDetails"] else False,
                            "isTempClose": is_temp_close,
                            "currencyCode": seller["_source"]['currencyCode'] if "currencyCode" in seller[
                                "_source"] else "INR",
                            "businessLocationAddress": seller["_source"]['businessLocationAddress'],
                            "billingAddress": seller["_source"]['billingAddress'] if "billingAddress" in seller[
                                "_source"] else {},
                            "headOffice": seller["_source"]['headOffice'] if "headOffice" in seller["_source"] else {},
                            "logoImages": seller["_source"]['logoImages'],
                            "listingImage": seller["_source"]['listingImage'],
                            "listingImageWeb": listing_image_web,
                            "distanceKm": round(distance_km, 2),
                            "freeDeliveryAbove": seller["_source"]['freeDeliveryAbove'] if "freeDeliveryAbove" in seller[
                                "_source"] else 0,
                            "currencySymbol": seller["_source"]['currencySymbol'] if "currencySymbol" in seller[
                                "_source"] else "₹",
                            "currency": seller["_source"]['currencySymbol'] if "currencySymbol" in seller["_source"] else "₹",
                            "priceForBookingTable": seller["_source"]['priceForBookingTable'] if "priceForBookingTable" in seller["_source"] else 0,
                            "tableReservations": seller["_source"]['tableReservations'] if "tableReservations" in seller["_source"] else False,
                            "storeTag": store_tag,
                            "offerName": offer_name,
                            "safetyStandards": True if 'safetyStandards' in seller["_source"] and seller["_source"]['safetyStandards'] and seller["_source"]['safetyStandards'] != "" and seller["_source"]['safetyStandards'] == 1 else False,
                            "cuisines": cusine_name,
                            "address": seller["_source"]['businessLocationAddress']['address'] if "address" in seller["_source"]['businessLocationAddress'] else "",
                            "locality": seller["_source"]['businessLocationAddress']['locality'] if "locality" in seller["_source"]['businessLocationAddress'] else "",
                            "postCode": seller["_source"]['businessLocationAddress']['postCode'] if "postCode" in seller["_source"]['businessLocationAddress'] else "",
                            "addressArea": seller["_source"]['businessLocationAddress']['addressArea'] if "addressArea" in seller["_source"]['businessLocationAddress'] else "",
                            "state": seller["_source"]['businessLocationAddress']['state'] if "state" in seller["_source"]['businessLocationAddress'] else "",
                            "country": seller["_source"]['businessLocationAddress']['country'] if "country" in seller["_source"]['businessLocationAddress'] else "",
                            "city": seller["_source"]['businessLocationAddress']['city'] if "city" in seller["_source"]['businessLocationAddress'] else "",
                            "paymentMethods": {
                                "cashOnPickUp": seller["_source"]['cashOnPickUp'] if "cashOnPickUp" in seller[
                                    "_source"] else False,
                                "pickUpPrePaymentCard": seller["_source"][
                                    'pickUpPrePaymentCard'] if "pickUpPrePaymentCard" in seller["_source"] else False,
                                "cashOnDelivery": seller["_source"]['cashOnDelivery'] if "cashOnDelivery" in seller[
                                    "_source"] else False,
                                "deliveryPrePaymentCard": seller["_source"][
                                    'deliveryPrePaymentCard'] if "deliveryPrePaymentCard" in seller["_source"] else False,
                                "cardOnDelivery": seller["_source"]['cardOnDelivery'] if "cardOnDelivery" in seller[
                                    "_source"] else False,
                                "acceptsCashOnDelivery": seller["_source"][
                                    'acceptsCashOnDelivery'] if "acceptsCashOnDelivery" in seller["_source"] else False,
                                "acceptsCard": seller["_source"]['acceptsCard'] if "acceptsCard" in seller[
                                    "_source"] else False,
                                "acceptsWallet": seller["_source"]['acceptsWallet'] if "acceptsWallet" in seller[
                                    "_source"] else False,
                            },
                            "distanceMiles": distance_miles,
                            "bannerImages": seller["_source"]['bannerImages'],
                            "minimumOrder": seller["_source"]['minimumOrder'],
                            "minimumOrderValue": str(seller["_source"]['minimumOrder']) if seller["_source"]['minimumOrder'] != 0 else "No Minimum" ,
                            "galleryImages": seller["_source"]['galleryImages'],
                            "supportedOrderTypes": seller["_source"]['supportedOrderTypes'] if "supportedOrderTypes" in
                                                                                            seller['_source'] else 3,
                            "cityId": seller["_source"]['cityId'],
                            "citiesOfOperation": seller["_source"]['citiesOfOperation'],
                            "averageDeliveryTime": str(seller["_source"][
                                                        'averageDeliveryTimeInMins']) + " " + "Mins" if "averageDeliveryTimeInMins" in
                                                                                                        seller[
                                                                                                            "_source"] else "",
                            "isExpressDelivery": int(seller["_source"]['isExpressDelivery']) if "isExpressDelivery" in
                                                                                                seller["_source"] else 0,
                            "parentSellerIdOrSupplierId": seller["_source"]['parentSellerIdOrSupplierId'],
                            "storeName": seller["_source"]['storeName'][language] if language in seller["_source"]['storeName'] else seller["_source"]['storeName']['en'],
                            "address": seller["_source"]['businessLocationAddress']['address'] if "address" in seller["_source"]['businessLocationAddress'] else "",
                            "sellerTypeId": seller["_source"]['sellerTypeId'],
                            "sellerType": seller["_source"]['sellerType'],
                            "averageDeliveryTimeInMins": seller["_source"][
                                'averageDeliveryTimeInMins'] if "averageDeliveryTimeInMins" in seller['_source'] else 0,
                            "storeFrontTypeId": seller["_source"]['storeFrontTypeId'],
                            "storeFrontType": seller["_source"]['storeFrontType'],
                            "driverTypeId": seller["_source"]['driverTypeId'] if "driverTypeId" in seller["_source"] else 0,
                            "driverType": seller["_source"]['driverType'] if "driverType" in seller["_source"] else 0,
                            "nextCloseTime": next_close_time,
                            "nextOpenTime": next_open_time,
                            "storeIsOpen": store_is_open,
                            "status": seller["_source"]['status'],
                            "percentageText": percentage_text,
                            "userLikeCount": fav_store_user_count,
                            "bookATable": seller["_source"]['bookATable'] if 'bookATable' in seller["_source"] else "",
                            "openTable": seller["_source"]['openTable'] if 'openTable' in seller["_source"] else "",
                            "hygieneRating": seller["_source"]['hygieneRating'] if 'hygieneRating' in seller["_source"] else 0,
                            "businessType": seller["_source"]['businessType'] if 'businessType' in seller["_source"] else [],
                            "hygieneRatingInspectionDate": seller["_source"]['hygieneRatingInspectionDate'] if 'hygieneRatingInspectionDate' in seller["_source"] else "",
                        })
            if int(sort_data) == 2:
                store_data_json = sorted(store_data_json, key=lambda k: k['avgRating'], reverse=True)
            else:
                pass
            return store_data_json, store_data_count,
        else:
            return store_data_json, store_data_count

    def food_offer_details(self, category_id, zone_id, language):
        banner_details = DbHelper.get_all_banner(
            {"storeCategoryId": category_id, "zones.zoneId": {"$in": [zone_id]}, "status": 1})
        banner_data = []
        if banner_details.count() > 0:
            for banner in banner_details:
                store_details = DbHelper.get_single_store({"_id": ObjectId(banner['data'][0]['id']), "status": 1},
                                                          {"storeName": 1})
                if store_details is not None:
                    banner_data.append({
                        "offerId": str(banner['data'][0]['id']),
                        "name": "",  # store_details['storeName'][language],
                        "images": {
                            "thumbnail": banner['image_mobile'],
                            "mobile": banner['image_mobile'],
                            "image": banner['image_mobile']
                        },
                        "webimages": {
                            "thumbnail": banner['image_web'],
                            "mobile": banner['image_web'],
                            "image": banner['image_web']
                        },
                    })
            return banner_data
        else:
            return banner_data

    def get_favourite_store_query(self, category_id, user_id, zone_id, user_latitude, user_longtitude, language,
                                  skip_data,
                                  limit_data, sort_data, cuisines_data, avg_cost_max, avg_cost_min, timezone,
                                  min_ratings_data, max_ratings_data, order_type,hygiene_rating_data,dietary_preferences,
                                  food_Prefereces, allergen,creator_product_type,delivery_filter_data):
        
        must_query = []
        range_query = []

        ##### adding conditions to query for stores data #####
        must_query.append({"match": {"categoryId": category_id}})
        must_query.append({"match": {"status": 1}})
        zone_ids = [ids for n in zone_id.split() for ids in n.split(",")]
        if len(zone_ids) > 0:
            must_query.append({"terms": {"serviceZones.zoneId": zone_ids}})
        must_query.append({"terms": {"favouriteUsers": [user_id]}})

        # if hygiene_rating_data is given then add query
        if hygiene_rating_data:
            must_query.append({"match": {"hygieneRating": {"gte":int(hygiene_rating_data)}}})

        # if food_Prefereces is given then add query
        food_pref_data = [food_pref for n in food_Prefereces.split() for food_pref in n.split(",")]
        if len(food_pref_data) > 0:
            must_query.append({"terms": {"foodPreferencesSupported": food_pref_data}})

        # if dietary_preferences is given then add query
        delivery_types_filter = [delivery_types for n in delivery_filter_data.split() for delivery_types in n.split(",")]
        if len(delivery_types_filter) > 0:
            must_query.append({"terms": {"deliveryTypes._id": delivery_types_filter}})

        # if dietary_preferences is given then add query
        diet_pref_data = [diet_pref for n in dietary_preferences.split() for diet_pref in n.split(",")]
        if len(diet_pref_data) > 0:
            must_query.append({"terms": {"dietaryPreferencesSupported": diet_pref_data}})

        # if allergen is given then add query
        allergen_list_data = [allergen_list for n in allergen.split() for allergen_list in n.split(",")]
        if len(allergen_list_data) > 0:
            must_query.append({"terms": {"allergenList": allergen_list_data}})
        
        # if creator_product_type is given then add query
        creator_prod_type_data = [creator_prod_type for n in creator_product_type.split() for creator_prod_type in n.split(",")]
        if len(creator_prod_type_data) > 0:
            must_query.append({"terms": {"creatorProductType": creator_prod_type_data}})



        ##### if ratings filter is given then add query #####
        # if float(min_ratings_data) != 0.0 and float(max_ratings_data) != 0.0:
        #     range_query.append(
        #         {"range": {"avgRating": {"gte": float(min_ratings_data), "lte": float(max_ratings_data)}}})
        # elif float(max_ratings_data) != 0.0:
        #     range_query.append(
        #         {"range": {"avgRating": {"gte": float(min_ratings_data), "lte": float(max_ratings_data)}}})
        if float(min_ratings_data) != float(max_ratings_data):
            range_query.append(
                {"range": {"avgRating": {"gte": float(min_ratings_data), "lte": float(max_ratings_data)}}})

        # ##### if cuisines filter is given then add to query #####
        # if cuisines_data != "":
        #     spec_data = []
        #     for n in cuisines_data.split():
                # for spec in n.split(","):
                #     spec_data.append(spec)
        spec_data = [spec for n in cuisines_data.split() for spec in n.split(",")]
        if len(spec_data) > 0:
            must_query.append({"terms": {"specialities": spec_data}})

        # if int(order_type) != 4 and int(order_type) != 3:
        #     must_query.append({"terms": {"supportedOrderTypes": [order_type, 3]}})
        # elif int(order_type) == 3:
        #     must_query.append({"terms": {"supportedOrderTypes": [1, 2, 3]}})
        # else:
        #     pass

        order_data = [int(spec) for n in order_type.split() for spec in n.split(",")]
        if len(order_data) > 0:
            # 3 is table reservation
            if order_data == [3]:
                must_query.append({"match": {"tableReservations": True}})
            elif 3 in order_data:
                must_query.append({"bool": {"should": [{"match": {"tableReservations": True}},
                                                       {"terms": {"supportedOrderTypes": order_data}}
                                                       ]}})
            # 0 means all store data
            elif order_data == [0]:
                must_query.append({"terms": {"supportedOrderTypes": [1, 2, 3]}})
            else:
                must_query.append({"terms": {"supportedOrderTypes": order_data + [3]}})
        else:
            pass

        ##### add query if store category is dine category #####
        if avg_cost_max == 0 and avg_cost_min == 0:
            pass
        else:
            range_query.append(
                    {
                        "range":
                            {
                                "averageCostForMealForTwo":
                                    {
                                        "lte": float(avg_cost_max),
                                        "gte": float(avg_cost_min),
                                    }
                            }
                    }
                )
        # if category_id == DINE_STORE_CATEGORY_ID:
        #     if avg_cost_max != 0 and avg_cost_min != 0:
        #         range_query.append(
        #             {
        #                 "range":
        #                     {
        #                         "averageCostForMealForTwo":
        #                             {
        #                                 "lte": float(avg_cost_max),
        #                                 "gte": float(avg_cost_min),
        #                             }
        #                     }
        #             }
        #         )
        #     elif avg_cost_max != 0 and avg_cost_min == 0:
        #         range_query.append(
        #             {
        #                 "range":
        #                     {
        #                         "averageCostForMealForTwo":
        #                             {
        #                                 "lte": float(avg_cost_max)
        #                             }
        #                     }
        #             }
        #         )
        #     else:
        #         range_query.append(
        #             {
        #                 "range":
        #                     {
        #                         "averageCostForMealForTwo":
        #                             {
        #                                 "gte": float(avg_cost_min)
        #                             }
        #                     }
        #             }
        #         )
        # else:
        #     if avg_cost_max != 0 and avg_cost_min != 0:
        #         range_query.append(
        #             {
        #                 "range":
        #                     {
        #                         "minimumOrder":
        #                             {
        #                                 "lte": float(avg_cost_max),
        #                                 "gte": float(avg_cost_min),
        #                             }
        #                     }
        #             }
        #         )
        #     elif avg_cost_max != 0 and avg_cost_min == 0:
        #         range_query.append(
        #             {
        #                 "range":
        #                     {
        #                         "minimumOrder":
        #                             {
        #                                 "lte": float(avg_cost_max)
        #                             }
        #                     }
        #             }
        #         )
        #     else:
        #         range_query.append(
        #             {
        #                 "range":
        #                     {
        #                         "minimumOrder":
        #                             {
        #                                 "gte": float(avg_cost_min)
        #                             }
        #                     }
        #             }
        #         )

        ##### queries on distance #####
        # if int(sort_data) == 4:
        geo_distance_sort = {
            "_geo_distance": {
                "distance_type": "plane",
                "location": {
                    "lat": float(user_latitude),
                    "lon": float(user_longtitude)
                },
                "order": "desc" if int(sort_data) == 4 else "asc",
                "unit": "km"
            }
        }
    # else:
        #     geo_distance_sort = {
        #         "_geo_distance": {
        #             "distance_type": "plane",
        #             "location": {
        #                 "lat": float(user_latitude),
        #                 "lon": float(user_longtitude)
        #             },
        #             "order": "asc",
        #             "unit": "km"
        #         }
        #     }
        # if int(sort_data) == 0:
        #     sort_query = [
        #         {
        #             "storeIsOpen": {
        #                 "order": "desc"
        #             }
        #         },
        #         geo_distance_sort
        #     ]
        # elif int(sort_data) == 1:
        #     sort_query = [
        #         {
        #             "storeIsOpen": {
        #                 "order": "desc"
        #             }
        #         },
        #         geo_distance_sort
        #     ]
        # elif int(sort_data) == 2:
        #     sort_query = [
        #         {
        #             "storeIsOpen": {
        #                 "order": "desc"
        #             }
        #         },
        #         {
        #             "avgRating": {
        #                 "order": "desc"
        #             }
        #         }, geo_distance_sort
        #     ]

        # elif int(sort_data) == 3:
        #     if category_id == DINE_STORE_CATEGORY_ID:
        #         sort_query = [
        #             {
        #                 "averageCostForMealForTwo": {
        #                     "order": "asc"
        #                 }
        #             },
        #             geo_distance_sort
        #         ]
        #     else:
        #         sort_query = [geo_distance_sort]
        # elif int(sort_data) == 4:
        #     if category_id == DINE_STORE_CATEGORY_ID:
        #         sort_query = [
        #             {
        #                 "averageCostForMealForTwo": {
        #                     "order": "desc"
        #                 }
        #             },
        #             geo_distance_sort
        #         ]
        #     else:
        #         sort_query = [geo_distance_sort]
        # else:
        #     sort_query = [
        #         {
        #             "storeIsOpen": {
        #                 "order": "desc"
        #             }
        #         },
        #         {
        #             "averageCostForMealForTwo": {
        #                 "order": "desc"
        #             }
        #         }, geo_distance_sort
        #     ]
        # must_query.append({
        #     "geo_distance": {
        #         "distance": "50km",
        #         "location": {
        #             "lat": float(user_latitude),
        #             "lon": float(user_longtitude)
        #         }
        #     }
        # })
        sorting_options = {
            0: [{"storeIsOpen": {"order": "desc"}}, geo_distance_sort],
            1: [{"storeIsOpen": {"order": "desc"}}, geo_distance_sort],
            # 2: [{"storeIsOpen": {"order": "desc"}}, {"avgRating": {"order": "desc"}}, geo_distance_sort],
            3: [{"averageCostForMealForTwo": {"order": "asc"}}, geo_distance_sort],
            4: [{"storeIsOpen": {"order": "desc"}},
                {"averageCostForMealForTwo": {"order": "desc"}}, geo_distance_sort]
            # Add other cases as needed
        }

        sort_query = sorting_options.get(int(sort_data), [{"storeIsOpen": {"order": "desc"}}, geo_distance_sort])
        store_query = {
            "query":
                {
                    "bool":
                        {
                            "must": must_query,
                            "filter": range_query,
                        }
                },
            "size": 500,
            "from": 0,
            "sort": sort_query
        }

        return store_query, must_query, range_query, sort_query

    def get_normal_store_query(self, category_id, user_id, zone_id, user_latitude, user_longtitude, language, skip_data,
                               limit_data, sort_data, cuisines_data, avg_cost_max, avg_cost_min, timezone,
                               min_ratings_data,
                               max_ratings_data, order_type,hygiene_rating_data,dietary_preferences,food_Prefereces,allergen,
                               creator_product_type,delivery_filter_data):
        new_query_time = time.time()
        must_query = []
        range_query = []
        must_not_query = []

        # if hygiene_rating_data is given then add query
        if hygiene_rating_data:
            must_query.append({"match": {"hygieneRating": {"gte":int(hygiene_rating_data)}}})

        # if dietary_preferences is given then add query
        delivery_types_filter = [delivery_types for n in delivery_filter_data.split() for delivery_types in n.split(",")]
        if len(delivery_types_filter) > 0:
            must_query.append({"terms": {"deliveryTypes._id": delivery_types_filter}})

        # if dietary_preferences is given then add query
        diet_pref_data = [diet_pref for n in dietary_preferences.split() for diet_pref in n.split(",")]
        if len(diet_pref_data) > 0:
            must_query.append({"terms": {"dietaryPreferencesSupported": diet_pref_data}})

        # if food_Prefereces is given then add query
        food_pref_data = [food_pref for n in food_Prefereces.split() for food_pref in n.split(",")]
        if len(food_pref_data) > 0:
            must_query.append({"terms": {"foodPreferencesSupported": food_pref_data}})

        # if allergen is given then add query
        allergen_list_data = [allergen_list for n in allergen.split() for allergen_list in n.split(",")]
        if len(allergen_list_data) > 0:
            must_query.append({"terms": {"allergenList": allergen_list_data}})
        
        # if creator_product_type is given then add query
        creator_prod_type_data = [creator_prod_type for n in creator_product_type.split() for creator_prod_type in n.split(",")]
        if len(creator_prod_type_data) > 0:
            must_query.append({"terms": {"creatorProductType": creator_prod_type_data}})

        must_query.append({"match": {"categoryId": category_id}})
        must_query.append({"match": {"status": 1}})
        zone_ids = [ids for n in zone_id.split() for ids in n.split(",")]
        if len(zone_ids) > 0:
            must_query.append({"terms": {"serviceZones.zoneId": zone_ids}})
        # must_not_query.append({"terms": {"favouriteUsers": [user_id]}})

        order_data = [int(spec) for n in order_type.split() for spec in n.split(",")]
        if len(order_data) > 0:
            # 3 is table reservation
            if order_data == [3]:
                must_query.append({"match": {"tableReservations": True}})
            elif 3 in order_data:
                must_query.append({"bool": {"should": [{"match": {"tableReservations": True}},
                                                       {"terms": {"supportedOrderTypes": order_data}}
                                                       ]}})
            # 0 means all store data
            elif order_data == [0]:
                must_query.append({"terms": {"supportedOrderTypes": [1, 2, 3]}})
            else:
                must_query.append({"terms": {"supportedOrderTypes": order_data + [3]}})
        else:
            pass
    
        # if float(min_ratings_data) != 0.0 and float(max_ratings_data) != 0.0:
        #     range_query.append(
        #         {"range": {"avgRating": {"gte": float(min_ratings_data), "lte": float(max_ratings_data)}}})
        # elif float(max_ratings_data) != 0.0:
        #     range_query.append(
        #         {"range": {"avgRating": {"gte": float(min_ratings_data), "lte": float(max_ratings_data)}}})
        if float(min_ratings_data) != float(max_ratings_data):
            range_query.append(
                {"range": {"avgRating": {"gte": float(min_ratings_data), "lte": float(max_ratings_data)}}})
        # if cuisines_data != "":
        #     spec_data = []
        #     for n in cuisines_data.split():
        #         for spec in n.split(","):
        #             spec_data.append(spec)
        spec_data = []
        if cuisines_data:
            spec_data = [spec for n in cuisines_data.split() for spec in n.split(",")]
        if len(spec_data) > 0:
            must_query.append({"terms": {"specialities": spec_data}})

        # if category_id == DINE_STORE_CATEGORY_ID:
        if avg_cost_max == 0 and avg_cost_min == 0:
            pass
        else:
            range_query.append(
                {
                    "range":
                        {
                            "averageCostForMealForTwo":
                                {
                                    "lte": float(avg_cost_max),
                                    "gte": float(avg_cost_min),
                                }
                        }
                }
            )
            # if avg_cost_max != 0 and avg_cost_min != 0:
            #     range_query.append(
            #         {
            #             "range":
            #                 {
            #                     "averageCostForMealForTwo":
            #                         {
            #                             "lte": float(avg_cost_max),
            #                             "gte": float(avg_cost_min),
            #                         }
            #                 }
            #         }
            #     )
            # elif avg_cost_max != 0 and avg_cost_min == 0:
            #     range_query.append(
            #         {
            #             "range":
            #                 {
            #                     "averageCostForMealForTwo":
            #                         {
            #                             "lte": float(avg_cost_max)
            #                         }
            #                 }
            #         }
            #     )
            # else:
            #     range_query.append(
            #         {
            #             "range":
            #                 {
            #                     "averageCostForMealForTwo":
            #                         {
            #                             "gte": float(avg_cost_min)
            #                         }
            #                 }
            #         }
            #     )
        # else:
        #     if avg_cost_max != 0 and avg_cost_min != 0:
        #         range_query.append(
        #             {
        #                 "range":
        #                     {
        #                         "minimumOrder":
        #                             {
        #                                 "lte": float(avg_cost_max),
        #                                 "gte": float(avg_cost_min),
        #                             }
        #                     }
        #             }
        #         )
        #     elif avg_cost_max != 0 and avg_cost_min == 0:
        #         range_query.append(
        #             {
        #                 "range":
        #                     {
        #                         "minimumOrder":
        #                             {
        #                                 "lte": float(avg_cost_max)
        #                             }
        #                     }
        #             }
        #         )
        #     else:
        #         range_query.append(
        #             {
        #                 "range":
        #                     {
        #                         "minimumOrder":
        #                             {
        #                                 "gte": float(avg_cost_min)
        #                             }
        #                     }
        #             }
        #         )
        # must_not_query.append({"terms": {"storeFrontTypeId": [2]}})
        # if int(sort_data) == 4:
        geo_distance_sort = {
            "_geo_distance": {
                "distance_type": "plane",
                "location": {
                    "lat": float(user_latitude),
                    "lon": float(user_longtitude)
                },
                "order": "desc" if int(sort_data) == 4 else "asc",
                "unit": "km"
            }
        }
        # else:
        #     geo_distance_sort = {
        #         "_geo_distance": {
        #             "distance_type": "plane",
        #             "location": {
        #                 "lat": float(user_latitude),
        #                 "lon": float(user_longtitude)
        #             },
        #             "order": "asc",
        #             "unit": "km"
        #         }
        #     }
        # if int(sort_data) == 0:
        #     sort_query = [
        #         {
        #             "storeIsOpen": {
        #                 "order": "desc"
        #             }
        #         },
        #         geo_distance_sort
        #     ]

        # if int(sort_data) == 1:
        #     sort_query = [
        #         {
        #             "storeIsOpen": {
        #                 "order": "desc"
        #             }
        #         },
        #         geo_distance_sort
        #     ]
        # elif int(sort_data) == 2:
        #     sort_query = [
        #         {
        #             "storeIsOpen": {
        #                 "order": "desc"
        #             }
        #         },
        #         {
        #             "avgRating": {
        #                 "order": "desc"
        #             }
        #         }, geo_distance_sort
        #     ]
        # elif int(sort_data) == 3:
        #     if category_id == DINE_STORE_CATEGORY_ID:
        #         sort_query = [
        #             {
        #                 "storeIsOpen": {
        #                     "order": "desc"
        #                 }
        #             },
        #             {
        #                 "averageCostForMealForTwo": {
        #                     "order": "asc"
        #                 }
        #             },
        #             geo_distance_sort
        #         ]
        #     else:
        #         sort_query = [{
        #             "storeIsOpen": {
        #                 "order": "desc"
        #             }
        #         }, geo_distance_sort]
        # elif int(sort_data) == 4:
        #     if category_id == DINE_STORE_CATEGORY_ID:
        #         sort_query = [
        #             {
        #                 "storeIsOpen": {
        #                     "order": "desc"
        #                 }
        #             }, {
        #                 "averageCostForMealForTwo": {
        #                     "order": "desc"
        #                 }
        #             },
        #             geo_distance_sort
        #         ]
        #     else:
        #         sort_query = [{
        #             "storeIsOpen": {
        #                 "order": "desc"
        #             }
        #         }, geo_distance_sort]
        # else:
        #     sort_query = [
        #         {
        #             "storeIsOpen": {
        #                 "order": "desc"
        #             }
        #         },
        #         {
        #             "averageCostForMealForTwo": {
        #                 "order": "desc"
        #             }
        #         }, geo_distance_sort
        #     ]
        # must_query.append({
        #     "geo_distance": {
        #         "distance": "50km",
        #         "location": {
        #             "lat": float(user_latitude),
        #             "lon": float(user_longtitude)
        #         }
        #     }
        # })
        sorting_options = {
            1: [{"storeIsOpen": {"order": "desc"}}, geo_distance_sort],
            # 2: [{"storeIsOpen": {"order": "desc"}}, {"avgRating": {"order": "desc"}}, geo_distance_sort],
            3: [{"storeIsOpen": {"order": "desc"}},
                {"averageCostForMealForTwo": {"order": "asc"}}, geo_distance_sort],
            4: [{"storeIsOpen": {"order": "desc"}},
                {"averageCostForMealForTwo": {"order": "desc"}}, geo_distance_sort]
        }

        sort_query = sorting_options.get(int(sort_data), [{"storeIsOpen": {"order": "desc"}}, geo_distance_sort])
        if len(must_not_query) > 0:
            store_query = {
                "query":
                    {
                        "bool":
                            {
                                "must": must_query,
                                "must_not": must_not_query,
                                "filter": range_query,
                            }
                    },
                "size": 500,
                "from": 0,
                "sort": sort_query
            }
        else:
            store_query = {
                "query":
                    {
                        "bool":
                            {
                                "must": must_query,
                                "filter": range_query,
                            }
                    },
                "size": 500,
                "from": 0,
                "sort": sort_query
            }
        
        return store_query, must_query, range_query, must_not_query, sort_query

    
    def get_quick_picks_store_query(self, category_id, user_id, zone_id, user_latitude, user_longtitude, language, skip_data,
                               limit_data, sort_data, cuisines_data, avg_cost_max, avg_cost_min, timezone,
                               min_ratings_data,
                               max_ratings_data, order_type,hygiene_rating_data,dietary_preferences,
                               food_Prefereces,allergen,creator_product_type,delivery_filter_data):
        new_query_time = time.time()
        must_query = []
        range_query = []
        must_not_query = []
        pref_response = []
        sort_query = []
        
        # get customer prefrences from customer collection
        print("user_id",user_id)
        customer_preferences = db.customer.find_one({"_id":ObjectId(str(user_id))},{"specialities":1,"dietaryPreferences":1,"allergenList":1,"foodPreferences":1})
        if customer_preferences is not None:
            try:
                # Check if "userCusinePreferences" key exists in the document
                if "specialities" in customer_preferences:
                    # Retrieve the list of user cuisine preferences
                    user_cuisine_preferences = customer_preferences["specialities"]
                    # Handle the case when the list is empty
                    if user_cuisine_preferences:
                        must_query.append({"terms": {"specialities": user_cuisine_preferences}})
                    else:
                        user_cuisine_preferences = []
                else:
                    user_cuisine_preferences = []
                
                        
                
                # Check if "dietaryPreferences" key exists in the document
                if "dietaryPreferences" in customer_preferences:
                    # Retrieve the list of dietary preferences
                    user_dietary_preferences = customer_preferences["dietaryPreferences"]
                    # Extract all "valueId"s from the dietary preferences
                    dietary_value_ids = [item["valueId"] for item in user_dietary_preferences if "valueId" in item]
                    if dietary_value_ids:
                        must_query.append({"terms": {"dietaryPreferencesSupported": dietary_value_ids}})
                    else:
                        dietary_value_ids = []
                else:
                    dietary_value_ids = []
                        
                # Check if "allergenList" key exists in the document
                if "allergenList" in customer_preferences:
                    # Retrieve the list of dietary preferences
                    user_allergen_iist = customer_preferences["allergenList"]
                    # Extract all "valueId"s from the dietary preferences
                    allergen_value_ids = [item["valueId"] for item in user_allergen_iist if "valueId" in item]
                    if allergen_value_ids:
                        must_query.append({"terms": {"allergenList": allergen_value_ids}})
                    else:
                        allergen_value_ids = []
                else:
                    allergen_value_ids = []
                        
                # Check if "foodPreferences" key exists in the document
                if "foodPreferences" in customer_preferences:
                    # Retrieve the list of dietary preferences
                    user_food_Preferences = customer_preferences["foodPreferences"]
                    # Extract all "valueId"s from the dietary preferences
                    food_value_ids = [item["valueId"] for item in user_food_Preferences if "valueId" in item]
                    if food_value_ids:
                        must_query.append({"terms": {"foodPreferencesSupported": food_value_ids}})
                    else:
                        food_value_ids = []
                else:
                    food_value_ids = []

            except Exception as e:
                print("An error occurred:", e)
                user_cuisine_preferences = []
                dietary_value_ids = []
                allergen_value_ids = []
                food_value_ids = []
        else:
            print("No document found for user_id:", user_id)
            return pref_response, must_query, range_query, must_not_query, sort_query
        
        if not any([user_cuisine_preferences, dietary_value_ids, allergen_value_ids, food_value_ids]):
            return pref_response, must_query, range_query, must_not_query, sort_query

        
        
            
        # if hygiene_rating_data is given then add query
        if hygiene_rating_data:
            must_query.append({"match": {"hygieneRating": {"gte":int(hygiene_rating_data)}}})

        # if dietary_preferences is given then add query
        delivery_types_filter = [delivery_types for n in delivery_filter_data.split() for delivery_types in n.split(",")]
        if len(delivery_types_filter) > 0:
            must_query.append({"terms": {"deliveryTypes._id": delivery_types_filter}})

        # if dietary_preferences is given then add query
        diet_pref_data = [diet_pref for n in dietary_preferences.split() for diet_pref in n.split(",")]
        if len(diet_pref_data) > 0:
            must_query.append({"terms": {"dietaryPreferencesSupported": diet_pref_data}})

        # if food_Prefereces is given then add query
        food_pref_data = [food_pref for n in food_Prefereces.split() for food_pref in n.split(",")]
        if len(food_pref_data) > 0:
            must_query.append({"terms": {"foodPreferencesSupported": food_pref_data}})

        # if allergen is given then add query
        allergen_list_data = [allergen_list for n in allergen.split() for allergen_list in n.split(",")]
        if len(allergen_list_data) > 0:
            must_query.append({"terms": {"allergenList": allergen_list_data}})
        
        # if creator_product_type is given then add query
        creator_prod_type_data = [creator_prod_type for n in creator_product_type.split() for creator_prod_type in n.split(",")]
        if len(creator_prod_type_data) > 0:
            must_query.append({"terms": {"creatorProductType": creator_prod_type_data}})

        must_query.append({"match": {"categoryId": category_id}})
        must_query.append({"match": {"status": 1}})
        zone_ids = [ids for n in zone_id.split() for ids in n.split(",")]
        if len(zone_ids) > 0:
            must_query.append({"terms": {"serviceZones.zoneId": zone_ids}})
        # must_not_query.append({"terms": {"favouriteUsers": [user_id]}})

        order_data = [int(spec) for n in order_type.split() for spec in n.split(",")]
        if len(order_data) > 0:
            # 3 is table reservation
            if order_data == [3]:
                must_query.append({"match": {"tableReservations": True}})
            elif 3 in order_data:
                must_query.append({"bool": {"should": [{"match": {"tableReservations": True}},
                                                       {"terms": {"supportedOrderTypes": order_data}}
                                                       ]}})
            # 0 means all store data
            elif order_data == [0]:
                must_query.append({"terms": {"supportedOrderTypes": [1, 2, 3]}})
            else:
                must_query.append({"terms": {"supportedOrderTypes": order_data + [3]}})
        else:
            pass
    
        # if float(min_ratings_data) != 0.0 and float(max_ratings_data) != 0.0:
        #     range_query.append(
        #         {"range": {"avgRating": {"gte": float(min_ratings_data), "lte": float(max_ratings_data)}}})
        # elif float(max_ratings_data) != 0.0:
        #     range_query.append(
        #         {"range": {"avgRating": {"gte": float(min_ratings_data), "lte": float(max_ratings_data)}}})
        if float(min_ratings_data) != float(max_ratings_data):
            range_query.append(
                {"range": {"avgRating": {"gte": float(min_ratings_data), "lte": float(max_ratings_data)}}})
        # if cuisines_data != "":
        #     spec_data = []
        #     for n in cuisines_data.split():
        #         for spec in n.split(","):
        #             spec_data.append(spec)
        spec_data = []
        if cuisines_data:
            spec_data = [spec for n in cuisines_data.split() for spec in n.split(",")]
        if len(spec_data) > 0:
            must_query.append({"terms": {"specialities": spec_data}})

        # if category_id == DINE_STORE_CATEGORY_ID:
        if avg_cost_max == 0 and avg_cost_min == 0:
            pass
        else:
            range_query.append(
                {
                    "range":
                        {
                            "averageCostForMealForTwo":
                                {
                                    "lte": float(avg_cost_max),
                                    "gte": float(avg_cost_min),
                                }
                        }
                }
            )

        geo_distance_sort = {
            "_geo_distance": {
                "distance_type": "plane",
                "location": {
                    "lat": float(user_latitude),
                    "lon": float(user_longtitude)
                },
                "order": "desc" if int(sort_data) == 4 else "asc",
                "unit": "km"
            }
        }
       
        sorting_options = {
            1: [{"storeIsOpen": {"order": "desc"}}, geo_distance_sort],
            # 2: [{"storeIsOpen": {"order": "desc"}}, {"avgRating": {"order": "desc"}}, geo_distance_sort],
            3: [{"storeIsOpen": {"order": "desc"}},
                {"averageCostForMealForTwo": {"order": "asc"}}, geo_distance_sort],
            4: [{"storeIsOpen": {"order": "desc"}},
                {"averageCostForMealForTwo": {"order": "desc"}}, geo_distance_sort]
        }

        sort_query = sorting_options.get(int(sort_data), [{"storeIsOpen": {"order": "desc"}}, geo_distance_sort])
        if len(must_not_query) > 0:
            store_query = {
                "query":
                    {
                        "bool":
                            {
                                "must": must_query,
                                "must_not": must_not_query
                                # "filter": range_query,
                            }
                    },
                "size": 500,
                "from": 0,
                "sort": sort_query
            }
        else:
            store_query = {
                "query":
                    {
                        "bool":
                            {
                                "must": must_query
                                # "filter": range_query,
                            }
                    },
                "size": 500,
                "from": 0,
                "sort": sort_query
            }
        
        return store_query, must_query, range_query, must_not_query, sort_query

    def NotificationLogs(user_id, app_name, store_category_id, limit, language):
        try:
            print("notification logs called")
            notification_list = []
            total_count = 0

            # for the notification data
            notification_id = ""
            # user type
            user_type = ""
            # notification type
            notification_type = ""

            notification_get_query = {"app_name": app_name, "isBusiness": False}
            if user_id != "" and store_category_id != "":
                notification_get_query["userid"] = user_id
                notification_get_query["store_category_id"] = store_category_id
            elif user_id != "":
                notification_get_query["userid"] = user_id
            elif notification_id != "":
                notification_get_query["notification_id"] = notification_id
            elif user_type != "":
                notification_get_query["user_type"] = int(user_type)
            elif notification_type != "":
                notification_get_query["notification_type"] = int(notification_type)
            else:
                notification_get_query["store_category_id"] = int(store_category_id)
            total_count = []
            notification_get_query["action"]=  {"$exists": False} 

            print("limit", limit)
            print("query",notification_get_query)

            notificatin_details = db.notificationLogs.aggregate(
                [
                    {
                        "$match": notification_get_query
                    },
                    {
                        "$group":
                            {
                                "_id": {
                                    "notificationId": "$notificationMongoId",
                                },
                                "id": {"$first": "$_id"}
                            }
                    },
                    {"$sort": {"id": -1}},
                    {"$limit": int(limit)}
                ]
            )
            notificatin_details_count = db.notificationLogs.aggregate(
                [
                    {
                        "$match": notification_get_query
                    },
                    {
                        "$group":
                            {
                                "_id": {
                                    "notificationId": "$notificationId"
                                },
                                "count": {"$sum": 1}
                            }
                    }
                ]
            )
            for count in notificatin_details_count:
                total_count.append(count['count'])

            for logs1 in notificatin_details:
                logs = db.notificationLogs.find_one({"_id": ObjectId(logs1['id'])})
                if logs is not None:
                    if logs['user_type'] == 1:
                        user_type_msg = "Individual"
                    elif logs['user_type'] == 2:
                        user_type_msg = "City"
                    elif logs['user_type'] == 3:
                        user_type_msg = "Zone"
                    else:
                        user_type_msg = "Radius"

                    from_zone = tz.gettz('UTC')
                    to_zone = tz.gettz('Asia/Kolkata')
                    date_time1 = datetime.datetime.fromtimestamp(logs['createdtimestamp'] / 1000)
                    print(date_time1)
                    date_time1 = (date_time1).strftime('%Y-%m-%d %H:%M:%S')
                    date_time1 = datetime.datetime.strptime(date_time1, '%Y-%m-%d %H:%M:%S')
                    utc = date_time1.replace(tzinfo=from_zone)
                    central = utc.astimezone(to_zone)
                    dt_object = date_time1  # logs['createdtimestamp']
                    day_s = datetime.datetime.now() - dt_object
                    if day_s.days == 0:
                        if int(day_s.seconds) > 59:
                            sec = datetime.timedelta(seconds=day_s.seconds)
                            if int(sec.seconds / 60) > 59:
                                time_create = str(
                                    int(sec.seconds / 3600)) + "hours ago"
                            else:
                                time_create = str(
                                    int(sec.seconds / 60)) + "minutes ago"
                        else:
                            time_create = str(
                                day_s.seconds) + "seconds ago"

                    else:
                        time_create = str(day_s.days) + "days ago"

                    if int(logs['notification_type']) == 1:
                        notification_type_msg = "Normal"
                    else:
                        notification_type_msg = "Riach"

                    try:
                        master_order_id = logs['master_order_id']
                    except:
                        master_order_id = ""

                    try:
                        store_order_id = logs['store_order_id']
                    except:
                        store_order_id = ""

                    try:
                        product_order_id = logs['product_order_id']
                    except:
                        product_order_id = ""

                    try:
                        package_id = logs['package_id']
                    except:
                        package_id = ""

                    try:
                        image_data = logs['image_url']
                    except:
                        image_data = ""

                    if image_data == None:
                        image_data = ""

                    try:
                        store_category_details = db.storeCategory.find_one({"_id": ObjectId(logs['store_category_id'])})
                        if store_category_details is not None:
                            store_type = store_category_details["type"]
                        else:
                            store_type = 0
                    except:
                        store_type = 0

                    notification_list.append({
                        "notificationId": str(logs['_id']),
                        "appName": logs['app_name'],
                        "storeCategoryType": store_type,
                        "storeCategoryId": logs['store_category_id'] if "store_category_id" in logs else "",
                        "userType": logs['user_type'],
                        "userTypeMsg": user_type_msg,
                        "body": logs['body'],
                        "image": image_data,
                        "userName": logs['user_name'],
                        "isSeen": logs['isSeen'] if "isSeen" in logs else False,
                        "isBusiness": logs['isBusiness'] if "isBusiness" in logs else False,
                        "masterOrderId": master_order_id,
                        "storeOrderId": store_order_id,
                        "productOrderId": product_order_id,
                        "packageId": package_id,
                        "notificationTypeMsg": notification_type_msg,
                        "topic": user_type_msg,
                        "notificationType": logs['notification_type'],
                        "title": logs['title'],
                        "date": (central).strftime('%Y-%m-%d %H:%M:%S'),
                        "day": time_create
                    })

            response = {
                "data": notification_list,
                "total_count": len(total_count)
            }
            return response
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            return notification_pb2.NotificationReply(message=message)



    def process_user_notification_get_api(self, user_id, app_name, store_category_id, to_data, from_data,language):
        """
        This method gets user notification data
        :return:
        """
        
        # user_id = "5df9c35ab1f73a18c396aa0c"
        update_query = {"userid": user_id}
        thread_logs = threading.Thread(target=DbHelper.update_notification_logs,
                                        args=(update_query, ))
        thread_logs.start()

        try:
            notification_response = OperationHelper.NotificationLogs(user_id, app_name, store_category_id, to_data, language)

                ##### get notification count #####
            notification_count_query = {"app_name": APP_NAME, "userid": user_id, "isSeen": False}
            if store_category_id != "":
                notification_count_query['store_category_id'] = store_category_id
            notification_count = DbHelper.get_notification_count(notification_count_query)

            ##### compile response and return response #####
            try:
                if len(notification_response['data']) == 0:
                    notification_response = OperationHelper.NotificationLogs(user_id, app_name, store_category_id, to_data, language)
                else:
                    pass
                response_data = {
                    "data": notification_response['data'][from_data:to_data],
                    "message": "data found",
                    "notificationCount": notification_count,
                    "total_count": notification_response['total_count']
                }
                return ResponseHelper.get_status_200(response_data)

            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                print('Error on line {}'.format(sys.exc_info()
                                                [-1].tb_lineno), type(ex).__name__, ex)
                response_data = {
                    "data": [],
                    "message": "data not found",
                    "total_count": 0
                }
                return ResponseHelper.get_status_404(response_data)
        except Exception as e:
            notification_response = OperationHelper.NotificationLogs(user_id, app_name, store_category_id, to_data, language)

            ##### get notification count #####
            notification_count_query = {"app_name": APP_NAME, "userid": user_id, "isSeen": False}
            if store_category_id != "":
                notification_count_query['store_category_id'] = store_category_id
            notification_count = DbHelper.get_notification_count(notification_count_query)

            ##### compile response and return response #####
            try:
                if len(notification_response['data']) == 0:
                    notification_response = OperationHelper.NotificationLogs(user_id, app_name, store_category_id, to_data, language)
                else:
                    pass
                response_data = {
                    "data": notification_response['data'][from_data:to_data],
                    "message": "data found",
                    "notificationCount": notification_count,
                    "total_count": notification_response['total_count']
                }
                return ResponseHelper.get_status_200(response_data)

            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                print('Error on line {}'.format(sys.exc_info()
                                                [-1].tb_lineno), type(ex).__name__, ex)
                response_data = {
                    "data": [],
                    "message": "data not found",
                    "total_count": 0
                }
                return ResponseHelper.get_status_404(response_data)

    def process_seller_rating_get_api(self, seller_id):
        """
        This api gets seller rating data and returns data to api
        :return:
        """
        ##### get data from database #####
        seller_list = []
        seller_id = seller_id.replace("%3F", "")
        seller_id = seller_id.replace("?", "")
        seller_details = DbHelper.get_seller_rating({"sellerId": str(seller_id), "attributeId": {"$ne": ""}})

        ##### for each seller rating entry, process data #####
        for seller in seller_details:
            if seller['attributeId'] != "":
                attribute_details = DbHelper.get_rating_parameter({"_id": ObjectId(seller['attributeId'])}, {"name": 1})
                if attribute_details != None:
                    attribute_name = attribute_details['name']['en']
                else:
                    attribute_name = ""
            else:
                attribute_name = ""

            ##### combine data as json in list #####
            if attribute_name != "":
                seller_list.append({
                    "reviewId": str(seller['_id']),
                    "name": attribute_name,
                    "attributeId": str(seller['attributeId']),
                    "rating": seller['rating']
                })
            else:
                pass

        ##### process data dn finf average of rating #####
        if len(seller_list) > 0:
            dataframe = pd.DataFrame(seller_list)
            dataframe['TotalStarRating'] = round(dataframe.groupby(
                ['attributeId'])['rating'].transform('mean'), 2)
            dataframe = dataframe.drop_duplicates(
                subset=['attributeId'], keep="last")
            dataframe_attribute = dataframe.to_json(orient='records')
            product_rating_data = json.loads(dataframe_attribute)
            response = {
                "data": product_rating_data,
                "message": "data found"
            }
            return ResponseHelper.get_status_200(response)
        else:
            response = {
                "data": [],
                "message": "data not found"
            }
            return ResponseHelper.get_status_404(response)

    def process_seller_reviews_get_api(self, seller_id, from_data, to_data):
        """
        This api gets seller review data
        :param seller_id:
        :return:
        """
        ##### get data and count from database #####
        seller_id = seller_id.replace("%3F", "")
        seller_id = seller_id.replace("?", "")
        seller_list = []
        query = {"sellerId": str(seller_id), "status": 1, "sellerReview": {"$ne": ""}}
        sort_query = [("createdTimestamp", -1)]
        seller_details = DbHelper.get_seller_rating(query, skip=int(from_data),
                                                    limit=int(to_data), sort_query=sort_query)
        seller_details_count = DbHelper.get_seller_review_rating_count(query)

        ##### for each review, process data #####
        for seller in seller_details:
            try:
                dt_object = datetime.datetime.fromtimestamp(
                    seller['createdTimestamp'])
            except:
                dt_object = seller['createdTimestamp']

            ##### convert review timestamp into date format #####
            from_zone = tz.gettz('UTC')
            to_zone = tz.gettz('Asia/Kolkata')
            date_time1 = (dt_object).strftime('%d %b %Y %H:%M:%S %p')
            date_time1 = datetime.datetime.strptime(
                date_time1, '%d %b %Y %H:%M:%S %p')
            utc = date_time1.replace(tzinfo=from_zone)
            central = utc.astimezone(to_zone)

            ##### find how much time ago, review was given #####
            day_s = datetime.datetime.now() - dt_object
            if day_s.days == 0:
                if int(day_s.seconds) > 59:
                    sec = datetime.timedelta(seconds=day_s.seconds)
                    if int(sec.seconds / 60) > 59:
                        time_create = str(int(sec.seconds / 3600)) + " hours ago"
                    else:
                        time_create = str(int(sec.seconds / 60)) + " minutes ago"
                else:
                    time_create = str(day_s.seconds) + " seconds ago"

            else:
                time_create = str(day_s.days) + " days ago"

            ##### query database for average rating for particular order #####
            avg_rating_value = 0
            seller_rating = DbHelper.get_aggregate_review_rating(str(seller_id), seller['orderId'])
            for avg_rating in seller_rating:
                avg_rating_value = avg_rating['avgRating']

            if avg_rating_value is None:
                avg_rating_value = 0

            if "plateformName" in seller:
                plateformName = seller["plateformName"]
            else:
                plateformName = ""            

            #####combine data #####
            seller_list.append({
                "dateTime": (central).strftime('%d %b %Y'),
                "reviewId": str(seller['_id']),
                "userName": seller['userName'],
                "sellerSince": time_create,
                "sellerReview": seller['sellerReview'],
                "attributeId": str(seller['attributeId']),
                "rating": round(avg_rating_value, 2),
                "plateformName" : plateformName
            })

        if len(seller_list) > 0:
            response = {
                "data": seller_list,
                "penCount": seller_details_count,
                "message": "data found"
            }
            return ResponseHelper.get_status_200(response)
        else:
            response = {
                "data": [],
                "penCount": 0,
                "message": "data not found"
            }
            return ResponseHelper.get_status_404(response)
