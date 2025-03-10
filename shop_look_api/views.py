import datetime
import time
from django.http import JsonResponse
import pandas as pd
from rest_framework.views import APIView
from mongo_query_module.query_module import zone_find
from search.views import get_linked_unit_attribute, search_read_stores
from search_api.settings import currency_exchange_rate, es, CASSANDRA_KEYSPACE, CENRAL_STORE_NAME, CENTRAL_PRODUCT_DOC_TYPE, CENTRAL_PRODUCT_INDEX, CENTRAL_PRODUCT_VARIENT_INDEX, CHILD_PRODUCT_DOC_TYPE, CHILD_PRODUCT_INDEX, ECOMMERCE_STORE_CATEGORY_ID, MEAT_STORE_CATEGORY_ID, OFFER_DOC_TYPE, OFFER_INDEX, PYTHON_PRODUCT_URL, STORE_CREATE_TIME, STORE_PRODUCT_DOC_TYPE, STORE_PRODUCT_INDEX, TIME_ZONE, WEBSITE_URL, db, DINE_STORE_CATEGORY_ID, session
from epic_api.utils import *
from drf_yasg.utils import *
from rest_framework.decorators import action
from validations.product_unit_validation import validate_units_data
from validations.product_validation import best_offer_function_validate, best_supplier_function_cust, cal_star_rating_product, get_avaialable_quantity, home_units_data, linked_unit_attribute, linked_variant_data, product_type_validation
from validations.product_colour_details import colour_data


central_store = CENRAL_STORE_NAME
central_zero_store_creation_ts = int(STORE_CREATE_TIME)

currentDate = datetime.datetime.now()

index_central_product = CENTRAL_PRODUCT_INDEX
doc_central_product = CENTRAL_PRODUCT_DOC_TYPE

index_central_varient_product = CENTRAL_PRODUCT_VARIENT_INDEX

index_products = CHILD_PRODUCT_INDEX
doc_type_products = CHILD_PRODUCT_DOC_TYPE

index_store = STORE_PRODUCT_INDEX
doc_type_store = STORE_PRODUCT_DOC_TYPE

index_offers = OFFER_INDEX
doc_type_offers = OFFER_DOC_TYPE

SERVER = PYTHON_PRODUCT_URL
try:
    session.set_keyspace(CASSANDRA_KEYSPACE)
except:
    pass

timezonename = TIME_ZONE



class ShoopLook(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["shopTheLook"],
        operation_description="API for shopTheLook",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="storeCategoryId",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="61f98062bd236905517efa9a",
                required=False,
                description="enter storeCategoryId if avalible ",
            ),
            openapi.Parameter(
                name="storeId",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="61f8dc2a610b34546196a44b",
                required=False,
                description="enter storeId if avalible",
            ),
            openapi.Parameter(
                name="status",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example="1",
                required=False,
                description="0: Pending  or 1: Active or 2: Deactive or 3: delete",
            ),
            openapi.Parameter(
                name="search",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="what",
                required=False,
                description="Enter title for",
            ),
        ],
        responses={
            200: "data added successfully",
            422: "mandatory fields are missing",
            500: "internal server error"
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            status = int(request.GET.get('status', "0"))
            store_id = request.GET.get("storeId", "")
            store_category_id = request.GET.get('storeCategoryId', "")
            search_query = request.GET.get("search", "")
            final_data = []
            payload = {'status': status}
            if store_id != "":
                payload['storeId'] = store_id
            else:
                pass
            if store_category_id != "":
                payload['storeCategoryId'] = store_category_id
            else:
                pass
            if search_query != "":
                search_title = '.*' + search_query
                payload['title.en'] = {'$regex': search_title, "$options": "i"}
            else:
                pass
            get_data = db.shopTheLook.find(payload)
            if get_data is None:
                response_data = {'message': "data not found"}
                return JsonResponse(response_data, status=404)
            else:
                count_data = db.shopTheLook.find(payload).count()
                for data in get_data:
                    final_data.append({
                        "id": str(data['_id']),
                        "storeId": data['storeId'],
                        "storeCategoryId": data['storeCategoryId'],
                        "title": data['title'],
                        "createdOn": data['createdOn'] if 'createdOn' in data else "",
                        "imageSize": data['imageSize'],
                        "imageUrl": data['imageUrl'],
                        "annotations": data['annotations'],
                    })
                if count_data > 0:
                    response_data = {
                        "data": final_data,
                        "penCount": int(count_data),
                        "message": "data found"
                    }
                    return JsonResponse(response_data, status=200)
                else:
                    response_data = {
                        "message": "data  not found",
                        "penCount": int(count_data),
                    }

                    return JsonResponse(response_data, status=404)

        except Exception as ex:
            return UtilsObj.raise_exception(ex)

    @swagger_auto_schema(
        method="post",
        tags=["shopTheLook"],
        operation_description="API for shopTheLook",
        required=["AUTHORIZATION"],
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            required=['storeId', 'storeCategoryId',
                      'title', 'imageSize', 'imageUrl', 'annotations'],
            properties={
                "storeId": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    example="61f8dc2a610b34546196a44b",
                    description="Add storeId"
                ),
                "storeCategoryId": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    example="5cc0846e087d924456427975",
                    description="Add storeCategoryId"
                ),
                "title": openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    required=['en'],
                    properties={
                        "en": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            example="baby shampoo",
                            description="baby shampoo"
                        ),
                    }
                ),
                "imageSize": openapi.Schema(
                    description="Add ImageSize in dict form",
                    type=openapi.TYPE_OBJECT,
                    required=['Horizontal', "Vertical"],
                    properties={
                        "Horizontal": openapi.Schema(
                            type=openapi.TYPE_INTEGER,
                            example=1,
                            description="Horizontal"
                        ),
                        "Vertical": openapi.Schema(
                            type=openapi.TYPE_INTEGER,
                            example=1,
                            description="Vertical"
                        ),
                    }
                ),
                "imageUrl": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    example="https://clean2go.s3.ca-central-1.amazonaws.com/admin/1612951844507.svg",
                    description="ImageUrl"
                ),
                "annotations": openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    description="Add annotaions",
                    properties={
                        "id": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            example="JHUYH66756545H",
                            description="Horizontal"
                        ),
                        "horizontalDisplacement": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            example="0.10616336017847061",
                            description="Vertical"
                        ),
                        "productName": openapi.Schema(
                            type=openapi.TYPE_OBJECT,
                            required=['en'],
                            properties={
                                "en": openapi.Schema(
                                    type=openapi.TYPE_STRING,
                                    example="baby shampoo",
                                    description="baby shampoo"
                                ),
                            }
                        ),
                        "description": openapi.Schema(
                            type=openapi.TYPE_OBJECT,
                            required=['en'],
                            properties={
                                "en": openapi.Schema(
                                    type=openapi.TYPE_STRING,
                                    example="Lorem Ipsum Dolor Sit Amet, Consectetur Adipiscing Elit, Sed Do Eiusmod Tempor Incididunt Ut Labore Et Dolore Magna Aliqua. Ut Enim Ad Minim Veniam, Quis Nostrud Exercitation",
                                    description="Lorem Ipsum Dolor Sit Amet, Consectetur Adipiscing Elit, Sed Do Eiusmod Tempor Incididunt Ut Labore Et Dolore Magna Aliqua. Ut Enim Ad Minim Veniam, Quis Nostrud Exercitation"
                                ),
                            }
                        ),
                        "verticalDisplacement": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            example="0.10616336017847061",
                            description="Vertical"
                        ),
                        "centralProductId": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            example="4521JHUYHGVQ8ss8",
                            description="Vertical"
                        ),
                        "boundingBox": openapi.Schema(
                            type=openapi.TYPE_OBJECT,
                            description="add boundbox",
                            properties={
                                "Width": openapi.Schema(
                                    type=openapi.TYPE_STRING,
                                    example="0.10616336017847061",
                                ),
                                "Height": openapi.Schema(
                                    type=openapi.TYPE_STRING,
                                    example="0.18528179824352264",
                                ),
                                "Left": openapi.Schema(
                                    type=openapi.TYPE_STRING,
                                    example="0.10616336017847061",
                                ),
                                "Top": openapi.Schema(
                                    type=openapi.TYPE_STRING,
                                    example="0.0037978808395564556",
                                ),
                            }
                        ),
                    }
                ),
            },
        ),
        responses={
            200: "data added successfully",
            422: "mandatory fields are missing",
            500: "internal server error"
        },
    )
    @action(detail=False, methods=["post"])
    def post(self, reuqest):
        data = reuqest.data
        ''' Validation '''
        params_data = ['storeId', 'storeCategoryId',
                       'title', 'imageSize', 'imageUrl', 'annotations']
        if len(data) > 0:
            try:
                err = UtilsObj.check_req_params(data, params_data)
                if err:
                    return err
                if len(data['title']) < 1:
                    response_data = {'message': 'title empty'}
                    return JsonResponse(response_data, status=422)
                elif len(data['imageSize']) < 1:
                    response_data = {'message': 'imageSize empty'}
                    return JsonResponse(response_data, status=422)
                elif len(data['annotations']) < 1:
                    response_data = {'message': 'annotatiosn fied is empty'}
                    return JsonResponse(response_data, status=422)
                else:
                    store_data = {
                        "storeId": data['storeId'],
                        "storeCategoryId": data['storeCategoryId'],
                        "title": data['title'],
                        "imageSize": data['imageSize'],
                        "status": 0,
                        "imageUrl": data['imageUrl'],
                        "createdOn": int(datetime.datetime.now().timestamp()),
                        "annotations": data['annotations'],
                    }

                    image_data = db.shopTheLook.insert_one(store_data)
                    response_data = {"message": "data added successfully"}
                    return JsonResponse(response_data, status=200)
            except Exception as ex:
                return UtilsObj.raise_exception(ex)
        else:
            response_data = {'message': 'request Body missind'}
            return JsonResponse(response_data, status=422)

    @swagger_auto_schema(
        method="patch",
        tags=["shopTheLook"],
        operation_description="API for shopTheLook",
        required=["AUTHORIZATION"],
        manual_parameters=[],
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            required=['id', 'status'],
            properties={
                "id": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    example="622f230119dba5409d0819b1",
                    description="Add answer"
                ),
                "status": openapi.Schema(
                    type=openapi.TYPE_INTEGER,
                    example="1",
                    description=" 0 for pending approval, 1 for active ,2 for inactive, 3 for delete "
                ),
            }
        ),
        responses={
            200: "data added successfully",
            404: "data not found",
            422: "mandatory fields are missing",
            500: "internal server error"
        },
    )
    @action(detail=False, methods=["patch"])
    def patch(self, request):
        ''' 0 for pending approval
            1 for active 
            2 for inactive 
        '''

        data = request.data
        if len(data) > 0:
            try:
                params_data = ['id', 'status']
                err = UtilsObj.check_req_params(data, params_data)
                if err:
                    return err
                id = data['id']
                check_data = db.shopTheLook.find_one(
                    {"_id": ObjectId(str(id))})

                if check_data is None:
                    response_data = {'message': 'document not found'}
                    return JsonResponse(response_data, status=404)
                else:
                    data_status = int(data['status'])
                    if data_status > 3:
                        response_ans = {
                            'message': 'status must be 0 or 1 or 2 or 3'}
                        return JsonResponse(response_ans, status=422)
                    update_data = db.shopTheLook.update_one(
                        {"_id": ObjectId(str(id))},
                        {"$set": {"status": data_status}})
                    response_data = {'message': 'data update successfully'}
                    return JsonResponse(response_data, status=200)
            except Exception as ex:
                return UtilsObj.raise_exception(ex)
        else:
            response_data = {'message': 'request Body missinng'}
            return JsonResponse(response_data, status=422)

    @swagger_auto_schema(
        method="delete",
        tags=["shopTheLook"],
        operation_description="API for shopTheLook",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name="id",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="622f230119dba5409d0819b1",
                required=True,
                description="Enter shopLook id",
            ),
        ],
        responses={
            200: "data added successfully",
            404: "data not found",
            422: "mandatory fields are missing",
            500: "internal server error"
        },
    )
    @action(detail=False, methods=["delete"])
    def delete(self, request):
        ''' status 3 for delete '''
        try:
            id = request.GET.get('id', "")
            if id == "":
                response_ans = {'message': "Id filed missing"}
                return JsonResponse(response_ans, status=422)
            else:
                find_data = db.shopTheLook.find_one({"_id": ObjectId(id)})
                if find_data is None:
                    response_ans = {"message": "data not found"}
                    return JsonResponse(response_ans, status=404)
                delete_data = db.shopTheLook.update_one(
                    {"_id": ObjectId(str(id))},
                    {"$set": {"status": 3}})
                response_data = {'message': 'data update successfully'}
                return JsonResponse(response_data, status=200)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)


class ShopTheLookPDP(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["shopTheLook"],
        operation_description="API for shopTheLook product details page",
        required=["AUTHORIZATION"],
        manual_parameters=[
            openapi.Parameter(
                name='Authorization',
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description="authorization token"),
            openapi.Parameter(
                name='language',
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=False,
                default="en",
                description="language"),
            openapi.Parameter(
                name='currencyCode',
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=False,
                default="INR",
                description="currencyCode default: INR"),
            openapi.Parameter(
                name="storeId",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="61f8dc2a610b34546196a44b",
                required=False,
                description="store id if available - this is not mandatory",
            ),
            openapi.Parameter(
                name="parentProductId",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="",
                required=True,
                description="parentProductId of the product.",
            ),
            openapi.Parameter(
                name="childProductId",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="",
                required=True,
                description="childProductId id of the product",
            ),
        ],
        responses={
            200: "data added successfully",
            422: "mandatory fields are missing",
            500: "internal server error"
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            parent_productId = request.GET.get("parentProductId", "")
            productId = request.GET.get("productId", "")
            store_id = request.GET.get("storeId", "")

            err = UtilsObj.check_req_params(request.GET, ["parentProductId", "productId"])
            if err: return err

            currency_code = request.META.get("currencyCode", "INR")
            language = request.META.get("language", "en")

            err = UtilsObj.check_authentication(request)
            if err: return err

            # user_id = "5ead9a07a70ab57b4bdb334c"
            user_id = UtilsObj.get_userId(request)
            if not type(user_id) == str: return user_id

            mquery = {
                "annotations.centralProductId": parent_productId,
                "status": 1,
            }

            child_product_ids = []
            annotations_prod = db.shopTheLook.find(mquery)
            if annotations_prod.count():
                for mpro in annotations_prod:
                    for pro in mpro["annotations"]:
                        child_product_ids.append(pro["productId"])

            prod_annotations = []
            if productId:
                mquery["annotations.productId"] = productId
            if store_id:
                mquery["storeId"] = store_id
            annotations_prod = db.shopTheLook.find(mquery)
            if annotations_prod.count():
                for mpro in annotations_prod:
                    for pro in mpro["annotations"]:
                        try: pro["productTitle"] = mpro["title"]["en"]
                        except: pro["productTitle"] = ""

                        try: pro["title"] = pro["title"]["en"]
                        except: pro["title"] = ""

                        try: pro["_id"] = str(mpro["_id"])
                        except: pro["_id"] = ""

                        try: pro["imageUrl"] = str(mpro["imageUrl"])
                        except: pro["imageUrl"] = ""

                        try: pro["imageSize"] = mpro["imageSize"]
                        except: pro["imageSize"] = ""
                        prod_annotations.append(pro)

            filters = []
            must_query = [
                {
                    "match": {
                        "parentProductId": str(parent_productId)
                    }
                },
            ]
            if len(child_product_ids):
                filters.append({
                    "terms": {
                        "childproductid": child_product_ids
                    }
                })

            if store_id != "" and store_id:
                must_query.append({
                    "match": {
                        "storeId": str(store_id)
                    }
                })

            bool_list = {
                "must": must_query,
                "filter": filters
            }
            final_product_query = {
                "query": {
                    "bool": bool_list
                },
                "track_total_hits": True,
            }

            print(final_product_query)
            res = es.search(index=index_products, body=final_product_query)
            all_product_ids = []
            policies = {
                "returnPolicy": {},
                "exchangePolicy": {},
                "replacementPolicy": {}
            }
            if "hits" in res:
                if "total" in res["hits"]:
                    if "value" in res["hits"]["total"]:
                        for pro in res["hits"]["hits"]:
                            pro["_source"]["_id"] = pro["_id"]
                            all_product_ids.append(pro["_source"])
            product_response = []
            product_data = []
            for child_product_details in all_product_ids:
                try:
                    # =====================this block for check the product is available in dc or not================
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
                    currency_details = db.currencies.find_one({"currencyCode": currency_code})
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
                                if "taxValue" in tax:
                                    tax_value.append({"value": tax["taxValue"]})
                                else:
                                    pass
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

                    query = {"parentProductId": str(child_product_details["parentProductId"]), "status": 1}
                    query["storeId"] = child_product_details["storeId"]
                    variant_count_data = db.childProducts.find(query).count()
                    if variant_count_data > 1:
                        variant_count = True
                    else:
                        variant_count = False
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

                    if "unitSizeGroupValue" in child_product_details["units"][0]:
                        if len(child_product_details["units"][0]["unitSizeGroupValue"]) > 0:
                            if language in child_product_details["units"][0]["unitSizeGroupValue"]:
                                if child_product_details["units"][0]["unitSizeGroupValue"][language] != "":
                                    is_size_available = True
                                else:
                                    is_size_available = False
                            elif "en" in child_product_details["units"][0]["unitSizeGroupValue"]:
                                if child_product_details["units"][0]["unitSizeGroupValue"]["en"] != "":
                                    is_size_available = True
                                else:
                                    is_size_available = False
                            else:
                                is_size_available = False
                        else:
                            is_size_available = False
                    else:
                        is_size_available = False

                    # ==========================highlight data=================================
                    hightlight_data = []
                    if "highlights" in child_product_details["units"][0]:
                        if child_product_details["units"][0]["highlights"] != None:
                            for highlight in child_product_details["units"][0]["highlights"]:
                                try:
                                    hightlight_data.append(
                                        highlight[language] if language in highlight else highlight["en"]
                                    )
                                except:
                                    pass
                        else:
                            pass

                    hightlight_data = list(set(hightlight_data))
                    hightlight_data = [x for x in hightlight_data if x]
                    colour_details = colour_data(
                        child_product_details["parentProductId"], []
                    )

                    if type(child_product_details["units"][0]) == list:
                        attribute_list = child_product_details["units"][0][0]["attributes"]
                    else:
                        attribute_list = child_product_details["units"][0]["attributes"]
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

                    if len(variant_data_list) == 1 and is_size_available == False:
                        is_size_available = True
                    detail_description = (
                        child_product_details["detailDescription"][language]
                        if language in child_product_details["detailDescription"]
                        else child_product_details["detailDescription"]["en"]
                    )

                    isShoppingList = False
                    if child_product_details["storeCategoryId"] == ECOMMERCE_STORE_CATEGORY_ID:
                        isShoppingList = True
                    else:
                        shoppinglist_product = db.userShoppingList.find(
                            {
                                "userId": user_id,
                                "products.centralProductId": child_product_details["parentProductId"],
                                "products.childProductId": str(child_product_details["_id"]),
                            }
                        )
                        if shoppinglist_product.count() == 0:
                            isShoppingList = False
                        else:
                            isShoppingList = True

                    try:
                        linked_attribute = get_linked_unit_attribute(child_product_details["units"])
                    except:
                        linked_attribute = []

                    response_casandra = session.execute(
                        """SELECT * FROM favouriteproductsuserwise where userid=%(userid)s AND childproductid=%(productid)s ALLOW FILTERING""",
                        {"userid": user_id, "productid": str(child_product_details["_id"])},
                    )

                    isFavourite = False
                    if not response_casandra:
                        isFavourite = False
                    else:
                        for fav in response_casandra:
                            isFavourite = True

                    model_data = []
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
                    image_data = []
                    if len(child_product_details["units"][0]["image"]) > 0:
                        image_data = child_product_details["units"][0]["image"]
                    else:
                        image_data = [
                            {"extraLarge": "", "medium": "", "altText": "", "large": "", "small": ""}
                        ]

                    try:
                        if "memberPrice" in child_product_details["units"][0] and "discountPriceForNonMembers" in \
                                child_product_details["units"][0]:
                            pass
                        else:
                            if not "memberPrice" in child_product_details["units"][0]:
                                child_product_details["units"][0]["memberPrice"] = child_product_details["units"][0][
                                    "floatValue"]
                            if not "discountPriceForNonMembers" in child_product_details["units"][0]:
                                child_product_details["units"][0]["discountPriceForNonMembers"] = \
                                child_product_details["units"][0]["floatValue"]
                    except:
                        pass

                    try:
                        reseller_commission = child_product_details['units'][0]['b2cPricing'][0]['b2cresellerCommission']
                    except:
                        reseller_commission = 0

                        # try:
                        #     reseller_commission_type = primary_child_product['units'][0]['b2cPricing']['b2cpercentageCommission']
                        # except:

                    if "returnPolicy" in child_product_details:
                        policies["returnPolicy"] = child_product_details["returnPolicy"]
                    if "exchangePolicy" in child_product_details:
                        policies["exchangePolicy"] = child_product_details["exchangePolicy"]
                    if "replacementPolicy" in child_product_details:
                        policies["replacementPolicy"] = child_product_details["replacementPolicy"]


                    reseller_commission_type = 0
                    product_data.append(
                        {
                            "isSizeAvailable": is_size_available,
                            "detailDescription": detail_description,
                            "resellerCommission": reseller_commission,
                            "resellerCommissionType": reseller_commission_type,
                            "maxQuantity": child_product_details["maxQuantity"]
                            if "maxQuantity" in child_product_details
                            else 0,
                            "isComboProduct": child_product_details["isComboProduct"]
                            if "isComboProduct" in child_product_details
                            else False,
                            "currencyRate": currency_rate,
                            "productStatus": "",
                            "hardLimit": 0,
                            "preOrder": False,
                            "procurementTime": 0,
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
                            "nextSlotTime": "",
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
                            "mainProductDetails": main_product_details,
                            "hightlight": hightlight_data,
                            "isShoppingList": isShoppingList,
                            "isSizeAvailable": is_size_available,
                            "detailDescription": detail_description,
                            "isOpenPdp": False,
                            "colourData": colour_details,
                            "colourCount": len(colour_details),
                            "linkedAttribute": linked_attribute,
                            "isFavourite": isFavourite,
                            "availableQty": child_product_details["units"][0]["availableQuantity"],
                            "productType": child_product_details['productType'] if "productType" in child_product_details else 1,
                            "modelImage": model_data,
                            "containsMeat": False,
                            "sizes": [],
                            "variantData": variant_data_list,
                            "addOnsCount": 0,
                            "mobileImage": image_data,
                            "mouDataUnit": "",
                            "moUnit": "Pcs",
                            "popularstatus": 0,
                            "storeCount": 0,
                            "isMembersOnly": child_product_details["isMembersOnly"] if "isMembersOnly" in child_product_details else False
                        }
                    )
                except Exception as ex:
                    print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            if len(product_data) > 0:
                product_dataframe = pd.DataFrame(product_data)
                product_dataframe = product_dataframe.apply(
                    get_avaialable_quantity,
                    next_availbale_driver_time="",
                    driver_roaster={},
                    zone_id="",
                    axis=1,
                )
                product_dataframe = product_dataframe.apply(
                    best_offer_function_validate, zone_id="", logintype=1, axis=1
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
                    logintype=1,
                    store_category_id="",
                    margin_price=True, city_id=""
                )
                del product_dataframe["childProductDetails"]
                del product_dataframe["mainProductDetails"]
                details = product_dataframe.to_json(orient="records")
                recent_data = json.loads(details)
                filter_responseJson = validate_units_data(recent_data, False)
                sort_type = 0
                if int(sort_type) == 0:
                    newlist = sorted(filter_responseJson, key=lambda k: k["price"], reverse=False)
                elif int(sort_type) == 1:
                    newlist = sorted(filter_responseJson, key=lambda k: k["price"], reverse=True)
                elif int(sort_type) == 3:
                    newlist = sorted(filter_responseJson, key=lambda k: k["productName"], reverse=True)
                else:
                    newlist = sorted(filter_responseJson, key=lambda k: k["productName"])
                product_response = newlist
                pen_count = len(product_response)

                response_data = {
                    "products": product_response,
                    "penCount": len(product_response),
                    "annotations": prod_annotations,
                    "policies": policies,
                    "message": "data found"
                }
                # print(product_response)
                print(len(product_response))
                return JsonResponse(response_data, status=200)

            else:
                return JsonResponse({
                    "products": [],
                    "penCount": 0,
                    "annotations": [],
                    "policies": {},
                    "message": "data not found",
                }, status=404)

        except Exception as ex:
            return UtilsObj.raise_exception(ex)
