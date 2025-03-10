import os, sys, json, warnings, traceback
from bson.objectid import ObjectId
from bson import json_util
from pathlib import Path
from drf_yasg import openapi
from django.http.response import JsonResponse
from django.utils.datastructures import MultiValueDictKeyError

from search_api.settings import db, DINE_STORE_CATEGORY_ID

warnings.filterwarnings('ignore')


class Utils(object):
    ################# responses #####################
    MSG_200 = "Data found."
    MSG_401 = "Unauthorized or token expired."
    MSG_404 = "Data not found."
    MSG_422 = "Mandatory parameter(s) `{}` is/are missing."
    MSG_500 = "Internal server error."
    USER_MSG = "Ops!!! something went wrong. Please try again later."
    INVALID_OBJECTID = "{} is not valid."
    UPDATED_MSG = "{} records updated."
    NOT_UPDATED_MSG = "No records updated."
    ARROW = u"\u2192"

    INTERNAL_SERVER_ERROR_RESPONSE = {
        "data": [],
        "message": MSG_500,
        "userMessage": USER_MSG,
        "totalCount": 0,
        "error": ""
    }

    AUTHENTICATION_ERROR_RESPONSE = {
        "data": [],
        "message": MSG_401,
        "userMessage": USER_MSG,
        "count": 0,
    }

    def gen_422_msg(self, param_name='entityId'):
        return self.MSG_422.format(param_name)

    def json_int_to_openapi_schema(self, data):
        return openapi.Schema(type=openapi.TYPE_INTEGER, example=data)

    def json_float_to_openapi_schema(self, data):
        return openapi.Schema(type=openapi.TYPE_INTEGER, example=data)

    def json_bool_to_openapi_schema(self, data):
        return openapi.Schema(type=openapi.TYPE_BOOLEAN, example=data)

    def json_str_to_openapi_schema(self, data):
        return openapi.Schema(type=openapi.TYPE_STRING, example=data)

    def json_list_to_openapi_schema(self, value):
        item_inner = None
        list_item = None
        for itm in value:
            if type(itm) == dict:
                item_inner = self.json_dict_to_openapi_schema(itm)
            if type(itm) == int:
                item_inner = self.json_int_to_openapi_schema(itm)
            if type(itm) == float:
                item_inner = self.json_float_to_openapi_schema(itm)
            if type(itm) == bool:
                item_inner = self.json_bool_to_openapi_schema(itm)
            if type(itm) == str:
                item_inner = self.json_str_to_openapi_schema(itm)
            if type(itm) == list:
                item_inner = self.json_list_to_openapi_schema(itm)
            list_item = openapi.Schema(type=openapi.TYPE_ARRAY,
                                       items=item_inner)
        return list_item

    def json_dict_to_openapi_schema(self, data):
        generate_dict = {}
        for key, value in data.items():
            item = None
            if type(value) == dict:
                item = self.json_dict_to_openapi_schema(value)
            if type(value) == list:
                item = self.json_list_to_openapi_schema(value)
            if type(value) == int:
                item = self.json_int_to_openapi_schema(value)
            if type(value) == float:
                item = self.json_float_to_openapi_schema(value)
            if type(value) == bool:
                item = self.json_bool_to_openapi_schema(value)
            if type(value) == str:
                item = self.json_str_to_openapi_schema(value)
            if item:
                generate_dict[key] = item
        return openapi.Schema(type=openapi.TYPE_OBJECT, properties=generate_dict)

    def json_to_openapi_schema(self, data):
        if type(data) == dict:
            return self.json_dict_to_openapi_schema(data)
        if type(data) == list:
            return self.json_list_to_openapi_schema(data)

    def gen_swag_resp(data_json, desc):
        return openapi.Response(
            description=desc,
            examples={
                "application/json": data_json
            }
        )

    def raise_exception(self, ex):
        resp = self.generate_exception(ex)
        return JsonResponse(resp, safe=False, status=500)

    JSON_RESPONSE_401 = {
        "data": [],
        "message": MSG_401,
        "totalCount": 0
    }

    JSON_RESPONSE_404 = {
        "data": [],
        "message": MSG_404,
        "totalCount": 0
    }

    JSON_RESPONSE_422 = {
        "data": [],
        "message": MSG_422,
        "totalCount": 0
    }

    JSON_RESPONSE_500 = {
        "data": [],
        "message": "Internal server error.",
        "totalCount": 0
    }

    def generate_exception(self, ex, request=None):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        traceback.print_exc()
        template = "An exception of type {0} occurred. Arguments: {1!r} in file: {2} at line: {3}"
        message = template.format(
            type(ex).__name__, ex.args, filename, exc_tb.tb_lineno)
        print(message)
        exception_response = self.INTERNAL_SERVER_ERROR_RESPONSE
        exception_response['error'] = message
        return exception_response

    def check_authentication(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            if token == "":
                return JsonResponse(self.JSON_RESPONSE_401, safe=False, status=401)
        except KeyError:
            return JsonResponse(self.AUTHENTICATION_ERROR_RESPONSE, safe=False, status=401)
        except Exception as ex:
            exception_resp = self.generate_exception(ex, request)
            return JsonResponse(exception_resp, safe=False, status=500)

    def get_auth_token(self, request):
        return request.META["HTTP_AUTHORIZATION"]

    def exception_response(self, ex):
        resp = self.generate_exception(ex)
        return JsonResponse(resp, safe=False, status=500)

    def get_userId(self, request):
        err = self.check_authentication(request)
        if err: return err
        return json.loads(request.META["HTTP_AUTHORIZATION"]).get('userId', False)

    def get_lang(self, request):
        return request.META.get("HTTP_LANGUAGE", "en")

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
                msg = self.gen_422_msg(", ".join(non_nullbale_param))
                req_param_resp = self.JSON_RESPONSE_422
                req_param_resp['message'] = msg
                return JsonResponse(req_param_resp, safe=False, status=422)
        except Exception as ex:
            exception_resp = self.generate_exception(ex)
            return JsonResponse(exception_resp, safe=False, status=500)

    def serialize_cursor(self, cursor):
        '''
            Method to json serialize the PymongoCurser object.
        '''
        return json.loads(json_util.dumps(cursor))


UtilsObj = Utils()

NEAREST_STORE_FILTER_JSON_RESPONSE = {
    "checkins": [
        {
            "_id": "6167f791e33c5c0014876db3",
            "storeId": "614205c8dc1dd00013aaa7f9",
            "userId": "613764faa2967023b6d2df58",
            "profilePic": "https://res.cloudinary.com/cuapptransfer/image/upload/v1634203537/20211014025532PM.jpg",
            "firstName": "Vikqs",
            "nickName": "Vikqs",
            "lastName": "Vikqs",
            "email": "vikas@gmail.com",
            "phone": "9999999999",
            "countryCode": "+91",
            "gender": 1,
            "dob": 945004253296.0,
            "age": 21,
            "checkInTime": 1634203537
        },
    ],
    "stores": [
        {
            "address": "Amazon Warehouse, Jakkuru, Bengaluru, Karnataka, India",
            "locality": "Bengaluru",
            "addressArea": "Jakkuru",
            "logoImages": {
                "logoImageMobile": "https://cdn.shoppd.net/StorePage/0/0/large/Amazon_icon_1631686883.png",
                "logoImageThumb": "https://cdn.shoppd.net/StorePage/0/0/small/Amazon_icon_1631686883.png",
                "logoImageweb": "https://cdn.shoppd.net/StorePage/0/0/large/Amazon_icon_1631686883.png"
            },
            "bannerImages": {
                "bannerImageMobile": "https://cdn.shoppd.net/StorePage/0/0/large/amazon-logo_1631716145.jpeg",
                "bannerImageThumb": "https://cdn.shoppd.net/StorePage/0/0/small/amazon-logo_1631716145.jpeg",
                "bannerImageweb": "https://cdn.shoppd.net/StorePage/0/0/large/amazon-logo_1631716145.jpeg"
            },
            "cityId": "61371411987246667e1e42a2",
            "minimumOrder": 0,
            "nextCloseTime": "",
            "nextOpenTime": "",
            "avgRating": 0,
            "storeIsOpen": False,
            "driverTypeId": 3,
            "driverType": "PartnerDelivery",
            "storeType": "e-CommercePartner",
            "location": {
                "lat": 13.0723349,
                "lon": 77.6074659
            },
            "averageDeliveryTime": "0 Mins",
            "state": "Karnataka",
            "country": "India",
            "city": "Bengaluru",
            "distanceKm": 5.95,
            "currencySymbol": "INR",
            "currency": "INR",
            "distanceMiles": 3.7,
            "storeName": "Amazon",
            "storeId": "61420484150c98000dcd608b",
            "favouriteUsers": []
        },
    ],
    "storeCount": 9,
    "checkinsCount": 13,
    "message": "Data found successfully."
}

COMBO_PRODUCT_JSON_RESPONSE = {
    "message": "Data Added successfully."
}

NEAREST_STORE_FILTER_SCHEMA_RESPONSE = UtilsObj.json_to_openapi_schema(NEAREST_STORE_FILTER_JSON_RESPONSE)
COMBO_PRODUCT_SCHEMA_RESPONSE = UtilsObj.json_to_openapi_schema(COMBO_PRODUCT_JSON_RESPONSE)

NEAREST_STORE_FILTER_RESPONSES = {
    200: NEAREST_STORE_FILTER_SCHEMA_RESPONSE,
    401: UtilsObj.json_to_openapi_schema(UtilsObj.JSON_RESPONSE_401),
    422: UtilsObj.json_to_openapi_schema(UtilsObj.JSON_RESPONSE_422),
    500: UtilsObj.json_to_openapi_schema(UtilsObj.INTERNAL_SERVER_ERROR_RESPONSE),
}

COMBO_PRODUCT_RESPONSES = {
    200: COMBO_PRODUCT_SCHEMA_RESPONSE,
    401: UtilsObj.json_to_openapi_schema(UtilsObj.JSON_RESPONSE_401),
    422: UtilsObj.json_to_openapi_schema(UtilsObj.JSON_RESPONSE_422),
    500: UtilsObj.json_to_openapi_schema(UtilsObj.INTERNAL_SERVER_ERROR_RESPONSE),
}

GET_BEST_BEVVY_SELLERS_RESPONSE = {
    200: UtilsObj.json_to_openapi_schema({
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
                                    }),
    401: UtilsObj.json_to_openapi_schema(UtilsObj.JSON_RESPONSE_401),
    422: UtilsObj.json_to_openapi_schema(UtilsObj.JSON_RESPONSE_422),
    500: UtilsObj.json_to_openapi_schema(UtilsObj.INTERNAL_SERVER_ERROR_RESPONSE),
}

POST_NEAREST_STORE_FILTER_MANUAL_PARAM = [openapi.Parameter(
    name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
    description="authorization token"),
    openapi.Parameter(
        name='language', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
        description="language", default="en"),
]

POST_COMBO_PRODUCTS_MANUAL_PARAM = [openapi.Parameter(
    name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
    description="authorization token"),
]


GET_BEST_BEVVY_SELLERS = [
    openapi.Parameter(
        name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
        description="authorization token"),
    openapi.Parameter(
        name='storeCategoryId', in_=openapi.IN_QUERY, type=openapi.TYPE_STRING, required=True,
        description="storeCategoryId: 620f964b13c0d364a231bcb3"),
    openapi.Parameter(
        name='cityId', in_=openapi.IN_QUERY, type=openapi.TYPE_STRING, required=True,
        description="cityId: 620f93b7c98d37382a5098c3"),
]


POST_NEAREST_STORE_FILTER_REQUEST_BODY = UtilsObj.json_to_openapi_schema({
    "data": {
        "lat": 13.0207388,
        "long": 77.5928944,
        "storeCategoryIds": ["6111157583fb5f447a3c3f12"],
        "projectIds": ["6149b582150c98000dcd608f"],
        "friendIds": ["6163d4c0f0bc9e04404d094f"],
        "matchIds": ["613764faa2967023b6d2df58"],
        "ratings": "",
        "distanceUpTo": 50,
    }
})

POST_COMBO_PRODUCTS_REQUEST_BODY = UtilsObj.json_to_openapi_schema(
    {
        "productName": "Fresho Baby Apple Shimla, 1 kg (Approx. 11-12 pcs)",
        "linkedProductCategory": [{
            'categoryId': '5f33d3fdb6bbc040e5834cae',
            'categoryName': {
                'en': 'Apples & Pomegranate'
            }
        }],
        "storeCategoryId": "5cc0846e087d924456427975",
        "detailDescription": "Baby Apples are the mini blush red apples with slight yellow streaks and has a smooth "
                             "texture.\r\nThe apple flesh is greenish white and grained, and it tastes sweet and "
                             "juicy. The crispiness and the aroma of the apples make it more attractive.\r\nApples "
                             "are best when it is consumed fresh after meals or as a healthy snack for kids. ",
        "availableStock": 100,
        "price": 100,
        "offerPrice": 100,
        "image": [
            "https://dvidm5lncff50.cloudfront.net/products/0/0/large/40134281-2_1-fresho-baby-apple-shimla_1643886913.jpeg",
            "https://dvidm5lncff50.cloudfront.net/products/0/0/large/40134281-2_1-fresho-baby-apple-shimla_1643886913.jpeg",
            "https://dvidm5lncff50.cloudfront.net/products/0/0/large/40134281-2_1-fresho-baby-apple-shimla_1643886913.jpeg"
        ],
        "storeId": "5e20914ac348027af2f9028e",
        "currencySymbol": "$",
        "currencyCode": "USD",
        'comboProducts': [{
            'productId': '610a7496d9c1440a71dfbd7c',
            'productName': 'Apple',
            'minimumQuantity': 1
        }, {
            'productId': '610a73c7d9c1440a71dfbd78',
            'productName': 'Apple Royal Gala',
            'minimumQuantity': 1
        }]
    }
)
