import os, sys, json, warnings, traceback
from bson.objectid import ObjectId
from pathlib import Path
from drf_yasg import openapi
from django.http.response import JsonResponse
from django.utils.datastructures import MultiValueDictKeyError

from search_api.settings import MEAT_STORE_CATEGORY_ID, db, DINE_STORE_CATEGORY_ID

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

    JSON_RESPONSE_200 = {
        "data": [],
        "message": MSG_200,
        "totalCount": 0
    }

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
                return JsonResponse(self.RESPONSE_401, safe=False, status=401)
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


UtilsObj = Utils()


POST_NEAREST_STORE_FILTER_MANUAL_PARAM = [openapi.Parameter(
    name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
    description="authorization token"),
    openapi.Parameter(
        name='language', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
        description="language", default="en"),
]

COMMON_RESPONSE = {
    200: UtilsObj.json_to_openapi_schema(UtilsObj.JSON_RESPONSE_200),
    401: UtilsObj.json_to_openapi_schema(UtilsObj.JSON_RESPONSE_401),
    422: UtilsObj.json_to_openapi_schema(UtilsObj.JSON_RESPONSE_422),
    500: UtilsObj.json_to_openapi_schema(UtilsObj.INTERNAL_SERVER_ERROR_RESPONSE),
}

CLUBMART_MANUAL_PARAM_REQ = [
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
        name="lat",
        required=True,
        default="13.05176",
        in_=openapi.IN_QUERY,
        type=openapi.TYPE_STRING,
        description="latitude of the user's location",
    ),
    openapi.Parameter(
        name="long",
        default="77.580448",
        required=True,
        in_=openapi.IN_QUERY,
        type=openapi.TYPE_STRING,
        description="longtitue of the user's location",
    ),
    openapi.Parameter(
        name="timezone",
        default="Asia/Calcutta",
        required=False,
        in_=openapi.IN_QUERY,
        type=openapi.TYPE_STRING,
        description="offset of the timezone",
    ),
]

CLUBMART_REQ_BODY = openapi.Schema(
    type=openapi.TYPE_OBJECT,
    required=["zoneId"],
    properties={
        "zoneId": openapi.Schema(
            type=openapi.TYPE_STRING,
            default="5df8b7628798dc19d926bd29",
            example="5df8b7628798dc19d926bd29",
            description="zoneId for zone stores",
        ),
        "cityId": openapi.Schema(
            type=openapi.TYPE_STRING,
            default="5df7b7218798dc2c1114e6bf",
            example="5df7b7218798dc2c1114e6bf",
            description="cityId for city stores",
        ),
        "storeCategoryId": openapi.Schema(
            type=openapi.TYPE_STRING,
            default="5df8766e8798dc2f236c95fa",
            example="5df8766e8798dc2f236c95fa",
            description="storeCategoryId for stores",
        ),
        "fromData": openapi.Schema(
            type=openapi.TYPE_INTEGER,
            default=0,
            example=0,
            description="from data",
        ),
        "toData": openapi.Schema(
            type=openapi.TYPE_INTEGER,
            default=50,
            example=20,
            description="to data",
        ),
    }
)

CLUBMART_STORES_REQ_BODY = openapi.Schema(
    type=openapi.TYPE_OBJECT,
    required=["zoneId"],
    properties={
        "zoneId": openapi.Schema(
            type=openapi.TYPE_STRING,
            default="5df8b7628798dc19d926bd29",
            example="5df8b7628798dc19d926bd29",
            description="zoneId for zone stores",
        ),
        "storeCategoryId": openapi.Schema(
            type=openapi.TYPE_STRING,
            default="5df8766e8798dc2f236c95fa",
            example="5df8766e8798dc2f236c95fa",
            description="storeCategoryId for stores",
        ),
        "specialityId": openapi.Schema(
            type=openapi.TYPE_STRING,
            default="60c89e2676224a432a68e264",
            example="60c89e2676224a432a68e264",
            description="speiciality id for store retrival",
        ),
        "fromData": openapi.Schema(
            type=openapi.TYPE_INTEGER,
            default=0,
            example=0,
            description="from data",
        ),
        "toData": openapi.Schema(
            type=openapi.TYPE_INTEGER,
            default=50,
            example=20,
            description="to data",
        ),
    }
)