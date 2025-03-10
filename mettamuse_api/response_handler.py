import json
import os, sys
from bson.objectid import ObjectId

from drf_yasg import openapi
from django.http.response import JsonResponse
from django.utils.datastructures import MultiValueDictKeyError


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)

    def encode(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return json.JSONEncoder.encode(self, obj)


class MettamuseResponseHandler(object):

    ################# static responses #####################
    MSG_200 = "Data found."
    MSG_SUCCESS = "success"
    MSG_DATA_UPDATED = "Updated {n} record(s)."
    MSG_401 = "Unauthorized or token expired."
    MSG_404 = "Data not found."
    MSG_422 = "Mandatory parameter(s) `{}` is/are missing."
    MSG_500 = "Internal server error."
    USER_MSG = "Ops!!! something went wrong. Please try again later."

    INTERNAL_SERVER_ERROR_RESPONSE = {
        "data": [],
        "message": MSG_500,
        "userMessage": USER_MSG,
        "error": ""
    }

    def gen_422_msg(self, param_name=""):
        '''
            Method to generate 422 response message
            string with variable name.
        '''
        return self.MSG_422.format(param_name)

    def gen_swag_resp(data_json, desc):
        '''
            Method to generate swagger json response object.
        '''
        return openapi.Response(
            description=desc,
            examples={
                "application/json": data_json
            }
        )

    JSON_RESPONSE_SUCCESS_MSG = {
        "data": [],
        "message": MSG_SUCCESS,
    }

    JSON_RESPONSE_200 = {
        "data": [],
        "message": MSG_200,
    }

    JSON_RESPONSE_401 = {
        "data": [],
        "message": MSG_401,
    }

    JSON_RESPONSE_404 = {
        "data": [],
        "message": MSG_404,
    }

    JSON_RESPONSE_422 = {
        "data": [],
        "message": MSG_422,
    }

    JSON_RESPONSE_500 = {
        "data": [],
        "message": MSG_500,
        "userMessage": USER_MSG
    }

    GET_METTAMUSE_HOMEPAGE_RESPONSE_200 = {
        "_id" : "5dcd58c6bd5f672d74e152de",
        "storeId" : "0",
        "Slider" : {
            "stories" : [ 
                {
                    "Title" : "Build from scratch",
                    "Description" : "We reinvent the idea of sustainable fashion and design by offering hand-made craft products from around the world that combine high quality with a strong contemporary design. Far from the fast fashion hype, we create products that are likewise timeless and unique.",
                    "image" : {
                        "web" : "https://cdn.shoppd.net/shoppd/0/0/1623392053284.jpg"
                    }
                },
            ]
        }
    }

    PATCH_METTAMUSE_HOMEPAGE_200_RESPONSE = JSON_RESPONSE_200
    PATCH_METTAMUSE_HOMEPAGE_200_RESPONSE['message'] = MSG_DATA_UPDATED

    GET_METTAMUSE_HOMEPAGE_RESP_OBJ = {
        200: gen_swag_resp(GET_METTAMUSE_HOMEPAGE_RESPONSE_200, 200),
        404: gen_swag_resp(JSON_RESPONSE_404, 404),
        401: gen_swag_resp(JSON_RESPONSE_401, 401),
        422: gen_swag_resp(JSON_RESPONSE_422, 422),
        500: gen_swag_resp(JSON_RESPONSE_500, 500)
    }

    PATCH_METTAMUSE_HOMEPAGE_RESP_OBJ = {
        200: gen_swag_resp(PATCH_METTAMUSE_HOMEPAGE_200_RESPONSE, 200),
        404: gen_swag_resp(JSON_RESPONSE_404, 404),
        401: gen_swag_resp(JSON_RESPONSE_401, 401),
        422: gen_swag_resp(JSON_RESPONSE_422, 422),
        500: gen_swag_resp(JSON_RESPONSE_500, 500)
    }


    def generate_exception(self, ex, request=None):
        '''
            Method to generate exception Json respone data.
        '''
        exc_type, exc_obj, exc_tb = sys.exc_info()
        filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        template = "An exception of type {0} occurred. Arguments:\n{1!r} in file: {2} at line: {3}"
        message = template.format(type(ex).__name__, ex.args, filename, exc_tb.tb_lineno)
        print(message)
        exception_response = self.INTERNAL_SERVER_ERROR_RESPONSE
        exception_response['error'] = message
        return exception_response

    def get_language(self, request):
        '''
            Method to get language from request.
            Default 'en' response.
        '''
        try:
            return request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
        except Exception as ex:
            exception_resp = self.generate_exception(ex, request)
            return JsonResponse(exception_resp, safe=False, status=500)

    def check_authentication(self, request):
        '''
            Method to check request authorization token.
        '''
        try:
            token = request.META.get("HTTP_AUTHORIZATION", "")
            if token == "":
                return JsonResponse(self.JSON_RESPONSE_401, safe=False, status=401)
        except Exception as ex:
            exception_resp = self.generate_exception(ex, request)
            return JsonResponse(exception_resp, safe=False, status=500)

    def check_req_params(self, data, params):
        '''
            Method to check required parametes.
            param dict data: MultiValueDict Object from request /GET/POST/DATA
            param list params: Required parameters' name
        '''
        try:
            non_nullbale_param = []
            for param_name in params:
                try:
                    value = data[param_name]
                    if value == '' or value == None:
                        non_nullbale_param.append(param_name)
                except MultiValueDictKeyError:
                    non_nullbale_param.append(param_name)
                except KeyError:
                    non_nullbale_param.append(param_name)
                except Exception as e:
                    print("** Error: ", str(e))
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
        return json.loads(JSONEncoder().encode(cursor))

    def gen_response_data(self, data, msg=None):
        '''
            Method to prepare the success data.
        '''
        resp_data = self.JSON_RESPONSE_SUCCESS_MSG
        resp_data['data'] = data if data is not None else {}
        resp_data['message'] = msg if msg is not None else self.MSG_404
        return self.serialize_cursor(resp_data)

ResponseHandlerObj = MettamuseResponseHandler()
