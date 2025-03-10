import json
import os, sys
from bson.objectid import ObjectId

from drf_yasg import openapi
from django.http.response import JsonResponse
from django.utils.datastructures import MultiValueDictKeyError


class ResponseHandler(object):

    ################# responses #####################
    MSG_200 = "Data found."
    MSG_401 = "Unauthorized or token expired."
    MSG_404 = "Data not found."
    MSG_422 = "Mandatory parameter(s) `{}` is/are missing."
    MSG_500 = "Internal server error."
    USER_MSG = "Ops!!! something went wrong. Please try again later."

    INTERNAL_SERVER_ERROR_RESPONSE = {
        "data": [],
        "message": MSG_500,
        "userMessage": USER_MSG,
        "penCount": 0,
        "error": ""
    }

    def gen_422_msg(self, param_name='entityId'):
        return self.MSG_422.format(param_name)

    def gen_swag_resp(data_json, desc):
        return openapi.Response(
            description=desc,
            examples={
                "application/json": data_json
            }
        )

    GET_ARTNICHE_200_JSON_RESPONSE = {
        "data": 
        [
            {
                "id": "document id",
                "title": "Music artists",
                "countTitle": "Choose From",
                "memberCount": 10,
                "Image": "<image link>"
            },
            {
                "id": "document id",
                "title": "Music artists",
                "countTitle": "Choose From",
                "memberCount": 10,
                "Image": "<image link>"
            }
        ],
        "message": MSG_200,
        "penCount": 100
    }

    GET_NICHE_RELATED_ARTISTS_RESPONSE_200 = {
        "data": [
            {
                "id": "document id",
                "firstName": "Nil",
                "lastName" : "Thakur",
                "profilePic": "<image link>"
            },
            {
                "id": "document id",
                "firstName": "Nil",
                "lastName" : "Thakur",
                "profilePic": "<image link>"
            }
        ],
        "message": "data found",
        "penCount": 100
        }


    JSON_RESPONSE_401 = {
        "data": [],
        "message": MSG_401,
        "penCount": 0
    }

    JSON_RESPONSE_404 = {
        "data": [],
        "message": MSG_404,
        "penCount": 0
    }

    JSON_RESPONSE_422 = {
        "data": [],
        "message": MSG_422,
        "penCount": 0
    }

    JSON_RESPONSE_500 = {
        "data": [],
        "message": "Internal server error.",
        "penCount": 0
    }

    GET_ARTNICHE_RESPONSES_RESPONSE = {
        200: gen_swag_resp(GET_ARTNICHE_200_JSON_RESPONSE, 200),
        404: gen_swag_resp(JSON_RESPONSE_404, 404),
        401: gen_swag_resp(JSON_RESPONSE_401, 401),
        422: gen_swag_resp(JSON_RESPONSE_422, 422),
        500: gen_swag_resp(JSON_RESPONSE_500, 500)
    }

    GET_NICHE_RELATED_ARTISTS_RESPONSES_RESPONSE = {
        200: gen_swag_resp(GET_NICHE_RELATED_ARTISTS_RESPONSE_200, 200),
        404: gen_swag_resp(JSON_RESPONSE_404, 404),
        401: gen_swag_resp(JSON_RESPONSE_401, 401),
        422: gen_swag_resp(JSON_RESPONSE_422, 422),
        500: gen_swag_resp(JSON_RESPONSE_500, 500)
    }

    def generate_exception(self, ex, request=None):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        template = "An exception of type {0} occurred. Arguments:\n{1!r} in file: {2} at line: {3}"
        message = template.format(type(ex).__name__, ex.args, filename, exc_tb.tb_lineno)
        print(message)
        exception_response = self.INTERNAL_SERVER_ERROR_RESPONSE
        exception_response['error'] = message
        return exception_response

    def check_authentication(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            if token == "":
                return JsonResponse(self.RESPONSE_401, safe=False, status=401)
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

            if len(non_nullbale_param):
                msg = self.gen_422_msg(", ".join(non_nullbale_param))
                req_param_resp = self.JSON_RESPONSE_422
                req_param_resp['message'] = msg
                return JsonResponse(req_param_resp, safe=False, status=422)
        except Exception as ex:
            exception_resp = self.generate_exception(ex)
            return JsonResponse(exception_resp, safe=False, status=500)

ResponseHandlerObj = ResponseHandler()


