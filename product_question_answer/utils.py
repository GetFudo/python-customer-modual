from http.client import responses
from django.http import JsonResponse
from drf_yasg import openapi
import json

#------API_Response--------#
Res_200 = "data added succesfully"
Res_401 = "Unauthorized or token expired."
Res_404 = "Data not found."
Res_422 = "Mandatory parameter(s) `{}` is/are missing."
Res_500 = "Internal server error."
Res_401 = "Unauthorized or token expired."

Json_RESPONSE_200 = {Res_200}
Json_RESPONSE_404 = {Res_404}
Json_RESPONSE_422 = {Res_422}
Json_RESPONSE_500 = {Res_500}
Json_RESPONSE_401 = {Res_401}

''' Documataions '''
REQUEST_BODY = openapi.Schema(
    type=openapi.TYPE_OBJECT,
    required=["productId" , "question" , "userId"],
    properties={
       "productId":openapi.Schema(
           type=openapi.TYPE_STRING,
           example="61fa18cdbd236905517efaae",
           description="Add childProduct id"
       ),
        "question": openapi.Schema(
            type=openapi.TYPE_STRING,
            default="Can we get live video of product?",
            example="Can we get live video of product?",
            description="Enter user question",
        ),

    }

)

# RESPONSES = {
#     openapi.Schema(
#         type=openapi.TYPE_OBJECT, properties=Json_RESPONSE_200
#     ),
#      openapi.Schema(
#         type=openapi.TYPE_OBJECT, properties=Json_RESPONSE_401
#     ),
#      openapi.Schema(
#         type=openapi.TYPE_OBJECT, properties=Json_RESPONSE_404
#     ),
#      openapi.Schema(
#         type=openapi.TYPE_OBJECT, properties=Json_RESPONSE_422
#     ),
    
# }

POST_PRODUCTQUESTION_MANUAL_PARAMS = [

]

POST_PRODUCTQUESTION_MANUAL_PARAMS = [
        openapi.Parameter(
        name="Authorization",
        in_=openapi.IN_HEADER,
        type=openapi.TYPE_STRING,
        required=True,
        example={"userId": "5ead7eb2985fdd515e2fae4a"},
        description="authorization token",
    ),
    # openapi.Parameter(
    #     name="userId",
    #     in_=openapi.IN_HEADER,
    #     type=openapi.TYPE_STRING,
    #     required=True,
    #     description="Add Customer userId",
    # ),
]

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

def get_userId(self, request):
    err = check_authentication(request)
    if err: return err
    return json.loads(request.META["HTTP_AUTHORIZATION"]).get('userId', False)

# def generate_exception(self, ex, request=None):
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         traceback.print_exc()
#         template = "An exception of type {0} occurred. Arguments: {1!r} in file: {2} at line: {3}"
#         message = template.format(
#             type(ex).__name__, ex.args, filename, exc_tb.tb_lineno)
#         print(message)
#         exception_response = self.INTERNAL_SERVER_ERROR_RESPONSE
#         exception_response['error'] = message
#         return exception_response