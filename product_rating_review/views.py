import datetime
import json
import threading
from bson.objectid import ObjectId
from bson import json_util
from django.http import JsonResponse
from django.shortcuts import render
from search_api.settings import db
from rest_framework.views import APIView
from rest_framework.decorators import action
import sys


class ProductReviewReply(APIView):
    ''' Add comment for the review for the product '''
    def get(self, request):
        try:
            replies_data = []
            token = request.META["HTTP_AUTHORIZATION"]
            review_id = request.GET.get("reviewId", "")
            # =============================================================================================================================
            if token == "":
                response_data = {
                    "message": "unauthorized",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif review_id == "" or review_id == "undefined" or review_id is None:
                response_data = {
                    "message": "review id blank",
                    "totalCount": 0,
                    "data": [],
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # find the replies for the review
                review_details = db.reviewRatings.find_one({"_id" : ObjectId(review_id)})
                if review_details is None:
                    response = {
                        "data": [],
                        "message": "data not found"
                    }
                    return JsonResponse(response, safe=False, status=404)
                else:
                    if "replies" in review_details:
                        if len(review_details['replies']) > 0:
                            for reply in review_details['replies']:
                                # customer_details = db.customer.find_one(
                                #     {
                                #         "_id": ObjectId(reply['customerId'])
                                #     }
                                # )
                                # if customer_details is not None:
                                replies_data.append(
                                    {
                                        "id": str(reply['id']),
                                        "text": reply['text'],
                                        "customerId": str(reply['customerId']),
                                        "storeId": str(reply['storeId']),
                                        "name": reply['name'],
                                        "email": reply['email'],
                                        "createdTimestamp": reply['createdTimestamp'],
                                    }
                                )
                                # else:
                                #     pass
                            if len(replies_data) > 0:
                                response = {
                                    "data": replies_data,
                                    "message": "data found"
                                }
                                return JsonResponse(response, safe=False, status=200)
                            else:
                                response = {
                                    "data": [],
                                    "message": "data not found"
                                }
                                return JsonResponse(response, safe=False, status=404)
                        else:
                            response = {
                                "data": [],
                                "message": "data not found"
                            }
                            return JsonResponse(response, safe=False, status=404)
                    else:
                        response = {
                            "data": [],
                            "message": "data not found"
                        }
                        return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)

    def post(self, request):
        try:
            replies_data = []
            token = request.META["HTTP_AUTHORIZATION"]
            request_data = request.data
            # =============================================================================================================================
            if token == "":
                response_data = {
                    "message": "unauthorized"
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif request_data == "":
                response_data = {
                    "message": "request body is blank"
                }
                return JsonResponse(response_data, safe=False, status=422)
            elif len(request_data) == 0:
                response_data = {
                    "message": "request body is blank"
                }
                return JsonResponse(response_data, safe=False, status=422)
            elif "reviewId" not in request_data:
                response_data = {
                    "message": "review id is missing"
                }
                return JsonResponse(response_data, safe=False, status=422)
            elif request_data['reviewId'] == "":
                response_data = {
                    "message": "review id is blank"
                }
                return JsonResponse(response_data, safe=False, status=422)
            elif "text" not in request_data:
                response_data = {
                    "message": "text is missing"
                }
                return JsonResponse(response_data, safe=False, status=422)
            elif request_data['text'] == "":
                response_data = {
                    "message": "text is blank"
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # find the replies for the review
                review_id = request_data['reviewId']
                review_details = db.reviewRatings.find_one({"_id": ObjectId(review_id)})
                if review_details is None:
                    response = {
                        "data": [],
                        "message": "data not found"
                    }
                    return JsonResponse(response, safe=False, status=404)
                else:
                    user_id = json.loads(token)["userId"]
                    # customer_details = db.customer.find_one(
                    #     {
                    #         "_id": ObjectId(user_id)
                    #     }
                    # )
                    # if customer_details is not None:
                    product_count = db.childProducts.find(
                        {
                            "storeId": ObjectId(review_details['sellerId']),
                            "_id": ObjectId(review_details['childProductId']),
                        }
                    )
                    if product_count.count() > 0:
                        review_data = {
                            "id": str(ObjectId()),
                            "text": request_data['text'],
                            "customerId": str(user_id),
                            "storeId": "0", #"str(customer_details['storeId'])",
                            "name":  "superadmin", #"customer_details['firstName'] + customer_details['lastName']",
                            "email": "superadmin@gmail.com", #customer_details['email']",
                            "createdTimestamp": int(datetime.datetime.now().timestamp()),
                        }
                        db.reviewRatings.update(
                            {
                                "_id": ObjectId(review_id)
                            },
                            {
                                "$push": {
                                    "replies": review_data
                                }
                            },
                            upsert=False
                        )
                        response = {
                            "message": "data added successfully"
                        }
                        return JsonResponse(response, safe=False, status=200)
                    else:
                        response = {
                            "message": "product is not available from the same store"
                        }
                        return JsonResponse(response, safe=False, status=404)
                    # else:
                    #     response = {
                    #         "message": "customer not found"
                    #     }
                    #     return JsonResponse(response, safe=False, status=404)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)

    def delete(self, request):
        try:
            token = request.META["HTTP_AUTHORIZATION"]
            review_id = request.GET.get("reviewId", "")
            reply_id = request.GET.get("replyId", "")
            # =============================================================================================================================
            if token == "":
                response_data = {
                    "message": "unauthorized"
                }
                return JsonResponse(response_data, safe=False, status=401)
            elif review_id == "":
                response_data = {
                    "message": "review id is blank"
                }
                return JsonResponse(response_data, safe=False, status=422)
            elif reply_id == "":
                response_data = {
                    "message": "reply id is blank"
                }
                return JsonResponse(response_data, safe=False, status=422)
            else:
                # find the replies for the review
                review_details = db.reviewRatings.find_one(
                    {
                        "_id": ObjectId(review_id),
                        "replies.id": reply_id
                    }
                )
                if review_details is None:
                    response = {
                        "data": [],
                        "message": "data not found"
                    }
                    return JsonResponse(response, safe=False, status=404)
                else:
                    replies_data = []
                    if "replies" in review_details:
                        for rep in review_details['replies']:
                            if rep['id'] == reply_id:
                                pass
                            else:
                                replies_data.append(rep)
                    else:
                        pass

                    db.reviewRatings.update(
                        {
                            "_id": ObjectId(review_id)
                        },
                        {
                            "$set": {
                                "replies": replies_data
                            }
                        },
                        upsert=False
                    )
                    response = {
                        "message": "data added successfully"
                    }
                    return JsonResponse(response, safe=False, status=200)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"data": [], "message": message}
            return JsonResponse(error, safe=False, status=500)