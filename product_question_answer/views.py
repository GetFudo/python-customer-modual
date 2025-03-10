from crypt import methods
import datetime
from doctest import Example
import json
from os import stat
import pdb
from tabnanny import check
from termios import PARODD
import threading
from tokenize import String
from traceback import print_tb
from unittest import skip
from urllib import request
from webbrowser import get
from bson.objectid import ObjectId
from bson import json_util
from django.http import JsonResponse
from django.shortcuts import render
from grpc import Status
from marshmallow import Schema
from rest_framework.views import APIView
from drf_yasg.utils import swagger_auto_schema
from rest_framework.decorators import action
from sqlalchemy import except_
from epic_api.utils import *
from home_page_api.utils import COMMON_RESPONSE
from product_question_answer.utils import *
from search_api.settings import db, DINE_STORE_CATEGORY_ID
from random import randrange
import pytz


class ProductQuestion(APIView):
    ''' Add user product question '''

    @swagger_auto_schema(
        method="get",
        tags=["Product Question"],
        operation_description="API for getting product question",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="skip",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example=0,
                required=True,
                description="set skip",
            ),
            openapi.Parameter(
                name="limit",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example=20,
                required=True,
                description="set limit",
            ),
            openapi.Parameter(
                name="parentProductId",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="61f98062bd236905517efa9a",
                required=False,
                description="enter parentProductId if avalible ",
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
                name="trigger",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example="1",
                required=False,
                description="1: most answer  or 2:recent question or 3: most recent answer or 4:  oldest question",
            ),
            openapi.Parameter(
                name="searchName",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="what",
                required=False,
                description="enter storeId if avalible",
            ),
        ],
        responses={
            200: "successfully found ",
            404: "data not found",
            422: "Invalide data",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        err = UtilsObj.check_authentication(request)
        if err:
            return err

        question_data = []
        try:
            first_name = ""
            last_name = ""
            user_email = ""
            answer_first_name = ""
            answer_last_name = ""
            answer_user_email = ""
            answer_profile_pic = ""
            profile_pic = ""
            trigger = int(request.GET.get("trigger", "0"))
            skip_data = int(request.GET.get("skip", "0"))
            limit_data = int(request.GET.get("limit", "20"))
            store_id = request.GET.get("storeId", "")
            parent_product_id = request.GET.get("parentProductId", "")
            aggrigate_query = {
                "status": 1
            }
            if parent_product_id != "":
                aggrigate_query['parentProductId'] = parent_product_id
            else:
                pass

            if store_id != "":
                if store_id == "0":
                    aggrigate_query['storeId'] = store_id
                else:
                    aggrigate_query['storeId'] = ObjectId(store_id)
            else:
                pass

            serach_text = request.GET.get('searchName', "")
            if serach_text != "":
                find_text = '.*' + serach_text
                aggrigate_query['$or'] = [
                    {"question": {'$regex': find_text, "$options": "i"}},
                    {"answer.answer": {'$regex': find_text, "$options": "i"}}
                ]
            if trigger == 1:
                ''' most answer '''
                sort_filter = [("answerCount", -1)]

            elif trigger == 3:
                sort_filter = [("answer.answerId", -1)]
                ''' most recent answer '''

            elif trigger == 4:
                sort_filter = [("_id", 1)]
                ''' oldest question '''

            else:
                sort_filter = [("_id", -1)]
            find_question_data = db.productQuestion.find(aggrigate_query).sort(
                sort_filter).skip(skip_data).limit(limit_data)
            total_count = db.productQuestion.find(
                aggrigate_query).count()
            get_question_data = list(find_question_data)

            if len(get_question_data) == 0 or get_question_data is None:
                response_ans = {'message': 'question not found'}
                return JsonResponse(response_ans, status=404)
            try:
                for question in get_question_data:
                    question_answer = []
                    try:
                        get_user_detail = db.customer.find_one({"_id": ObjectId(question["userId"])})
                        first_name = get_user_detail['firstName']
                        last_name = get_user_detail['lastName']
                        user_email = get_user_detail['email']
                        profile_pic = get_user_detail['profilePic']
                    except:pass
                    answe_check =  question['answer'] if 'answer' in question else []
                    if len(answe_check) > 0:
                        for get_answer_id in answe_check:
                            answer_userid = get_answer_id["userId"] if 'userId' in get_answer_id and get_answer_id["userId"] != "" else ""
                            if answer_userid != "":
                                get_user_detail = db.customer.find_one({"_id": ObjectId(answer_userid)})
                                if get_user_detail is not None and get_user_detail != "":
                                    try:
                                        answer_first_name = get_user_detail['firstName']
                                        answer_last_name = get_user_detail['lastName']
                                        answer_user_email = get_user_detail['email']
                                        answer_profile_pic = get_user_detail['profilePic']
                                    except:pass
                            answer_status = get_answer_id['status']
                            if answer_status == 1:
                                question_answer.append({
                                    "answerId" : get_answer_id['answerId'],
                                    "answer" : get_answer_id['answer'],
                                    "userId" : get_answer_id['userId'],
                                    "userName": answer_first_name + " " + answer_last_name,
                                    "emil": answer_user_email,
                                    "postedAsAnonymous" : get_answer_id['postedAsAnonymous'],
                                    "userType" : get_answer_id['userType'],
                                    "profilePic" : answer_profile_pic,
                                    "status" : get_answer_id['status'],
                                    "postedOn" : get_answer_id['postedOn'],
                                    "upVoteCount" : get_answer_id['upVoteCount'],
                                    "downVoteCount" : get_answer_id['downVoteCount'],
                                    "abuseCount" : get_answer_id['abuseCount'],
                                    "upVotes" : get_answer_id['upVotes'],
                                    "downVote" : get_answer_id['downVote'],
                                    "abuse" : get_answer_id['abuse']
                                })
                    
                    question_data.append(
                        {
                            "_id": question['_id'],
                            "productName": question['productName'],
                            "productId": question['productId'],
                            "parentProductId": question['parentProductId'] if 'parentProductId' in question else "",
                            "storeId": question['storeId'] if 'storeId' in question else "",
                            "storeName": question['storeName'] if 'storeName' in question else "",
                            "question": question['question'],
                            "userId": question['userId'] if 'userId' in question else "",
                            "userName": first_name + " " + last_name,
                            "email":user_email,
                            "profilePic": profile_pic,
                            "postedOn": question['postedOn'],
                            "answerCount": question['answerCount'] if "answerCount" in question else 0,
                            "answer": question_answer,
                            "postedOn": question['postedOn']
                        }
                    )

            except Exception as ex:
                return UtilsObj.raise_exception(ex)

            final_data = json.loads(json_util.dumps(question_data))
            if len(get_question_data) == 0:
                response_ans = {
                    "message": "question not found"
                }
                return JsonResponse(response_ans, status=204)

            response_ans = {
                'data': final_data,
                'count': total_count,
                'penCount': total_count,
                "message": "data found"
            }
            return JsonResponse(response_ans, status=200)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)

    @swagger_auto_schema(
        method='post', tags=["Product Question"],
        operation_description="API for Question about Product",
        required=["Authorization"],
        manual_parameters=POST_PRODUCTQUESTION_MANUAL_PARAMS,
        request_body=REQUEST_BODY,
        responses=COMMON_RESPONSE,
        operation_summary="customer question"
    )
    @action(detail=False, methods=['post'])
    def post(self, request):
        ''' check Tocken validation  '''
        try:
            err = UtilsObj.check_authentication(request)
            if err:
                return err

            ''' Check request Data validation'''
            data = request.data
            params = ["productId", "question"]
            err = UtilsObj.check_req_params(data, params)
            if err:
                return err

            '''Globle variable'''
            get_storeid = None
            store_name = None

            ''' Get childproduct data '''
            collect_data = db.childProducts.find_one(
                {"_id": ObjectId(str(data['productId']))})
            if collect_data is None:
                response_data = {"message": "Your Product is not found"}
                return JsonResponse(response_data, status=404)

            ''' get store and user data '''
            get_child_product_storeid = collect_data['storeId']
            if get_child_product_storeid == "0":
                get_storeid = "0"
                store_name = "Center Store"

            else:
                store_data = db.stores.find_one(
                    {'storeId': str(get_child_product_storeid)})
                get_storeid = get_child_product_storeid
                store_name = store_data['storeName']['en']
            try:
                user_id = json.loads(
                    request.META["HTTP_AUTHORIZATION"]).get('userId', False)
            except:
                response_ans = {'message': 'token Invalide'}
                return JsonResponse(response_ans, status=400)

            try:
                customer_id = db.customer.find_one(
                    {"_id": ObjectId(str(user_id))})
            except:
                response_ans = {'message': 'token value empty'}
                return JsonResponse(response_ans, status=422)
            add_seqid = db.productQuestion.find(
                {"productId": collect_data['_id']}).count()

            if customer_id is not None:
                profile_pic = customer_id['profilePic']
            else:
                profile_pic = ""

            ''' fill Data '''
            product_questiondata = {
                "productId": collect_data['_id'],
                "parentProductId": collect_data['parentProductId'],
                "productName": collect_data['pName']['en'],
                "storeCategoryId": collect_data['storeCategoryId'],
                "storeId": get_storeid,
                "storeName": store_name,
                "question": str(data['question']),
                "userId": user_id,
                "profilePic": profile_pic,
                "postedOn": datetime.datetime.now().timestamp(),
                "seqId": add_seqid + 1,
                "status": int(1),
                "answerCount": 0
            }
            que_data = db.productQuestion.insert_one(product_questiondata)
            response_data = {
                "message": "Question added succesfully",
                "_id": str(que_data.inserted_id)
            }
            return JsonResponse(response_data, status=200)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)

    @swagger_auto_schema(
        method='patch', tags=["Product Question"],
        operation_description="API for Question about Product",
        # required=["Authorization"],
        # manual_parameters=POST_PRODUCTQUESTION_MANUAL_PARAMS,
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            required=['questionId', 'status'],
            properties={
                "status": openapi.Schema(
                    type=openapi.TYPE_INTEGER,
                    example=3,
                    description="Add status (1: active , 2: inactive, 3: delete)"
                ),

                "questionId": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    example="61fa0f0f261015cf0a74c5d0",
                    description="Add question Id"
                ),
            }
        ),

        responses=COMMON_RESPONSE,
        operation_summary="customer question status change"
    )
    @action(detail=False, methods=['patch'])
    def patch(self, request):
        '''question status
        1 for active, 
        2 for inactive, 
        3 for delete
        '''

        # check request data
        data = request.data
        params_data = ['questionId', 'status']
        err = UtilsObj.check_req_params(data, params_data)
        if err:
            return err
        try:
            status_code = int(data['status'])
        except:
            response_ans = {'message': 'status invalide'}
            return JsonResponse(response_ans, status=400)

        if status_code == 1 or status_code == 2 or status_code == 3:
            # find question
            try:
                find_question = db.productQuestion.find_one(
                    {"_id": ObjectId(str(data['questionId']))})
                if find_question is None:
                    response_ans = {'message': 'question not found'}
                    return JsonResponse(response_ans, status=404)
            except:
                response_ans = {'message': 'Invalide questionId'}
                return JsonResponse(response_ans, status=400)

            try:
                update_status_code = db.productQuestion.update_one(
                    {"_id": ObjectId(str(data['questionId']))},
                    {"$set": {
                        "status": int(status_code)
                    }
                    }
                )
                response_ans = {'messge': 'status update successfully'}
                return JsonResponse(response_ans, status=200)
            except:
                response_ans = {'message': 'Invalide questionId'}
                return JsonResponse(response_ans, status=400)
        else:
            response_ans = {'message': 'status valuse invalide'}
            return JsonResponse(response_ans, status=400)


class ProductQuestionAnswer(APIView):
    ''' save product answer '''

    @swagger_auto_schema(
        method="get",
        tags=["ProductQuestionAnswer"],
        operation_description="API for getting product question answer",
        # required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="skip",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example=0,
                required=True,
                description="set skip",
            ),
            openapi.Parameter(
                name="limit",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example=20,
                required=True,
                description="set limit",
            ),
            openapi.Parameter(
                name="questionId",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="61fa12c66214d421928aa729",
                required=True,
                description="enter questionId ",
            ),
        ],
        responses={
            200: "successfully found ",
            404: "data not found",
            422: "Invalide data",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        '''globle variablr '''
        answer_data = []
        first_name = ""
        last_name = ""
        profile_pic = ""
        user_email = ""
        try:
            try:
                skip_data = int(request.GET.get("skip", "0"))
            except:
                response_ans = {'message': 'skip field missing'}
                return JsonResponse(response_ans, status=422)
            try:
                limit_data = int(request.GET.get("limit", "20"))
            except:
                response_ans = {'message': 'limit field missing'}
                return JsonResponse(response_ans, status=422)
            try:
                question_id = str(request.GET.get("questionId", ""))
                if question_id == "":
                    response_ans = {'message': 'questionId field missing'}
                    return JsonResponse(response_ans, status=422)
            except:
                response_ans = {'message': 'questionID Invalide'}
                return JsonResponse(response_ans, status=422)
            payload = {"_id": ObjectId(str(question_id)), 'status': 1}
            search = request.GET.get('search', '')
            if search != "":
                final_text = '.*'+ search
                payload['answer.answer'] = {
                    '$regex': final_text, "$options": "i"}

            try:
                find_question_id = db.productQuestion.find_one(payload)
                if find_question_id is None:
                    response_ans = {'message': 'question not found'}
                    return JsonResponse(response_ans, status=404)
            except:
                response_ans = {'message': 'questionId key Invalide'}
                return JsonResponse(response_ans, status=400)

            get_answer = db.productQuestion.aggregate([
                {"$match": {"_id": ObjectId(question_id)}},
                {"$unwind": "$answer"},
                {"$match": {"answer.status": 1}},
                {"$project": {"answer": 1}},
                {"$skip": skip_data},
                {"$limit": limit_data},
                {"$sort":{"answer.answerId":-1}}
            ])
            final_data = list(get_answer)
            for i in final_data:
                try:
                    data = i['answer']
                    try:
                        user_id_check = data['userId'] if 'userId' in data and data['userId'] != "" else ""
                        if user_id_check !="":
                            get_user_detail = db.customer.find_one({"_id": ObjectId(user_id_check)})
                            first_name = get_user_detail['firstName']
                            last_name = get_user_detail['lastName']
                            user_email = get_user_detail['email']
                            profile_pic = get_user_detail['profilePic']
                    except:
                        pass
                    
                    else:
                        pass
                    answer_data.append({
                        "_id": data['answerId'],
                        "answer": data['answer'],
                        "userId": data['userId'],
                        "userName": first_name + ' ' + last_name,
                        "email":user_email,
                        "postedAsAnonymous": data['postedAsAnonymous'],
                        "userType": data['userType'],
                        "profilePic": profile_pic,
                        "status": data['status'],
                        "postedOn": data['postedOn'],
                        "upVoteCount": data['upVoteCount'],
                        "downVoteCount": data['downVoteCount'],
                        "abuseCount": data['abuseCount']
                    }
                    )
                except:
                    pass
                    
            if int(len(answer_data)) == 0:
                response_ans = {
                    "message": "answer not avalible"
                }
                return JsonResponse(response_ans, status=204)
            # set_answer_count = db.productQuestion.update_one({
            #     "_id": ObjectId(question_id)
            # }, {
            #     "$set": {"answerCount": int(len(answer_data))}
            # })
            final_data = json.loads(json_util.dumps(answer_data))
            response_ans = {
                "data": final_data,
                "count": int(len(answer_data)),
                "message": 'found successfully'}
            return JsonResponse(response_ans, status=200)

        except Exception as ex:
            return UtilsObj.raise_exception(ex)

    @swagger_auto_schema(
        method='post', tags=['ProductQuestionAnswer'],
        opration_desription='API for save answer of product related question',
        required=["Authorization"],
        manual_parameters=POST_PRODUCTQUESTION_MANUAL_PARAMS,
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            required=['answer', 'questionId', 'postedAsAnonymous'],
            properties={
                "answer": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    example="we will give you warrenty card with product.",
                    description="Add answer"
                ),
                "questionId": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    example="",
                    description="Add questionId"
                ),
                "postedAsAnonymous": openapi.Schema(
                    type=openapi.TYPE_BOOLEAN,
                    example=False,
                    description="Add postedAsAnonymous"
                ),
            }
        ),
        responses=COMMON_RESPONSE,
        operation_summary="answer of customer question"
    )
    @action(detail=False, methods=['post'])
    def post(self, request):
        # globle variable
        user_type = "anonymous"
        user_profile_id = ""
        post_anonymous = False
        '''check token validation'''
        err = UtilsObj.check_authentication(request)
        if err:
            return err
        # check request data
        try:
            data = request.data
            params_data = ['answer', 'questionId', 'postedAsAnonymous']
            err = UtilsObj.check_req_params(data, params_data)
            if type(data['postedAsAnonymous']) is not bool:
                response_ans = {
                    'message': 'Invalide postedAsAnonymous field value'}
                return JsonResponse(response_ans, status=400)
            if err:
                return err

            question_id = data['questionId']
            try:
                get_product_question = db.productQuestion.find_one(
                    {'_id': ObjectId(str(question_id))})
            except:
                response_ans = {'message': 'Invalide questionId'}
                return JsonResponse(response_ans, status=400)

            if get_product_question is None:
                response_ans = {
                    "message": "question is not found"
                }
                return JsonResponse(response_ans, status=404)

            # Get user data
            user_id = json.loads(
                request.META["HTTP_AUTHORIZATION"]).get('userId', "")
            if user_id == "":
                pass
            else:
                # Get user type
                check_roadyo_customer = db.customer.find_one(
                    {"_id": ObjectId(str(user_id))})
                if check_roadyo_customer is None:
                    check_roadyo_seller = db.store.find_one(
                        {"storeId": (str(user_id))})
                    if check_roadyo_seller is None:
                        user_type = "anonymous"
                        user_profile_id = ""
                    else:
                        try:
                            user_type = "roadyo seller"
                            user_profile_id = str(
                                check_roadyo_seller['contactPerson']['profilePic'])
                        except:
                            user_type = "roadyo seller"
                            user_profile_id = ""
                else:
                    user_type = "roadyo customer"
                    user_profile_id = str(check_roadyo_customer['profilePic'])
            if user_type == 'anonymous':
                post_anonymous = True
            create_answerid = ObjectId()
            answer_data = {
                "answerId": create_answerid,
                "answer": str(data['answer']),
                "userId": str(user_id),
                "postedAsAnonymous": post_anonymous,
                "userType": user_type,
                "profilePic": user_profile_id,
                "status": 1,
                "postedOn": datetime.datetime.now().timestamp(),
                "upVoteCount": 0,
                "downVoteCount": 0,
                "abuseCount": 0,
                "upVotes": [],
                "downVote": [],
                "abuse": []
            }
            answer_save = db.productQuestion.update_one(
                {"_id": ObjectId(str(question_id))},
                {
                    "$push": {
                        "answer":
                            answer_data
                    }
                })

            question_find = db.productQuestion.find_one(
                {"_id": ObjectId(str(question_id))})
            answer_count = len(question_find['answer'])

            # for count in question_find:
            #     answer_count = len(count['answer'])
            set_answer = db.productQuestion.update_one({"_id": ObjectId(question_id)}, {
                "$set": {"answerCount": int(answer_count)}
            })
            response_ans = {
                "message": "answer added successfully",
                "answerId": str(create_answerid),
                "answerCount": answer_count
            }
            return JsonResponse(response_ans, status=200)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)

    @swagger_auto_schema(
        method='patch', tags=["ProductQuestionAnswer"],
        operation_description="API for Question about Product",
        # required=["Authorization"],
        # manual_parameters=POST_PRODUCTQUESTION_MANUAL_PARAMS,
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            required=['answerId', 'status'],
            properties={
                "status": openapi.Schema(
                    type=openapi.TYPE_INTEGER,
                    example=3,
                    description="Add status (1: active , 2: inactive, 3: delete)"
                ),

                "answerId": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    example="61fa0f0f261015cf0a74c5d0",
                    description="Add question Id"
                ),
            }
        ),

        responses=COMMON_RESPONSE,
        operation_summary="customer question status change"
    )
    @action(detail=False, methods=['patch'])
    def patch(self, request):
        '''
        1 for active,
        2 for inactive,
        3 for delete)
        '''
        try:
            # check request data
            data = request.data
            params_data = ['answerId', 'status']
            err = UtilsObj.check_req_params(data, params_data)
            if err:
                return err
            # check answerId
            try:
                check_answerid = db.productQuestion.find_one(
                    {"answer.answerId": ObjectId(str(data['answerId']))})
                if check_answerid is None:
                    responses_ans = {'message': 'answer not found'}
                    return JsonResponse(responses_ans, status=404)
            except:
                responses_ans = {'message': 'answerId invalide'}
                return JsonResponse(responses_ans, status=400)

            try:
                status_code = int(data['status'])
            except:
                responses_ans = {'message': 'invalide status'}
                return JsonResponse(responses_ans, status=400)
            if status_code == 1 or status_code == 2 or status_code == 3:
                update_answer_status = db.productQuestion.update_one(
                    {"answer.answerId": ObjectId(str(data['answerId']))},
                    {"$set": {
                        "answer.$.status": status_code
                    }
                    }
                )
                responses_ans = {'message': 'status add successfully'}
                return JsonResponse(responses_ans, status=200)
            else:
                responses_ans = {'message': 'enter valide value'}
                return JsonResponse(responses_ans, status=400)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)


class ProductQuestionAnswerLike(APIView):
    '''' Add Answer like or dislike
    0: like
    1: dislike
    '''

    @swagger_auto_schema(
        method="get",
        tags=["ProductQuestionAnswerLikeDislike"],
        operation_description="API for getting product question answer Like",
        # required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="answerId",
                example="620116b11dcaa6c25b4f2c74",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="answerId",
            ),
        ],
        responses={
            200: "successfully found ",
            404: "data not found",
            422: "Invalide data",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        '''globle variable '''
        try:
            try:
                # answer_id = json.loads(request.META["HTTP_AUTHORIZATION"]).get('answerId', "")
                answer_id = str(request.GET.get("answerId", ""))
                check_answerid = db.productQuestion.find_one(
                    {"answer.answerId": ObjectId(str(answer_id))})
                if check_answerid is None:
                    response_ans = {'message': 'answer not found'}
                    return JsonResponse(response_ans, status=404)
            except:
                response_ans = {'message': 'enter valide token'}
                return JsonResponse(response_ans, status=400)

            get_answer_data = db.productQuestion.aggregate([
                {"$match": {"answer.answerId": ObjectId(str(answer_id))}},
                {"$unwind": "$answer"},
                {"$match": {"answer.answerId": ObjectId(str(answer_id))}},
            ])
            final_answer_data = list(get_answer_data)
            like_count = int(final_answer_data[0]['answer']['upVoteCount']) if final_answer_data[0]['answer'][
                'upVoteCount'] != "" or \
                final_answer_data[0]['answer'][
                'upVoteCount'] is not None else 0

            dislike_count = int(final_answer_data[0]['answer']['downVoteCount']) if final_answer_data[0]['answer'][
                'downVoteCount'] != "" or \
                final_answer_data[0]['answer'][
                'downVoteCount'] is not None else 0
            responses_ans = {
                'message': 'data get successfully',
                "like": int(like_count),
                "dislike": int(dislike_count)
            }
            return JsonResponse(responses_ans, status=200)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)

    @swagger_auto_schema(
        method='post', tags=['ProductQuestionAnswerLikeDislike'],
        operation_desription="API for like or dislike question's answer",
        required=['Authorization'],
        manual_parameters=POST_PRODUCTQUESTION_MANUAL_PARAMS,
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            required=['answerId', 'vote'],
            properties={
                'answerId': openapi.Schema(
                    type=openapi.TYPE_STRING,
                    example='61fb6c2eda8e918a57754253',
                    description="Enter answer Id"
                ),
                "vote": openapi.Schema(
                    type=openapi.TYPE_INTEGER,
                    example=0,
                    description='0: like || 1: dislike'
                ),
            }
        ),
        responses=COMMON_RESPONSE
    )
    @action(detail=False, methods=['post'])
    def post(self, request):
        # check token
        err = UtilsObj.check_authentication(request)
        if err:
            return err
        try:
            # check Data
            data = request.data
            params_data = ['answerId', 'vote']
            err = UtilsObj.check_req_params(data, params_data)
            if err:
                return err

            # check value type
            # if type(data['answerId']) is not String:
            #     response_ans = {"message": "Invalide answerId"}
            #     return JsonResponse(response_ans, status=400)

            try:
                search_answer_id = db.productQuestion.find_one(
                    {"answer.answerId": ObjectId(str(data['answerId']))})
            except:
                response_ans = {'message': 'invalide answerId'}
                return JsonResponse(response_ans, status=400)
            if search_answer_id is None:
                response_ans = {'message': 'answerId not found'}
                return JsonResponse(response_ans, status=404)

            # get userId
            try:
                user_id = json.loads(
                    request.META["HTTP_AUTHORIZATION"]).get('userId', "")
            except:
                response_ans = {'message': 'token invalide'}
                return JsonResponse(response_ans, status=400)

            # check answerId
            try:
                check_answer = db.productQuestion.find_one(
                    {"answer.answerId": ObjectId(str(data['answerId']))})
                if check_answer is None:
                    response_ans = {"message": "answer not found"}
                    return JsonResponse(response_ans, status=404)
            except:
                response_ans = {'message': 'invalide answerId'}
                return JsonResponse(response_ans, status=400)
            try:
                answer_vote = int(data['vote'])
            except:
                response_ans = {'message': 'invalide vote'}
                return JsonResponse(response_ans, status=400)
            if answer_vote == 0:
                # like
                if user_id != "":

                    check_user_id = db.productQuestion.find_one({"answer": {
                        "$elemMatch": {
                            "answerId": ObjectId(str(data['answerId'])),
                            "upVotes": {
                                "$elemMatch": {
                                    "userId": str(user_id)
                                }
                            }
                        }
                    }},
                        {"answer.$.upVotes": 1}
                    )

                    if check_user_id is not None:
                        response_ans = {'message': 'alreay vote given'}
                        return JsonResponse(response_ans, status=422)

                    else:
                        product_vote = db.productQuestion.update_one(
                            {"answer.answerId": ObjectId(
                                str(data['answerId']))},
                            {
                                "$addToSet": {
                                    "answer.$.upVotes": {
                                        "userId": str(user_id),
                                        "createdTimestamp": int(datetime.datetime.now().timestamp())
                                    }
                                }
                            }
                        )

                else:
                    response_ans = {'message': 'enter valide userId'}
                    return JsonResponse(response_ans, status=400)
                threading.Thread(target=self.updateLikeCount,
                                 args=(data,)).start()

            else:
                if answer_vote == 1:

                    if user_id != "":
                        check_user_id = db.productQuestion.find_one({"answer": {
                            "$elemMatch": {
                                "answerId": ObjectId(str(data['answerId'])),
                                "downVote": {
                                    "$elemMatch": {
                                        "userId": str(user_id)
                                    }
                                }
                            }
                        }},
                            {"answer.$.downVote": 1}
                        )
                        if check_user_id is None:
                            product_vote = db.productQuestion.update_one(
                                {"answer.answerId": ObjectId(
                                    str(data['answerId']))
                                 },
                                {
                                    "$addToSet": {
                                        "answer.$.downVote": {
                                            "userId": (user_id),
                                            "createdTimestamp": int(datetime.datetime.now().timestamp())
                                        }
                                    }
                                }
                            )

                        else:
                            response_ans = {'message': 'alreay vote given'}
                            return JsonResponse(response_ans, status=422)
                    else:
                        response_ans = {'message': 'enter valide userId'}
                        return JsonResponse(response_ans, status=400)
                    threading.Thread(
                        target=self.updateDislikeCount, args=(data,)).start()
                else:
                    response_ans = ({'message': 'Enter valide vote value'})
                    return JsonResponse(response_ans, status=400)
            response_ans = {'message': "vote added successfully"}
            return JsonResponse(response_ans, status=200)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)

    @swagger_auto_schema(
        method='patch', tags=['ProductQuestionAnswerLikeDislike'],
        operation_desription="API for change like or dislike question's answer",
        required=['Authorization'],
        manual_parameters=POST_PRODUCTQUESTION_MANUAL_PARAMS,
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            required=['answerId', 'vote'],
            properties={
                'answerId': openapi.Schema(
                    type=openapi.TYPE_STRING,
                    example='61fb6c2eda8e918a57754253',
                    description="Enter answer Id"
                ),
                "vote": openapi.Schema(
                    type=openapi.TYPE_INTEGER,
                    example=0,
                    description='0: like || 1: dislike'
                ),
            }
        ),
        responses=COMMON_RESPONSE
    )
    @action(detail=False, methods=['patch'])
    def patch(self, request):
        ''' change user like or dislike '''
        # check token
        # err = UtilsObj.check_authentication(request)
        # if err:
        #     return err
        try:
            # check Data
            data = request.data
            params_data = ['answerId', 'vote']
            err = UtilsObj.check_req_params(data, params_data)
            if err:
                return err

            # check value type
            # if type(data['answerId']) is not String:
            #     response_ans = {"message": "Invalide answerId"}
            #     return JsonResponse(response_ans, status=400)

            try:
                search_answer_id = db.productQuestion.find_one(
                    {"answer.answerId": ObjectId(str(data['answerId']))})
            except:
                response_ans = {'message': 'invalide answerId'}
                return JsonResponse(response_ans, status=400)
            if search_answer_id is None:
                response_ans = {'message': 'answerId not found'}
                return JsonResponse(response_ans, status=404)

            # get userId
            try:
                user_id = json.loads(
                    request.META["HTTP_AUTHORIZATION"]).get('userId', "")
            except:
                response_ans = {'message': 'token invalide'}
                return JsonResponse(response_ans, status=400)

            # check answerId exist or not
            try:
                check_answer = db.productQuestion.find_one(
                    {"answer.answerId": ObjectId(str(data['answerId']))})
                if check_answer is None:
                    response_ans = {"message": "answer not found"}
                    return JsonResponse(response_ans, status=404)
            except:
                response_ans = {'message': 'invalide answerId'}
                return JsonResponse(response_ans, status=400)
            try:
                answer_vote = int(data['vote'])
            except:
                response_ans = {'message': 'invalide vote'}
                return JsonResponse(response_ans, status=400)
            if answer_vote == 0:
                # like

                if user_id != "":
                    check_user_id = db.productQuestion.find_one({"answer": {
                        "$elemMatch": {
                            "answerId": ObjectId(str(data['answerId'])),
                            "upVotes": {
                                "$elemMatch": {
                                    "userId": str(user_id)
                                }
                            }
                        }
                    }},
                        {"answer.$.upVotes": 1}
                    )
                    if check_user_id is None:
                        response_ans = {'message': 'user not found'}
                        return JsonResponse(response_ans, status=422)

                    else:

                        product_vote = db.productQuestion.update_one(
                            {"answer.answerId": ObjectId(
                                str(data['answerId']))},
                            {
                                "$pull": {
                                    "answer.$.upVotes": {
                                        "userId": str(user_id),
                                    }
                                }
                            }
                        )
                    threading.Thread(
                        target=self.updateLikeCount, args=(data,)).start()
                else:
                    response_ans = {'message': 'enter valide userId'}
                    return JsonResponse(response_ans, status=400)
                # threading.Thread(target=self.updateLikeCount,
                #                  args=(data, )).start()

                # def updateLikeCount(self, data):

            else:

                if answer_vote == 1:
                    if user_id != "":
                        check_user_id = db.productQuestion.find_one({"answer": {
                            "$elemMatch": {
                                "answerId": ObjectId(str(data['answerId'])),
                                "downVote": {
                                    "$elemMatch": {
                                        "userId": str(user_id)
                                    }
                                }
                            }
                        }},
                            {"answer.$.downVote": 1}
                        )
                        if check_user_id is not None:
                            product_vote = db.productQuestion.update_one(
                                {"answer.answerId": ObjectId(
                                    str(data['answerId']))},
                                {
                                    "$pull": {
                                        "answer.$.downVote": {
                                            "userId": str(user_id),
                                        }
                                    }
                                }
                            )

                        else:
                            response_ans = {'message': 'user not found'}
                            return JsonResponse(response_ans, status=422)
                    else:
                        response_ans = {'message': 'enter valide userId'}
                        return JsonResponse(response_ans, status=400)
                    threading.Thread(
                        target=self.updateDislikeCount, args=(data,)).start()
                else:
                    response_ans = ({'message': 'Enter valide vote value'})
                    return JsonResponse(response_ans, status=400)
            response_ans = {'message': "vote added successfully"}
            return JsonResponse(response_ans, status=200)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)

    def updateLikeCount(self, data):
        get_length = db.productQuestion.find_one(
            {"answer.answerId": ObjectId(str(data['answerId']))})

        answer_id_check = data['answerId']
        like_count = db.productQuestion.find_one(
            {"answer.answerId": ObjectId(str(answer_id_check))},
            {"answer.$.answerId": 1}
        )
        get_like_count = like_count
        set_like = db.productQuestion.update_one(
            {"answer.answerId": ObjectId(str(answer_id_check))},
            {
                "$set": {"answer.$.upVoteCount": int(len(get_like_count['answer'][0]['upVotes']))}
            }
        )

    def updateDislikeCount(self, data):
        get_length = db.productQuestion.find_one(
            {"answer.answerId": ObjectId(str(data['answerId']))})

        answer_id_check = data['answerId']
        dislike_count = db.productQuestion.find_one(
            {"answer.answerId": ObjectId(
                str(answer_id_check))},
            {"answer.$.answerId": 1}
        )
        get_like_count = dislike_count

        set_dilike = db.productQuestion.update_one(
            {"answer.answerId": ObjectId(
                str(answer_id_check))},
            {
                "$set": {"answer.$.downVoteCount": int(len(get_like_count['answer'][0]['downVote']))}
            }
        )


class ReportProductQuestionAnswer(APIView):
    ''' report answer '''

    @swagger_auto_schema(
        method="get",
        tags=["reportProductQuestionAnswer"],
        operation_description="API for getting product question answer report",
        # required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="answerId",
                example="6201121af46f63251bad9a11",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description="answerId",
            ),
        ],
        responses={
            200: "successfully found ",
            204: "data not available",
            404: "data not found",
            422: "Invalide data",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            # check answerId
            try:
                check_answerid = request.GET.get("answerId", "")

                if check_answerid is None or check_answerid == "":
                    response_ans = {'message': 'answer not found'}
                    return JsonResponse(response_ans, status=404)
            except:
                response_ans = {'message': 'enter valide answerId'}
                return JsonResponse(response_ans, status=400)
            report_data = db.productQuestion.aggregate([
                {"$match": {"answer.answerId": ObjectId(str(check_answerid))}},
                {"$unwind": "$answer"},
                {"$match": {"answer.answerId": ObjectId(str(check_answerid))}},
            ])
            get_repoet_count = list(report_data)
            report_count = int(get_repoet_count[0]['answer']['abuseCount']) if get_repoet_count[0]['answer'][
                'abuseCount'] != "" or \
                get_repoet_count[0]['answer'][
                'abuseCount'] is not None else 0
            if int(report_count) == 0:
                response_ans = {'message': 'data not available'}
                return JsonResponse(response_ans, status=204)
            response_ans = {
                'message': "data found",
                "count": int(report_count)
            }
            return JsonResponse(response_ans, status=200)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)

    @swagger_auto_schema(
        method='post', tags=['reportProductQuestionAnswer'],
        operation_desription="API for report answer",
        required=['Authorization'],
        manual_parameters=POST_PRODUCTQUESTION_MANUAL_PARAMS,
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            required=['answerId'],
            properties={
                'answerId': openapi.Schema(
                    type=openapi.TYPE_STRING,
                    example='61fb6c2eda8e918a57754253',
                    description="Enter answer Id"
                ),
            }
        ),
        responses=COMMON_RESPONSE
    )
    @action(detail=False, methods=['post'])
    def post(self, request):
        ''' report answer '''

        # check token
        err = UtilsObj.check_authentication(request)
        if err:
            return err
        # check request data value
        data = request.data

        params_data = ['answerId']
        err = UtilsObj.check_req_params(data, params_data)
        if err:
            return err
        # check answerId
        answer_id = data['answerId']
        try:
            check_answer_id = db.productQuestion.find_one(
                {
                    "answer.answerId": ObjectId(str(answer_id))
                },

                {"answer.$.answerId": 1}
            )
            if check_answer_id is None:
                response_ans = {'message': 'answer not found'}
                return JsonResponse(response_ans, status=404)
        except:
            response_ans = {'message': 'invalide answerId'}
            return JsonResponse(response_ans, status=400)
        # get userId
        try:
            user_id = json.loads(
                request.META["HTTP_AUTHORIZATION"]).get('userId', "")
        except:
            response_ans = {'message': 'token invalide'}
            return JsonResponse(response_ans, status=400)

        # fill data
        if user_id != "":
            check_user_id = db.productQuestion.find_one({"answer": {
                "$elemMatch": {
                    "answerId": ObjectId(str(data['answerId'])),
                    "abuse": {
                        "$elemMatch": {
                            "userId": str(user_id)
                        }
                    }
                }
            }},
                {"answer.$.abuse": 1}
            )
            if check_user_id is not None:
                response_ans = {'message': 'vote already given'}
                return JsonResponse(response_ans, status=422)
            product_report = db.productQuestion.update_one(
                {"answer.answerId": ObjectId(str(answer_id))},
                {
                    "$addToSet": {
                        "answer.$.abuse": {
                            "userId": str(user_id),
                            "createdTimestamp": int(datetime.datetime.now().timestamp())
                        }
                    }
                }
            )
        else:
            response_ans = {'message': 'invalide userid'}
            return JsonResponse(response_ans, status=400)
        threading.Thread(target=self.updateAbuseCount, args=(data,)).start()
        response_ans = {'message': 'repoted successfully'}
        return JsonResponse(response_ans, status=200)

    def updateAbuseCount(self, data):
        get_length = db.productQuestion.find_one(
            {"answer.answerId": ObjectId(str(data['answerId']))})

        answer_id_check = data['answerId']
        abuse_count = db.productQuestion.find_one(
            {"answer.answerId": ObjectId(
                str(answer_id_check))},
            {"answer.$.answerId": 1}
        )
        get_like_count = abuse_count

        set_like = db.productQuestion.update_one(
            {"answer.answerId": ObjectId(
                str(answer_id_check))},
            {
                "$set": {"answer.$.abuseCount": int(len(get_like_count['answer'][0]['abuse']))}
            }
        )


class AdminProductQuestion(APIView):
    ''' Add user product question '''

    @swagger_auto_schema(
        method="get",
        tags=["Product Question"],
        operation_description="API for getting product question",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="skip",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example=0,
                required=True,
                description="set skip",
            ),
            openapi.Parameter(
                name="limit",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example=20,
                required=True,
                description="set limit",
            ),
            openapi.Parameter(
                name="parentProductId",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="61f98062bd236905517efa9a",
                required=False,
                description="enter parentProductId if avalible ",
            ),
            openapi.Parameter(
                name="sellerName",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="61f8dc2a610b34546196a44b",
                required=False,
                description="enter storeId if avalible",
            ),
            openapi.Parameter(
                name="trigger",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example="1",
                required=False,
                description="1: New to old  0:Old to new ",
            ),
            openapi.Parameter(
                name="questionType",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example="1",
                required=False,
                description="0:all Question 1: Question without answer , 2: question with answer",
            ),
            openapi.Parameter(
                name="searchName",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="what",
                required=False,
                description="enter searchname if avalible",
            ),
        ],
        responses={
            200: "successfully found ",
            404: "data not found",
            422: "Invalide data",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        ''' Globle variable '''
        question_with_answer = []
        question_without_answer = []
        err = UtilsObj.check_authentication(request)
        if err:
            return err

        question_data = {}
        final_data = []
        question_without_answer = []
        question_with_answer = []
        try:
            user_email = ""
            first_name = ""
            last_name = ""
            trigger = int(request.GET.get("trigger", "0"))
            skip_data = int(request.GET.get("skip", "0"))
            limit_data = int(request.GET.get("limit", "20"))
            store_id = request.GET.get("storeId", "")
            parent_product_id = request.GET.get("parentProductId", "")
            store_category_id = request.GET.get("storeCategoryId", "")
            question_type = int(request.GET.get("questionType", "0"))
            aggrigate_query = {
                "status": 1
            }
            if parent_product_id != "":
                aggrigate_query['parentProductId'] = parent_product_id
            else:
                pass

            if store_id != "":
                if store_id == "0":
                    aggrigate_query['storeId'] = store_id
                else:
                    aggrigate_query['storeId'] = ObjectId(store_id)

            if store_category_id != "":
                aggrigate_query['storeCategoryId'] = store_category_id
            else:
                pass

            serach_text = request.GET.get('searchName', "")
            if serach_text != "":
                final_text = '.*' + serach_text
                aggrigate_query['$or'] = [
                    {"question": {'$regex': final_text, "$options": "i"}},
                    {"productName": {'$regex': final_text, "$options": "i"}}
                ]

            # if question_type == 1:
            #     aggrigate_query['$or'] = [{"answer": {"$exists": False}}, {
            #         '$where': 'this.answer.length<=0'}]
            # elif question_type == 2:
            #     aggrigate_query['$and'] = [{"answer": {"$exists": True}}, {
            #         '$where': 'this.answer.length>0'}]
            else:
                pass

            if trigger == 1:
                ''' most answer '''
                sort_filter = [("_id", -1)]
            else:
                sort_filter = [("_id", 1)]
            find_question_data = db.productQuestion.find(aggrigate_query).sort(
                sort_filter).skip(skip_data).limit(limit_data)
            total_count = db.productQuestion.find(
                aggrigate_query).count()
            get_question_data = list(find_question_data)
            if len(get_question_data) == 0 or get_question_data is None:
                response_ans = {'message': 'question not found'}
                return JsonResponse(response_ans, status=404)
            try:
                final_data = []
                for question in get_question_data:
                    check_userid = question['userId'] if 'userId' in question and question['userId'] != "" else ""
                    if check_userid != "":
                        get_user_data = db.customer.find_one({"_id": ObjectId(question['userId'])})
                        try:
                            get_user_data = db.customer.find_one({"_id": ObjectId(question["userId"])})
                            if get_user_data is None:
                                get_manager_detail = db.managers.find_one({"_id": ObjectId(question["userId"])})
                                if get_manager_detail is not None:
                                    first_name = get_manager_detail['firstName']
                                    last_name = get_manager_detail['lastName']
                                    user_email = get_manager_detail['email']
                                    profile_pic = get_manager_detail['profilePic']
                            else:
                                first_name = get_user_data['firstName']
                                last_name = get_user_data['lastName']
                                user_email = get_user_data['email']
                                profile_pic = get_user_data['profilePic']
                        except:
                            pass
                    answer_data = []
                    que_ans = question['answer'] if 'answer' in question else []
                    if que_ans == []:
                        pass
                    else:
                        for que in que_ans:
                            que['answerId'] = str(que['answerId'])
                            if int(que['status']) == 1:
                                answer_data.append(que)
                            else:
                                pass
                    question_data =   {
                            "_id": str(question['_id']),
                            "productName": question['productName'],
                            "productId": str(question['productId']),
                            "parentProductId": question['parentProductId'] if 'parentProductId' in question else "",
                            "storeId": str(question['storeId']) if 'storeId' in question else "",
                            "storeCategoryId": question['storeCategoryId'] if 'storeCategoryId' in question else "",
                            "storeName": question['storeName'] if 'storeName' in question else "",
                            "question": question['question'],
                            "userId": str(question['userId']) if 'userId' in question else "",
                            "userName": first_name + " " + last_name if first_name != "" else 'anonymous',
                            "email":user_email if user_email != '' else '-',
                            "profilePic": question['profilePic'] if 'profilePic' in question else "-",
                            "postedOn": question['postedOn'],
                            "answerCount": len(answer_data),
                            "answer": answer_data,
                        }
                    check_status = int(question_data['answerCount'])
                    if check_status >= 1:
                        question_with_answer.append(question_data)
                    elif check_status == 0:
                        question_without_answer.append(question_data)
                    else:
                        pass
                if question_type == 1:
                    final_data = question_without_answer
                else:
                    final_data = question_with_answer
            except Exception as ex:
                return UtilsObj.raise_exception(ex)

            if len(question_data) == 0:
                response_ans = {
                    "message": "question not found"
                }
                return JsonResponse(response_ans, status=204)
            else:
                response_ans = {
                    'data': final_data,
                    'count': len(final_data),
                    'penCount': len(final_data),
                    "message": "data found"
                }
                return JsonResponse(response_ans, status=200)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)


class UserLikeCount(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["question answer likeDislike count"],
        operation_description="API for getting product question",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="skip",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example=0,
                required=True,
                description="set skip",
            ),
            openapi.Parameter(
                name="limit",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example=20,
                required=True,
                description="set limit",
            ),
            openapi.Parameter(
                name="answerId",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="621364bfef620ade097de7ed",
                required=True,
                description="Enter answerId",
            ),
        ],
        responses={
            200: "successfully found ",
            404: "data not found",
            422: "Invalide data",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            err = UtilsObj.check_authentication(request)
            if err:
                return err
            first_name = ""
            last_name= ""
            user_email = ""
            profile_pic = ""
            like_count = []
            answer_id = request.GET.get('answerId', "")
            skip = int(request.GET.get('skip', "0"))
            limit = int(request.GET.get('limit', "20"))

            if answer_id == "":
                response = {"message": "answerId empty"}
                return JsonResponse(response, status=404)
            find_answer_id = db.productQuestion.find_one(
                {"answer.answerId": ObjectId(answer_id)})
            if find_answer_id is None or find_answer_id == "":
                response = {"message": "answer not found"}
                return JsonResponse(response, status=204)

            answer_likecount = db.productQuestion.aggregate([
                {"$unwind": "$answer"},
                {"$match": {'answer.answerId': ObjectId(answer_id)}},
                {"$project": {"upVotes": "$answer.upVotes", "_id": 0}},
                {"$unwind": "$upVotes"},
                {"$project": {"userId": "$upVotes.userId",
                              "createdTimestamp": "$upVotes.createdTimestamp"}},
                {"$skip": skip},
                {"$limit": limit}
            ])
            length_like = db.productQuestion.aggregate([
                {"$unwind": "$answer"},
                {"$match": {'answer.answerId': ObjectId(answer_id)}},
                {"$project": {"upVotes": "$answer.upVotes", "_id": 0}},
                {"$unwind": "$upVotes"},
                {"$project": {"userId": "$upVotes.userId",
                              "createdTimestamp": "$upVote.createdTimestamp"}}
            ])
            user_like_length = list(length_like)
            if len(user_like_length) == 0:
                response = {'message': 'data not available'}
                return JsonResponse(response, status=204)
            count_list = answer_likecount

            for count_data in count_list:
                try:
                    get_user_detail = db.customer.find_one({"_id": ObjectId(count_data["userId"])})
                    if get_user_detail is None:
                        get_manager_detail = db.managers.find_one({"_id": ObjectId(count_data["userId"])})
                        if get_manager_detail is not None:
                            first_name = get_manager_detail['firstName']
                            last_name = get_manager_detail['lastName']
                            user_email = get_manager_detail['email']
                            profile_pic = get_manager_detail['profilePic']
                    else:
                        first_name = get_user_detail['firstName']
                        last_name = get_user_detail['lastName']
                        user_email = get_user_detail['email']
                        profile_pic = get_user_detail['profilePic']
                except:
                    pass
                # dislike_count.append(count_data)
                like_count.append({
                    "userId": count_data["userId"] if "userId" in count_data else "",
                    "firstName": first_name if first_name != "" else 'anonymous',
                    "lastName": last_name if last_name != "" else '-',
                    "email": user_email if user_email != "" else "-",
                    "profilePic": profile_pic if profile_pic !="" else "-",
                    "time": count_data["createdTimestamp"],
                })

            response = {'message': "data found", "data": like_count,
                        "totalCount": len(user_like_length)}
            return JsonResponse(response, status=200)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)


class UserDislikeCount(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["question answer likeDislike count"],
        operation_description="API for getting product question",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="skip",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example=0,
                required=True,
                description="set skip",
            ),
            openapi.Parameter(
                name="limit",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example=20,
                required=True,
                description="set limit",
            ),
            openapi.Parameter(
                name="answerId",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="621364bfef620ade097de7ed",
                required=True,
                description="Enter answerId",
            ),
        ],
        responses={
            200: "successfully found ",
            404: "data not found",
            422: "Invalide data",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            err = UtilsObj.check_authentication(request)
            if err:
                return err
            first_name = ""
            last_name = ""
            user_email = ""
            profile_pic = ""
            dislike_count = []
            answer_id = request.GET.get('answerId', "")
            skip = int(request.GET.get('skip', "0"))
            limit = int(request.GET.get('limit', "20"))
            if answer_id == "":
                response = {"message": "answerId empty"}
                return JsonResponse(response, status=404)

            find_answer_id = db.productQuestion.find_one(
                {"answer.answerId": ObjectId(answer_id)})
            if find_answer_id is None or find_answer_id == "":
                response = {"message": "answer not found"}
                return JsonResponse(response, status=204)

            answer_dislikecount = db.productQuestion.aggregate([
                {"$unwind": "$answer"},
                {"$match": {'answer.answerId': ObjectId(
                            answer_id)}},
                {"$project": {"downVote": "$answer.downVote", "_id": 0}},
                {"$unwind": "$downVote"},
                {"$project": {"userId": "$downVote.userId",
                              "createdTimestamp": "$downVote.createdTimestamp"}},
                {"$skip": skip},
                {"$limit": limit}
            ])
            length_dislike = db.productQuestion.aggregate([
                {"$unwind": "$answer"},
                {"$match": {'answer.answerId': ObjectId(
                            answer_id)}},
                {"$project": {"downVote": "$answer.downVote", "_id": 0}},
                {"$unwind": "$downVote"},
                {"$project": {"userId": "$downVote.userId",
                              "createdTimestamp": "$downVote.createdTimestamp"}},
            ])
            user_dislike_length = list(length_dislike)
            if len(user_dislike_length) == 0:
                response = {'message': 'data not available'}
                return JsonResponse(response, status=204)
            count_list = answer_dislikecount

            for count_data in count_list:
                get_user_detail = db.customer.find_one({"_id": ObjectId(count_data["userId"])})
                try:
                    get_user_detail = db.customer.find_one({"_id": ObjectId(count_data["userId"])})
                    if get_user_detail is None:
                        get_manager_detail = db.managers.find_one({"_id": ObjectId(count_data["userId"])})
                        if get_manager_detail is not None:
                            first_name = get_manager_detail['firstName']
                            last_name = get_manager_detail['lastName']
                            user_email = get_manager_detail['email']
                            profile_pic = get_manager_detail['profilePic']
                    else:
                        first_name = get_user_detail['firstName']
                        last_name = get_user_detail['lastName']
                        user_email = get_user_detail['email']
                        profile_pic = get_user_detail['profilePic']
                except:
                    pass
                # dislike_count.append(count_data)
                dislike_count.append({
                    "userId": count_data["userId"] if "userId" in count_data else "",
                    "firstName": first_name if first_name != "" else 'anonymous',
                    "lastName": last_name if last_name != "" else '-',
                    "email": user_email if user_email != "" else '-',
                    "profilePic": profile_pic if profile_pic != "" else '-',
                    "time": count_data["createdTimestamp"],
                })


            response = {'message': "data found", "data": dislike_count,
                        "totalCount": len(user_dislike_length)}
            return JsonResponse(response, status=200)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)


class UserReportCount(APIView):
    @swagger_auto_schema(
        method="get",
        tags=["questionAnswer user report count"],
        operation_description="API for getting product question",
        required=["AUTHORIZATION", "language"],
        manual_parameters=[
            openapi.Parameter(
                name="skip",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example=0,
                required=False,
                description="set skip",
            ),
            openapi.Parameter(
                name="limit",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                example=20,
                required=False,
                description="set limit",
            ),
            openapi.Parameter(
                name="answerId",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                example="621364bfef620ade097de7ed",
                required=True,
                description="Enter answerId",
            ),
        ],
        responses={
            200: "successfully found ",
            404: "data not found",
            422: "Invalide data",
            500: "Internal Server Error. if server is not working that time",
        },
    )
    @action(detail=False, methods=["get"])
    def get(self, request):
        try:
            try:
                err = UtilsObj.check_authentication(request)
                if err:
                    return err
            except:
                return JsonResponse(status=400)
            first_name = ""
            last_name = ""
            user_email = ""
            profile_pic = ""
            totle_count = 0
            report_count = []

            answer_id = request.GET.get('answerId', "")
            skip = int(request.GET.get('skip', "0"))
            limit = int(request.GET.get('limit', "20"))

            if answer_id is None or answer_id == "":
                response = {"message": "enter answerId"}
                return JsonResponse(response, status=422)
            check_answe_id = db.productQuestion.find_one({
                "answer.answerId": ObjectId(answer_id)
            })
            if check_answe_id is None or check_answe_id == "":
                response = {"message": "answer not found"}
                return JsonResponse(response, status=404)

            get_report_count = db.productQuestion.aggregate([
                {"$unwind": "$answer"},
                {"$match": {'answer.answerId': ObjectId(
                            answer_id)}},
                {"$project": {"abuse": "$answer.abuse", "_id": 0}},
                {"$unwind": "$abuse"},
                {"$project": {"userId": "$abuse.userId",
                              "createdTimestamp": "$abuse.createdTimestamp"}},
                {"$skip": skip},
                {"$limit": limit}
            ])
            length_report = db.productQuestion.aggregate([
                {"$unwind": "$answer"},
                {"$match": {'answer.answerId': ObjectId(
                            answer_id)}},
                {"$project": {"abuse": "$answer.abuse", "_id": 0}},
                {"$unwind": "$abuse"},
                {"$project": {"userId": "$abuse.userId",
                              "createdTimestamp": "$abuse.createdTimestamp"}},
                {"$count": "totleCount"}
            ])

            for check_count in length_report:
                totle_count = check_count['totleCount']
            if totle_count == 0:
                response = {'message': 'data not available'}
                return JsonResponse(response, status=204)
            
            count_list = get_report_count
            
            for count_data in count_list:
                try:
                    get_user_detail = db.customer.find_one({"_id": ObjectId(count_data["userId"])})
                    if get_user_detail is None:
                        get_manager_detail = db.managers.find_one({"_id": ObjectId(count_data["userId"])})
                        if get_manager_detail is not None:
                            first_name = get_manager_detail['firstName']
                            last_name = get_manager_detail['lastName']
                            user_email = get_manager_detail['email']
                            profile_pic = get_manager_detail['profilePic']
                    else:
                        first_name = get_user_detail['firstName']
                        last_name = get_user_detail['lastName']
                        user_email = get_user_detail['email']
                        profile_pic = get_user_detail['profilePic']
                except:
                    pass
                # dislike_count.append(count_data)
                report_count.append({
                    "userId": count_data["userId"] if "userId" in count_data else "",
                    "firstName": first_name if first_name != "" else 'anonymous',
                    "lastName": last_name if last_name != "" else '-',
                    "email": user_email if user_email != "" else '-',
                    "profilePic": profile_pic,
                    "time": count_data["createdTimestamp"],
                })


            response = {'message': "data found", "data": report_count,
                        "totalCount": totle_count}
            return JsonResponse(response, status=200)
        except Exception as ex:
            return UtilsObj.raise_exception(ex)