# Create your views here.
import os
import sys
import time
import re
from array import array

from bson.objectid import ObjectId
from django.http import JsonResponse
from drf_yasg.utils import swagger_auto_schema
from requests.adapters import Response
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.views import APIView

from .response_handler import ResponseHandlerObj
from .api_doc import APIDocObj
from search_api.settings import db

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")


class ArtNiche(APIView):
    '''
        API to get the niches for various arts or skills.
    '''
    @swagger_auto_schema(
        method='get', tags=["BookStar"],
        operation_description="API to get the niches for various arts or skills.",
        required=['AUTHORIZATION', 'entityId'],
        manual_parameters=APIDocObj.GET_ARTNICHE_MANUAL_PARAMS,
        responses= ResponseHandlerObj.GET_ARTNICHE_RESPONSES_RESPONSE,
        operation_summary="Get Categories")
    @action(detail=False, methods=['get'])
    def get(self, request):
        try:
            # check for the authentication
            auth_er = ResponseHandlerObj.check_authentication(request)
            if auth_er: return auth_er

            # get params
            search_query = request.GET.get('search', '')
            from_data = int(request.GET['from']) if not ValueError else 0
            to_data = int(request.GET['to']) if not ValueError else 10
            data = []

            query = {}
            if bool(search_query):
                query['entityName'] = {
                    '$regex': search_query,
                    '$options' : 'i'
                }
            result_count = db.featuredSections.find(query).count()
            result = db.featuredSections.find(query).skip(from_data).limit(to_data)
            data = [
                {
                'id': str(d['_id']),
                'title': d['entityName'],
                'countTitle': "Choose From",
                'memberCount': len(d['entityIds']),
                'Image': d['imgUrl'],
            } for d in result]

            json_resp = {
                'data': data,
                'message': ResponseHandlerObj.MSG_200,
                'penCount': result_count
            }
            return JsonResponse(json_resp, safe=False, status=200)
        except Exception as ex:
            exception_resp = ResponseHandlerObj.generate_exception(ex, request)
            return JsonResponse(exception_resp, status=500)


class NicheRelatedArtists(APIView):
    '''
        API to get the artists or user from specific niche or skill.
    '''
    @swagger_auto_schema(
        method='get', tags=["BookStar"],
        operation_description="API to get the artists or user from specific niche or skill.",
        required=['AUTHORIZATION', 'featureId'],
        manual_parameters=APIDocObj.GET_NICHE_RELATED_ARTISTS_MANUAL_PARAMS,
        responses= ResponseHandlerObj.GET_NICHE_RELATED_ARTISTS_RESPONSES_RESPONSE,
        operation_summary="Get Category Users"
        )
    @action(detail=False, methods=['get'])
    def get(self, request):
        try:
            # check for the authentication
            auth_er = ResponseHandlerObj.check_authentication(request)
            if auth_er: return auth_er
            featureId = request.GET.get('featureId', '')

            # # check for the required params
            # params = ['featureId']
            # error = ResponseHandlerObj.check_req_params(request.GET, params)
            # if error: return error

            search_query = request.GET.get('search', '')
            from_data = int(request.GET['from']) if not ValueError else 0
            to_data = int(request.GET['to']) if not ValueError else 10
            penCount = 0


            niche_artists_search_agg = [
                {
                    "$lookup":
                    {
                        "from": "customer",
                        "localField": "entityIds",
                        "foreignField": "_id",
                        "as": "user"
                    }
                },
                {
                    "$unwind": "$user"
                },
                {
                    "$project": {
                        '_id': '$_id',
                        'data': {
                            'id': '$user._id',
                            'firstName': '$user.firstName',
                            'lastName': '$user.lastName',
                            'profilePic': '$user.profilePic',
                            'storeId': '$user.storeId',
                            'featuredCategoryName': '$entityName',
                            'featuredCategoryId': '$_id'
                        }
                    }
                },
                {"$skip": int(from_data)},
                {"$limit": int(to_data)}
            ]

            result_count_query = [
                {
                    "$lookup":
                    {
                        "from": "customer",
                        "localField": "entityIds",
                        "foreignField": "_id",
                        "as": "user"
                    }
                },
                {
                    "$unwind": "$user"
                },
                {
                    "$project": {
                        '_id': '$_id',
                        'data': {
                            'data': '$user._id'
                        }
                    }
                },
                {
                    '$group':
                        {
                            '_id': "$user",
                            "penCount": {"$sum":1},
                        }
                },
            ]

            if bool(search_query):
                search_agg = {
                    '$match': {
                        "$or":
                        [
                            {"user.firstName":{"$regex": search_query, "$options": "i"}},
                            {"user.lastName":{"$regex": search_query, "$options": "i"}},
                            {"user.userName":{"$regex": search_query, "$options": "i"}}
                        ]
                    }
                }
                # insert aggregate query in index for proper output
                niche_artists_search_agg.insert(2, search_agg)
                result_count_query.insert(2, search_agg)

            if bool(featureId):
                match_agg_que = {"$match": {'_id': ObjectId(featureId)}}
                niche_artists_search_agg.insert(0, match_agg_que)
                result_count_query.insert(0, match_agg_que)


            result_count = db.featuredSections.aggregate(result_count_query)
            for res in result_count:
                penCount += res['penCount']

            result = db.featuredSections.aggregate(niche_artists_search_agg)
            # print(niche_artists_search_agg)

            artist_data = []
            artist_store_ids = []
            for rdata in result:
                storeId = rdata['data']['storeId']
                if storeId != 0 and storeId != '0':
                    artist_store_ids.append(rdata['data']['storeId'])
                artist_data.append({
                    'id': str(rdata['data']['id']),
                    'firstName': rdata['data']['firstName'],
                    'lastName': rdata['data']['lastName'],
                    'profilePic': rdata['data']['profilePic'],
                    'storeId': rdata['data']['storeId'],
                    'featuredCategoryName': rdata['data']['featuredCategoryName'],
                    'featuredCategoryId': str(rdata['data']['featuredCategoryId']),
                })

            artist_availability_query = [
                {
                    "$match":
                    {
                        'storeId':
                        {
                            '$in': artist_store_ids
                        }
                    }
                },
                {
                    "$project": {
                        'data': {
                            'id': '$_id',
                            'isVideoCallSlot': '$isVideoCallSlot',
                            'isVideoShoutout': '$isVideoShoutout',
                        }
                    }
                },
            ]

            result = db.stores.aggregate(artist_availability_query)
            for avail in result:
                for adata in artist_data:
                    if str(adata['storeId']) == str(avail['data']['id']):
                        adata['isVideoCallSlot'] = avail['data']['isVideoCallSlot']
                        adata['isVideoShoutout'] = avail['data']['isVideoShoutout']
                    else:
                        adata['isVideoCallSlot'] = False
                        adata['isVideoShoutout'] = False

            json_resp = {
                'data': artist_data,
                'message': ResponseHandlerObj.MSG_200,
                'penCount': penCount
            }
            return JsonResponse(json_resp, safe=False, status=200)
        except Exception as ex:
            exception_resp = ResponseHandlerObj.generate_exception(ex, request)
            return JsonResponse(exception_resp, status=500)
