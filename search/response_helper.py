from rest_framework.response import Response
from rest_framework import status
from drf_yasg import openapi

class ResponseHelper:

    MSG_200 = "Data found successfully."
    MSG_SUCCESS = "success"
    MSG_401 = "Unauthorized or token expired."
    MSG_404 = "Data not found."
    MSG_422 = "Mandatory parameter(s) `{}` is/are missing."
    MSG_BAD_REQ = "Bad request parmeters."
    MSG_500 = "Internal server error."
    USER_MSG = "Ops!!! something went wrong. Please try again later."

    INTERNAL_SERVER_ERROR_RESPONSE = {
        "data": [],
        "message": MSG_500,
        "userMessage": USER_MSG,
        "penCount": 0,
        "error": ""
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

    def gen_422_msg(self, param_name='entityId'):
        return self.MSG_422.format(param_name)

    def get_status_401(self, msg="unauthorized"):
        return Response(msg, status=status.HTTP_401_UNAUTHORIZED)

    def get_status_200(self, data):
        return Response(data, status=status.HTTP_200_OK)

    def get_status_404(self, data):
        return Response(data, status=status.HTTP_404_NOT_FOUND)

    def get_status_422(self, data):
        return Response(data, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

    def get_status_500(self, data):
        return Response(data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    COMMON_MANUAL_PARAMS = [
        openapi.Parameter(
        name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
        description="Auth token"),
        openapi.Parameter(
        name='language', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
        description="language of in which language need data", default="en"),
    ]

    STORE_LAST_CHECKIN_MANUAL_PARAMS = [
        openapi.Parameter(
            name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
            description="authorization token"),
        openapi.Parameter(
            name='language', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
            description="language of in which language need data", default="en"),
        openapi.Parameter(
            name='storeId',
            default="60f6b5e7a35d274eb07d6849",
            in_=openapi.IN_QUERY,
            required=False,
            type=openapi.TYPE_STRING,
            description="store id"
    )]

    STORE_DETAILS_MANUAL_PARAMS = [
        openapi.Parameter(
            name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
            description="authorization token"),
        openapi.Parameter(
            name='language', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
            description="language of in which language need data", default="en"),
        openapi.Parameter(
            name='lat', required=True, default="13.05176", in_=openapi.IN_QUERY,
            type=openapi.TYPE_STRING, description="latitude of the user's location"),
        openapi.Parameter(
            name='long', default="77.580448", required=True, in_=openapi.IN_QUERY,
            type=openapi.TYPE_STRING, description="longitude of the user's location"),
        openapi.Parameter(
            name='storeId',
            default="61372abc4b25ef001308bfcd",
            in_=openapi.IN_QUERY,
            required=True,
            type=openapi.TYPE_STRING,
            description="store id"
        ),
    ]

    PROJECT_POST_REQ_BODY = openapi.Schema(
        type=openapi.TYPE_OBJECT,
        properties={
            'data': openapi.Schema(type=openapi.TYPE_ARRAY,
            items=openapi.Items(
                type=openapi.TYPE_OBJECT,
                required=["projectId", "review", "reviewTitle", "rating", "customerId", "storeId"],
                properties={
                    "_id": openapi.Schema(type=openapi.TYPE_STRING,
                        description="required to update existing record.",
                        example="61642a0bb0bf6c855ba991f1"),
                    "projectId": openapi.Schema(type=openapi.TYPE_STRING, example="60f8fd659db8255ed18f9b6b"),
                    "reviewTitle": openapi.Schema(type=openapi.TYPE_STRING, example="Good Quality"),
                    "review": openapi.Schema(type=openapi.TYPE_STRING, example="Best product"),
                    "rating": openapi.Schema(type=openapi.TYPE_INTEGER, example=5),
                    "customerId": openapi.Schema(type=openapi.TYPE_STRING, example='61372abc4b25ef001308bfcf'),
                    "storeId": openapi.Schema(type=openapi.TYPE_STRING, example='61372abc4b25ef001308bfcf', default=0),
                    "status": openapi.Schema(type=openapi.TYPE_NUMBER,
                        description="1 for active and 2 for inactive",
                        example=1, default=1),
                },
            ),
            description="array of project reviews"),
        },
    )

    PROJECT_REVIEW_RATING_GET_MANUAL_PARAMS = [
        openapi.Parameter(
        name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
        description="Auth token"),
        openapi.Parameter(
        name='language', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True,
        description="language of in which language need data", default="en"),
        openapi.Parameter(
            name='projectIds',
        type=openapi.TYPE_ARRAY, items=openapi.Items(
                type=openapi.TYPE_STRING, example='61642a0bb0bf6c855ba991f1'),
            in_=openapi.IN_QUERY,
        )
    ]

    RESPONSES = {
        200: openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'message': openapi.Schema(type=openapi.TYPE_STRING,
                                        description="response message of the request",
                                        example='data found successfully')
            }),
        401: openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'message': openapi.Schema(type=openapi.TYPE_STRING,
                                        description="token expire message",
                                        example="unauthorized")
            }),
        404: openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'message': openapi.Schema(type=openapi.TYPE_STRING,
                                        description="data not found",
                                        example="stores not found")
            }),
        500: openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'message': openapi.Schema(type=openapi.TYPE_STRING,
                                        description="message for the error",
                                        example="Internal Server Error. if server is not working that time")
            }),
        }

RespHelperObj = ResponseHelper()

STORE_DETAILS_GET_REPONSES = RespHelperObj.RESPONSES
STORE_DETAILS_GET_REPONSES[200] = RespHelperObj.json_to_openapi_schema({
    "message": "Data found successfully.",
    "data": {
        "_id": {
        "$oid": "61372abc4b25ef001308bfcd"
        },
        "storeId": "61372abc4b25ef001308bfcd",
        "shopifyId": "",
        "reSellerType": 1,
        "supplierType": 1,
        "storeManagedBy": 2,
        "distributionCenter": {},
        "parentStore": 1,
        "multipleStoreFronts": False,
        "parentSellerIdOrSupplierId": "0",
        "uniqStoreId": "61372abc4b25ef001308bfce",
        "parentSellerNameOrSupplierName": "",
        "storeFrontTypeId": 6,
        "storeFrontType": "Parent Store",
        "sellerTypeId": 1,
        "sellerType": "Retailer",
        "storeTypeId": 2,
        "storeType": "grocery",
        "shopPickerAndPackerBy": 1,
        "shopPickerAndPackerByText": "Store Manager",
        "categoryName": {
        "en": "Retail Stores"
        },
        "categoryId": "6111157583fb5f447a3c3f12",
        "storeFronts": [],
        "status": 1,
        "statusMsg": "approved & active",
        "lastStatusLog": {
        "action": "approved & active",
        "status": 1,
        "reason": "",
        "actionByUserType": "admin",
        "actionByUserId": "0",
        "timestamp": 1631005414
        },
        "statusLogs": [
        {
            "action": "pendingApproval",
            "actionByUserType": "byUser",
            "actionByUserId": "0",
            "timestamp": 1631005372
        },
        {
            "action": "approved & active",
            "status": 1,
            "reason": "",
            "actionByUserType": "admin",
            "actionByUserId": "0",
            "timestamp": 1631005414
        }
        ],
        "workingHour": 0,
        "storeName": {
        "en": "Appscrip"
        },
        "companyName": "Appscrip",
        "isVideoCallSlot": True,
        "isVideoShoutout": True,
        "priceVideoCallSlot": 0,
        "priceVideoShoutout": 0,
        "planIdVideoCallIAP": "",
        "planIdVideoShoutoutIAP": "",
        "logoImages": {
        "logoImageMobile": "https://cdn.shoppd.net/StorePage/0/0/medium/download_1631005188.png",
        "logoImageThumb": "https://cdn.shoppd.net/StorePage/0/0/small/download_1631005188.png",
        "logoImageweb": "https://cdn.shoppd.net/StorePage/0/0/medium/download_1631005188.png"
        },
        "listingImage": {
        "listingImageMobile": "https://cdn.shoppd.net/StorePage/0/0/medium/download_1631005222.png",
        "listingImageThumb": "https://cdn.shoppd.net/StorePage/0/0/small/download_1631005222.png",
        "listingImageweb": "https://cdn.shoppd.net/StorePage/0/0/medium/download_1631005222.png"
        },
        "bannerImages": {
        "bannerImageMobile": "https://cdn.shoppd.net/StorePage/0/0/medium/download_1631005205.png",
        "bannerImageThumb": "https://cdn.shoppd.net/StorePage/0/0/small/download_1631005205.png",
        "bannerImageweb": "https://cdn.shoppd.net/StorePage/0/0/medium/download_1631005205.png"
        },
        "galleryImages": [
        {
            "thumbnail": "https://cdn.shoppd.net/StorePage/0/0/small/download_1631005359.png",
            "mobile": "https://cdn.shoppd.net/StorePage/0/0/medium/download_1631005359.png",
            "image": "https://cdn.shoppd.net/StorePage/0/0/medium/download_1631005359.png",
            "altText": "Appscrip-Bengaluru-560024"
        }
        ],
        "contactEmail": "rinkesh@appscrip.co",
        "officeNumber": {
        "Phone": "9737972524",
        "countryCode": "+91"
        },
        "storeSeo": {
        "title": "Appscrip-Ganganagar-Bengaluru",
        "metaTags": "Appscrip,Ganganagar,Bengaluru",
        "slug": "www.appscrip.com/",
        "description": "Appscrip.co",
        "facebook": "https://cdn.shoppd.net/StorePage/0/0/medium/download_1631005188.png",
        "twitter": "https://cdn.shoppd.net/StorePage/0/0/medium/download_1631005188.png",
        "socialGraph": "https://cdn.shoppd.net/StorePage/0/0/medium/download_1631005188.png"
        },
        "contactPhone": {
        "countryCode": "+91",
        "number": "9737972524"
        },
        "contactPerson": {
        "firstName": "Rinkesh",
        "lastName": "Kalathiya",
        "designation": "CEO",
        "profilePic": "https://cdn.shoppd.net/StorePage/0/0/small/download_1631005271.png"
        },
        "documents": [],
        "citiesOfOperation": {
        "country": [
            {
            "_id": "613713c44f219a11cb5b4a82",
            "country": "India"
            }
        ],
        "city": [
            {
            "_id": "61371411987246667e1e42a2",
            "cityName": "Bengaluru"
            }
        ]
        },
        "businessLogoImages": {
        "businessLogoThumbPath": "",
        "businessLogoMobilePath": "",
        "businessLogoWebPath": ""
        },
        "businessLocationAddress": {
        "googlePlaceName": "3Embed Software Technologies Pvt. Ltd.",
        "route": "10th Cross Street",
        "sublocality": "RBI Colony",
        "areaOrDistrict": "Ganganagar",
        "locality": "Bengaluru",
        "city": "Bengaluru",
        "state": "Karnataka",
        "country": "India",
        "postCode": "560024",
        "addressLine1": "10th Cross Street",
        "addressLine2": "RBI Colony",
        "addressArea": "Ganganagar",
        "lat": "13.0287034",
        "long": "77.5895845",
        "address": "3Embed Software Technologies Pvt. Ltd., 10th Cross Street, RBI Colony, Ganganagar, Bengaluru, Karnataka, India"
        },
        "storeAttributes": [],
        "specialities": [],
        "isExpressDelivery": 0,
        "billingAddress": {
        "googlePlaceName": "3Embed Software Technologies Pvt. Ltd.",
        "route": "10th Cross Street",
        "sublocality": "RBI Colony",
        "areaOrDistrict": "Ganganagar",
        "locality": "Bengaluru",
        "city": "Bengaluru",
        "state": "Karnataka",
        "country": "India",
        "postCode": "560024",
        "addressLine1": "10th Cross Street",
        "addressLine2": "RBI Colony",
        "addressArea": "Ganganagar",
        "lat": "13.0287034",
        "long": "77.5895845",
        "address": "3Embed Software Technologies Pvt. Ltd., 10th Cross Street, RBI Colony, Ganganagar, Bengaluru, Karnataka, India"
        },
        "convenientFeeType": 1,
        "convenienceFee": 0,
        "headOffice": {
        "headOfficeCountryCode": "+91",
        "headOfficeNumber": "9737972524",
        "headOfficeArea": "",
        "headOfficeCountry": "",
        "headOfficeAddress": "",
        "headOfficeCity": "",
        "headOfficeCityId": "",
        "headOfficeCountryId": "",
        "headOfficeZipCode": "",
        "headOfficeLat": 0,
        "headOfficeLongi": 0
        },
        "cityId": "61371411987246667e1e42a2",
        "cityName": "Bengaluru",
        "countryId": "613713c44f219a11cb5b4a82",
        "countryName": "India",
        "location": {
        "lat": 13.0287034,
        "lon": 77.5895845
        },
        "storeListing": False,
        "hyperlocal": False,
        "ecommerce": False,
        "deliveryDoneViaDc": False,
        "about": "Appscrip.co",
        "websiteUrl": "Appscrip.co",
        "taxId": "TAX121",
        "orderSettingsFlag": 1,
        "orderSettings": "acceptanceRequired",
        "driverTypeId": 3,
        "driverType": "PartnerDelivery",
        "cashOnPickUp": True,
        "pickUpPrePaymentCard": False,
        "cardOnPickUp": False,
        "cashOnDelivery": True,
        "deliveryPrePaymentCard": False,
        "cardOnDelivery": False,
        "acceptsCashOnDelivery": False,
        "acceptsCard": False,
        "acceptsWallet": False,
        "statusAvailability": 1,
        "statusAvailabilityMsg": "Online",
        "sellerOpen": False,
        "currencyCode": "INR",
        "currencySymbol": "₹",
        "currencyCountryCode": "IN",
        "pricingModel": "2",
        "sellerAverageRating": {
        "attributeId": "",
        "avgValue": ""
        },
        "averageCostForMealForTwo": 0,
        "averagePreparationTimeInMins": "",
        "averageDeliveryTimeInMins": 0,
        "socialLinks": {
        "google": "",
        "youtube": "Appscrip.co",
        "facebook": "Appscrip.co",
        "twitter": "Appscrip.co",
        "instagram": "Appscrip.co",
        "linkedIn": "Appscrip.co"
        },
        "seqId": 1,
        "serviceZones": [],
        "registrationDateTimeStamp": 1631005372,
        "freeDeliveryAbove": 0,
        "minimumOrder": 0,
        "termsAndCondition": "<p>Appscrip.co</p>\n",
        "safetyStandards": 0,
        "safetyStandardsSortDiscription": "",
        "safetyStandardsDynamicContent": "",
        "pricingSettings": 0,
        "operatorServiceType": 0,
        "fixedPricing": [],
        "mileagePriceSetting": {
        "enable": False,
        "baseFare": 0,
        "mileagePrice": 0,
        "mileagePriceAfterDistance": 0,
        "minimumFare": 0
        },
        "volumeFixedPricing": [],
        "weightFixedPricing": [],
        "autoAcceptOrders": False,
        "planId": "61371465987246667e1e42a5",
        "planName": "Bengaluru-plan",
        "planLogs": [
        {
            "planId": "61371465987246667e1e42a5",
            "planName": "Bengaluru-plan",
            "timeStamp": 1631005372
        },
        {
            "planId": "61371465987246667e1e42a5",
            "planName": "Bengaluru-plan",
            "timeStamp": 1631005414
        }
        ],
        "autoAcceptProduct": False,
        "autoDispatch": False,
        "supportedOrderTypes": 3,
        "packaging": {
        "type": 2,
        "value": 0,
        "typeText": "Percentage"
        },
        "commision": {
        "type": 2,
        "typeText": "Percentage",
        "fullCustomize": 0,
        "productCustomize": 0
        },
        "storeAliasName": "Appscrip",
        "directChatEnable": False,
        "avgRating": 0,
        "batchPicking": {
        "enable": False,
        "time": 0,
        "distance": 0,
        "pickerCapacity": 0
        },
        "shopifyStoreDetails": {
        "enable": False,
        "apiKey": "",
        "apiPassword": "",
        "shopifyStoreName": "",
        "webhookUrl": ""
        },
        "setCustomizeDeliveryFee": 0,
        "isSetCustomizeDeliveryFee": "",
        "buyerAccountId": "61372abc4b25ef001308bfcf",
        "ownerName": "Rinkesh",
        "password": "$2a$10$LeyLt5J7MJv0cmkl1.09FueolnrlKyW6Rhd4DKA0x0oxTMLEB5qHm",
        "ownerMobile": "9737972524",
        "sellerMongoId": "61372abc4b25ef001308bfcd",
        "Name": {
        "en": "Appscrip"
        },
        "string_facet": [],
        "full_text": "Appscrip.co",
        "full_text_boosted": "Appscrip",
        "reason": "",
        "statusUpdatedOn": 1631005414,
        "nextCloseTime": "",
        "nextOpenTime": "",
        "storeIsOpen": False,
        "favouriteUsers": [
        "6163d4c0f0bc9e04404d094f",
        "6141e2f0a2967023b6d2df6e"
        ],
        "isFavourite": False,
        "distance_km": 2.75,
        "distance_miles": 1.71,
        "checkinsCount": 8
    }
})