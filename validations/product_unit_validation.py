import os
import sys
from pytz import timezone
from bson.objectid import ObjectId
import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")

'''
    function for the validate the products data
    :parameter
    product_list: list of the products(array of object) 
'''


def validate_units_data(product_list, hot_deals):
    resData = []
    for k in product_list:
        if k['unitsData']['basePrice'] == 0:
            pass
        else:
            percentage = 0
            if hot_deals == True and len(k['offers']) == 0:
                pass
            else:
                if len(k['offer']) != 0:
                    if "discountType" in k['offer']:
                        if k['offer']['discountType'] == 0:
                            percentage = 0
                        else:
                            percentage = int(k['offer']['discountValue'])
                    else:
                        percentage = 0
                else:
                    percentage = 0

                try:
                    reseller_commission = k['units'][0]['b2cPricing'][0]['b2cresellerCommission']
                except:
                    reseller_commission = 0

                # try:
                #     reseller_commission_type = k['units'][0]['b2cPricing'][0]['b2cpercentageCommission']
                # except:
                try:
                    if "_id" in k:
                        p_id = k['_id']
                    else:
                        p_id = ""
                except:
                    print('id not get')
                    p_id = ""
                reseller_commission_type = 0
                try:
                    if float(k['unitsData']['finalPrice']) > 0:
                        resData.append({
                            "_id": p_id,
                            "outOfStock": k['outOfStock'],
                            "score": k['score'] if "score" in k else 0,
                            "childProductId": k['childProductId'],
                            "productName": k['productName'],
                            "colourData": k["colourData"] if "colourData" in k else [],
                            "colourCount": k["colourCount"] if "colourCount" in k else 0,
                            "detailDescription": k["detailDescription"] if "detailDescription" in k else "",
                            "isShoppingList": k["isShoppingList"] if "isShoppingList" in k else False,
                            "parentProductId": k['parentProductId'],
                            "maxQuantity": k['maxQuantity'],
                            "unitId": k['unitId'],
                            "variantData": k['variantData'],
                            "availableQuantity": k['availableQuantity'],
                            "images": k['images'],
                            "offers": k['offer'],
                            "productStatus": k['productStatus'] if "productStatus" in k else 1,
                            "isComboProduct": k['isComboProduct'] if "isComboProduct" in k else False,
                            "needsIdProof": k['needsIdProof'] if "needsIdProof" in k else False,
                            "isFavourite": k['isFavourite'] if "isFavourite" in k else False,
                            "productTag": k['productTag'],
                            "brandName": k['brandName'],
                            "manufactureName": k['manufactureName'],
                            "productType": k['productType'] if "productType" in k else 1,
                            "mobimages": k['mobimages'],
                            "productSeo": k['productSeo'],
                            "variantCount": k["variantCount"],
                            "prescriptionRequired": k['prescriptionRequired'],
                            "extraAttributeDetails": k['extraAttributeDetails'] if "extraAttributeDetails" in k else [],
                            "saleOnline": k['saleOnline'],
                            "uploadProductDetails": k['uploadProductDetails'],
                            "allowOrderOutOfStock": k['allowOrderOutOfStock'],
                            "supplier": k['suppliers'],
                            "storeCategoryId": k['storeCategoryId'],
                            "discountType": k['offer']['discountType'] if "discountType" in k['offer'] else 0,
                            "offerDetailsData": k['offerDetailsData'] if "offerDetailsData" in k else [],
                            "nextSlotTime": k['nextSlotTime'] if "nextSlotTime" in k else "",
                            "TotalStarRating": k['TotalStarRating'],
                            "discountPrice": k['unitsData']['discountPrice'],
                            "discountPercentage": percentage,
                            "finalPriceList": {
                                "basePrice": round(k['unitsData']['basePrice'], 2),
                                "finalPrice": round(k['unitsData']['finalPrice'], 2),
                                "discountPrice": round(k['unitsData']['discountPrice'], 2),
                                "sellerPrice": round(k['unitsData']['sellerPrice'], 2) if "sellerPrice" in k['unitsData'] else round(k['unitsData']['basePrice'], 2),
                                "discountPercentage": percentage
                            },
                            "MOQData": k['MOQData'] if "MOQData" in k else {
                                "minimumOrderQty": 1,
                                "unitPackageType": "Box",
                                "unitMoqType": "Box",
                                "MOQ": "1 Box",
                            },
                            "currencySymbol": k['currencySymbol'],
                            "createdTimestamp": k['createdTimestamp'] if "createdTimestamp" in k else (
                                                                                                        int(datetime.datetime.now().timestamp())) * 1000,
                            "currency": k['currency'],
                            "mouData": {
                                "mou": "",
                                "mouUnit": "",
                                "mouQty": 0,
                                "minimumPurchaseUnit": ""
                            },
                            "isSizeAvailable": k['isSizeAvailable'] if "isSizeAvailable" in k else False,
                            "isOpenPdp": k['isOpenPdp'] if "isOpenPdp" in k else False,
                            "linkedAttribute": k['linkedAttribute'] if "linkedAttribute" in k else [],
                            "hightlight": k['hightlight'] if "hightlight" in k else [],
                            "resellerCommission": reseller_commission,
                            "resellerCommissionType": reseller_commission_type,
                            "mouDataUnit": k['mouDataUnit'] if "mouDataUnit" in k else {},
                            "addOnsCount": k["addOnsCount"] if "addOnsCount" in k else 0,
                            "containsMeat": k['containsMeat'] if "containsMeat" in k else False,
                            "moUnit": "Pcs",
                            "price": round(k['unitsData']['finalPrice'], 2),
                            "modelImage": k["modelImage"] if "modelImage" in k else [],
                            "storeCount": k["storeCount"] if "storeCount" in k else 0,
                            "popularScore": k["popularScore"] if "popularScore" in k else 0,
                            "isSubstituteAvailable": k['isSubstituteAvailable'] if 'isSubstituteAvailable' in k else False
                        }
                        )
                    else:
                        pass
                except:
                    pass
    return resData
