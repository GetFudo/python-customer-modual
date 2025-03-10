from search_api.settings import REDIS_BEST_DEALS_DB, db
from bson.objectid import ObjectId

'''
    Function for get the best offer from redis
'''


def product_get_best_offer_data(product_id, zone_id):
    try:
        child_product_data = db.childProducts.find_one({"_id": ObjectId(product_id)})
        best_offer = {}
        if zone_id != "":
            pass
        else:
            store_details = db.stores.find_one({"_id": ObjectId(child_product_data['storeId'])})
            if store_details is not None:
                if "serviceZones" in store_details:
                    if len(store_details['serviceZones']) > 0:
                        zone_id = store_details['serviceZones'][0]['zoneId']
                    else:
                        zone_id = ""
                else:
                    zone_id = ""
            else:
                zone_id = ""

        if zone_id != "":
            best_offer_data = REDIS_BEST_DEALS_DB.hgetall("bestoffer_" + str(product_id) + "_" + zone_id)
            if len(best_offer_data) > 0:
                best_offer_data_new = {key.decode(): val.decode() for key, val in best_offer_data.items()}
                best_offer["offerId"] = best_offer_data_new['offerId']
                best_offer["images"] = {
                    "image": "https://cdn.shoppd.net/shoppd/0/0/1618237490504.jpg",
                    "thumbnail": "https://cdn.shoppd.net/shoppd/0/0/1618237490504.jpg",
                    "mobile": "https://cdn.shoppd.net/shoppd/0/0/1618237490504.jpg"
                }
                best_offer["webimages"] = {
                    "image": "https://cdn.shoppd.net/shoppd/0/0/1618237490504.jpg",
                    "thumbnail": "https://cdn.shoppd.net/shoppd/0/0/1618237490504.jpg",
                    "mobile": "https://cdn.shoppd.net/shoppd/0/0/1618237490504.jpg"
                }
                best_offer["status"] = int(best_offer_data_new['status'])
                best_offer["discountValue"] = int(float(best_offer_data_new['discountValue']))
                best_offer["offerFor"] = int(best_offer_data_new['offerFor'])
                best_offer["discountType"] = int(float(best_offer_data_new['discountType']))
                best_offer["offerName"] = {"en": best_offer_data_new['offerName']}
            else:
                pass
        else:
            best_offer = {}
        return best_offer
    except:
        best_offer = {}
        return best_offer


'''
    Function for the add the best offer for add in redis
'''


def product_best_offer_data(product_id):
    child_product_data = db.childProducts.find_one({"_id": ObjectId(product_id)})
    offers_details = []
    if "offer" in child_product_data:
        for offer in child_product_data['offer']:
            offer_count = db.offers.find({"_id": ObjectId(offer['offerId']), "status": 1}).count()
            if offer_count > 0:
                if offer['status'] == 1:
                    offer['name'] = offer['offerName']['en']
                    offer['discountValue'] = offer['discountValue']
                    offer['discountType'] = offer['discountType']
                    offers_details.append(offer)
                else:
                    pass
    if len(offers_details) > 0:
        best_offer = max(offers_details, key=lambda x: x['discountValue'])
        best_offer["images"] = {
            "image": "https://cdn.shoppd.net/shoppd/0/0/1618237490504.jpg",
            "thumbnail": "https://cdn.shoppd.net/shoppd/0/0/1618237490504.jpg",
            "mobile": "https://cdn.shoppd.net/shoppd/0/0/1618237490504.jpg"
        }
        best_offer["webimages"] = {
            "image": "https://cdn.shoppd.net/shoppd/0/0/1618237490504.jpg",
            "thumbnail": "https://cdn.shoppd.net/shoppd/0/0/1618237490504.jpg",
            "mobile": "https://cdn.shoppd.net/shoppd/0/0/1618237490504.jpg"
        }
    else:
        best_offer = {}

    if len(best_offer) > 0:
        if child_product_data is not None:
            if child_product_data['storeId'] != "0":
                store_data = db.stores.find_one({"_id": ObjectId(child_product_data['storeId'])})
                if store_data is not None:
                    if "serviceZones" in store_data:
                        for zones in store_data['serviceZones']:
                            REDIS_BEST_DEALS_DB.hmset(
                                'bestoffer_' + str(product_id) + "_" + str(zones['zoneId']),
                                {
                                    'productId': str(child_product_data['_id']),
                                    'parentProductId': str(child_product_data['parentProductId']),
                                    "zoneId": str(zones['zoneId']),
                                    "sellerId": str(child_product_data['storeId']),
                                    "status": best_offer['status'],
                                    "offerId": best_offer['offerId'],
                                    "discountValue": best_offer['discountValue'],
                                    "offerFor": best_offer['offerFor'],
                                    "discountType": best_offer['discountType'],
                                    "offerName": best_offer['offerName']['en'],
                                }
                            )
                    else:
                        pass
                else:
                    pass
            else:
                pass
        else:
            pass
    else:
        pass
    return best_offer
