import os
import sys
from bson.objectid import ObjectId
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from search_api.settings import db, ECOMMERCE_STORE_CATEGORY_ID, GROCERY_STORE_CATEGORY_ID


def validate_variant(parent_product_id, store_id, zone_id, store_category_id):
    central_query = {"parentProductId": str(parent_product_id), "status": 1}
    variant_data = []
    if store_id != "" and store_id != "0":
        central_query['storeId'] = ObjectId(store_id)
    elif zone_id != "" and store_category_id != ECOMMERCE_STORE_CATEGORY_ID:
        store_list = []
        store_data = db.stores.find({"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id, "status": 1})
        for s_data in store_data:
            store_list.append(ObjectId(s_data['_id']))
        central_query['storeId'] = {"$in": store_list}
    child_products_details = db.childProducts.find(central_query)
    if child_products_details.count() > 1:
        return True
    else:
        return False