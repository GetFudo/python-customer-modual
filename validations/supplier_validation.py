import os
import sys
from bson.objectid import ObjectId
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from search_api.settings import db, MEAT_STORE_CATEGORY_ID, REDIS_BEST_SUPPLIER_DB

def best_supplier_function(
        parent_product_id, suppliers, store_id, remove_central,
        zone_id, store_category_id, pharmacy_cat_id):
    product_tag = ""
    supplier_list = []
    store_list_json = []
    # ===========================================get the store data======================================
    if zone_id != "" and store_category_id == pharmacy_cat_id:
        store_query = {"status": 1, "categoryId": str(store_category_id), "serviceZones.zoneId": zone_id, "storeFrontTypeId": {"$nin": [2]}}
        stores_list = db.stores.find(store_query)
        for s in stores_list:
            store_list_json.append(str(s['_id']))
    elif zone_id != "" and store_category_id == MEAT_STORE_CATEGORY_ID:
        stores_list = db.stores.find({"status": 1, "serviceZones.zoneId": zone_id, "storeFrontTypeId": 5})
        for s in stores_list:
            store_list_json.append(str(s['_id']))
    else:
        pass
    # ===========================================get the best supplier======================
    if store_id == "" and remove_central == False:
        for s in suppliers:
            if s['id'] != "0":
                supplier_list.append(s)
            else:
                pass
    elif store_id != "":
        for s in suppliers:
            if s['id'] == store_id:
                child_product_count = db.childProducts.find({"_id": ObjectId(s['productId']), "status": 1}).count()
                if child_product_count > 0:
                    supplier_list.append(s)
            else:
                pass
    elif zone_id != "":
        if any(d['id'] in store_list_json for d in suppliers):
            product_tag = ""
        else:
            product_tag = "We Can not delivering in your area"
        for sup in suppliers:
            if sup['id'] == "0":
                pass
            else:
                store_count = db.stores.find(
                    {"_id": ObjectId(sup['id']), "serviceZones.zoneId": zone_id, "status": 1}
                ).count()
                if store_count > 0:
                    child_product_count = db.childProducts.find({"_id": ObjectId(sup['productId']), "status": {"$in": [1, 2]}}).count()
                    if child_product_count > 0:
                        supplier_list.append(sup)
                    else:
                        pass
                else:
                    pass
    else:
        for sup in suppliers:
            if sup['id'] != "0":
                if store_id == "" or store_id == "0":
                    if zone_id != "":
                        store_count_more = db.stores.find({"status": 1, "serviceZones.zoneId": zone_id, "_id": ObjectId(sup['id']), "storeFrontTypeId": {"$nin": [2]}}).count()
                    else:
                        store_count_more = db.stores.find({"status": 1, "_id": ObjectId(sup['id']), "storeFrontTypeId": {"$nin": [2]}}).count()
                    if store_count_more != 0:
                        child_product_count = db.childProducts.find({"_id": ObjectId(sup['productId']), "status": 1}).count()
                        if child_product_count > 0:
                            supplier_list.append(sup)
                        else:
                            pass
                    else:
                        pass
                else:
                    if sup['id'] == store_id:
                        child_product_count = db.childProducts.find({"_id": ObjectId(sup['productId']), "status": 1}).count()
                        if child_product_count > 0:
                            supplier_list.append(sup)
                        else:
                            pass
    if len(supplier_list) > 0:
        best_supplier = min(supplier_list, key=lambda x: x['retailerPrice'])
        if best_supplier['retailerQty'] == 0:
            best_supplier = max(supplier_list, key=lambda x: x['retailerQty'])
        else:
            best_supplier = best_supplier
    else:
        best_supplier = {}
    if len(best_supplier) > 0:
        # best_supplier['storeName'] = best_supplier["storeName"][]
        if best_supplier['id'] != "0":
            service_zones_data = db.stores.find_one({"_id": ObjectId(best_supplier['id'])})
            if service_zones_data is not None:
                if "serviceZones" in service_zones_data:
                    for zone in service_zones_data['serviceZones']:
                        try:
                            REDIS_BEST_SUPPLIER_DB.hmset(
                                'bestseller_' + str(parent_product_id) +"_"+ str(zone['zoneId']),
                                {
                                    'productId': str(best_supplier['productId']),
                                    'parentProductId': str(parent_product_id),
                                    "zoneId": str(zone['zoneId']),
                                    "sellerId": best_supplier['id'],
                                    "price": best_supplier['retailerPrice'],
                                    "currencySymbol": best_supplier['currencySymbol'] if "currencySymbol" in best_supplier else "",
                                    "currency": best_supplier['currency'] if "currency" in best_supplier else "",
                                    "quantity": best_supplier['retailerQty']
                                }
                            )
                        except:
                            pass
            else:
                pass
        else:
            pass
    else:
        pass
    return best_supplier, product_tag
