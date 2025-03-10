from search_api.settings import REDIS_BEST_DEALS_DB, db
from bson.objectid import ObjectId


def update_best_suppliers_redis(product_id):
    zone_details = db.zones.find({"status": 1})
    for zone in zone_details:
        store_details = db.stores.find(
            {"serviceZones.zoneId": str(zone['_id']), "status": 1, "storeFrontTypeId": {"$ne": 5}})
        store_json = []
        for store in store_details:
            store_json.append(str(store['_id']))
        if len(store_json) > 0:
            child_product_data = db.products.find(
                {"_id": ObjectId(product_id), "childProducts.suppliers.id": {"$in": store_json}})
            for product in child_product_data:
                supplier_json = []
                if "childProducts" in product:
                    for child in product['childProducts']:
                        if "suppliers" in child:
                            for supplier in child['suppliers']:
                                if supplier['id'] == "0":
                                    pass
                                else:
                                    if supplier['id'] in store_json:
                                        supplier_json.append(supplier)
                                    else:
                                        pass
                        else:
                            pass
                        if len(supplier_json) == 0:
                            for supplier in child['suppliers']:
                                if supplier['id'] == "0":
                                    supplier_json.append(supplier)
                                else:
                                    pass
                        else:
                            pass
                if len(supplier_json) > 0:
                    best_supplier = min(supplier_json, key=lambda x: x['retailerPrice'])
                    if best_supplier['retailerQty'] == 0:
                        best_supplier = max(supplier_json, key=lambda x: x['retailerQty'])
                    else:
                        best_supplier = best_supplier
                else:
                    best_supplier = {}
                if len(best_supplier) > 0:
                    try:
                        REDIS_BEST_DEALS_DB.delete('bestseller_' + str(product['_id']) + "_" + str(zone['_id']))
                    except:
                        pass

        # ===========================================insert in redis after remove===================================
        if len(store_json) > 0:
            child_product_data = db.products.find(
                {"_id": ObjectId(product_id), "childProducts.suppliers.id": {"$in": store_json}})
            for product in child_product_data:
                supplier_json = []
                if "childProducts" in product:
                    for child in product['childProducts']:
                        if "suppliers" in child:
                            for supplier in child['suppliers']:
                                if supplier['id'] == "0":
                                    pass
                                else:
                                    child_product_count = db.childProducts.find_one(
                                        {"_id": ObjectId(supplier['productId'])})
                                    if child_product_count is not None:
                                        if supplier['id'] in store_json:
                                            supplier['retailerPrice'] = \
                                                child_product_count['units'][0]['b2cPricing'][0][
                                                    'b2cproductSellingPrice']
                                            supplier_json.append(supplier)
                                        else:
                                            pass
                            if len(supplier_json) == 0:
                                for supplier in child['suppliers']:
                                    if supplier['id'] == "0":
                                        child_product_count = db.childProducts.find_one(
                                            {"_id": ObjectId(supplier['productId'])})
                                        if child_product_count is not None:
                                            supplier['retailerPrice'] = \
                                                child_product_count['units'][0]['b2cPricing'][0][
                                                    'b2cproductSellingPrice']
                                            supplier_json.append(supplier)
                                    else:
                                        pass
                            else:
                                pass
                        else:
                            pass
                else:
                    pass
                if len(supplier_json) > 0:
                    best_supplier = min(supplier_json, key=lambda x: x['retailerPrice'])
                    if best_supplier['retailerQty'] == 0:
                        best_supplier = max(supplier_json, key=lambda x: x['retailerQty'])
                    else:
                        best_supplier = best_supplier
                else:
                    best_supplier = {}
                if len(best_supplier) > 0:
                    try:
                        REDIS_BEST_DEALS_DB.hmset(
                            'bestseller_' + str(product['_id']) + "_" + str(zone['_id']),
                            {
                                'productId': str(best_supplier['productId']),
                                'parentProductId': str(product['_id']),
                                "zoneId": str(zone['_id']),
                                "sellerId": best_supplier['id'],
                                "price": best_supplier['retailerPrice'],
                                "currencySymbol": best_supplier['currencySymbol'],
                                "currency": best_supplier['currency'],
                                "quantity": best_supplier['retailerQty']
                            }
                        )
                    except:
                        pass

    return {"message": "data updated successfully"}
