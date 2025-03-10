from mongo_query_module.query_module import store_find


def store_validation_function(store_category_id, zone_id):
    store_data_details = []
    store_query = {"categoryId": str(store_category_id), "serviceZones.zoneId": zone_id, "storeFrontTypeId": {"$ne": 5},
                   "status": 1}
    store_data = store_find(store_query)
    if store_data.count() > 0:
        for store in store_data:
            store_data_details.append(str(store['_id']))
    return store_data_details
