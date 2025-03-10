import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
import time
from search_api.settings import db
from bson.objectid import ObjectId
import datetime
from dateutil import tz

'''
    Function for the get the next roaster for driver
'''


def next_availbale_driver_roaster(zoneid):
    buffer_data = db.appConfig.find_one({}, {"dispatch_settings": 1})
    if buffer_data is not None:
        buffer_time = int(buffer_data['dispatch_settings']['later']['bufferHour']
                          * 60) + buffer_data['dispatch_settings']['later']['bufferMinute']
    else:
        buffer_time = 0
    store_list = []
    store_details = db.stores.find({"serviceZones.zoneId": zoneid, "status": 1, "storeFrontTypeId": 5})
    zone_details = db.zones.find_one({"_id": ObjectId(zoneid)}, {"timeZone": 1, "timeOffset": 1})
    for store in store_details:
        store_list.append(str(store['_id']))

    # ===============================date count================================================
    current_date = datetime.datetime.now()
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz(zone_details['timeZone'])
    date_time1 = (current_date).strftime('%d %b %Y %H:%M:%S %p')
    date_time1 = datetime.datetime.strptime(date_time1, '%d %b %Y %H:%M:%S %p')
    utc = date_time1.replace(tzinfo=from_zone)
    central = utc.astimezone(to_zone)
    current_time = central.strftime('%H:%M:%S')
    roaster_query = {"status": 1}
    try:
        roaster_query['startDateTime'] = {
            "$gt": (int(central.timestamp() + int(zone_details['timeOffset']))) + int(buffer_time) * 60}
    except:
        roaster_query['startDateTime'] = {"$gt": (int(central.timestamp()))}
    roaster_query['roasterStoreId'] = {"$in": store_list}
    roaster_query['$where'] = 'this.jobs.length<this.noOfJob'
    # roaster_query['$and'] = [
    #     {
    #         '$where': 'this.jobs.length<this.noOfJob'
    #     },
    #     {
    #         '$where': 'this.drivers.length>0'
    #     }
    # ]
    roaster_query["typeId"] = "1"
    print("roaster_query", roaster_query)
    driver_roaster = db.driverRoasterDaily.find(roaster_query).sort([("startDateTime", 1)]).limit(1)
    json_data = {}
    tomorrow_date = datetime.datetime.today() + datetime.timedelta(days=1)
    tomorrow_date_midnight = tomorrow_date.replace(hour=0, minute=0, second=0)
    tomorrow_date_midnight_timestamp = int(tomorrow_date_midnight.timestamp())
    if driver_roaster.count() > 0:
        for driver in driver_roaster:
            start_time = datetime.datetime.strptime(driver['startTime'], '%H:%M:%S').strftime('%I:%M %p')
            end_time = datetime.datetime.strptime(driver['endTime'], '%H:%M:%S').strftime('%I:%M %p')
            if driver['startDateTime'] > tomorrow_date_midnight_timestamp:
                text = "Next Delivery Slot : Tomorrow " + start_time + " - " + end_time
                product_text = "Tomorrow Between " + start_time + " - " + end_time
            else:
                text = "Next Delivery Slot : Today " + start_time + " - " + end_time
                product_text = "Today Between " + start_time + " - " + end_time
            json_data = {
                "text": text,
                "slotId": str(driver['_id']),
                "productText": product_text
            }
    else:
        json_data = {
            "text": "",
            "slotId": "",
            "productText": ""
        }
    return json_data


'''
    Function for the get the next roaster for driver
'''


def next_availbale_driver_shift_in_stock(zoneid):
    buffer_data = db.appConfig.find_one({}, {"dispatch_settings": 1})
    if buffer_data is not None:
        buffer_time = int(buffer_data['dispatch_settings']['later']['bufferHour']
                          * 60) + buffer_data['dispatch_settings']['later']['bufferMinute']
    else:
        buffer_time = 0
    store_list = []
    store_details = db.stores.find({"serviceZones.zoneId": zoneid, "status": 1, "storeFrontTypeId": 5})
    zone_details = db.zones.find_one({"_id": ObjectId(zoneid)}, {"timeZone": 1, "timeOffset": 1})
    for store in store_details:
        store_list.append(str(store['_id']))

    # ===============================date count================================================
    current_date = datetime.datetime.now()
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz(zone_details['timeZone'])
    date_time1 = (current_date).strftime('%d %b %Y %H:%M:%S %p')
    date_time1 = datetime.datetime.strptime(date_time1, '%d %b %Y %H:%M:%S %p')
    utc = date_time1.replace(tzinfo=from_zone)
    central = utc.astimezone(to_zone)
    current_time = central.strftime('%H:%M:%S')
    roaster_query = {
        "status": 1, 'startDateTime': {
            "$gt": (int(central.timestamp() + int(zone_details['timeOffset']))) + int(buffer_time) * 60},
        'roasterStoreId': {"$in": store_list},
        '$where': 'this.jobs.length<this.noOfJob',
        # "$and": [
        #     {
        #         '$where': 'this.jobs.length<this.noOfJob'
        #     },
        #     {
        #         '$where': 'this.drivers.length>0'
        #     }
        # ],
        "typeId": "1"
    }
    tomorrow_date = datetime.datetime.today() + datetime.timedelta(days=1)
    tomorrow_date_midnight = tomorrow_date.replace(hour=0, minute=0, second=0)
    tomorrow_date_midnight_timestamp = int(tomorrow_date_midnight.timestamp())
    driver_roaster = db.driverRoasterDaily.find(roaster_query).sort([("startDateTime", 1)]).limit(1)
    json_data = {}
    if driver_roaster.count() > 0:
        for driver in driver_roaster:
            start_time = datetime.datetime.strptime(driver['startTime'], '%H:%M:%S').strftime('%I:%M %p')
            end_time = datetime.datetime.strptime(driver['endTime'], '%H:%M:%S').strftime('%I:%M %p')

            if driver['startDateTime'] > tomorrow_date_midnight_timestamp:
                text = "Next Delivery Slot : Tomorrow " + start_time + " - " + end_time
                product_text = "Tomorrow Between " + start_time + " - " + end_time
            else:
                text = "Next Delivery Slot : Today " + start_time + " - " + end_time
                product_text = "Today Between " + start_time + " - " + end_time
            json_data = {
                "text": text,
                "slotId": str(driver['_id']),
                "productText": product_text
            }
    else:
        json_data = {
            "text": "",
            "slotId": "",
            "productText": ""
        }
    return json_data


def next_availbale_driver_out_stock_shift(zoneid, procurement_time):
    buffer_data = db.appConfig.find_one({}, {"dispatch_settings": 1})
    if buffer_data is not None:
        buffer_time = int(buffer_data['dispatch_settings']['later']['bufferHour']
                          * 60) + buffer_data['dispatch_settings']['later']['bufferMinute']
    else:
        buffer_time = 0
    store_list = []
    store_details = db.stores.find({"serviceZones.zoneId": zoneid, "status": 1, "storeFrontTypeId": 5})
    zone_details = db.zones.find_one({"_id": ObjectId(zoneid)}, {"timeZone": 1, "timeOffset": 1})
    for store in store_details:
        store_list.append(str(store['_id']))

    # ===============================date count================================================
    current_date = datetime.datetime.now()
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz(zone_details['timeZone'])
    date_time1 = (current_date).strftime('%d %b %Y %H:%M:%S %p')
    date_time1 = datetime.datetime.strptime(date_time1, '%d %b %Y %H:%M:%S %p')
    utc = date_time1.replace(tzinfo=from_zone)
    central = utc.astimezone(to_zone)
    current_time = (central).strftime('%H:%M:%S')
    roaster_query = {"status": 1, 'startDateTime': {
        "$gt": (int(central.timestamp() + int(zone_details['timeOffset']))) + int(buffer_time) * 60 +
               int(procurement_time) * 3600}, 'roasterStoreId': {"$in": store_list}, "typeId": "2"}
    # roaster_query['$where'] = 'this.jobs.length<this.noOfJob'
    driver_roaster = db.driverRoasterDaily.find(roaster_query).sort([("startDateTime", 1)]).limit(1)
    json_data = []
    if driver_roaster.count() > 0:
        for driver in driver_roaster:
            driver['_id'] = str(driver['_id'])
            json_data.append(driver)
    return json_data


'''
    Function for the get the next roaster for driver
'''


def next_availbale_driver_shift_out_stock(zoneid, procurement_time, hard_limit, product_id):
    buffer_data = db.appConfig.find_one({}, {"dispatch_settings": 1})
    if buffer_data is not None:
        buffer_time = int(buffer_data['dispatch_settings']['later']['bufferHour']
                          * 60) + buffer_data['dispatch_settings']['later']['bufferMinute']
    else:
        buffer_time = 0
    store_list = []
    store_details = db.stores.find({"serviceZones.zoneId": zoneid, "status": 1, "storeFrontTypeId": 5})
    zone_details = db.zones.find_one({"_id": ObjectId(zoneid)}, {"timeZone": 1, "timeOffset": 1})
    for store in store_details:
        store_list.append(str(store['_id']))
    # ===============================date count================================================
    current_date = datetime.datetime.now()
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz(zone_details['timeZone'])
    date_time1 = (current_date).strftime('%d %b %Y %H:%M:%S %p')
    date_time1 = datetime.datetime.strptime(date_time1, '%d %b %Y %H:%M:%S %p')
    utc = date_time1.replace(tzinfo=from_zone)
    central = utc.astimezone(to_zone)
    current_time = central.strftime('%H:%M:%S')
    roaster_query = {"status": 1, 'startDateTime': {
        "$gt": (int(central.timestamp() + int(zone_details['timeOffset'])))  # +int(procurement_time)*3600
    }, 'roasterStoreId': {"$in": store_list}, '$or': [
        {
            "products.productId": {"$ne": product_id}
        },
        {
            "products": {
                "$elemMatch": {
                    "productId": product_id,
                    "qty": {"$lt": hard_limit}
                }
            }
        }
    ], "typeId": "2", '$where': 'this.drivers.length>0'
                     }
    # roaster_query['$where'] = 'this.jobs.length<this.noOfJob'
    tomorrow_date = datetime.datetime.today() + datetime.timedelta(days=1)
    tomorrow_date_midnight = tomorrow_date.replace(hour=0, minute=0, second=0)
    tomorrow_date_night = tomorrow_date.replace(hour=23, minute=59, second=59)
    tomorrow_date_midnight_timestamp = int(tomorrow_date_midnight.timestamp())
    tomorrow_date_night_timestamp = int(tomorrow_date_night.timestamp())
    driver_roaster = db.driverRoasterDaily.find(roaster_query).sort([("startDateTime", 1)]).limit(1)
    json_data = {}
    if driver_roaster.count() > 0:
        for driver in driver_roaster:
            delivery_query = {
                "status": 1,
                'startDateTime': {
                    # "$gt": (int(driver['endDateTime'] + int(buffer_time) * 60)) + int(zone_details['timeOffset'])},
                    "$gt": (int(driver['endDateTime'] + int(buffer_time) * 60))},
                'roasterStoreId': {"$in": store_list},
                '$where': 'this.jobs.length<this.noOfJob',
                # "$and": [
                #     {
                #         '$where': 'this.jobs.length<this.noOfJob'
                #     },
                #     {
                #         '$where': 'this.drivers.length>0'
                #     }
                # ],
                "typeId": "1"
            }
            delivery_driver_roaster = db.driverRoasterDaily.find(delivery_query).sort([("startDateTime", 1)]).limit(1)
            for delivery in delivery_driver_roaster:
                delivery_start_time = datetime.datetime.strptime(delivery['startTime'], '%H:%M:%S').strftime('%I:%M %p')
                delivery_end_time = datetime.datetime.strptime(delivery['endTime'], '%H:%M:%S').strftime('%I:%M %p')
                delivery_date = datetime.datetime.strptime(delivery['date'], '%d-%m-%Y').strftime('%d %b')
                if delivery['startDateTime'] > tomorrow_date_midnight_timestamp and delivery[
                    'endDateTime'] < tomorrow_date_night_timestamp:
                    text = "Next Delivery Slot : Tomorrow " + delivery_start_time + " - " + delivery_end_time
                    product_text = "Tomorrow Between " + delivery_start_time + " - " + delivery_end_time
                elif delivery['startDateTime'] > tomorrow_date_night_timestamp:
                    text = "Next Delivery Slot : Today " + delivery_start_time + " - " + delivery_end_time
                    product_text = "On " + delivery_date + " " + delivery_start_time + " - " + delivery_end_time
                else:
                    text = "Next Delivery Slot : Today " + delivery_start_time + " - " + delivery_end_time
                    product_text = "Today Between " + delivery_start_time + " - " + delivery_end_time
                json_data = {
                    "text": text,
                    "slotId": str(driver['_id']),
                    "productText": product_text
                }
                return json_data
    else:
        json_data = {
            "text": "",
            "slotId": "",
            "productText": ""
        }
        return json_data
