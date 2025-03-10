from bson.objectid import ObjectId
from search_api.settings import PYTHON_BASE_URL, db
import requests
import time


def plp_page_update(product_id):
    time.sleep(5)
    api_header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ"
                         ".IW6kBQzeg3-MSxDUBWIk2OqaqHAA4N0vl4WD-5V-RJhrUY5aRXPt2QF"
                         "-wqzuMJt33IaQWUgVpV3b2H8OzqUbgnQ9JqIW_SF9NhbznXnK"
                         "-HExRww0GaV5fs2GE6k9tU9pgcCG29nJ1YQ4gZNHmHQogho71o4jndeDsztdYmEyHvY.ItaQWH-E-VXG_IMC.cN0SfW"
                         "--XI-k-mMEt2e7D4VFs6zpyTjMFWZrs0Fd4nAMIWML4xHZeFuCYYdUIiVMmzPkjC1H8__xUkVMhlrTIFU0S6"
                         "-8HcfaeP4zVSdo7gyXjAwM0d_cBmx_JbE1PgkABOJD5VpVnNX29dQ2TecKuAcwJ1wIPFVkRNKDhGrirZQouVWd5gnuDKn"
                         "87owwOF9Gq6yuo5Nin_vrOyCdClU2siTiAjfpwqWTV2hlN7DR0I21A1inx--da2qUK9SyfhgsCgIkz99iMPopsFfhQnWz"
                         "pfd3zfBNLNS8UecKeMMCzoyu-qmW60XbuP8BW8e93TwQV5srH9mHZTkAuctC5wH5YOdcUdbYRLHqZCwvLnFo6CM5EY4aG"
                         "5KPk9qZR0isN5BEo5LcDm6bMbcyngTNWqajpNsCWPV3qnP8psE8Mc12pwm6m9fO6Uk16uSeJ67Yz5lUFGIVqwP_9UpmuF"
                         "2B4lHRq5iuYbKMX-Eu39ZKabpWrll9HTMkFR_krXzDace0sFQc6OavIB9bI2myaK5iEqJ8t6V5ySJUg2y3xl1Y2F8JcHG"
                         "Fn6edeZCy9oEjGb3DBTLtmSJfJcxped2TTAjCvN705HiqBSCkfOoG8qgKI_WyUMMN0lBLUIrVjgDvcEFLBEzGR5-AbbBO"
                         "l3jW02WiS4CugoTSYmcNlmmLWA4YJfhkKe73FHjY7xhtRNHEeVk-6NGhlU0uDrhKBjCa7tRCiskXHVJiL-E1WxD27Ah88"
                         "kekYCYM3e8NWoFjh5U6IE8XauxpWbcfhKObqHb52Usj9NGnf02BxVEJuoFuOcEW9wbLkCekGktwCq_S4cZvtGHt7P5ra"
                         "m8l9RHel8YCiVCw-tHV6vFKbB2q0egFPW_uJxx6MNKjo60n2nPnKfm0tM2jAQ00cLSTjaCwIrUxT8t73lE3Ya4ufJvq7t"
                         "BtFG8RL3U2EQKPcRG7uqlr0_NcxppIfOqBaegPpA46nZq0NzzNOQrhlTDrvutedQR4oRO6SnDcR_W3wzSmpnGkM6uoCFl"
                         "tBBv3MbUwrpNNMbclo0uQqTVFNBHCW-G8BPSCGUHhyta1s5g.tG0KwqSLUdm0Zyv48PYNww",
        "currencycode": "INR",
        "language": "en",
        "searchType": "3"
    }
    product_details = db.childProducts.find_one({"_id": ObjectId(product_id)})
    if product_details is not None:
        api_header['storeCategoryId'] = product_details['storeCategoryId']
        store_details = db.stores.find_one({"_id": ObjectId(product_details['storeId'])})
        if store_details is not None:
            category_query = {
                "storeCategory.storeCategoryId": product_details['storeCategoryId'],
                "_id": ObjectId(store_details['cityId'])
            }
            categoty_details = db.cities.find_one(category_query, {"storeCategory": 1})
            hyperlocal = False
            storelisting = False
            if categoty_details is not None:
                if "storeCategory" in categoty_details:
                    for cat in categoty_details["storeCategory"]:
                        if cat["storeCategoryId"] == product_details['storeCategoryId']:
                            if cat["hyperlocal"] == True and cat["storeListing"] == 1:
                                hyperlocal = True
                                storelisting = True
                            elif cat["hyperlocal"] == True and cat["storeListing"] == 0:
                                hyperlocal = True
                                storelisting = False
                            else:
                                hyperlocal = False
                                storelisting = False
                        else:
                            pass
                else:
                    hyperlocal = False
                    storelisting = False
            else:
                hyperlocal = False
                storelisting = False
            for main_category in product_details['categoryList']:
                first_category_name = main_category['parentCategory']['categoryName']['en']
                if "childCategory" in main_category["parentCategory"]:
                    for child_category in main_category['parentCategory']['childCategory']:
                        second_category_name = child_category['categoryName']['en']
                        if hyperlocal == True and storelisting == True:
                            request_url = "python/searchFilter?&fname="+first_category_name+"&page=1&s_id="+str(product_details['storeId'])+"&sname="+second_category_name+"&called=1"
                            expireOffer = requests.get(
                                PYTHON_BASE_URL + request_url,  headers=api_header
                            )
                            print(expireOffer.status_code)
                        elif hyperlocal == True and storelisting == False:
                            for zone_id in store_details['serviceZones']:
                                request_url = "python/searchFilter?&fname="+first_category_name+"&page=1&z_id="+str(zone_id['zoneId'])+"&sname="+second_category_name+"&called=1"
                                expireOffer = requests.get(
                                    PYTHON_BASE_URL + request_url,  headers=api_header
                                )
                                print(expireOffer.status_code)
                        else:
                            request_url = "python/searchFilter?&fname=" + first_category_name + "&page=1&sname=" + second_category_name+"&called=1"
                            expireOffer = requests.get(
                                PYTHON_BASE_URL + request_url,  headers=api_header
                            )
                            print(expireOffer.status_code)
                if hyperlocal == True and storelisting == True:
                    request_url = "python/searchFilter?&fname=" + first_category_name + "&page=1&s_id=" + str(product_details['storeId'])+"&called=1"
                    expireOffer = requests.get(PYTHON_BASE_URL + request_url, headers=api_header)
                    print(expireOffer.status_code)
                elif hyperlocal == True and storelisting == False:
                    for zone_id in store_details['serviceZones']:
                        request_url = "python/searchFilter?&fname=" + first_category_name + "&page=1&z_id=" + str(
                            zone_id['zoneId'])+"&called=1"
                        expireOffer = requests.get(PYTHON_BASE_URL + request_url,  headers=api_header)
                        print(expireOffer.status_code)
                else:
                    request_url = "python/searchFilter?&fname=" + first_category_name + "&page=1"+"&called=1"
                    expireOffer = requests.get(PYTHON_BASE_URL + request_url,  headers=api_header)
                    print(expireOffer.status_code)
    else:
        pass
    print("all request are done....!!!!")
    return True