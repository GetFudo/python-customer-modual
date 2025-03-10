from search_api.settings import db, IS_B2B_ENABLE, MEAT_STORE_CATEGORY_ID
from bson.objectid import ObjectId
from validations.product_normal_price_cal import cal_product_noraml_pricing


def cal_product_city_pricing(institution_type, city_id, child_product_details):
    unit_package_type = ""
    unit_moq_type = ""
    moq_data = ""
    minimum_order_qty = 1

    # get the product price base on city details(city check we need to add for meaty type product)
    # if product is none meaty type then need to call normal price function for get the normal price of the product
    if city_id != "" and child_product_details['storeCategoryId'] == MEAT_STORE_CATEGORY_ID:
        child_product_details = db.childProducts.find_one(
            {
                "units.unitId": child_product_details['units'][0]['unitId'],
                "storeId": "0"
            }
        )
        if child_product_details is not None:
            if IS_B2B_ENABLE == 0:
                if "b2bCityPricing" in child_product_details and institution_type == 2:
                    city_price = {}
                    for x in child_product_details['b2bCityPricing']:
                        if x['cityId'] == city_id:
                            city_price = x
                        else:
                            pass
                    if "afterTax" in city_price:
                        if city_price['afterTax'] is not None:
                            if "b2bunitPackageType" in child_product_details["units"][0]:
                                try:
                                    if child_product_details["units"][0]["b2bbulkPackingEnabled"] == 0:
                                        minimum_order_qty = child_product_details["units"][0]["b2bminimumOrderQty"]
                                        if type(child_product_details["units"][0]["b2bunitPackageType"]["en"]) == str:
                                            unit_package_type = child_product_details["units"][0]["b2bunitPackageType"][
                                                "en"]
                                            unit_moq_type = child_product_details["units"][0]["b2bunitPackageType"][
                                                "en"]
                                        else:
                                            unit_package_type = \
                                                child_product_details["units"][0]["b2bunitPackageType"]["en"]["en"]
                                            unit_moq_type = \
                                                child_product_details["units"][0]["b2bunitPackageType"]["en"]["en"]
                                        moq_data = str(minimum_order_qty) + " " + unit_package_type
                                    else:
                                        minimum_order_qty = child_product_details["units"][0]["b2bpackingNoofUnits"]
                                        unit_package_type = child_product_details["units"][0]["b2bpackingPackageUnits"][
                                            "en"]
                                        unit_moq_type = child_product_details["units"][0]["b2bpackingPackageType"]["en"]
                                        moq_data = str(
                                            minimum_order_qty) + " " + unit_package_type + " Per " + unit_moq_type
                                except:
                                    unit_package_type = "Box"
                            else:
                                pass
                            try:
                                base_price = city_price['afterTax'][0]['priceAfterTax']
                            except:
                                sorted_price_data = sorted(child_product_details["units"][0]["b2cPricing"], key=lambda x: x['b2cproductSellingPrice'])
                                try:
                                    base_price = sorted_price_data[0]["b2cproductSellingPrice"]
                                except:
                                    base_price = child_product_details["units"][0]["floatValue"]
                            seller_price = base_price
                        else:
                            base_price, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price = cal_product_noraml_pricing(
                                institution_type, child_product_details)
                    else:
                        base_price, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price = cal_product_noraml_pricing(
                            institution_type, child_product_details)
                elif "b2cCityPricing" in child_product_details and institution_type == 1:
                    city_price = {}
                    for x in child_product_details['b2cCityPricing']:
                        if x['cityId'] == city_id:
                            city_price = x
                        else:
                            pass
                    if "afterTax" in city_price:
                        if city_price['afterTax'] is not None:
                            minimum_order_qty = 1
                            unit_moq_type = ""
                            moq_data = ""
                            unit_package_type = "Box"
                            try:
                                base_price = city_price['afterTax'][0]['priceAfterTax']
                            except:
                                sorted_price_data = sorted(child_product_details["units"][0]["b2cPricing"], key=lambda x: x['b2cproductSellingPrice'])
                                try:
                                    base_price = sorted_price_data[0]["b2cproductSellingPrice"]
                                except:
                                    base_price = child_product_details["units"][0]["floatValue"]
                            seller_price = base_price
                        else:
                            base_price, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price = cal_product_noraml_pricing(
                                institution_type, child_product_details)
                    else:
                        base_price, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price = cal_product_noraml_pricing(
                            institution_type, child_product_details)
                else:
                    base_price, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price = cal_product_noraml_pricing(
                        institution_type, child_product_details)
            else:
                base_price, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price = cal_product_noraml_pricing(
                    institution_type, child_product_details)
        else:
            base_price, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price = cal_product_noraml_pricing(
                institution_type, child_product_details)
    else:
        base_price, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price = cal_product_noraml_pricing(
            institution_type, child_product_details)
    return base_price, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price
