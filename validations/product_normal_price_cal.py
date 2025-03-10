from search_api.settings import db, IS_B2B_ENABLE
from bson.objectid import ObjectId


def cal_product_noraml_pricing(institution_type, child_product_details):
    unit_package_type = ""
    unit_moq_type = ""
    moq_data = ""
    minimum_order_qty = 1
    base_price = 0
    if institution_type == 1 or institution_type == 0:
        try:
            sorted_price_data = sorted(child_product_details["units"][0]["b2cPricing"], key=lambda x: x['b2cproductSellingPrice'])
            base_price = sorted_price_data[0]["b2cproductSellingPrice"]
        except:
            base_price = child_product_details["units"][0]["floatValue"]
        try:
            sorted_price_data = sorted(child_product_details["units"][0]["b2cPricing"], key=lambda x: x['b2cpriceWithTax'])
            seller_price = sorted_price_data[0]["b2cpriceWithTax"]
        except:
            seller_price = base_price
    else:
        if IS_B2B_ENABLE == 0:
            if "b2bunitPackageType" in child_product_details["units"][0]:
                try:
                    if child_product_details["units"][0]["b2bbulkPackingEnabled"] == 0:
                        minimum_order_qty = child_product_details["units"][0]["b2bminimumOrderQty"]
                        if type(child_product_details["units"][0]["b2bunitPackageType"]["en"]) == str:
                            unit_package_type = child_product_details["units"][0]["b2bunitPackageType"]["en"]
                            unit_moq_type = child_product_details["units"][0]["b2bunitPackageType"]["en"]
                        else:
                            unit_package_type = child_product_details["units"][0]["b2bunitPackageType"]["en"]["en"]
                            unit_moq_type = child_product_details["units"][0]["b2bunitPackageType"]["en"]["en"]
                        moq_data = str(minimum_order_qty) + " " + unit_package_type
                    else:
                        minimum_order_qty = child_product_details["units"][0]["b2bpackingNoofUnits"]
                        unit_package_type = child_product_details["units"][0]["b2bpackingPackageUnits"]["en"]
                        unit_moq_type = child_product_details["units"][0]["b2bpackingPackageType"]["en"]
                        moq_data = str(minimum_order_qty) + " " + unit_package_type + " Per " + unit_moq_type
                except:
                    unit_package_type = "Box"
            else:
                pass

            try:
                sorted_price_data = sorted(child_product_details["units"][0]["b2cPricing"], key=lambda x: x['b2cproductSellingPrice'])
                base_price = sorted_price_data[0]["b2cproductSellingPrice"]
            except:
                base_price = round(child_product_details["units"][0]["floatValue"])
        else:
            try:
                sorted_price_data = sorted(child_product_details["units"][0]["b2cPricing"], key=lambda x: x['b2cproductSellingPrice'])
                base_price = sorted_price_data[0]["b2cproductSellingPrice"]
            except:
                base_price = child_product_details["units"][0]["floatValue"]
        seller_price = base_price
    return base_price, minimum_order_qty, unit_package_type, unit_moq_type, moq_data, seller_price
