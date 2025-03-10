from search_api.settings import db
from bson.objectid import ObjectId

'''
    productType: 1 for individual, 2 for combo and 3 for special(BUY 1 GET 1 FREE)
'''


def combo_special_type_validation(product_id):
    product_details = db.childProducts.find_one({"_id": ObjectId(product_id)})
    if product_details is not None:
        if "productType" in product_details:
            product_type = int(product_details['productType'])
        else:
            product_type = 1
    else:
        product_type = 1
    return product_type
