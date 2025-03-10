import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from mongo_query_module.query_module import city_find_one


def validate_store_category(store_category_id, ecomm_store_category):
    hide_recent_view = False
    is_ecommerce = False
    store_listing = False
    remove_central = False
    categoty_details = city_find_one({"storeCategory.storeCategoryId": store_category_id})
    if store_category_id == ecomm_store_category:
        store_listing = False
        is_ecommerce = True
        remove_central = False
        hide_recent_view = False
    else:
        if categoty_details is not None:
            if "storeCategory" in categoty_details:
                for cat in categoty_details['storeCategory']:
                    if "storeListing" in cat:
                        if cat['storeCategoryId'] == store_category_id:
                            if cat['hyperlocal'] is True and cat['storeListing'] == 1:
                                store_listing = True
                                is_ecommerce = False
                                remove_central = True
                                hide_recent_view = False
                            elif cat['hyperlocal'] is True and cat['storeListing'] == 0:
                                store_listing = False
                                is_ecommerce = False
                                remove_central = True
                                hide_recent_view = True
                            elif cat['hyperlocal'] is False and cat['storeListing'] == 0:
                                store_listing = False
                                is_ecommerce = False
                                remove_central = True
                                hide_recent_view = True
                            else:
                                store_listing = False
                                is_ecommerce = True
                                remove_central = False
                                hide_recent_view = False
                        else:
                            pass
                    else:
                        pass
            else:
                store_listing = False
                is_ecommerce = True
                remove_central = False
                hide_recent_view = False
        else:
            store_listing = False
            is_ecommerce = True
            remove_central = False
            hide_recent_view = False
    return is_ecommerce, remove_central, hide_recent_view, store_listing
