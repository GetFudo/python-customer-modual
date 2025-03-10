import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from search_api.settings import db


def city_find_one(query):
    city_details = db.cities.find_one(query)
    return city_details

def store_find(query):
    store_data = db.stores.find(query)
    return store_data

def zone_find(query):
    zone_data = db.zones.find_one(query)
    return zone_data

def category_find_one(query):
    base_category = db.category.find_one(query)
    return base_category

def category_find(query, from_data, to_data):
    base_category = db.category.find(query).sort([("seqId", -1)]).skip(int(from_data)).limit(int(to_data))
    return base_category

def category_find_count(query):
    base_category = db.category.find(query).count()
    return base_category

def brand_find(query, from_data, to_data):
    brand_data = db.brands.find(query).sort([("_id", -1)]).skip(int(from_data)).limit(int(to_data))
    return brand_data

def brand_find_count(query):
    brand_data = db.brands.find(query).count()
    return brand_data

def brand_find_one(query):
    brand_data = db.brands.find_one(query)
    return brand_data

def symptom_find(query, from_data, to_data):
    symptom_data = db.symptom.find(query).sort([("_id", -1)]).skip(int(from_data)).limit(int(to_data))
    return symptom_data

def symptom_find_count(query):
    symptom_data = db.symptom.find(query).count()
    return symptom_data

def symptom_find_one(query):
    symptom_data = db.symptom.find_one(query)
    return symptom_data

def banner_find(query):
    banner_data = db.banner.find(query).sort([("_id", -1)])
    return banner_data

def offer_find_one(query):
    offer_data = db.offers.find_one(query)
    return offer_data

def product_find_count(query):
    product_data = db.products.find(query).count()
    return product_data

def home_page_find_one():
    product_data = db.homepage.find_one({})
    return product_data

