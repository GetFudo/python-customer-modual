import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from search_api.settings import es, CHILD_PRODUCT_INDEX, CHILD_PRODUCT_DOC_TYPE, CENTRAL_PRODUCT_VARIENT_INDEX, \
    CENTRAL_PRODUCT_DOC_TYPE, CENTRAL_PRODUCT_INDEX, STORE_PRODUCT_INDEX, STORE_PRODUCT_DOC_TYPE


def product_es_aggrigate_data(query):
    res = es.search(index=CENTRAL_PRODUCT_INDEX, body=query)
    return res


def child_product_es_aggrigate_data(query):
    res = es.search(index=CHILD_PRODUCT_INDEX, body=query)
    return res


def product_es_search_data(query):
    res_filter_parameters = es.search(
        index=CENTRAL_PRODUCT_INDEX,
        body=query,
        filter_path=[
            "hits.total",
            "hits.hits._id",
            "hits.hits._source"
        ],
    )
    return res_filter_parameters

def child_product_es_search_data(query):
    res_filter_parameters = es.search(
        index=CHILD_PRODUCT_INDEX,
        body=query,
        filter_path=[
            "hits.total",
            "hits.hits._id",
            "hits.hits._source"
        ],
    )
    return res_filter_parameters

def product_es_variant_search_data(query):
    res_filter_parameters = es.search(
        index=CENTRAL_PRODUCT_VARIENT_INDEX,
        body=query,
        filter_path=[
            "hits.hits._id",
            "hits.hits._source"
        ],
    )
    return res_filter_parameters