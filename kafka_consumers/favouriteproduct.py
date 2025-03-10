import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from django.core.wsgi import get_wsgi_application
# application = get_wsgi_application()
from search_api.settings import CASSANDRA_KEYSPACE, session,db,APP_NAME

try:
    session.set_keyspace(CASSANDRA_KEYSPACE)
except:
    pass


def fav_product_add(data):
    try:
        body = data
        session.execute(
            """ INSERT INTO favouriteproductsuserwise (parentproductid,childproductid, productname,userid, 
            affiliateid, createdtimestamp,
            ipaddress,latitude,longitude,cityid,countryid, session_id, storecategoryid, storeid)
            VALUES (%(parentproductid)s,%(childproductid)s, %(productname)s ,%(userid)s,
            %(affiliateid)s, 
            %(createdtimestamp)s,%(ipaddress)s,
            %(latitude)s,%(longitude)s,%(cityid)s,%(countryid)s,%(session_id)s,%(storecategoryid)s, %(storeid)s)
            """,
            {
                'parentproductid': str(body['parentproductid']), 'childproductid': str(body["childproductid"]),
                'productname': str(body["productname"]), 'userid': str(body["userid"]),
                "affiliateid": "0",
                "createdtimestamp": body["createdtimestamp"],
                'ipaddress': str(body['ipaddress']), 'latitude': body["latitude"],
                'longitude': body['longitude'],
                'cityid': str(body['cityid']), 'countryid': str(body['countryid']),
                'session_id': str(body['session_id']), 'storecategoryid': str(body['storeCategoryId']),
                "storeid": str(body['storeid'])
            }
        )
        #fav product data insert to db 
        fav_data={
                'parentproductid': str(body['parentproductid']), 'childproductid': str(body["childproductid"]),
                'productname': str(body["productname"]), 'userid': str(body["userid"]),
                "affiliateid": "0",
                "createdtimestamp": body["createdtimestamp"],
                'ipaddress': str(body['ipaddress']), 'latitude': body["latitude"],
                'longitude': body['longitude'],
                'cityid': str(body['cityid']), 'countryid': str(body['countryid']),
                'session_id': str(body['session_id']), 'storecategoryid': str(body['storeCategoryId']),
                "storeid": str(body['storeid']),
                "isInfluencer":body["isInfluencer"]
            }
        if APP_NAME == "GetFudo":
            db.likesProducts.insert(fav_data)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        print('Error on line {}'.format(sys.exc_info()
                                        [-1].tb_lineno), type(ex).__name__, ex)
        pass
