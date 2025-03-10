import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from django.core.wsgi import get_wsgi_application
# application = get_wsgi_application()
# from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
from search_api.settings import KAFKA_URL, CASSANDRA_SERVER, CASSANDRA_KEYSPACE, CATEGORY_KAFKA_CONSUMER, session

session.set_keyspace(CASSANDRA_KEYSPACE)

# consumer = KafkaConsumer(
# 	bootstrap_servers=KAFKA_URL,
# 	auto_offset_reset='latest')
# consumer.subscribe(CATEGORY_KAFKA_CONSUMER)
#
# for message in consumer:
# 	try:
# 		body = (message.value).decode("utf-8")
# 		body = json.loads(body)
# 		if int(body['searchType']) != 4:
# 			session.execute(
# 				"""
# 				INSERT INTO categorylogs (categoryname, categorytype, userid, partnerid, platform, ipaddress, latitude,
# 				longitude, cityname, countryname, store_category_id, session_id,storeid, createdtimestamp)
# 				VALUES (%(categoryname)s, %(categorytype)s, %(userid)s, %(partnerid)s, %(platform)s,%(ipaddress)s, %(latitude)s,
# 				%(longitude)s, %(cityname)s, %(countryname)s, %(store_category_id)s,%(session_id)s,%(storeid)s,
# 					%(createdtimestamp)s)
# 				""",
# 				{
# 					"categoryname": str(body['catName']), "categorytype": 1,
# 					"userid": str(body['userId']), "partnerid": "",
# 					"platform": int(body['seachPlatform']), "ipaddress": str(body['ipAddress']),
# 					"latitude": str(body['latitude']), "longitude": str(body['longitude']),
# 					"cityname": str(body['cityName']),
# 					"countryname": str(body['countryName']), "store_category_id": body['storeCategoryId'],"session_id": body['session_id'],"storeid": body['storeid'],
# 					"createdtimestamp": int(body['createdtimestamp'])
# 				}
# 			)
# 		else:
# 			session.execute(
# 				"""
# 					INSERT INTO recentsearchlogs (search_item, userid, platform, ipaddress, latitude,
# 					longitude, cityname, countryname, store_category_id, session_id, search_in,
# 					storeid, is_product, parent_product_id, child_product_id, createdtimestamp)
# 					VALUES (%(categoryname)s, %(userid)s, %(platform)s,%(ipaddress)s, %(latitude)s,
# 					%(longitude)s, %(cityname)s, %(countryname)s, %(store_category_id)s,%(session_id)s,
# 					%(search_in)s,%(storeid)s,%(is_product)s, %(parent_product_id)s, %(child_product_id)s,
# 					%(createdtimestamp)s)
# 				""",
# 				{
# 					"categoryname": str(body['searchQuery']),
# 					"userid": str(body['userId']),
# 					"platform": int(body['seachPlatform']), "ipaddress": str(body['ipAddress']),
# 					"latitude": str(body['latitude']), "longitude": str(body['longitude']),
# 					"cityname": str(body['cityName']),
# 					"countryname": str(body['countryName']), "store_category_id": body['storeCategoryId'],"session_id": body['session_id'],
# 					"search_in": body['searchIn'], "storeid": body['storeid'],
# 					"is_product": body['isProduct'] if "isProduct" in body else False,
# 					"parent_product_id": body["parent_product_id"] if "parent_product_id" in body else "",
# 					"child_product_id": body["child_product_id"] if "child_product_id" in body else "",
# 					"createdtimestamp": int(body['createdtimestamp'])
# 				}
# 			)
#
# 	except Exception as ex:
# 		template = "An exception of type {0} occurred. Arguments:\n{1!r}"
# 		message = template.format(type(ex).__name__, ex.args)
# 		print(message)
# 		pass
