import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from django.core.wsgi import get_wsgi_application
# application = get_wsgi_application()
from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
from search_api.settings import KAFKA_URL, CASSANDRA_KEYSPACE, RECENTVIEW_KAFKA_CONSUMER, session

session.set_keyspace(CASSANDRA_KEYSPACE)

# consumer = KafkaConsumer(
# 	bootstrap_servers=KAFKA_URL,
# 	auto_offset_reset='latest')
# consumer.subscribe(RECENTVIEW_KAFKA_CONSUMER)
#
# for message in consumer:
# 	try:
# 		body = (message.value).decode("utf-8")
# 		body = json.loads(body)
# 		product_details = session.execute(
# 			"""SELECT * FROM recentviews where userid=%(userid)s and store_category_id=%(store_category_id)s and productid=%(childproductid)s ALLOW FILTERING""",
# 			{"userid": body['userid'], "store_category_id": body['store_category_id'], 'childproductid': str(body["productId"])}
# 		)
# 		if not product_details:
# 			session.execute(
# 				"""
# 				INSERT INTO recentviews (productid, userid, store_category_id, platform, ipaddress, latitude, longitude, cityname,countryname, session_id, storeid, createdtimestamp)
# 				VALUES (%(childproductid)s, %(userid)s, %(store_category_id)s,%(platform)s,%(ipaddress)s,%(latitude)s,%(longitude)s,%(cityname)s,%(countryname)s,%(session_id)s, %(storeid)s, %(createdtimestamp)s)
# 				""",
# 				{'childproductid': str(body["productId"]), 'userid': body['userid'],
# 				 'store_category_id': body['store_category_id'],
# 				 'platform': int(body['seach_platform']),
# 				 'ipaddress': str(body['ipAddress']),
# 				 'latitude': str(body['latitute']),
# 				 'longitude': str(body['longitude']),
# 				 'cityname': body['cityName'],
# 				 'countryname': body['countryName'],
# 				 'session_id': body['session_id'],
# 				 'storeid': body['storeid'],
# 				 "createdtimestamp": body['createdtimestamp']}
# 			)
# 	except Exception as ex:
# 		template = "An exception of type {0} occurred. Arguments:\n{1!r}"
# 		message = template.format(type(ex).__name__, ex.args)
# 		print(message)
# 		pass
