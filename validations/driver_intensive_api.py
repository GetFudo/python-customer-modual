import requests
from search_api.settings import NODE_URL
def driver_intensive(details, token):
    API_URL = "http://"+str(NODE_URL)+"/v1/incentiveCampaigns"
    print("NODE_URL",NODE_URL)
    print("API_URL",NODE_URL)
    json_data = {
        "driverId": details["driverId"],
        "serviceType": details["serviceType"],
        "triggerType": 2,
        "bookingId": details["bookingId"],
    }
    header = {"Authorization": token, "language": "en", "content-type": "application/json"}
    expireOffer = requests.post(API_URL, json=json_data, headers=header)
    return True
