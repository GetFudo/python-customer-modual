from rejson import Client, Path
from search_api.settings import currency_exchange_rate, REDIS_JSON_IP


from . response_handler import JSONEncoder as CustomEncoder

rj = Client(host=REDIS_JSON_IP, port=6379, decode_responses=True, encoder=CustomEncoder())

class ProcessHandler:
    '''
        Class for utility methods.
    '''

    def generate_rj_id(self, collectionName, id):
        '''
            Method to generate id for redis json.
        '''
        return f"rj:{collectionName}:{str(id)}"

    def check_rj_data_mongo(self, rjId, query, collection):
        '''
            Method to generate and store RedisJson from mongoDB.
        '''
        #check for database
        jsonData = collection.find_one(query)
        if jsonData is not None:
            # update in the redis json
            rj.jsonset(rjId, Path.rootPath(), jsonData)
            rjData = rj.jsonget(rjId)
            return rjData, True
        return False, False

    def update_rj_data_mongo(self, rjId, query, collection):
        '''
            Method to update RedisJson data from mongoDB.
        '''
        #check for database
        jsonData = collection.find_one(query)
        if bool(jsonData):
            # update in the redis json
            rj.jsondel(rjId, Path.rootPath())
            result = rj.jsonset(rjId, Path.rootPath(), jsonData)
            print("Redis data updated -> ", result)
            return True
        return False

    def get_currency_rate(self, currency_code):
        '''
            Method to get current price of currency.
        '''
        try:
            currency_rate = float(currency_exchange_rate[str(currency_code) + "_" + str(currency_code)])
        except:
            currency_rate = 0
        return currency_rate

    def convert_product_price(self, currency_rate, rjData, section_names, currency_code):
        '''
            Method to convert product price currency wise.
        '''
        if rjData is not None and currency_rate != 0:
            for sec_name in section_names:
                if str(sec_name) in str(rjData):
                    try:
                        for prods in rjData[str(sec_name)]['stories']:
                            if prods['productPrice'] is not None:
                                final_price = int(prods['productPrice']['finalPrice']) * int(currency_rate)
                                base_price = int(prods['productPrice']['basePrice']) * int(currency_rate)
                                discount_price = int(prods['productPrice']['discountPrice']) * int(currency_rate)
                                prods['productPrice']['finalPrice'] = final_price
                                prods['productPrice']['basePrice'] = base_price
                                prods['productPrice']['discountPrice'] = discount_price
                                prods['productPrice']['currency'] = currency_code
                    except:
                        pass
        return rjData


ProcessHandlerObj = ProcessHandler()

