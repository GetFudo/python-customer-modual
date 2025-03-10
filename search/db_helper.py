from search_api.settings import db, es, session
import sys

class DbHelper:

    def get_stores(self, query, project_query=None):
        if project_query is not None:
            result = db.stores.find(query, project_query)
        else:
            result = db.stores.find(query)
        return result

    def get_single_store(self, query, project_query=None):
        if project_query is not None:
            result = db.stores.find_one(query, project_query)
        else:
            result = db.stores.find_one(query)
        return result

    def get_categories(self, query, project_query, from_data, to_data, sort_query):
        if from_data is not None and to_data is not None and sort_query is not None:
            result = db.category.find(query, project_query).skip(int(from_data)).limit(int(to_data)).sort(sort_query)
        elif from_data is not None and to_data is not None and sort_query is None:
            result = db.category.find(query, project_query).skip(int(from_data)).limit(int(to_data))
        elif from_data is None and to_data is None and sort_query is not None:
            result = db.category.find(query, project_query).skip(int(from_data)).limit(int(to_data)).sort(sort_query)
        else:
            result = db.category.find(query, project_query).skip(int(from_data)).limit(int(to_data))
        return result

    def get_categories_count(self, query, project_query=None):
        if project_query is not None:
            result = db.category.find(query, project_query).count()
        else:
            result = db.category.find(query).count()
        return result

    def get_child_product(self, query, project_query=None):
        if project_query is None:
            result = db.childProducts.find_one(query)
        else:
            result = db.childProducts.find_one(query, project_query)
        return result

    def get_product(self, query, project_query=None):
        if project_query is not None:
            result = db.products.find_one(query, project_query)
        else:
            result = db.products.find_one(query)
        return result

    def get_favourite_product(self, user_id, store_category_id=None):
        if store_category_id is not None:
            response_casandra = session.execute(
                """SELECT * FROM favouriteproductsuserwise where userid=%(userid)s AND storecategoryid=%(store_category_id)s ALLOW FILTERING""", {"userid": user_id, "store_category_id": store_category_id})
            response_casandra_count = session.execute(
                """SELECT COUNT(*) FROM favouriteproductsuserwise where userid=%(userid)s AND storecategoryid=%(store_category_id)s ALLOW FILTERING""",{"userid": user_id, "store_category_id": store_category_id})
        else:
            response_casandra = session.execute(
                """SELECT * FROM favouriteproductsuserwise where userid=%(userid)s ALLOW FILTERING""",
                {"userid": user_id})
            response_casandra_count = session.execute(
                """SELECT COUNT(*) FROM favouriteproductsuserwise where userid=%(userid)s ALLOW FILTERING""",
                {"userid": user_id})
        return response_casandra, response_casandra_count

    def get_review_ratings_count(self, query):
        result = db.reviewRatings.find(query)
        return result

    def get_currency(self, query):
        result = db.currencies.find_one(query)
        return result

    def get_brands(self, query, project_query=None, from_data=None, to_data=None, sort_query=None):
        if project_query is not None:
            if from_data is not None and to_data is not None and sort_query is not None:
                result = db.brands.find(query, project_query).skip(int(from_data)).limit(int(to_data)).sort(sort_query)
            elif from_data is not None and to_data is not None and sort_query is None:
                result = db.brands.find(query, project_query).skip(int(from_data)).limit(int(to_data))
            elif from_data is None and to_data is None and sort_query is not None:
                result = db.brands.find(query, project_query).sort(sort_query)
            else:
                result = db.brands.find(query, project_query)
        else:
            if from_data is not None and to_data is not None and sort_query is not None:
                result = db.brands.find(query).skip(int(from_data)).limit(int(to_data)).sort(sort_query)
            elif from_data is not None and to_data is not None and sort_query is None:
                result = db.brands.find(query).skip(int(from_data)).limit(int(to_data))
            elif from_data is None and to_data is None and sort_query is not None:
                result = db.brands.find(query).sort(sort_query)
            else:
                result = db.brands.find(query)
        return result

    def get_brands_count(self, query):
        result = db.brands.find(query).count()
        return result

    def get_main_product_count(self, query):
        result =  db.childProducts.find(query).count()
        return result

    def get_symptoms(self, query, project_query=None, from_data=None, to_data=None, sort_query=None):
        if project_query is not None:
            if from_data is not None and to_data is not None and sort_query is not None:
                result = db.symptom.find(query, project_query).skip(int(from_data)).limit(int(to_data)).sort(sort_query)
            elif from_data is not None and to_data is not None and sort_query is None:
                result = db.symptom.find(query, project_query).skip(int(from_data)).limit(int(to_data))
            elif from_data is None and to_data is None and sort_query is not None:
                result = db.symptom.find(query, project_query).sort(sort_query)
            else:
                result = db.symptom.find(query, project_query)
        else:
            if from_data is not None and to_data is not None and sort_query is not None:
                result = db.symptom.find(query).skip(int(from_data)).limit(int(to_data)).sort(sort_query)
            elif from_data is not None and to_data is not None and sort_query is None:
                result = db.symptom.find(query).skip(int(from_data)).limit(int(to_data))
            elif from_data is None and to_data is None and sort_query is not None:
                result = db.symptom.find(query).sort(sort_query)
            else:
                result = db.symptom.find(query)
        return result

    def get_symptoms_count(self, query):
        result = db.symptom.find(query).count()
        return result

    def get_specialities(self, query):
        result = db.specialities.find(query).sort([("_id", -1)])
        return result

    def get_specialities_count(self, query):
        result = db.specialities.find(query).sort([("_id", -1)]).count()
        return result

    def get_stores_count(self, query):
        result = db.stores.find(query).count()
        return result

    def get_city_data(self, condition):
        """"
        This method fetches cities data from database based on condition
        :return: zone_details
        """
        project_data = {"_id": 1, "cityId": 1, "countryId": 1, "currency": 1, "currencySymbol": 1, "cityName": 1,
                        "state": 1, "laundry": 1, "storeCategory": 1}
        zone_details = db.cities.find_one(condition, project_data)
        return zone_details

    def get_zone_data(self, zone_condition):
        """
        This method fetches zone details from databse based on condition
        :return: zone_data
        """
        zone_data = db.zones.find_one(zone_condition, {"_id": 1, "title": 1})
        return zone_data

    def get_store_category(self, query):
        """
        This method fetches store category from database
        :param query: query
        :return: store_category
        """
        project_data = {
            "storeCategoryName": 1,
            "storeCategoryDescription": 1,
            "bannerImage": 1,
            "logoImage": 1,
            "allowOrderOutOfStock": 1,
            "signatureRequire": 1,
            "deliveryTime": 1,
            "nowBooking": 1,
            "scheduleBooking": 1,
            "shiftSelection": 1,
            "deliverTo": 1,
            "shippedToText": 1,
            "deliveryTimeText": 1,
            "iconlogoimg": 1,
            "type": 1,
            "typeName": 1,
            "colorCode": 1
        }
        store_category = db.storeCategory.find_one(query, project_data)
        return store_category

    def get_stores_from_elastic(self, store_query, index_store, filter_path_keys):
        """
        this method query elastic search based on query
        :param store_query: search query
        :return: res_store data of stores
        """
        try:
            res_store = es.search(
                index=index_store,
                # doc_type=doc_central_product,
                body=store_query,
                filter_path=filter_path_keys,
            )
            return res_store
        except Exception as ex:
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            return {}

    def get_notification_count(self, query):
        """
        This method gets unseed notification count #####
        :param query: query
        :return: notification_count
        """
        notification_count = db.notificationLogs.find(query).count()
        return notification_count

    def get_seller_review_rating(self, seller_id):
        """"
        This method gets seller review rating
        """
        seller_rating = db.sellerReviewRatings.aggregate(
            [
                {
                    "$match":
                        {
                            "sellerId": str(seller_id),
                            "status": 1,
                            "rating": {"$ne": 0}
                        }
                },
                {
                    "$group":
                        {
                            "_id": "$sellerId",
                            "avgRating": {"$avg": "$rating"}
                        }
                }
            ]
        )
        return seller_rating

    def get_offer_details(self, seller_id):
        offer_details = db.offers.find({"storeId": {"$in": [str(seller_id)]}, "status": 1}, {"name":1, "offerType":1, "discountValue":1})
        return offer_details

    def get_single_speciality(self, query, project_query=None):
        if project_query is not None:
            result = db.specialities.find_one(query, project_query)
        else:
            result = db.specialities.find_one(query)
        return result

    def get_all_banner(self, query):
        result = db.banner.find(query)
        return result

    def update_notification_logs(self, query):
        result = db.notificationLogs.update(query, {"$set": {"isSeen": True}}, multi=True)
        return result

    def get_seller_rating(self, query, skip=None, limit=None, sort_query=None):
        if skip is not None and limit is not None and sort_query is not None:
            result = db.sellerReviewRatings.find(query).sort(sort_query).skip(skip).limit(limit)
        elif skip is not None and limit is not None:
            result = db.sellerReviewRatings.find(query).skip(skip).limit(limit)
        elif sort_query is not None:
            result = db.sellerReviewRatings.find(query).sort(sort_query)
        else:
            result = db.sellerReviewRatings.find(query)
        return result

    def get_seller_review_rating_count(self, query):
        result = db.sellerReviewRatings.find(query).count()
        return result

    def get_rating_parameter(self, query, project_query):
        result = db.ratingParams.find_one(query, project_query)
        return result

    def get_aggregate_review_rating(self, seller_id, order_id):
        result = db.sellerReviewRatings.aggregate(
                [
                    {
                        "$match":
                            {
                                "sellerId": str(seller_id),
                                "orderId": order_id,
                                "status": 1,
                                "rating": {"$ne": 0}
                            }
                    },
                    {
                        "$group":
                            {
                                "_id": "$sellerId",
                                "avgRating": {"$avg": "$rating"}
                            }
                    }
                ]
            )
        return result