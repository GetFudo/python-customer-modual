from search_api.settings import db

def product_avg_rating(product_id):
    # avg_rating = 0
    # product_rating = db.reviewRatings.aggregate(
    #     [
    #         {"$match": {
    #             "productId": str(product_id),
    #             "rating": {"$ne": 0}
    #         }},
    #         {
    #             "$group":
    #                 {
    #                     "_id": "$productId",
    #                     "avgRating": {"$avg": "$rating"}
    #                 }
    #         }
    #     ]
    # )
    # for avg_rat in product_rating:
    #     avg_rating = avg_rat['avgRating']
    # if avg_rating == None:
    #     avg_rating = 0
    # return round(avg_rating, 2)
    avg_product_rating_value_new = 0
    try:
        product_rating = db.reviewRatings.aggregate(
            [
                {
                    "$match": {
                        "productId": str(product_id),
                        "rating": {"$ne": 0},
                        "status": 1,
                    }
                },
                {"$group": {"_id": "$orderId", "avgRating": {"$avg": "$rating"}}},
            ]
        )
        p_ratting = 0
        rat_count = 0
        for avg_product_rating in product_rating:
            rat_count = rat_count + 1
            ratting_get = avg_product_rating['avgRating']
            p_ratting = p_ratting + ratting_get

        avg_product_rating_value = p_ratting / rat_count
        try:
            avg_product_rating_value_new = round(avg_product_rating_value, 2)
        except:
            avg_product_rating_value_new = 0
    except:
        avg_product_rating_value_new = 0
    return avg_product_rating_value_new