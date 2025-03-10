from search_api.settings import db
import pandas as pd


def colour_data(parent_product_id, store_details):
    # need to fetch all products base on parent product id
    product_query = {"parentProductId": parent_product_id, "status": 1}
    if len(store_details) > 0:
        product_query['storeId'] = {"$in": store_details}
    else:
        pass
    colour_details = []
    product_details = db.childProducts.find(product_query, {"units": 1})
    if product_details.count() > 0:
        # iterate on child products
        for product in product_details:
            try:
                if "colorName" in product['units'][0]:
                    if product['units'][0]['colorName'] != "":
                        try:
                            rgb_color = str("rgb(") + product['units'][0]['color'] + ")"
                        except:
                            rgb_color = ""
                        colour_details.append(
                            {
                                "name": product['units'][0]['colorName'],
                                "childProductId": str(product['_id']),
                                "parentProductId": str(product['parentProductId']),
                                "rgb": rgb_color
                            }
                        )
            except:
                pass
    else:
        pass

    # need to remove duplicate colour name from the list using the dataframe from pandas
    if len(colour_details) > 0:
        dataframe = pd.DataFrame(colour_details)
        dataframe = dataframe.drop_duplicates(subset="name", keep="last")
        product_list = dataframe.to_dict(orient="records")
    else:
        product_list = []
    return product_list
