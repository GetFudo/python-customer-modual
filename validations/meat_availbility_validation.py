from validations.driver_roaster import next_availbale_driver_shift_out_stock, next_availbale_driver_out_stock_shift, \
    next_availbale_driver_roaster, next_availbale_driver_shift_in_stock


def meat_availability_check(child_product_details, available_qty, is_dc_linked, next_availbale_driver_time, hard_limit,
                            pre_order, driver_roaster, dc_child_product, zone_id, procurementTime):
    if child_product_details is not None:
        if str(child_product_details['storeId']) == "0":
            outOfStock = True
            next_availbale_time = ""
        else:
            if available_qty > 0 and is_dc_linked == True and next_availbale_driver_time != "":
                next_delivery_slot = driver_roaster['text']
                next_availbale_time = driver_roaster['productText']
                if next_delivery_slot != "" and next_availbale_time != "":
                    outOfStock = False
                else:
                    outOfStock = True
            elif available_qty < 0 and hard_limit != 0 and is_dc_linked == True and next_availbale_driver_time != "":
                delivery_slot = next_availbale_driver_shift_out_stock(
                    zone_id, 0, hard_limit, str(child_product_details['_id']))
                try:
                    next_availbale_time = delivery_slot['productText']
                except:
                    next_availbale_time = ""
                if next_availbale_time != "":
                    outOfStock = False
                else:
                    outOfStock = True
            elif available_qty <= 0 and is_dc_linked == True and pre_order == True:
                if "seller" in dc_child_product:
                    if len(dc_child_product["seller"]) > 0:
                        delivery_slot = next_availbale_driver_shift_out_stock(zone_id, procurementTime, hard_limit,
                                                                              str(child_product_details['_id']))
                    else:
                        delivery_slot = next_availbale_driver_shift_out_stock(zone_id, 0, hard_limit,
                                                                              str(child_product_details['_id']))
                    try:
                        next_availbale_time = delivery_slot['productText']
                    except:
                        next_availbale_time = ""
                    # if allow_out_of_order == True and next_availbale_time != "":
                    if next_availbale_time != "":
                        outOfStock = False
                    else:
                        outOfStock = True
                else:
                    next_availbale_time = ""
                    outOfStock = True
            elif available_qty == 0 and is_dc_linked == False:
                next_availbale_time = ""
                outOfStock = True
            else:
                next_availbale_time = ""
                outOfStock = True
        return outOfStock, next_availbale_time
    else:
        next_availbale_time = ""
        outOfStock = True
        return outOfStock, next_availbale_time
