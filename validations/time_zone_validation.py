from datetime import datetime
from dateutil import tz
import sys
import os
import pytz


def time_zone_converter(time_zone, store_timestamp, timeZoneWorkingHour):
    try:
        if timeZoneWorkingHour == time_zone or time_zone == "":
            dt_object = datetime.fromtimestamp(store_timestamp)
            date_time1 = dt_object.strftime("%Y-%m-%d %H:%M:%S")
            date_time1 = datetime.strptime(date_time1, "%Y-%m-%d %H:%M:%S")
            return date_time1.timestamp()
        else:
            dt_object = datetime.fromtimestamp(store_timestamp)
            old_timezone = pytz.timezone(timeZoneWorkingHour)
            new_timezone = pytz.timezone(time_zone)
            localized_timestamp = old_timezone.localize(dt_object)
            new_timezone_timestamp = localized_timestamp.astimezone(new_timezone)
            date_time1 = new_timezone_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            date_time1 = datetime.strptime(date_time1, "%Y-%m-%d %H:%M:%S")
            return date_time1.timestamp()
    except Exception as ex:
        print(
            "Error on time zone line {}".format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex
        )
        return store_timestamp
