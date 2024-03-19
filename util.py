
import time

def convert_date(hl7_date):
    date_seconds = time.strptime(hl7_date, '%Y%m%d')
    print(date_seconds)
    omop_date = time.strftime('%Y-%m-%d', date_seconds)

    return omop_date
