
def checkTableName(destination_table_id):
    table = 'service_provider_unavailability_period'
    if table == destination_table_id:
        return 'CONCAT("[",unavailability_period_start,"," ,DATE_ADD(unavailability_period_end, INTERVAL 1 DAY),")") unavailability_period_range'
    else:
        return None