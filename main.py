import datetime
from datetime import timedelta

import data_load as dl
import dbConnector as sqlconn
import report


def main():
    files = ['Customer Rating Agency A Inc.csv',
             'Customer Rating Agency B Inc.csv',
             'Customer Rating Agency C Inc.csv',
             'Customer Rating Agency D Inc.csv']
    cyc_dt = get_cyc_dt()
    dl.start_data_load(files, cyc_dt)
    df = report.aggregate(cyc_dt)
    report.write_to_csv(df, cyc_dt)

    print(f'Data import for {cyc_dt} finished successfully.')


def get_cyc_dt():
    """
    Gets the new cycle date by adding one day to the last date if it is
    Monday to Thursday, and adding 3 days if it is a Friday.
    If it is the first entry in the table then the cycle date is today's date
    :return: new cycle date
    """

    cnx = sqlconn.DbConnector()
    curs = cnx.conn.cursor(buffered=True)

    query = "SELECT max(cyc_dt) from CUSTOMERRATINGDATA.agency_a;"

    if curs.execute(query) is not None:
        cyc_dt = curs.execute(query).fetchone()
        cyc_dt = datetime.date(cyc_dt)
    else:
        cyc_dt = datetime.date.today()

    new_cyc_dt = cyc_dt + timedelta(days=3) if cyc_dt.weekday() == 5 else cyc_dt + timedelta(days=1)

    # Close the connection
    curs.close()
    cnx.conn.close()

    return new_cyc_dt


main()
