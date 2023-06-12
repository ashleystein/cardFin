import pandas as pd
import mysql.connector


def aggregate(cyc_dt):
    """
    Creates and executes the queries to generate the report.
    All queries are created and executed in one method to avoid creating
    multiple connections.
    :param cyc_dt:
    :return: a dataframe with the values
    """

    conn = mysql.connector.connect(user='root', password='my-secret-password',
                                   host='127.0.0.1', port='3306')

    # Get the number of customers that were added on the cycle date ##
    cust_count_query = {
        "agency_a": "select count(customer_number) from customerratingdata.agency_a where cyc_dt = %s ;",
        "agency_b": "select count(customer_number) from customerratingdata.agency_b where cyc_dt = %s ;",
        "agency_c": "select count(customer_number) from customerratingdata.agency_c where cyc_dt = %s ;",
        "agency_d": "select count(customer_number) from customerratingdata.agency_d where cyc_dt = %s ;",
    }

    df = pd.DataFrame(columns=["agency", "cust_num_count"])
    curs = conn.cursor(buffered=True)
    for key in cust_count_query:
        curs.execute(cust_count_query[key], (cyc_dt,))
        cust_num_count = int(curs.fetchone()[0])

        df2 = pd.DataFrame([[key, cust_num_count]],
                           columns=["agency", "cust_num_count"])
        df = pd.concat([df, df2])

    # Get the sum of the customer rating values ##
    sum_query = {
        "agency_a": "select sum(customer_rating) from customerratingdata.agency_a where cyc_dt = %s ;",
        "agency_b": "select sum(customer_rating) from customerratingdata.agency_b where cyc_dt = %s ;",
        "agency_c": "select sum(customer_rating) from customerratingdata.agency_c where cyc_dt = %s ;",
        "agency_d": "select sum(customer_rating) from customerratingdata.agency_d where cyc_dt = %s ;",
    }
    cust_rt_sum = []
    for key in sum_query:
        curs.execute(sum_query[key], (cyc_dt,))
        res = curs.fetchone()[0]
        cust_rt_sum.append(int(res))
    df["cust_rt_sum"] = cust_rt_sum

    # Gets the number of high-valued customers ##
    cust_sts_query = {
        "agency_a": "select count(customer_status) from customerratingdata.agency_a where cyc_dt = %s and customer_status = 'high-value';",
        "agency_b": "select count(customer_status) from customerratingdata.agency_b where cyc_dt = %s and customer_status = 'high-value';",
        "agency_c": "select count(customer_status) from customerratingdata.agency_c where cyc_dt = %s and customer_status = 'high-value';",
        "agency_d": "select count(customer_status) from customerratingdata.agency_d where cyc_dt = %s and customer_status = 'high-value';",
    }
    cust_sts_count = []
    for key in cust_sts_query:
        curs.execute(cust_sts_query[key], (cyc_dt,))
        sts_count = curs.fetchone()[0]
        cust_sts_count.append(int(sts_count))
    df["cust_sts_count"] = cust_sts_count

    ## Get the number of invalid customer number ##
    invalid_cust_count_query = {
        "agency_a": (
            "select invalid_cust_num from CUSTOMERRATINGDATA.invalid_cust_data where cyc_dt = %s and agency = 'agency_a';"),
        "agency_b": (
            "select invalid_cust_num from CUSTOMERRATINGDATA.invalid_cust_data where cyc_dt = %s and agency = 'agency_b';"),
        "agency_c": (
            "select invalid_cust_num from CUSTOMERRATINGDATA.invalid_cust_data where cyc_dt = %s and agency = 'agency_c';"),
        "agency_d": (
            "select invalid_cust_num from CUSTOMERRATINGDATA.invalid_cust_data where cyc_dt = %s and agency = 'agency_d';"),
    }

    invalid_cust_count = []
    for key in invalid_cust_count_query:
        curs.execute(invalid_cust_count_query[key], (cyc_dt,))
        count = curs.fetchone()[0]
        invalid_cust_count.append(int(count))

    df["invalid_cust_count"] = invalid_cust_count

    # Percentage of invalid customer numbers for the day.
    df["percent_invalid_data"] = df.apply(
        lambda x: round(float(x['invalid_cust_count'] / float(x['cust_num_count'] + x['invalid_cust_count'])),
                        2), axis=1)

    # Closing connection
    conn.cursor().close()
    conn.close()

    return df


def write_to_csv(df, cyc_dt):
    """
    Writes the dataframe to a csv file
    :param df:
    :param cyc_dt:
    :return:
    """
    file_nm = create_file_nm(cyc_dt)
    df.to_csv(file_nm, index=False)

    print(f'Report {file_nm} is ready.')


def create_file_nm(cyc_dt):
    # Assuming cyc_dt is in the format YYYY-MM-DD
    cyc_dt = str(cyc_dt)
    dt = cyc_dt[0:4] + cyc_dt[5:7] + cyc_dt[8:]
    return 'Customer_Rating_Aggregate_Report_' + dt + '.csv'
