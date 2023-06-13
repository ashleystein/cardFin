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

    conn = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1', port='3306')
    # Check if cyc_dt exists in the table
    sql = ("SELECT COUNT(cyc_dt) "
           "FROM CUSTOMERRATINGDATA.invalid_cust_data "
           "WHERE cyc_dt = %s;"
           )
    curs = conn.cursor(buffered=True)
    curs.execute(sql, (cyc_dt,))
    cyc_dt_count = curs.fetchone()[0]

    if cyc_dt_count > 0:
        tbl_nm = ["agency_a", "agency_b", "agency_c", "agency_d"]

        df = pd.DataFrame(columns=["agency"])
        df["agency"] = tbl_nm

        cust_cnt_lst = []
        cust_rt_sum = []
        cust_sts_count = []
        invalid_cust_count = []

        curs = conn.cursor(buffered=True)

        for tbl in tbl_nm:
            # Total number of customer numbers for the cycle date
            cust_count_query = build_aggregate_stmt(tbl, "count_cust_num")
            curs.execute(cust_count_query, (cyc_dt,))
            cust_num_count = curs.fetchone()[0]
            cust_cnt_lst.append(int(cust_num_count))

            # Sum of customer rates
            rt_sum_query = build_aggregate_stmt(tbl, "sum_cust_rt")
            curs.execute(rt_sum_query, (cyc_dt,))
            rt_sum = curs.fetchone()[0]
            cust_rt_sum.append(int(rt_sum))

            # Number of 'high-value' customers
            cust_sts_query = build_aggregate_stmt(tbl, "count_cust_sts")
            curs.execute(cust_sts_query, (cyc_dt,))
            sts_count = curs.fetchone()[0]
            cust_sts_count.append(int(sts_count))

            # Number of invalid customer numbers
            invalid_cnt_query = build_aggregate_stmt(tbl, "count_invalid")
            curs.execute(invalid_cnt_query, (cyc_dt,))
            invalid_cnt = curs.fetchone()[0]
            invalid_cust_count.append(int(invalid_cnt))

        df["cust_num_count"] = cust_cnt_lst
        df["cust_rt_sum"] = cust_rt_sum
        df["cust_sts_count"] = cust_sts_count
        df["invalid_cust_count"] = invalid_cust_count

        # Percentage of invalid customer numbers for the day.
        df["percent_invalid_data"] = df.apply(
            lambda x: round(float(x['invalid_cust_count'] / float(x['cust_num_count'] + x['invalid_cust_count'])),
                            2), axis=1)

        # Closing connection
        conn.cursor().close()
        conn.close()

        return df
    else:
        print(f'Data for {cyc_dt} does not exist')


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


def build_aggregate_stmt(tbl_nm, oprtn):
    """
    Builds the queries to do aggregations
    :param tbl_nm:
    :param oprtn:
    :return: the query for the tbl and operation
    """
    sql = ''
    if oprtn == 'count_cust_num':
        sql = ("select count(customer_number) from customerratingdata."
               + tbl_nm + " " +
               "where cyc_dt = %s ;"
               )
    elif oprtn == 'sum_cust_rt':
        sql = ("select sum(customer_rating) from customerratingdata."
               + tbl_nm + " " +
               "where cyc_dt = %s ;"
               )
    elif oprtn == 'count_cust_sts':
        sql = ("select count(customer_status) from customerratingdata."
               + tbl_nm + " " +
               "where cyc_dt = %s and customer_status = 'high-value';")

    elif oprtn == 'count_invalid':
        sql = ("select invalid_cust_num from CUSTOMERRATINGDATA.invalid_cust_data where cyc_dt = %s "
               "and agency = '" + tbl_nm + "';")

    return sql


aggregate('2023-06-09')
