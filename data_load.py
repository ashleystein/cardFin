import csv
import pandas as pd
import db_connector as sqlconn
import mysql.connector


def read_csv(file_path, cyc_dt):
    """
    Reads the csv file, creates a dataframe with the
    :param file_path: file name or path
    :param cyc_dt:
    :return: a dataframe with the values in the csv file and
        the number of invalid customer numbers.
    """
    invalid_count = 0
    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            valid = check_data_quality(row[0], row[1], row[2], row[3])

            if line_count == 1 and valid:
                data = [[int(row[0]), int(row[1]), int(row[2]), row[3], cyc_dt]]
                # Create the dataframe if it is the first row from the file
                cust_df = pd.DataFrame(data, columns=['customer_number', 'customer_rating',
                                                      'customer_rating_limit', 'customer_status',
                                                      'cyc_dt'])
            elif line_count > 1 and valid:
                d = {
                    "customer_number": int(row[0]),
                    "customer_rating": int(row[1]),
                    "customer_rating_limit": int(row[2]),
                    "customer_status": row[3],
                    "cyc_dt": cyc_dt
                }
                df1 = pd.DataFrame(d, index=[0])
                cust_df = pd.concat([cust_df, df1], ignore_index=True)
            if line_count >= 1 and not valid:
                invalid_count += 1
            line_count += 1
    return cust_df, invalid_count


def update_cust_tables(cust_df, tbl_nm, cyc_dt, invalid_cust_num):
    """
    Inserts the new data into the customer rating tables and updates the
    table with the count of invalid customer numbers for each source.
    """

    # The insert statements
    insert_stmt, invalid_data_stmnt = built_insrt_stmt(tbl_nm)

    try:
        db = sqlconn.DbConnector()

        # Updating the table, all the rows for a table are
        # inserted together in one insert statement
        data = list(cust_df.itertuples(index=False, name=None))
        db.conn.cursor().executemany(insert_stmt, data)

        # Inserting the number of invalid customer numbers for that agency
        misses_data = (tbl_nm, invalid_cust_num, cyc_dt)
        db.conn.cursor().execute(invalid_data_stmnt, misses_data)

        db.conn.cursor().execute("COMMIT;")

        db.conn.cursor().close()
        db.conn.close()

    except mysql.connector.Error as err:
        print("Data loading failed: {}".format(err))
    except Exception:
        print("Data loading failed")
        raise Exception


def check_data_quality(cust_num, cust_rt, cust_rt_lmt, cust_sts):
    """
    Checks if the data is in the right data type
    """

    sts_str = (cust_sts == 'low-value' or cust_sts == 'high-value')

    valid = (cust_num.isnumeric()
             and cust_rt.isnumeric()
             and cust_rt_lmt.isnumeric()
             and isinstance(cust_sts, str)
             and sts_str)

    return valid


def built_insrt_stmt(tbl_nm):
    """Builds the 'insert' statements
        for the tables
    """
    insrt = "INSERT INTO CUSTOMERRATINGDATA."
    vals = ("("
            "customer_number, "
            "customer_rating, "
            "customer_rating_limit, "
            "customer_status, "
            "cyc_dt) "
            "VALUES (%s, %s, %s, %s, %s)"
            )
    # If there are duplicate customer numbers, then
    # update the columns with the new data
    updt = ("ON DUPLICATE KEY UPDATE "
            "customer_rating = VALUES(customer_rating), "
            "customer_rating_limit = VALUES(customer_rating_limit), "
            "customer_status = VALUES(customer_status), "
            "cyc_dt = VALUES(cyc_dt);"
            )
    invalid_data_stmnt = (
        "INSERT INTO CUSTOMERRATINGDATA.invalid_cust_data("
        "agency, "
        "invalid_cust_num, "
        "cyc_dt) "
        "VALUES (%s, %s, %s);")
    return insrt + tbl_nm + vals + updt, invalid_data_stmnt


def start_data_load(files, cyc_dt):
    """
    Processes each file and inserts the data into the db
    :param files:
    :param cyc_dt:
    :return:
    """
    tbl_nm = ''
    for f in files:
        tbl = f[-9]
        if tbl == 'A':
            tbl_nm = 'agency_a'
        elif tbl == 'B':
            tbl_nm = 'agency_b'
        elif tbl == 'C':
            tbl_nm = 'agency_c'
        elif tbl == 'D':
            tbl_nm = 'agency_d'
        cust_df, invalid_num = read_csv(f, cyc_dt)
        update_cust_tables(cust_df, tbl_nm, cyc_dt, invalid_num)

    print("Loading data into the tables finished successfully.")


