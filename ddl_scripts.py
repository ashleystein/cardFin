import dbConnector as sqlconn


def create_db():
    sql = "CREATE DATABASE CUSTOMERRATINGDATA"
    db = sqlconn.DbConnector()
    db.conn.cursor().execute(sql)
    db.conn.close()


def create_customer_data_tables():
    # TODO: make customer number primary key
    """
    Create the four agency tables and one table to store
    the count of missed customer number
    :return:
    """
    tbl_nm = ["agency_a", "agency_b", "agency_c", "agency_d"]
    ddl_stmnt = []
    try:
        for tbl in tbl_nm:  # Gets the 'create' statement for the tables
            sql = build_ddl_stmnt(tbl)
            ddl_stmnt.append(sql)

        # Creates a table to store the number of invalid customer number count
        # for each cycle date and agency
        invalid_cust_data = """
                CREATE TABLE CUSTOMERRATINGDATA.invalid_cust_data(
                    agency varchar(255),
                    invalid_cust_num int,
                    cyc_dt date
                    );
                """
        ddl_stmnt.append(invalid_cust_data)

        db = sqlconn.DbConnector()

        # Execute the statements to create
        # the customer rating data tables
        for sql in ddl_stmnt:
            db.conn.cursor().execute(sql)

        # Closing connection
        db.conn.cursor().close()
        db.conn.close()

        print("Tables were created successfully.")

    except Exception as err:
        print(f"Creating the tables failed: {err=}, {type(err)=}")
        raise


def build_ddl_stmnt(tbl_nm):
    """
    Builds the ddls for the table
    :param tbl_nm:
    :return: the 'create' statement
    """
    create = "CREATE TABLE CUSTOMERRATINGDATA."
    columns = ("(customer_number int NOT NULL,"
               "customer_rating int,"
               "customer_rating_limit int,"
               "customer_status varchar(25),"
               "cyc_dt date,"
               "PRIMARY KEY (customer_number) "
               ");"
               )
    return create + tbl_nm + columns


#create_customer_data_tables()
