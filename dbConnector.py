import mysql.connector

'''
Creating a class to create an object to hold
the connection. This object can be passed to functions instead of 
creating new connections.
'''


class DbConnector:
    def __init__(self):
        """
        Create a connection object to connect to the database
        """
        try:
            # TODO: In the ideal scenario this will be stored in secure place, like HCV
            self.conn = mysql.connector.connect(user='root', password='my-secret-password',
                                                host='127.0.0.1', port='3306')
        except mysql.connector.Error as err:
            print("Connection to database failed: {}".format(err))
