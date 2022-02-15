import csv
import mysql.connector
import pandas as pd
from datetime import datetime, timedelta

def get_db_connection():

    MySqldf = pd.read_csv('MySql.cfg')
    user = MySqldf['Value'][0]
    password = MySqldf['Value'][1]
    host = MySqldf['Value'][2]

    cnx = None
    try:
        config = {
            'user': user,
            'password': password,
            'host': host,
            'database': 'minioop',
            'raise_on_warnings': True}
        cnx = mysql.connector.connect(**config)
    except Exception as error:
        print("Error while connecting to database for job tracker", error)
        return cnx

        quit()

    return cnx

def load_third_party(connection, file_path_csv):
    cursor = connection.cursor()
    with open(file_path_csv,'r') as csv_feed:
        data=csv.reader(csv_feed)

    for line in data:
        _col = 'ticket_id,trans_date,event_id,event_name,event_date,event_type,event_city,customer_id,price,num_tickets'
        _val = '{},"{}","{}","{}",{},"{}",{}'.format(line[0], line[1], line[2], line[3], line[4],\
                                                     line[5], line[6], line[7], line[8], line[9])
        _sql = 'INSERT orders ({}) VALUES ({});'.format(_col, _val)
        connection.commit()
    cursor.close()
    return


def query_popular_tickets(connection):
# Get the most popular ticket in the past month
    last_month=datetime.today()-timedelta(months=-1)
    sql_statement = '''
                    SELECT 
                        event_id,
                        
                    FROM
                        sales
                    GROUP BY 
                        event_id
                    WHERE
                        event_date>{}
                    '''.format(last_month.strftime())
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records
