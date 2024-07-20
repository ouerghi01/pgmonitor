#!/usr/bin/python

import psycopg2 as pg
import psycopg2.errorcodes


def getSQLResult ( connParam  ,query ) :
	host = connParam['host']
	user = connParam['user']
	port = connParam['port']
	dbname = connParam['dbname']
	password = connParam['password']
	row = None
	
        try :
		conn_str = "host={0:s} user={1:s} port={2:s} dbname={3:s} password={4:s}".format( host , user , str(port) , dbname , password )
                conn = pg.connect( conn_str )
                cur = conn.cursor()
                cur.execute(query)
                row = cur.fetchall()
		conn.close()
		cur.close()
		return [ 0 , row] 
        except pg.Error as err :
		return [None , str(err) ]
		
