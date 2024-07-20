#!/usr/bin/python 

import time
import sql
import math


def getConnections( param=None ) :
        item_name = 'POSTGRES_CONNECTIONS'
        status = []
        perfdata = '-'
        output = ''
        if param != None :
                query = "SELECT \
                              datname \
                         FROM \
                              pg_database \
                         WHERE datistemplate is FALSE"
	
		host = param['host'][0]
		port = param['port'][0]
		dbname = param.get('dbname')[0]

                results = sql.getSQLResult ( {'host': host , 'port' : port, 'dbname': dbname, 'user' : param['user'] ,'password' : param['password'] } ,query )
		if results[0] == None :
			return '2' + ' ' + 'POSTGRES_CONNECTIONS' + ' ' + '-' + ' ' + results[1]
	
		dbs = results[1]

                exclude_db = param.get('exclude_db')
                for db in exclude_db :
                        if db in dbs :
                                dbnames.remove(db)

		for db in dbs :
			query = "select version()"
			begin = time.time()
			row = sql.getSQLResult ( {'host': host , 'port' : port, 'dbname': db[0], 'user' : param['user'] ,'password' : param['password'] } ,query )
			duration = int ( math.ceil ( ( time.time() - begin ) * 1000 ) )
			out_add = 'connection test took {0:s} ms '
			if row != None :
				status.append(0)
			else :
				status.append(2)
				out_add = ' refuses connection !!! {0:s}'
				duration = ''

                       	if perfdata == '-' :
                               	perfdata = db[0] + '=' + str( duration )
                               	output =  '{0:s} {1:s}'.format(db[0], out_add.format(str(duration) ) )
                       	elif perfdata != '-'  :
                               	perfdata = perfdata + '|' + db[0] + '=' + str ( duration )
                               	output =  output + ';\n {0:s} {1:s}'.format(db[0], out_add.format(str(duration) ) )


                status.sort( reverse=True )
                return str(status[0]) + ' ' + item_name + ' ' + str(perfdata) + ' ' + output
