#!/usr/bin/python

import sql
import checkStatus as st
import factors as fac
import perfdataText as perf

def getQuery ( check,warn_factor,warn_val ) :
	query = ''
	col_name = check.replace('size','name')
	if check == 'table_size' or check == 'index_size':
		query = "SELECT \
			{0:s} , \
			pg_relation_size({0:s})/{1:d}  FROM (\
			SELECT relid::regclass table_name,indexrelid::regclass index_name \
			FROM pg_stat_user_indexes \
			) foo \
			WHERE pg_relation_size({0:s}) / {1:d}  >= {2:d}	".format(col_name,int(warn_factor),int(warn_val))
	elif check == 'database_size' :
		query = "SELECT \
	                      datname, \
			      (pg_database_size(datname) / {0:d} ) as display_size \
                         FROM \
	                      pg_database \
                         WHERE \
	                      datistemplate IS FALSE \
                         AND \
	                      ( pg_database_size(datname) / {0:d} ) >= {1:d}".format(int(warn_factor),int(warn_val))

	return query 

def getRelationSizes( param=None ) :
        item_name = 'POSTGRES_'
        status = []
        perfdata = '-'
        output = 'OK'
	warning = []
	critical = []
        if param != None :
		retval = fac.warningAndOrCriticalProvided (param.get('warning'),param.get('critical')) 
		if retval != None :
			warning = retval.get('warning')
			critical = retval.get('critical')
		else :
			return '2' + ' ' + item_name  + ' ' + '-' + ' ' + 'Invalid parameters passed !'

		item_name = item_name + str(param['check']).upper()
		col_name = 'table_name' 
                query = getQuery ( param['check'],warning[1],warning[0] ) 
		dbnames = param.get('dbname')

                exclude_db = param.get('exclude_db')
                for db in exclude_db :
                        if db in dbnames :
                                dbnames.remove(db)

		for dbname in dbnames : 
                	results = sql.getSQLResult ( {'host': param['host'][0] , 'port' : param['port'][0], 'dbname': dbname, 'user' : param['user'] ,'password' : param['password'] } ,query )
		
			if results[0] == None : 
				return '2' + ' ' + item_name  + ' ' + perfdata + ' ' + str(results[1])

			rows = results[1]

			if len(rows) > 0 :
                		for row in rows :
					status.append( st.getStatus(row[1] , warning[0] , critical[0] )  )
                        		if perfdata == '-' :
                                		perfdata = perf.getPerfStm (row[0],row[1],warning[0],critical[0])
                                		output =  '{0:s} is {1:d} {2:s} big'.format(row[0],row[1],warning[2])
                        		elif perfdata != '-'  :
                                		perfdata = perfdata + '|' + perf.getPerfStm (row[0],row[1],warning[0],critical[0])
                                		output =  output + ';\n {0:s} is {1:d} {2:s} big'.format(row[0],row[1],warning[2])

		if perfdata != '-' :
			status.sort( reverse=True )
			return str(status[0]) + ' ' + item_name + ' ' + str(perfdata) + ' ' + output
		else :
                	return '0' + ' ' + item_name + ' ' + str(perfdata) + ' ' + output


