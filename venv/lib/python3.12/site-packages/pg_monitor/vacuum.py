#!/usr/bin/python

import sql 
import factors as fac
import perfdataText as perf
import checkStatus as st


def getReturnIndex(check) :
	if check.lower() == 'vacuum' or check.lower() == 'autovacuum' :
		return [1,"pg_stat_user_tables.last_vacuum","pg_stat_user_tables.last_autovacuum"]
	elif check.lower() == 'analyze' or check.lower() == 'autoanalyze':
		return [1,"pg_stat_user_tables.last_analyze", "pg_stat_user_tables.last_autoanalyze" ] 


def getVacuums( param=None ) :
        item_name = 'POSTGRES_'
        status = []
        perfdata = '-'
        output = ''
	warning = []
	critical = []
        if param != None :
	
		dbnames = param.get('dbname')
		check = param['check']
		item_name = item_name + check.upper()
		index =  getReturnIndex(check)
		retval = fac.getTimeDefaults ( param.get('warning'), param.get('critical') )
		if retval != None :
			warning = retval.get('warning') 
			critical = retval.get('critical') 
		else  :
			return '2' + ' ' + item_name  + ' ' + '-' + ' ' + 'Invalid Parameters supplied !'	 

		query = "SELECT table_name,vacuum_time FROM ( \
                                SELECT (schemaname || '.' || relname) table_name,\
                                CASE WHEN {0:s} IS NULL  AND {1:s} IS NULL THEN  \
                                        date_part('epoch',clock_timestamp() - '1970-01-01 00:00:00'::timestamp ) /60  \
                                WHEN {0:s} IS NOT NULL  AND {1:s} IS NULL THEN \
                                        date_part('epoch',clock_timestamp() - {0:s} ) /60 \
                                WHEN {0:s} IS NULL  AND {1:s} IS NOT NULL THEN \
                                       date_part('epoch',clock_timestamp() - {1:s} ) /60 \
                               WHEN {0:s} IS NOT NULL  AND {1:s} IS NOT NULL THEN \
                                       GREATEST(date_part('epoch',clock_timestamp() - {0:s} ) /60, \
                                               date_part('epoch',clock_timestamp() - {1:s} ) /60 ) \
                                END AS vacuum_time \
                         FROM \
                               pg_stat_user_tables \
                                ) foo \
			 WHERE \
				( vacuum_time )  >=  ( {2:d} * {3:d} ) \
				ORDER BY vacuum_time DESC LIMIT 5".format( index[1],index[2] , int(warning[0]) , int(warning[1]) )

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
					status.append( st.getStatus( int(row[index[0]]) , int(warning[0]) * int(warning[1]) , int(critical[0]) * int(critical[1]) )  ) 
					out_unit = str ( int(row[1]/(60*24)) ) + ' days ago ' 
                        		if perfdata == '-' :
                                		perfdata = perf.getPerfStm (row[0],str(row[index[0]]),str(warning[0]),str(critical[0]))
                                		output =  '{0:s} last {1:s} time was {2:s} '.format(row[0],check,out_unit)
                        		elif perfdata != '-'  :
                                		perfdata = perfdata + '|' + perf.getPerfStm (row[0],str(row[index[0]]),str(warning[0]),str(critical[0])) 
                                		output =  output + ';\n {0:s} last {1:s} time was {2:s}'.format(row[0],check,out_unit)
	        		
				status.sort( reverse=True )


		if perfdata != '-' :
                	return str(status[0]) + ' ' + item_name + ' ' + str(perfdata) + ' ' + output 
		else :
			return str('0') + ' ' + item_name + ' ' + '-'  + ' ' + 'OK'
