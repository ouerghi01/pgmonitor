#!/usr/bin/python

import sql 
import checkStatus as st
import factors as fac
import perfdataText as perf


def getWALs( param=None ) :
        item_name = 'POSTGRES_WALS'
        status = []
        perfdata = '-'
        output = ''
        if param != None :
		dbname = param.get('dbname')[0]
                query = "SELECT count(bar.*) wal_count , \
				ceil(( ( 2 + current_setting('checkpoint_completion_target')::double precision)* current_setting('checkpoint_segments')::double precision) + 1) max_wal \
			FROM \
			( \
				SELECT  (pg_stat_file('pg_xlog/' || foo.files )).* FROM \
					( SELECT pg_ls_dir('pg_xlog') files  ) foo \
				WHERE  (pg_stat_file('pg_xlog/' || foo.files )).isdir IS FALSE \
			) bar \
			WHERE bar.size >= 16777216" 

                results = sql.getSQLResult ( {'host': param['host'][0] , 'port' : param['port'][0], 'dbname': dbname, 'user' : param['user'] ,'password' : param['password'] } ,query )

		if results[0] == None : 
			return '2' + ' ' + item_name + ' ' + '-' + ' ' + str(results[1])


		rows = results[1]
		if len(rows) > 0 :
			warning = fac.getNumberPercentLimits( param.get('warning'), rows[0][1])
			critical = fac.getNumberPercentLimits( param.get('critical'), rows[0][1])
                	for row in rows :
                        	if perfdata == '-' :
					perfdata = perf.getPerfStm ('WALS',row[0],warning,str(critical))
                                	output =  '{0:s} WAL file(s) found'.format( str(row[0]) )
	                	status.append( st.getStatus( row[0],int(warning) , int(critical) ) )

                	status.sort( reverse=True )
                	return str(status[0]) + ' ' + item_name + ' ' + str(perfdata) + ' ' + output
		else :
			return '2' + ' ' + item_name + ' ' + '-' + ' ' + 'No WAL file found, at least One WAL file should be present.'

