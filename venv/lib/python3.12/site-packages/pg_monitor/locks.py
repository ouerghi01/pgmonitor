#!/usr/bin/python

import sql
import factors as fac
import checkStatus as st
import perfdataText as perf

def getVersionParam (version) :
        query = ''
        pid = ''
        if version >= 9.2 :
                query = 'query'
                pid = 'pid'
        elif version <= 9.1 :
                query = 'current_query'
                pid = 'procpid'

	return [query, pid]

def getNonBlockingVersionQuery (version,w1,w2) :
	query , pid = getVersionParam (version) 
	return "SELECT \
			l.relation::regclass::text as table_name ,\
			l.locktype,\
			pa.{0:s},\
			pa.{1:s}, \
			pa.usename ,\
			pa.client_addr,\
                        date_part('epoch',clock_timestamp()::timestamp - pa.query_start::timestamp)/60 running_time, \
			l.mode \
                FROM \
			 pg_catalog.pg_locks l \
                join pg_catalog.pg_stat_user_tables ut on ut.schemaname || '.' || ut.relname = l.relation::regclass::text \
                join pg_catalog.pg_database db on db.oid = l.database \
                join pg_catalog.pg_stat_activity pa on  l.pid = pa.{0:s} \
                where l.granted and lower(l.mode) = any ( \
                         values(lower('ExclusiveLock')),(lower('AccessExclusiveLock')), (lower('ShareLock')) , (lower('RowExclusiveLock')) , \
                         (lower('RowShareLock'))  ) \
                         And date_part('epoch',clock_timestamp()::timestamp - pa.query_start::timestamp)/60 >= ( {2:d} * {3:d} ) \
			LIMIT 5 ".format(pid,query, int(w1), int(w2) ) 

def getBlockingVersionQuery (version) :
	query , pid = getVersionParam (version)
	return "SELECT \
			a.{0:s}	 	AS blocked_statement, \
			ka.{0:s}	AS blocking_statement,\
                        kl.locktype     AS blocking_type, \
                        bl.locktype     AS blocked_type, \
                        ka.usename      AS blocking_user, \
                        kl.pid		AS blocking_pid, \
                        date_part('epoch',clock_timestamp()::timestamp - ka.query_start::timestamp)/60  blocking_time, \
                        bl.pid          AS blocked_pid, \
                        a.usename       AS blocked_user, \
                        date_part('epoch',clock_timestamp()::timestamp - a.query_start::timestamp)/60  waiting_time \
  		FROM  \
			pg_catalog.pg_locks         bl \
   		JOIN pg_catalog.pg_stat_activity a  ON a.{1:s} = bl.pid \
   		JOIN pg_catalog.pg_locks         kl ON kl.transactionid = bl.transactionid AND kl.pid != bl.pid \
   		JOIN pg_catalog.pg_stat_activity ka ON ka.{1:s} = kl.pid \
  		WHERE NOT bl.GRANTED LIMIT 5".format( query , pid)

def getBlockingIterator(rows,item_name,findText, status,warning,critical) :
        perfdata = '-'
        output = ''
        for row in rows :
                if perfdata == '-' :
                        perfdata = perf.getPerfStm ('Blocking_query',row[6],warning[0],str(critical[0]))
                        output =  '{0:s}(pid->{1:s}) has been blocked({2:s}) by {3:s}(pid->{4:s}) for {5:s} mins \n  Blocked Query : {6:s} \
                                   \n  Blocking Query : {7:s} \n Waiting time : {8:s}'.format( str(row[8]) , str(row[7]) , str(row[2]) , str(row[4]), \
                                    str(row[5]), str(row[6]), str(row[0]), str(row[1]), str(row[9])  )
                elif perfdata != '-'  :
                        perfdata = perfdata + '|' + perf.getPerfStm ('Blocking_query',row[6],1,'0')
			output =  output + '; \n {0:s}(pid->{1:s}) has been blocked({2:s}) by {3:s}(pid->{4:s}) for {5:s} mins \n  Blocked Query : {6:s} \
                                   \n  Blocking Query : {7:s} \n Waiting time : {8:s}'.format( str(row[8]) , str(row[7]) , str(row[2]) , str(row[4]), \
                                    str(row[5]), str(row[6]), str(row[0]), str(row[1]), str(row[9])  )


	return {'status' : '2', 'item' : item_name , 'perfdata' : perfdata, 'output' : output }


def getNonBlockingIterator(rows,item_name,warning,critical, status) :
	perfdata = '-'
	output = ''
	for row in rows :
		if perfdata == '-' :
                	perfdata = perf.getPerfStm (row[0],row[6],warning[0],str(critical[0]))
                        output =  '{0:s} has been locked({1:s}) by {2:s}(pid->{3:s},lock_mode->{6:s}) for {4:s} mins \n Locking Query : {5:s}'.format( \
				  str(row[0]),str(row[1]),str(row[4]),str(row[2]),str(row[6]), str(row[3]), str(row[7])  )
                elif perfdata != '-'  :
                	perfdata = perfdata + '|' + perf.getPerfStm (row[0],row[6],warning[0],str(critical[0]))
                        output =  output + ';\n {0:s} has been locked({1:s}) by {2:s}(pid->{3:s},lock_mode->{6:s}) for {4:s} mins \n Locking Query : {5:s}'.format( \
                                  str(row[0]),str(row[1]),str(row[4]),str(row[2]),str(row[6]), str(row[3]),str(row[7])  )
		status.append( st.getStatus( row[6],int(warning[0]) , int(critical[0])  ) )
	status.sort( reverse=True )
	return {'status' : status[0], 'item' : item_name , 'perfdata' : perfdata, 'output' : output }


def getLocks( param=None ) :

        item_name = 'POSTGRES_'
        status = []
        perfdata = '-'
        output = ''
        if param != None :
		check = (param['check']).lower()
		findText = param.get('find')
		item_name = item_name + check.upper() + '_LOCKS'
		dbnames = param.get('dbname')
		user = param['user']
		password = param.get('password')
		host = param['host'][0]
		port = param['port'][0]
		warning = []
		critical = []

		query = "SELECT substring(version() FROM '(\d.\d)')::double precision"
                results = sql.getSQLResult ( {'host': host , 'port' : port, 'dbname': dbnames[0], 'user' : param['user'] ,'password' : param['password'] } ,query )
		
		if results[0] == None :
                        return str('2') + ' ' + str(item_name) + ' ' + '-' + ' ' + str(results[1])
		

		version = (results[1])[0][0]
		retvals = fac.getTimeDefaults ( param.get('warning'), param.get('critical') ) 
		if retvals !=None :
			warning = retvals.get('warning')
			critical = retvals.get('critical')
		else :
			return '2' + ' ' + item_name + ' ' + '-' + ' ' + 'Invalid parameter passed !'
		
		results = []
		exclude_db = param.get('exclude_db')
		for db in exclude_db :
			if db in dbnames :
				dbnames.remove(db)

		for dbname in dbnames :
			if check == 'nonblocking' :
				results = sql.getSQLResult ( {'host': host , 'port' : port , 'dbname': dbname,\
					 'user' : user ,'password' : password } ,getNonBlockingVersionQuery(version,warning[0], warning[1])  )
			elif check == 'blocking' :
				results  = sql.getSQLResult ( {'host': host , 'port' : port , 'dbname': dbname,\
                                	 'user' : user ,'password' : password } ,getBlockingVersionQuery(version)  )

			if results[0] == None :
				return str('2') + ' ' + str(item_name) + ' ' + str(perfdata) + ' ' + str(results[1])

			retval = {}
			if len(results[1]) > 0 and check == 'nonblocking' : 	
				retval = getNonBlockingIterator(results[1],'POSTGRES_NONBLOCKING_LOCKS',warning,critical, status)
			elif len(results[1]) > 0 and check == 'blocking' :
				retval = getBlockingIterator(results[1],'POSTGRES_BLOCKING_LOCKS',findText, status,warning,critical)

			status.append(retval.get('status'))
			if perfdata != '-' and len(results[1]) > 0 :
				perfdata = perfdata + retval.get('perfdata')
				output = retval.get('output')
			elif perfdata == '-' and len(results[1]) > 0  :
				perfdata = retval.get('perfdata')
				output = output + ';\n' + retval.get('output')	

		if perfdata != '-' :
			status.sort(reverse=True)
			return str(status[0]) + ' ' + str(item_name) + ' ' +  str(perfdata)  + ' ' + str(output)
		else : 
			return '0' + ' ' + item_name + ' ' + '-' + ' ' + 'OK'	

