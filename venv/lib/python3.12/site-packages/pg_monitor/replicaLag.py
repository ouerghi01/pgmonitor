#!/usr/bin/python

import sys
import checkStatus as st
import sql
import factors as fac
import hexa
import perfdataText as perf

# var=val;warn;crit;min;max
# check_mk output format 
# status 	item_name	perfdata	output


def getReplicaLags( param=None ) :
	item_name = 'POSTGRES_REPLICA_LAG'
	status = []
	perfdata = '-'
	output = ''
	if param != None :
		
		warning = int( param['warning'] )
		critical = int( param['critical'] )

                query = "SELECT pg_current_xlog_location() "
		
		dbname = param.get('dbname')[0]
		
		hosts = param['host']
		ports = param['port']	
		if len(hosts) > len(ports) :
			ports = ports * len(hosts)

		master = sql.getSQLResult ( {'host': hosts[0] , 'port' : ports[0], 'dbname': dbname, 'user' : param['user'] ,'password' : param['password'] } ,query ) 
		
		
		if master[0] == None :
			return '2' + ' ' + item_name  + ' ' + '-' + ' ' + str(master[1])

		counter = 1
		for rep_host in hosts[1:] :
			query = "SELECT pg_last_xlog_replay_location()"
			replica = sql.getSQLResult ( {'host': rep_host , 'port' : ports[counter], 'dbname': dbname \
					, 'user' : param['user'] ,'password' : param['password'] } ,query )
			if replica[0] == 0 :
				wal_lag = hexa.computeMegaByteDiff ( master[1] , replica[1] ) / 16
			elif replica[0] == None :
				wal_lag = -1 
			if perfdata == '-' :
				perfdata = perf.getPerfStm (rep_host , wal_lag ,warning,str(critical)) 
				output =  'replica on {0:s} lags behind master {1:s} by a total of {2:s} WALs.'\
					  .format( rep_host , hosts[0] , str(wal_lag) )
			elif perfdata != '-'  :
				perfdata = perfdata + '|' + perf.getPerfStm (rep_host , wal_lag ,warning,str(critical))
				output =  output + ';\n replica on {0:s} lags behind master {1:s} by a total of {2:s} WALs'\
						.format( rep_host , hosts[0] , str(wal_lag) )
			if wal_lag != -1 :
				status.append( st.getStatus( wal_lag , warning , critical ) )
			elif wal_lag == -1 :
				status.append(2)
			counter += 1

		status.sort(reverse=True)
		output = output + '\n NOTE: a -1 lag value means the replica is not available !!!'
		return str(status[0]) + ' ' + item_name + ' ' + str(perfdata) + ' ' + output
