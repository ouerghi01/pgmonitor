#!/usr/bin/python 

# list of valid actions 
# backends 
#connections
# wals
# autovacuum
# vacuum
# autoanalyze 
# analyze 
# tale_bloat 
# index_bloat 
# table_size
# index_size 
# database_size 
# nonblocking
# blocking
# checkpoints 
# duplicate_indexes
# replica_lag

import factors as fac


def getDefaults (check , warning , critical ) :
	if check == 'backends' : 
		# only whole digits and percentages allowed, an or separator is also allowed 
		return fac.getNumberPercentMix (warning, critical, '80%', '85%' )
	elif check == 'connections' :
                return {'warning':'dummy', 'critical' :'dummy'}
	elif check == 'autovacuum' or check == 'vacuum' or check == 'autoanalyze' or check == 'analyze':
		crit = critical if critical != None else '0min'
		warn = warning if warning != None else '1month'
		if fac.checkDigit(crit.split()) == True and fac.checkDigit(warn.split()) == True :
			return {'warning' : warn , 'critical' : crit}
		else :
			return None		
	elif check == 'table_size' or check == 'index_size' or check == 'database_size' :
		crit = critical if critical != None else '0kb'
		warn = warning
		if warn == None and crit != '0kb' :
			warn = critical
		elif warn == None and crit == '0kb' :
			return None

		if fac.checkDigit(crit.split()) == True and fac.checkDigit(warn.split()) == True :
                        return {'warning' : warn , 'critical' : crit}
                else :
                        return None
	elif check == 'table_bloat' or check == 'index_bloat' :
		crit = critical if critical != None else '0kb'
		warn = warning if warning != None else '1gb'
		if fac.checkDigit(crit.split()) == True and fac.checkDigit(warn.split()) == True :
			return {'warning' : warn , 'critical' : crit}
		else :
                        return None
	elif check == 'nonblocking' or check == 'blocking' :
                crit = critical if critical != None else '0min'
                warn = warning if warning != None else '1min'
                if fac.checkDigit(crit.split()) == True and fac.checkDigit(warn.split()) == True :
                        return {'warning' : warn , 'critical' : crit}
                else :
                        return None  
	elif check == 'replica_lag' :
		return fac.getNumberPercentMix (warning, critical, '5', '10' ) 
	else :
		if warning != None or critical != None :
			warn = warning 
			crit = critical if critical != None else 0			


                        if crit != 0 and warning == None :
                        	warn = warning if warning != None else crit
			

			if check == 'wals' :
				# -- warning and/or critical must be supplied
				if str(warn).isdigit() and str(crit).isdigit()  :
					return {'warning' : warn , 'critical' : crit}
				else :
					return None  
			#elif check == 'checkpoints' :
				# warning and/or critical value must be supplied
			#	return None
		else :
			print ("Warning and/or critical value required for this check")
			return None 
