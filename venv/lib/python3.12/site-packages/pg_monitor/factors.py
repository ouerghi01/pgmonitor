#!/usr/bin/python

import math
import re

def getTimeFactor ( check_val ) :
	check_val = str(check_val)
        factor = 0
        val = re.findall(r'\d+', check_val.lower() )[0]
        unit = ''
        if check_val.lower().find('min') != -1  or check_val.lower().find('minute') != -1:
                factor = 1
                unit = 'min'
        elif check_val.lower().find('hr') != -1 or check_val.lower().find('hour') != -1 :
                factor = 60
                unit = 'hr'
        elif check_val.lower().find('day') != -1 or check_val.lower().find('dy') != -1 :
                factor = 24*60
                unit = 'day'
        elif check_val.lower().find('wk') != -1 or check_val.lower().find('week') != -1 :
                factor = 60 * 24 * 7
                unit = 'week'
	elif check_val.lower().find('month') != -1 or check_val.lower().find('mon') != -1 :
                factor = 60 * 24 * 7 * 4
                unit = 'month'	
        elif check_val.lower().find('yr')  != -1 or check_val.lower().find('year')  != -1 :
                factor = 60 * 24 * 7 * 52
                unit = 'year'

	if str(val).isdigit() :
        	return [val , factor, unit]
	else :
		return None


def getSizeFactor ( check_val ) :
	check_val = str(check_val)
        factor = 0
        val = re.findall(r'\d+', check_val.lower() )[0]
        unit = ''
        if check_val.lower().find('kb') != -1 :
                factor = 1024
                unit = 'KB'
        elif check_val.lower().find('mb') != -1 :
                factor = 1024 * 1024
                unit = 'MB'
        elif check_val.lower().find('gb') != -1  :
                factor = 1024 * 1024 * 1024
                unit = 'GB'
        elif check_val.lower().find('tb') != -1 :
                factor = 1024 * 1024 * 1024 * 1024
                unit = 'TB'
        elif check_val.lower().find('pb') != -1  :
                factor = 1024 * 1024 * 1024 * 1024 * 1024
                unit = 'PB'

	if str(val).isdigit() :
                return [val , factor, unit]
        else :
                return None


def getNumberPercentLimits( limit, total ) :
        if limit != None :
                pLimit = str(limit).split('or')
                nperc = []
		
                for ele in pLimit :
			val = re.findall(r'\d+',ele)[0]
			val = math.ceil( ( float ( val ) / 100 ) * total )  if ele.find('%') != -1 else int ( val ) 
			nperc.append (val) 
                return int (max( nperc))



def checkDigit (check_val) :
	retval = []
	for e in check_val :
		val = re.findall(r'\d+', e )[0] if len(re.findall(r'\d+', e ) ) == 0 else 'text'			
        	retval.append( val )
        return False not in  [True for element in retval if element.isdigit()]

	

def getNumberPercentMix (warning=None, critical=None, defaultWarn=None, defaultCritical=None ) :
        retval = {}
        warn = warning   if warning != None else defaultWarn
        crit = critical  if critical != None else defaultCritical
        retval.update ({'warning' : warn})
        retval.update ({'critical' : crit})

        if checkDigit(str(warn).split('or'))  and  checkDigit(str(crit).split('or'))  : 
                return retval
        else :
                return None


def warningAndOrCriticalProvided (warning,critical) :
        # as an heuristic 
        # warning = 0.8 * critical
	if warning != None and critical != None :
		warning = getSizeFactor ( warning )
		critical = getSizeFactor ( critical )
		retval = {}
		if ( int(critical[0]) == 0 ) or  ( ( int(critical[0]) != 0 )  and ( int(critical[0]) > int(warning[0])  ) ) :
			retval.update( {'warning' : warning }  )
			retval.update( {'critical' : critical }  )
		else :
			retval = None

		return retval
	else :
		return None




def getTimeDefaults ( warning, critical , defaultWarning=None , defaultCritical=None ) :
                warn = warning if warning != None else defaultWarning
                crit = critical if critical != None else defaultCritical
                warn = getTimeFactor( warn )
                crit = getTimeFactor( crit )
                retval = {}
                if warn != None and crit != None :
			# check for the larger of the 2
			if ( int(crit[0]) == 0 ) or ( int(crit[0]) != 0 )  and ( int(crit[0]) > int(warn[0])  ) :
                        	retval.update( {'warning' : warn }  )
                        	retval.update( {'critical' : crit }  )
			else :
				retval = None 
                        return retval
                else :
                        return None

