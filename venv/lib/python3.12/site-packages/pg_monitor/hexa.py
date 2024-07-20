#!/usr/bin/python

import math

def getDecimalEquivalent(hexadigit) :
	hexaDict = {'0' : 0, '1' : 1, '2' : 2 , '3' : 3, '4' : 4, '5' : 5 , '6' : 6 , '7' : 7, '8' : 8 , '9' : 9, 'A' : 10, \
		   'B' : 11, 'C' : 12, 'D' : 13, 'E' : 14, 'F' : 15}

	return hexaDict.get( str(hexadigit) )


def convertHex2Dec (hexa) :
# example - C921
	dec = 0
	counter = 1
	for hexadigit in str(hexa).upper() :
		dec = dec + getDecimalEquivalent(hexadigit) * 16 ** ( len(hexa) - counter )
		counter += 1

	return dec

def computeMegaByteDiff (master , replica ) :
	multiplier = convertHex2Dec('FF000000')
	byte = ( multiplier * convertHex2Dec( str(master[0][0]).split('/')[0] )  + convertHex2Dec( str(master[0][0]).split('/')[1] ) )  - \
		( multiplier * convertHex2Dec( str(replica[0][0]).split('/')[0] )  + convertHex2Dec( str(replica[0][0]).split('/')[1] ) ) 
	return int( math.ceil(byte / (1024 ** 2)) )

#if __name__ == '__main__' :
#	print (convertHex2Dec ('D4445B8'))
