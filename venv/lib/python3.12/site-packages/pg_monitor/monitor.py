#!/usr/bin/python

import backends as bac
import bloats as blt
import connection as con
import duplicateIndexes as dup
import locks as loc
import relationSizes as rel
import vacuum as vac
import wals
import replicaLag as rep

def getChecks (param) :
	check = param['check']
        if check == 'backends' :
                return bac.getBackends (param)
        elif check == 'wals' :
                return wals.getWALs(param)
        elif check == 'autovacuum' or check == 'vacuum' or check == 'autoanalyze' or check == 'analyze':
		return vac.getVacuums(param)
        elif check == 'table_bloat' or check == 'index_bloat' :
		return blt.getBloats(param)
        elif check == 'table_size' or check == 'index_size' or check == 'database_size' :
		return rel.getRelationSizes(param)
        elif check == 'nonblocking' or check == 'blocking' :
		return loc.getLocks(param)
        elif check == 'checkpoints' :
		return None
        elif check == 'replica_lag' :
		return rep.getReplicaLags(param) 
	elif check == 'connections' :
		return con.getConnections(param)
