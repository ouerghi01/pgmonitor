#!/usr/bin/python

import sql 
import factors as fac
import checkStatus as st
import perfdataText as perf


def getQuery (check) :
	if check == 'table_bloat' :
		return "WITH constants AS ( \
    			SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 8 AS ma \
			),\
			no_stats AS ( \
    			SELECT table_schema, table_name, \
        		n_live_tup::numeric as est_rows,\
        		pg_table_size(relid)::numeric as table_size \
    			FROM information_schema.columns \
        		JOIN pg_stat_user_tables as psut \
           		ON table_schema = psut.schemaname \
           		AND table_name = psut.relname \
        		LEFT OUTER JOIN pg_stats \
        		ON table_schema = pg_stats.schemaname \
            		AND table_name = pg_stats.tablename \
            		AND column_name = attname  \
    			WHERE attname IS NULL \
        		AND table_schema NOT IN ('pg_catalog', 'information_schema') \
    			GROUP BY table_schema, table_name, relid, n_live_tup \
		),\
		null_headers AS ( \
    			SELECT \
        		hdr+1+(sum(case when null_frac <> 0 THEN 1 else 0 END)/8) as nullhdr, \
        		SUM((1-null_frac)*avg_width) as datawidth, \
        		MAX(null_frac) as maxfracsum, \
        		schemaname,\
        		tablename,\
        		hdr, ma, bs \
    			FROM pg_stats CROSS JOIN constants \
        		LEFT OUTER JOIN no_stats \
            		ON schemaname = no_stats.table_schema \
            		AND tablename = no_stats.table_name \
    			WHERE schemaname NOT IN ('pg_catalog', 'information_schema') \
        		AND no_stats.table_name IS NULL \
        		AND EXISTS ( SELECT 1 \
            		FROM information_schema.columns \
                	WHERE schemaname = columns.table_schema \
                    	AND tablename = columns.table_name ) \
    			GROUP BY schemaname, tablename, hdr, ma, bs \
		),\
		data_headers AS ( \
    			SELECT \
        		ma, bs, hdr, schemaname, tablename, \
        		(datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,\
        		(maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2 \
    			FROM null_headers \
		),\
		table_estimates AS ( \
    			SELECT schemaname, tablename, bs,\
        		reltuples::numeric as est_rows, relpages * bs as table_bytes,\
    			CEIL((reltuples*\
            		(datahdr + nullhdr2 + 4 + ma - (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END) \
                	)/(bs-20))) * bs AS expected_bytes, \
        		reltoastrelid \
    			FROM data_headers \
        		JOIN pg_class ON tablename = relname \
        		JOIN pg_namespace ON relnamespace = pg_namespace.oid \
            		AND schemaname = nspname \
   			WHERE pg_class.relkind = 'r' \
		),\
		estimates_with_toast AS ( \
    			SELECT schemaname, tablename,  \
        		TRUE as can_estimate, \
        		est_rows, \
        		table_bytes + ( coalesce(toast.relpages, 0) * bs ) as table_bytes, \
        		expected_bytes + ( ceil( coalesce(toast.reltuples, 0) / 4 ) * bs ) as expected_bytes \
    			FROM table_estimates LEFT OUTER JOIN pg_class as toast \
        		ON table_estimates.reltoastrelid = toast.oid \
            		AND toast.relkind = 't' \
		),\
		table_estimates_plus AS ( \
    			SELECT current_database() as databasename,\
            		schemaname, tablename, can_estimate, \
            		est_rows, \
            		CASE WHEN table_bytes > 0 \
                	THEN table_bytes::NUMERIC \
                	ELSE NULL::NUMERIC END \
                	AS table_bytes, \
            		CASE WHEN expected_bytes > 0  \
                	THEN expected_bytes::NUMERIC \
                	ELSE NULL::NUMERIC END \
                    	AS expected_bytes,\
            		CASE WHEN expected_bytes > 0 AND table_bytes > 0 \
                	AND expected_bytes <= table_bytes \
                	THEN (table_bytes - expected_bytes)::NUMERIC \
                	ELSE 0::NUMERIC END AS bloat_bytes \
    			FROM estimates_with_toast \
    			UNION ALL \
    			SELECT current_database() as databasename,  \
        		table_schema, table_name, FALSE, \
        		est_rows, table_size,\
        		NULL::NUMERIC, NULL::NUMERIC\
    			FROM no_stats\
		),\
		bloat_data AS ( \
    			select current_database() as databasename,\
        		schemaname, tablename, can_estimate, \
        		expected_bytes, expected_bytes as expected_mb,\
        		round(bloat_bytes*100/table_bytes) as pct_bloat,\
        		bloat_bytes ,table_bytes, expected_bytes, est_rows\
    			FROM table_estimates_plus\
		)\
		SELECT \
			schemaname || '.' || tablename AS table_name,\
			table_bytes AS table_size_bytes,\
			pct_bloat AS percentage_bloat,\
			round(bloat_bytes/{0:d}) AS bloat_size_bytes\
		FROM \
			bloat_data \
		WHERE \
			bloat_bytes/{0:d} >= {1:d}  \
		ORDER BY  \
			pct_bloat DESC LIMIT 5"


	elif check == 'index_bloat' :
		return "WITH btree_index_atts AS ( \
    			SELECT nspname, \
        		indexclass.relname as index_name, \
        		indexclass.reltuples, \
        		indexclass.relpages, \
        		indrelid, indexrelid,\
        		indexclass.relam,\
       	 		tableclass.relname as tablename,\
        		regexp_split_to_table(indkey::text, ' ')::smallint AS attnum,\
        		indexrelid as index_oid \
    			FROM pg_index \
    			JOIN pg_class AS indexclass ON pg_index.indexrelid = indexclass.oid \
    			JOIN pg_class AS tableclass ON pg_index.indrelid = tableclass.oid \
    			JOIN pg_namespace ON pg_namespace.oid = indexclass.relnamespace \
    			JOIN pg_am ON indexclass.relam = pg_am.oid \
    			WHERE pg_am.amname = 'btree' and indexclass.relpages > 0 \
         		AND nspname NOT IN ('pg_catalog','information_schema') \
    		),\
		index_item_sizes AS ( \
    			SELECT \
    			ind_atts.nspname, ind_atts.index_name,  \
    			ind_atts.reltuples, ind_atts.relpages, ind_atts.relam, \
    			indrelid AS table_oid, index_oid, \
    			current_setting('block_size')::numeric AS bs, \
    			8 AS maxalign, \
    			24 AS pagehdr, \
    			CASE WHEN max(coalesce(pg_stats.null_frac,0)) = 0 \
        		THEN 2 \
        		ELSE 6 \
    			END AS index_tuple_hdr, \
    			sum( (1-coalesce(pg_stats.null_frac, 0)) * coalesce(pg_stats.avg_width, 1024) ) AS nulldatawidth \
    			FROM pg_attribute \
    			JOIN btree_index_atts AS ind_atts ON pg_attribute.attrelid = ind_atts.indexrelid AND pg_attribute.attnum = ind_atts.attnum \
    			JOIN pg_stats ON pg_stats.schemaname = ind_atts.nspname \
          		AND ( (pg_stats.tablename = ind_atts.tablename AND pg_stats.attname = pg_catalog.pg_get_indexdef(pg_attribute.attrelid, pg_attribute.attnum, TRUE)) \
          		OR   (pg_stats.tablename = ind_atts.index_name AND pg_stats.attname = pg_attribute.attname)) \
    			WHERE pg_attribute.attnum > 0 \
    			GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9 \
		), \
		index_aligned_est AS ( \
    			SELECT maxalign, bs, nspname, index_name, reltuples,\
        		relpages, relam, table_oid, index_oid, \
        		coalesce ( \
            		ceil ( \
                	reltuples * ( 6  \
                    	+ maxalign \
                    	- CASE \
                        WHEN index_tuple_hdr%maxalign = 0 THEN maxalign \
                        ELSE index_tuple_hdr%maxalign \
                      	END \
                    	+ nulldatawidth \
                    	+ maxalign \
                    	- CASE \
                        WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign \
                        ELSE nulldatawidth::integer%maxalign \
                      	END \
                	)::numeric \
              		/ ( bs - pagehdr::NUMERIC ) \
              		+1 ) \
         		, 0 ) \
      			as expected \
    			FROM index_item_sizes \
		),\
		raw_bloat AS ( \
    			SELECT current_database() as dbname, nspname, pg_class.relname AS table_name, index_name, \
        		bs*(index_aligned_est.relpages)::bigint AS totalbytes, expected, \
        		CASE \
            		WHEN index_aligned_est.relpages <= expected \
                	THEN 0 \
                	ELSE bs*(index_aligned_est.relpages-expected)::bigint \
            		END AS wastedbytes, \
        		CASE \
            		WHEN index_aligned_est.relpages <= expected \
                	THEN 0 \
                	ELSE bs*(index_aligned_est.relpages-expected)::bigint * 100 / (bs*(index_aligned_est.relpages)::bigint) \
            		END AS realbloat, \
        		pg_relation_size(index_aligned_est.table_oid) AS table_bytes, \
        		stat.idx_scan AS index_scans \
    			FROM index_aligned_est \
    			JOIN pg_class ON pg_class.oid=index_aligned_est.table_oid \
    			JOIN pg_stat_user_indexes AS stat ON index_aligned_est.index_oid = stat.indexrelid \
		), \
		format_bloat AS ( \
			SELECT dbname AS database_name, nspname as schema_name, table_name, index_name, \
        		round(realbloat) as bloat_pct, wastedbytes AS bloat_bytes, \
        		totalbytes AS index_bytes, \
        		table_bytes, \
        		index_scans \
			FROM raw_bloat \
		) \
		SELECT \
			schema_name || '.' || '.' || table_name || '.' || index_name AS table_name, \
			table_bytes  AS table_size_bytes, \
			bloat_pct AS bloat_percentage, \
			round(bloat_bytes/{0:d})  AS bloat_size_bytes, \
			index_bytes  AS index_size_bytes, \
			index_scans \
		FROM format_bloat \
		WHERE bloat_bytes/{0:d} >= {1:d}   \
		ORDER BY bloat_bytes DESC LIMIT 5"


def getBloats( param=None ) :
        item_name = 'POSTGRES_'
        status = []
        perfdata = '-'
        output = ''
        if param != None :
		check = param['check']
		item_name = item_name + check.upper()
		dbnames = param.get('dbname')
		retval = fac.warningAndOrCriticalProvided (param.get('warning'),param.get('critical'))
		warning = []
		critical = []
		if retval != None :
			warning = retval.get('warning')
			critical = retval.get('critical')
		else :
			return '2' + ' ' + item_name + ' ' + '-' + ' ' + 'Invalid parameters supplied'
                query = getQuery(check)
		query = query.format(int(warning[1]),int(warning[0]))

                exclude_db = param.get('exclude_db')
                for db in exclude_db :
                        if db in dbnames :
                                dbnames.remove(db)

		
		for dbname in dbnames :
                	results = sql.getSQLResult ( {'host': param['host'][0] , 'port' : param['port'][0], 'dbname': dbname, 'user' : param['user'] ,'password' : param['password'] } ,query )

			if results[0] == None :
				return '2' + ' ' + item_name + ' ' + perfdata + ' ' + results[1]
			
			rows = results[1]
			if len(rows) > 0 :
                		for row in rows :
                                	out_unit = ''
					status.append(st.getStatus(row[3] , warning[0] , critical[0]) )

                        		if perfdata == '-' :
                                		perfdata = perf.getPerfStm (row[0],row[3],warning[0],str(critical[0]))
                                		output =  '{0:s} has {1:s} {2:s} ({3:s})% worth of bloat'.format(row[0],str(row[3]), warning[2], str(row[2]) )
                        		elif perfdata != '-'  :
                                		perfdata = perfdata + '|' + perf.getPerfStm (row[0],row[3],warning[0],str(critical[0]))
                                		output =  output + ';\n {0:s} has {1:s} {2:s} ({3:s})% worth of bloat'.format(row[0],str(row[3]), warning[2], str(row[2]) )

                	
		if perfdata != '-' :
			status.sort( reverse=True )
			return str(status[0]) + ' ' + item_name + ' ' + str(perfdata) + ' ' + output
		else : 
			return '0' + ' ' + item_name  + ' ' + '-' + ' ' + 'OK'
