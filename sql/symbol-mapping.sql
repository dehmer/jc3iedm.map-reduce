SELECT count(*) FROM mig.mip_type; -- 126,486 rows

SELECT DISTINCT mir_type, mir_sub_type FROM mig.mip_type ORDER BY 1, 2;
SELECT * FROM mig.mip_type WHERE mir_sub_type IS NULL;


DROP VIEW mig.sym_2525c_map;
CREATE TABLE mig.sym_2525c_map AS
SELECT DISTINCT mir_type || ':' || mir_sub_type || ':' || descr_txt AS KEY,
       symbol_2525c_id AS VALUE
FROM   mig.mip_type
JOIN   mig.symbol_map USING (mir_id)
JOIN   mig.symbol USING (symbol_id)
WHERE  mir_sub_type IS NOT NULL
AND    symbol_2525c_id IS NOT NULL
-- EXCLUDE METOC:
AND    symbol_2525c_id NOT LIKE 'W%';

SELECT json_object_agg(key, value) FROM mig.sym_2525c_map;
SELECT DISTINCT key FROM mig.sym_2525c_map; -- 125,267

-- AMBIGUOUS MAPPINGS:
SELECT key, count(*) FROM mig.sym_2525c_map GROUP BY 1 HAVING count(*) > 1 ORDER BY 2 DESC;

-- 633 rows
SELECT * FROM mig.sym_2525c_map JOIN (
  SELECT key FROM mig.sym_2525c_map GROUP BY 1 HAVING count(*) > 1
) _ USING (key) ORDER BY key;

-- FACILITY:AIRFIELD-TYPE:A -> WAR.GRDTRK.INS.MILBF.AB (POINT)
-- FACILITY:AIRFIELD-TYPE:A -> TACGRP.C2GM.GNL.ARS.AIRFZ (POLYGON)
-- FACILITY:MILITARY-OBSTACLE-TYPE:ATDTCH -> TACGRP.MOBSU.OBST.ATO.ATD.ATDUC (POLYLINE)
-- FACILITY:MILITARY-OBSTACLE-TYPE:ATDTCH -> TACGRP.MOBSU.OBST.ATO.ATD.ATDC (POLYLINE)
-- FACILITY:MILITARY-OBSTACLE-TYPE:NOS -> TACGRP.MOBSU.OBST.GNL.LNE (POLYLINE)
-- FACILITY:MILITARY-OBSTACLE-TYPE:NOS -> TACGRP.MOBSU.OBST.GNL.Z (POLYGON)
-- FEATURE:CONTROL-FEATURE-TYPE:AXIS -> TACGRP.C2GM.OFF.LNE.AXSADV.GRD.MANATK (CORRIDOR)
-- FEATURE:CONTROL-FEATURE-TYPE:AXIS -> TACGRP.C2GM.OFF.LNE.AXSADV.GRD.SUPATK (CORRIDOR)

SELECT * FROM mig.sym_2525c_map
WHERE  key = 'FEATURE:CONTROL-FEATURE-TYPE:AXIS'
OR     key = 'FEATURE:CONTROL-FEATURE-TYPE:MAXIS';

-- mir_id = 1168
SELECT *
FROM   mig.mip_type
JOIN   mig.symbol_map USING (mir_id)
JOIN   mig.symbol USING (symbol_id)
WHERE  mir_sub_type = 'CONTROL-FEATURE-TYPE'
AND    descr_txt = 'AXIS';

-- symbol_map is ambiguous.
-- some can be disambiguated by geometry.
-- 316 rows
SELECT mir_id
FROM   mig.symbol_map
JOIN   mig.symbol USING (symbol_id)
WHERE  symbol_2525c_id IS NOT NULL
GROUP  BY 1
HAVING count(*) > 1;

SELECT *
FROM   mig.symbol_map
JOIN   mig.symbol USING (symbol_id)
WHERE  mir_id = 279443;

-- 28 rows
SELECT symbol_2525c_id, count(*)
FROM   mig.symbol
--JOIN   mig.symbol_map USING (symbol_id)
WHERE  symbol_2525c_id IS NOT NULL
GROUP  BY 1
HAVING count(*) > 1;


SELECT * FROM mig.symbol WHERE symbol_2525c_id = 'G*MPOADU--****X';
SELECT * FROM mig.symbol WHERE symbol_2525c_id = 'G*MPOADC--****X';
SELECT * FROM mig.symbol WHERE symbol_2525c_id = 'G*M*OMP---****X';


