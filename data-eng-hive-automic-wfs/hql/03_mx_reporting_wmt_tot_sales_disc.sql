#==========================================================================================================================#
# FILE.......: ............................................................................................................#
# AUTHOR.....: ArBR 2021 [arcbrth@gmail.com................................................................................#
# DESCRIPTION: For WMT-MX to be executed manually or via Automic Workflow .................................................#
# PARAMETERS.: ............................................................................................................#
#...$hadoop_queue                  &HADOOP_QUEUE#                    intlprcincld..........................................#
#...$region_code                   &REGION_CODE#                     'MX'..................................................#
#==========================================================================================================================#

SET hive.execution.engine=tez;
SET tez.queue.name=${hadoop_queue};
SET hive.tez.container.size=5120;
SET hive.tez.java.opts=-Xmx4096m;
SET tez.runtime.unordered.output.buffer.size-mb=512;
SET hive.auto.convert.join.noconditionaltask.size=128435456;
SET hive.exec.reducers.bytes.per.reducer=67108864;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=4000;
SET tez.task.timeout-ms=900000;
SET tez.grouping.min-size=32000000;
SET tez.grouping.max-size=128000000;
SET tez.grouping.split-waves=10.0;
SET mapreduce.input.fileinputformat.split.maxsize=67108864;
SET hive.exec.parallel=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.vectorized.execution.reduce.groupby.enabled=true;
SET hive.merge.tezfiles=true;
SET hive.merge.smallfiles.avgsize=128000000;
SET hive.merge.size.per.task=128000000;

CREATE TABLE IF NOT EXISTS ${target_schema}.${target_table_sales_disc}
(    
	  STORE_NBR         INT
	, BANNER_CD         CHAR(2)
    , BANNER_DESC       VARCHAR(20)    
    , SALE_AMT          DECIMAL(12,2) 
	, DISC_AMT          DECIMAL(12,2) 
	, SALE_NET_AMT      DECIMAL(12,2) 
)
PARTITIONED BY ( VISIT_DT DATE )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u0001'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'auto.purge'='true')
;

INSERT OVERWRITE TABLE ${target_schema}.${target_table_sales_disc}
PARTITION (VISIT_DT)
SELECT 
  SALES.STORE_NBR
, SALES.BANNER_CD
, SALES.BANNER_DESC
, SALES.SALE_AMT AS SALE_AMT
, COALESCE(DISC.DISC_AMT, 0) AS DISC_AMT
, SALES.SALE_AMT + COALESCE(DISC.DISC_AMT, 0) AS SALE_NET_AMT
, SALES.VISIT_DT
FROM
	(
	SELECT 
		SCAN.STORE_NBR,
		SCAN.BANNER_CD,
		SCAN.BANNER_DESC,
		SCAN.VISIT_DT,
		SUM(SCAN.SCAN_RTL_AMT) SALE_AMT
	FROM ${src_schema_mx_spot_run}.${src_table_visit_scan_wmt} SCAN
	WHERE 
	SCAN.GEO_REGION_CD = ${region_code}
	AND SCAN.OP_CMPNY_CD = ${company_code}
	AND SCAN.DEPT_NBR NOT IN (49,50,72,75,99)
	AND SCAN.VISIT_DT BETWEEN ${beg_date} AND ${end_date}
	GROUP BY
		SCAN.STORE_NBR,
		SCAN.BANNER_CD,
		SCAN.BANNER_DESC,
		SCAN.VISIT_DT
	) SALES
	LEFT JOIN
	(
	SELECT 
		D.DSC_DT DISC_DT
		, D.STORE_NBR
		,-1 * SUM(COALESCE(DSC_AMT, 0)) DISC_AMT
	FROM ${src_schema_mx_spot_run}.${src_discount_sales} D
	WHERE D.DSC_CD IS NOT NULL
		AND D.GEO_REGION_CD = ${region_code}
		AND D.OP_CMPNY_CD = ${company_code}
		AND D.DSC_DT BETWEEN ${beg_date} AND ${end_date}
	GROUP BY 
		D.DSC_DT,
		D.STORE_NBR
	) DISC
ON SALES.STORE_NBR = DISC.STORE_NBR 
AND SALES.VISIT_DT = DISC.DISC_DT;  

dfs -chgrp -R ${group_name} /user/hive/warehouse/${target_schema}.db/${target_table_sales_disc}; 
dfs -chmod -R ${permission} /user/hive/warehouse/${target_schema}.db/${target_table_sales_disc};
