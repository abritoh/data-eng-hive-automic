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

CREATE TABLE IF NOT EXISTS ${target_schema}.${target_table_squad}
(    
	BANNER_CD					CHAR(2)
    , BANNER_DESC				VARCHAR(20)
    , TRIBE_NBR					INT
    , TRIBE_DESC				VARCHAR(50)
    , SQUAD_NBR					INT
    , SQUAD_DESC				VARCHAR(50)

	, CUST_LY_TOT_UNIT_QTY		INT
	, CUST_CY_TOT_UNIT_QTY		INT
	, CUST_LY_EQ_UNIT_QTY		INT
	, CUST_CY_EQ_UNIT_QTY		INT
	, TKT_SALE_LY_TOT_UNIT_AMT	DECIMAL(19,2)
	, TKT_SALE_CY_TOT_UNIT_AMT	DECIMAL(19,2)
	, TKT_SALE_LY_EQ_UNIT_AMT	DECIMAL(19,2)
	, TKT_SALE_CY_EQ_UNIT_AMT	DECIMAL(19,2)
	, TKT_LY_TOT_UNIT_QTY		INT
	, TKT_CY_TOT_UNIT_QTY		INT
	, TKT_LY_EQ_UNIT_QTY		INT
	, TKT_CY_EQ_UNIT_QTY		INT

    , VISIT_LY_DT				DATE
)
PARTITIONED BY ( VISIT_CY_DT DATE )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u0001'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'auto.purge'='true')
;

INSERT OVERWRITE TABLE ${target_schema}.${target_table_squad}
PARTITION (VISIT_CY_DT)
SELECT 
	 BANNER_CD
	, BANNER_DESC
    , TRIBE_NBR
    , TRIBE_DESC
    , SQUAD_NBR
    , SQUAD_DESC

	, SUM(CUST_LY_TOT_UNIT_QTY) AS CUST_LY_TOT_UNIT_QTY
	, SUM(CUST_CY_TOT_UNIT_QTY) AS CUST_CY_TOT_UNIT_QTY
	, SUM(CUST_LY_EQ_UNIT_QTY) AS CUST_LY_EQ_UNIT_QTY
	, SUM(CUST_CY_EQ_UNIT_QTY) AS CUST_CY_EQ_UNIT_QTY
	, SUM(TKT_SALE_LY_TOT_UNIT_AMT) AS TKT_SALE_LY_TOT_UNIT_AMT
	, SUM(TKT_SALE_CY_TOT_UNIT_AMT) AS TKT_SALE_CY_TOT_UNIT_AMT
	, SUM(TKT_SALE_LY_EQ_UNIT_AMT) AS TKT_SALE_LY_EQ_UNIT_AMT
	, SUM(TKT_SALE_CY_EQ_UNIT_AMT) AS TKT_SALE_CY_EQ_UNIT_AMT
	, SUM(TKT_LY_TOT_UNIT_QTY) AS TKT_LY_TOT_UNIT_QTY
	, SUM(TKT_CY_TOT_UNIT_QTY) AS TKT_CY_TOT_UNIT_QTY
	, SUM(TKT_LY_EQ_UNIT_QTY) AS TKT_LY_EQ_UNIT_QTY
	, SUM(TKT_CY_EQ_UNIT_QTY) AS TKT_CY_EQ_UNIT_QTY

    , VISIT_LY_DT
	, VISIT_CY_DT
FROM ${target_schema}.${target_table_catg}
WHERE VISIT_CY_DT BETWEEN ${beg_date} AND ${end_date}
GROUP BY 
	BANNER_CD
	, BANNER_DESC
    , TRIBE_NBR
    , TRIBE_DESC
    , SQUAD_NBR
    , SQUAD_DESC
    , VISIT_LY_DT
	, VISIT_CY_DT
;

dfs -chgrp -R ${group_name} /user/hive/warehouse/${target_schema}.db/${target_table_squad}; 
dfs -chmod -R ${permission} /user/hive/warehouse/${target_schema}.db/${target_table_squad};

DROP TABLE IF EXISTS ${stg_schema}.${stg_table_tot_ly};
DROP TABLE IF EXISTS ${stg_schema}.${stg_table_tot_cy};
