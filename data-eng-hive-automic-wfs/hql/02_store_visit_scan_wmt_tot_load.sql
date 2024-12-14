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

CREATE TABLE IF NOT EXISTS ${target_schema}.${target_table_scan_tot} 
(
     YEAR_NBR				 SMALLINT
    ,MONTH_NBR				 SMALLINT
    ,DAY_NBR				 SMALLINT
    ,ITEM_NBR				 INT
    ,ITEM_DESC				 VARCHAR(50)
    ,SIZE_DESC				 VARCHAR(50)
    ,COLOR_DESC				 VARCHAR(50)
    ,BRAND_ID				 INT
    ,BRAND_NM				 VARCHAR(50)
    ,UPC_NBR				 INT
    ,CAT_NBR				 SMALLINT
    ,CAT_DESC				 VARCHAR(50)
    ,FINELINE_NBR			 SMALLINT
    ,SCAN_ID				 INT
    ,TRIBE_NBR				 INT
    ,TRIBE_DESC				 VARCHAR(50)
    ,SQUAD_NBR				 INT
    ,SQUAD_DESC				 VARCHAR(50)
    ,STORE_NBR				 INT
    ,STORE_NM				 VARCHAR(100)
	,BANNER_CD				 CHAR(2)
    ,BANNER_DESC			 VARCHAR(20)
    ,STORE_COMP_IND			 TINYINT    
    ,DEPT_DESC				 VARCHAR(50)
	,DEPT_NBR				 SMALLINT
	,MDS_FAM_ID				 INT
	,FINELINE_DESC			 VARCHAR(80)
	,ITEM_STATUS_CD			 CHAR(1)
	,ITEM_TYPE_CD			 SMALLINT
	,VENDOR_NBR				 INT
	,VENDOR_NM				 VARCHAR(80)
	,SALE_TYPE_IND           SMALLINT

    ,SALE_CY_EQ_UNIT_AMT		  DECIMAL(12,2)
    ,SALE_CY_TOT_UNIT_AMT		  DECIMAL(12,2)
    ,SALE_CY_EQ_UNIT_QTY		  DECIMAL(12,2)
    ,SALE_CY_TOT_UNIT_QTY		  DECIMAL(12,2)
    ,SALE_COMP_LY_EQ_UNIT_AMT     DECIMAL(12,2)
    ,SALE_COMP_LY_TOT_UNIT_AMT    DECIMAL(12,2)
    ,SALE_COMP_LY_EQ_UNIT_QTY     DECIMAL(12,2)
    ,SALE_COMP_LY_TOT_UNIT_QTY    DECIMAL(12,2)
    ,SALE_CAL_LY_EQ_UNIT_AMT      DECIMAL(12,2)
    ,SALE_CAL_LY_TOT_UNIT_AMT     DECIMAL(12,2)
    ,SALE_CAL_LY_EQ_UNIT_QTY      DECIMAL(12,2)
    ,SALE_CAL_LY_TOT_UNIT_QTY     DECIMAL(12,2)
	,VISIT_COMP_LY_DT		 DATE
    ,VISIT_CAL_LY_DT		 DATE
)
PARTITIONED BY (VISIT_CY_DT DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY','auto.purge'='true');

INSERT OVERWRITE TABLE ${target_schema}.${target_table_scan_tot}
PARTITION (VISIT_CY_DT)
SELECT
	year(VISIT_CY_DT) as YEAR_NBR, month(VISIT_CY_DT) as MONTH_NBR, day(VISIT_CY_DT) as DAY_NBR,
	ITEM_NBR, ITEM_DESC, SIZE_DESC, COLOR_DESC, BRAND_ID, BRAND_NM, 
	UPC_NBR, CAT_NBR, CAT_DESC, FINELINE_NBR, SCAN_ID, TRIBE_NBR, TRIBE_DESC, 
	SQUAD_NBR, SQUAD_DESC, STORE_NBR, STORE_NM, BANNER_CD, BANNER_DESC, STORE_COMP_IND, DEPT_DESC, DEPT_NBR,
	MDS_FAM_ID, FINELINE_DESC, ITEM_STATUS_CD, ITEM_TYPE_CD, VENDOR_NBR, VENDOR_NM, SALE_TYPE_IND,
	SUM(SALE_CY_EQ_UNIT_AMT), 
	SUM(SALE_CY_TOT_UNIT_AMT), 
	SUM(SALE_CY_EQ_UNIT_QTY), 
	SUM(SALE_CY_TOT_UNIT_QTY),
	SUM(SALE_COMP_LY_EQ_UNIT_AMT), 
	SUM(SALE_COMP_LY_TOT_UNIT_AMT), 
	SUM(SALE_COMP_LY_EQ_UNIT_QTY), 
	SUM(SALE_COMP_LY_TOT_UNIT_QTY), 
	SUM(SALE_CAL_LY_EQ_UNIT_AMT), 
	SUM(SALE_CAL_LY_TOT_UNIT_AMT), 
	SUM(SALE_CAL_LY_EQ_UNIT_QTY), 
	SUM(SALE_CAL_LY_TOT_UNIT_QTY),	
	VISIT_COMP_LY_DT, 
	VISIT_CAL_LY_DT, 
	VISIT_CY_DT
FROM 
(
	SELECT
		ITEM_NBR, ITEM_DESC, SIZE_DESC, COLOR_DESC, BRAND_ID, BRAND_NM, 
		UPC_NBR, CAT_NBR, CAT_DESC, FINELINE_NBR, SCAN_ID, TRIBE_NBR, TRIBE_DESC, 
		SQUAD_NBR, SQUAD_DESC, STORE_NBR, STORE_NM, BANNER_CD, BANNER_DESC, STORE_COMP_IND, DEPT_DESC, DEPT_NBR,
		MDS_FAM_ID, FINELINE_DESC, ITEM_STATUS_CD, ITEM_TYPE_CD, VENDOR_NBR, VENDOR_NM, SALE_TYPE_IND,
		SALE_EQ_UNIT_AMT AS SALE_CY_EQ_UNIT_AMT, SALE_TOT_UNIT_AMT AS SALE_CY_TOT_UNIT_AMT, SALE_EQ_UNIT_QTY AS SALE_CY_EQ_UNIT_QTY, SALE_TOT_UNIT_QTY AS SALE_CY_TOT_UNIT_QTY,
		0 AS SALE_COMP_LY_EQ_UNIT_AMT, 0 AS SALE_COMP_LY_TOT_UNIT_AMT, 0 AS SALE_COMP_LY_EQ_UNIT_QTY, 0 AS SALE_COMP_LY_TOT_UNIT_QTY, 
		0 AS SALE_CAL_LY_EQ_UNIT_AMT, 0 AS SALE_CAL_LY_TOT_UNIT_AMT, 0 AS SALE_CAL_LY_EQ_UNIT_QTY, 0 AS SALE_CAL_LY_TOT_UNIT_QTY,
		VISIT_COMP_LY_DT, VISIT_CAL_LY_DT, VISIT_CY_DT
	FROM ${stg_schema}.${stg_table_tot_cy}
	UNION ALL
	SELECT
		ITEM_NBR, ITEM_DESC, SIZE_DESC, COLOR_DESC, BRAND_ID, BRAND_NM, 
		UPC_NBR, CAT_NBR, CAT_DESC, FINELINE_NBR, SCAN_ID, TRIBE_NBR, TRIBE_DESC, 
		SQUAD_NBR, SQUAD_DESC, STORE_NBR, STORE_NM, BANNER_CD, BANNER_DESC, STORE_COMP_IND, DEPT_DESC, DEPT_NBR,
		MDS_FAM_ID, FINELINE_DESC, ITEM_STATUS_CD, ITEM_TYPE_CD, VENDOR_NBR, VENDOR_NM, SALE_TYPE_IND,
		0 AS SALE_CY_EQ_UNIT_AMT, 0 AS SALE_CY_TOT_UNIT_AMT, 0 AS SALE_CY_EQ_UNIT_QTY, 0 AS SALE_CY_TOT_UNIT_QTY,
		SALE_EQ_UNIT_AMT AS SALE_COMP_LY_EQ_UNIT_AMT, SALE_TOT_UNIT_AMT	AS SALE_COMP_LY_TOT_UNIT_AMT, SALE_EQ_UNIT_QTY AS SALE_COMP_LY_EQ_UNIT_QTY, SALE_TOT_UNIT_QTY AS SALE_COMP_LY_TOT_UNIT_QTY, 
		0 AS SALE_CAL_LY_EQ_UNIT_AMT, 0 AS SALE_CAL_LY_TOT_UNIT_AMT, 0 AS SALE_CAL_LY_EQ_UNIT_QTY, 0 AS SALE_CAL_LY_TOT_UNIT_QTY,
		VISIT_COMP_LY_DT, VISIT_CAL_LY_DT, VISIT_CY_DT
	FROM ${stg_schema}.${stg_table_tot_ly_comp}
	UNION ALL
	SELECT
		ITEM_NBR, ITEM_DESC, SIZE_DESC, COLOR_DESC, BRAND_ID, BRAND_NM, 
		UPC_NBR, CAT_NBR, CAT_DESC, FINELINE_NBR, SCAN_ID, TRIBE_NBR, TRIBE_DESC, 
		SQUAD_NBR, SQUAD_DESC, STORE_NBR, STORE_NM, BANNER_CD, BANNER_DESC, STORE_COMP_IND, DEPT_DESC, DEPT_NBR,
		MDS_FAM_ID, FINELINE_DESC, ITEM_STATUS_CD, ITEM_TYPE_CD, VENDOR_NBR, VENDOR_NM, SALE_TYPE_IND,
		0 AS SALE_CY_EQ_UNIT_AMT, 0 AS SALE_CY_TOT_UNIT_AMT, 0 AS SALE_CY_EQ_UNIT_QTY, 0 AS SALE_CY_TOT_UNIT_QTY,
		0 AS SALE_COMP_LY_EQ_UNIT_AMT,	0 AS SALE_COMP_LY_TOT_UNIT_AMT,	0 AS SALE_COMP_LY_EQ_UNIT_QTY,	0 AS SALE_COMP_LY_TOT_UNIT_QTY, 
		SALE_EQ_UNIT_AMT AS SALE_CAL_LY_EQ_UNIT_AMT, SALE_TOT_UNIT_AMT AS SALE_CAL_LY_TOT_UNIT_AMT, SALE_EQ_UNIT_QTY AS SALE_CAL_LY_EQ_UNIT_QTY, SALE_TOT_UNIT_QTY AS SALE_CAL_LY_TOT_UNIT_QTY,
		VISIT_COMP_LY_DT, VISIT_CAL_LY_DT, VISIT_CY_DT
	FROM ${stg_schema}.${stg_table_tot_ly_cal}
) X
GROUP BY
	year(VISIT_CY_DT), month(VISIT_CY_DT), day(VISIT_CY_DT),
	ITEM_NBR, ITEM_DESC, SIZE_DESC, COLOR_DESC, BRAND_ID, BRAND_NM, 
	UPC_NBR, CAT_NBR, CAT_DESC, FINELINE_NBR, SCAN_ID, TRIBE_NBR, TRIBE_DESC, 
	SQUAD_NBR, SQUAD_DESC, STORE_NBR, STORE_NM, BANNER_CD, BANNER_DESC, STORE_COMP_IND, DEPT_DESC, DEPT_NBR,
	MDS_FAM_ID, FINELINE_DESC, ITEM_STATUS_CD, ITEM_TYPE_CD, VENDOR_NBR, VENDOR_NM, SALE_TYPE_IND,
	VISIT_COMP_LY_DT, VISIT_CAL_LY_DT, VISIT_CY_DT
;

dfs -chgrp -R ${group_name} /user/hive/warehouse/${target_schema}.db/${target_table_scan_tot}; 
dfs -chmod -R ${permission} /user/hive/warehouse/${target_schema}.db/${target_table_scan_tot};

DROP TABLE IF EXISTS ${stg_schema}.${stg_table_tot_cy};
DROP TABLE IF EXISTS ${stg_schema}.${stg_table_tot_ly_comp};
DROP TABLE IF EXISTS ${stg_schema}.${stg_table_tot_ly_cal};
DROP TABLE IF EXISTS ${stg_schema}.${stg_table_dates};
