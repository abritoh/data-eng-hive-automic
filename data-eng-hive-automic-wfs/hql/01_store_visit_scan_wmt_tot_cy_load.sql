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

DROP TABLE IF EXISTS ${stg_schema}.${stg_table_dates};
DROP TABLE IF EXISTS ${stg_schema}.${stg_table_tot_cy};
DROP TABLE IF EXISTS ${stg_schema}.${stg_table_tot_ly_comp};
DROP TABLE IF EXISTS ${stg_schema}.${stg_table_tot_ly_cal};

CREATE TABLE IF NOT EXISTS ${stg_schema}.${stg_table_dates}
(
	 VISIT_CY_DT		DATE
	,VISIT_COMP_LY_DT	DATE 
	,VISIT_CAL_LY_DT	DATE
);

CREATE TABLE IF NOT EXISTS ${stg_schema}.${stg_table_tot_cy}
(
     ITEM_NBR           INT
    ,ITEM_DESC          VARCHAR(50)
    ,SIZE_DESC			VARCHAR(50)
    ,COLOR_DESC			VARCHAR(50)
    ,BRAND_ID			INT
    ,BRAND_NM			VARCHAR(50)
    ,UPC_NBR			INT
    ,CAT_NBR			SMALLINT
    ,CAT_DESC			VARCHAR(50)
    ,FINELINE_NBR		SMALLINT
    ,SCAN_ID			INT
    ,TRIBE_NBR			INT
    ,TRIBE_DESC			VARCHAR(50)
    ,SQUAD_NBR			INT
    ,SQUAD_DESC			VARCHAR(50)
    ,STORE_NBR          INT
    ,STORE_NM           VARCHAR(100)
	,BANNER_CD			CHAR(2)
    ,BANNER_DESC        VARCHAR(20)
    ,STORE_COMP_IND		TINYINT
    ,DEPT_DESC			VARCHAR(50)	
	,DEPT_NBR			SMALLINT
	,MDS_FAM_ID			INT
	,FINELINE_DESC		VARCHAR(80)
	,ITEM_STATUS_CD		CHAR(1)
	,ITEM_TYPE_CD		SMALLINT
	,VENDOR_NBR			INT
	,VENDOR_NM			VARCHAR(80)
	,SALE_TYPE_IND      SMALLINT

    ,SALE_EQ_UNIT_AMT	DECIMAL(12,2)
    ,SALE_TOT_UNIT_AMT	DECIMAL(12,2)
    ,SALE_EQ_UNIT_QTY	DECIMAL(12,2)
    ,SALE_TOT_UNIT_QTY	DECIMAL(12,2)
    ,VISIT_COMP_LY_DT   DATE
	,VISIT_CAL_LY_DT    DATE
)
PARTITIONED BY (VISIT_CY_DT DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY','auto.purge'='true');

CREATE TABLE IF NOT EXISTS ${stg_schema}.${stg_table_tot_ly_comp}
(
     ITEM_NBR            INT
    ,ITEM_DESC           VARCHAR(50)
    ,SIZE_DESC			 VARCHAR(50)
    ,COLOR_DESC			 VARCHAR(50)
    ,BRAND_ID			 INT
    ,BRAND_NM			 VARCHAR(50)
    ,UPC_NBR			 INT
    ,CAT_NBR			 SMALLINT
    ,CAT_DESC			 VARCHAR(50)
    ,FINELINE_NBR		 SMALLINT
    ,SCAN_ID			 INT
    ,TRIBE_NBR			 INT
    ,TRIBE_DESC			 VARCHAR(50)
    ,SQUAD_NBR			 INT
    ,SQUAD_DESC			 VARCHAR(50)
    ,STORE_NBR           INT
    ,STORE_NM            VARCHAR(100)
	,BANNER_CD			 CHAR(2)
    ,BANNER_DESC         VARCHAR(20)
    ,STORE_COMP_IND		 TINYINT
    ,DEPT_DESC			 VARCHAR(50)	
	,DEPT_NBR			 SMALLINT
	,MDS_FAM_ID			 INT
	,FINELINE_DESC		 VARCHAR(80)
	,ITEM_STATUS_CD		 CHAR(1)
	,ITEM_TYPE_CD		 SMALLINT
	,VENDOR_NBR			 INT
	,VENDOR_NM			 VARCHAR(80)
	,SALE_TYPE_IND       SMALLINT

    ,SALE_EQ_UNIT_AMT	 DECIMAL(12,2)
    ,SALE_TOT_UNIT_AMT	 DECIMAL(12,2)
    ,SALE_EQ_UNIT_QTY	 DECIMAL(12,2)
    ,SALE_TOT_UNIT_QTY	 DECIMAL(12,2)
    ,VISIT_COMP_LY_DT    DATE
	,VISIT_CAL_LY_DT     DATE
)
PARTITIONED BY (VISIT_CY_DT DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY','auto.purge'='true');

CREATE TABLE IF NOT EXISTS ${stg_schema}.${stg_table_tot_ly_cal}
(
     ITEM_NBR            INT
    ,ITEM_DESC           VARCHAR(50)
    ,SIZE_DESC			 VARCHAR(50)
    ,COLOR_DESC			 VARCHAR(50)
    ,BRAND_ID			 INT
    ,BRAND_NM			 VARCHAR(50)
    ,UPC_NBR			 INT
    ,CAT_NBR			 SMALLINT
    ,CAT_DESC			 VARCHAR(50)
    ,FINELINE_NBR		 SMALLINT
    ,SCAN_ID			 INT
    ,TRIBE_NBR			 INT
    ,TRIBE_DESC			 VARCHAR(50)
    ,SQUAD_NBR			 INT
    ,SQUAD_DESC			 VARCHAR(50)
    ,STORE_NBR           INT
    ,STORE_NM            VARCHAR(100)
	,BANNER_CD			 CHAR(2)
    ,BANNER_DESC         VARCHAR(20)
    ,STORE_COMP_IND		 TINYINT
    ,DEPT_DESC			 VARCHAR(50)	
	,DEPT_NBR			 SMALLINT
	,MDS_FAM_ID			 INT
	,FINELINE_DESC		 VARCHAR(80)
	,ITEM_STATUS_CD		 CHAR(1)
	,ITEM_TYPE_CD		 SMALLINT
	,VENDOR_NBR			 INT
	,VENDOR_NM			 VARCHAR(80)
	,SALE_TYPE_IND       SMALLINT

    ,SALE_EQ_UNIT_AMT	 DECIMAL(12,2)
    ,SALE_TOT_UNIT_AMT	 DECIMAL(12,2)
    ,SALE_EQ_UNIT_QTY	 DECIMAL(12,2)
    ,SALE_TOT_UNIT_QTY	 DECIMAL(12,2)
    ,VISIT_COMP_LY_DT    DATE
	,VISIT_CAL_LY_DT     DATE
)
PARTITIONED BY (VISIT_CY_DT DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY','auto.purge'='true');

INSERT OVERWRITE TABLE ${stg_schema}.${stg_table_dates}
SELECT DISTINCT VISIT_DT as VISIT_CY_DT, LY_COMP_VISIT_DT as VISIT_COMP_LY_DT, LY_CAL_DT as VISIT_CAL_LY_DT
FROM ${src_schema_mx_spot_run}.${src_table_visit_scan_wmt} WHERE VISIT_DT BETWEEN ${beg_date} AND ${end_date};

INSERT OVERWRITE TABLE ${stg_schema}.${stg_table_tot_cy}
PARTITION (VISIT_CY_DT)
SELECT
    COALESCE(IT.ITEM_NBR,-999) AS ITEM_NBR,
    COALESCE(IT.ITEM_DESC_1, 'XXXX') AS ITEM_DESC,
    COALESCE(IT.SIZE_DESC, 'XXXX') AS SIZE_DESC,
    COALESCE(IT.COLOR_DESC, 'XXXX') AS COLOR_DESC,
    COALESCE(IT.BRAND_ID,-999) AS BRAND_ID,
    COALESCE(IT.BRAND_NM, 'XXXX') AS BRAND_NM,
    COALESCE(IT.UPC_NBR,-999) AS UPC_NBR,
    COALESCE(SCAN.CAT_NBR, -999) AS CAT_NBR,
    COALESCE(SCAN.CAT_DESC, 'XXXX') AS CAT_DESC,
    COALESCE(SCAN.FINELINE_NBR, -999) AS FINELINE_NBR,
    COALESCE(SCAN.SCAN_ID, -999) AS SCAN_ID,
    COALESCE(SCAN.TRIBE_NBR,-999) AS TRIBE_NBR,
    COALESCE(SCAN.TRIBE_DESC,'XXXX') AS TRIBE_DESC,
    COALESCE(SCAN.SQUAD_NBR,-999) AS SQUAD_NBR,
    COALESCE(SCAN.SQUAD_DESC,'XXXX') AS SQUAD_DESC,
    COALESCE(SCAN.STORE_NBR,-999) AS STORE_NBR,
    COALESCE(SCAN.STORE_NM,'XXXX') AS STORE_NM,
	COALESCE(SCAN.BANNER_CD, 'XX') AS BANNER_CD,
    COALESCE(SCAN.BANNER_DESC, 'XXXX') AS BANNER_DESC,
    COALESCE(SCAN.STORE_COMP_IND,-999) AS STORE_COMP_IND,    
    COALESCE(SCAN.DEPT_DESC,'XXXX') AS DEPT_DESC,
	COALESCE(SCAN.DEPT_NBR,-999) AS DEPT_NBR,
	COALESCE(IT.MDS_FAM_ID, -999) AS MDS_FAM_ID,
	COALESCE(IT.FINELINE_DESC, 'XXXX') AS FINELINE_DESC,
	COALESCE(IT.ITEM_STATUS_CD, 'X') AS ITEM_STATUS_CD,
	COALESCE(IT.ITEM_TYPE_CD, -999) AS ITEM_TYPE_CD,
	COALESCE(IT.VENDOR_NBR, -999) AS VENDOR_NBR,
	COALESCE(IT.VENDOR_NM, 'XXXX') AS VENDOR_NM,
	COALESCE(SCAN.SALE_TYPE_IND, -999) AS SALE_TYPE_IND,

    SUM(CASE WHEN SCAN.STORE_COMP_IND = 1 THEN SCAN.SCAN_RTL_AMT ELSE 0 END) AS SALE_EQ_UNIT_AMT,
    SUM(SCAN.SCAN_RTL_AMT) AS SALE_TOT_UNIT_AMT,    
    SUM(CASE WHEN SCAN.STORE_COMP_IND = 1 THEN SCAN.UNIT_QTY ELSE 0 END) AS SALE_EQ_UNIT_QTY,
	SUM(SCAN.UNIT_QTY) AS SALE_TOT_UNIT_QTY,
    DTS.VISIT_COMP_LY_DT, 
	DTS.VISIT_CAL_LY_DT, 
	DTS.VISIT_CY_DT
FROM ${src_schema_mx_spot_run}.${src_table_visit_scan_wmt} AS SCAN
INNER JOIN ${stg_schema}.${stg_table_dates} AS DTS ON SCAN.VISIT_DT = DTS.VISIT_CY_DT
LEFT  JOIN ${src_schema_mx_core_dim}.${src_table_item_dim} AS IT ON SCAN.SCAN_ID = IT.MDS_FAM_ID AND SCAN.OP_CMPNY_CD = IT.OP_CMPNY_CD AND IT.CURR_IND = 1 
WHERE SCAN.GEO_REGION_CD = ${region_code} AND SCAN.OP_CMPNY_CD = ${company_code} AND SCAN.DEPT_NBR NOT IN (49,50,72,75,99)
GROUP BY
    COALESCE(IT.ITEM_NBR,-999),
    COALESCE(IT.ITEM_DESC_1, 'XXXX'),
    COALESCE(IT.SIZE_DESC, 'XXXX'),
    COALESCE(IT.COLOR_DESC, 'XXXX'),
    COALESCE(IT.BRAND_ID,-999),
    COALESCE(IT.BRAND_NM, 'XXXX'),
    COALESCE(IT.UPC_NBR,-999),
    COALESCE(SCAN.CAT_NBR, -999),
    COALESCE(SCAN.CAT_DESC, 'XXXX'),
    COALESCE(SCAN.FINELINE_NBR, -999),
    COALESCE(SCAN.SCAN_ID, -999),
    COALESCE(SCAN.TRIBE_NBR,-999),
    COALESCE(SCAN.TRIBE_DESC,'XXXX'),
    COALESCE(SCAN.SQUAD_NBR,-999),
    COALESCE(SCAN.SQUAD_DESC,'XXXX'),
    COALESCE(SCAN.STORE_NBR,-999),
    COALESCE(SCAN.STORE_NM,'XXXX'),
	COALESCE(SCAN.BANNER_CD, 'XX'),
    COALESCE(SCAN.BANNER_DESC, 'XXXX'),
    COALESCE(SCAN.STORE_COMP_IND,-999),    
    COALESCE(SCAN.DEPT_DESC,'XXXX'),		
	COALESCE(SCAN.DEPT_NBR,-999),
	COALESCE(IT.MDS_FAM_ID, -999),
	COALESCE(IT.FINELINE_DESC, 'XXXX'),
	COALESCE(IT.ITEM_STATUS_CD, 'X'),
	COALESCE(IT.ITEM_TYPE_CD, -999),
	COALESCE(IT.VENDOR_NBR, -999),
	COALESCE(IT.VENDOR_NM, 'XXXX'),
	COALESCE(SCAN.SALE_TYPE_IND, -999),
    DTS.VISIT_COMP_LY_DT, 
	DTS.VISIT_CAL_LY_DT, 
	DTS.VISIT_CY_DT;

INSERT OVERWRITE TABLE ${stg_schema}.${stg_table_tot_ly_comp}
PARTITION (VISIT_CY_DT)
SELECT
    COALESCE(IT.ITEM_NBR,-999) AS ITEM_NBR,
    COALESCE(IT.ITEM_DESC_1, 'XXXX') AS ITEM_DESC,
    COALESCE(IT.SIZE_DESC, 'XXXX') AS SIZE_DESC,
    COALESCE(IT.COLOR_DESC, 'XXXX') AS COLOR_DESC,
    COALESCE(IT.BRAND_ID,-999) AS BRAND_ID,
    COALESCE(IT.BRAND_NM, 'XXXX') AS BRAND_NM,
    COALESCE(IT.UPC_NBR,-999) AS UPC_NBR,
    COALESCE(SCAN.CAT_NBR, -999) AS CAT_NBR,
    COALESCE(SCAN.CAT_DESC, 'XXXX') AS CAT_DESC,
    COALESCE(SCAN.FINELINE_NBR, -999) AS FINELINE_NBR,
    COALESCE(SCAN.SCAN_ID, -999) AS SCAN_ID,
    COALESCE(SCAN.TRIBE_NBR,-999) AS TRIBE_NBR,
    COALESCE(SCAN.TRIBE_DESC,'XXXX') AS TRIBE_DESC,
    COALESCE(SCAN.SQUAD_NBR,-999) AS SQUAD_NBR,
    COALESCE(SCAN.SQUAD_DESC,'XXXX') AS SQUAD_DESC,
    COALESCE(SCAN.STORE_NBR,-999) AS STORE_NBR,
    COALESCE(SCAN.STORE_NM,'XXXX') AS STORE_NM,
	COALESCE(SCAN.BANNER_CD, 'XX') AS BANNER_CD,
    COALESCE(SCAN.BANNER_DESC, 'XXXX') AS BANNER_DESC,
    COALESCE(SCAN.STORE_COMP_IND,-999) AS STORE_COMP_IND,    
    COALESCE(SCAN.DEPT_DESC,'XXXX') AS DEPT_DESC,
	COALESCE(SCAN.DEPT_NBR,-999) AS DEPT_NBR,
	COALESCE(IT.MDS_FAM_ID, -999) AS MDS_FAM_ID,
	COALESCE(IT.FINELINE_DESC, 'XXXX') AS FINELINE_DESC,
	COALESCE(IT.ITEM_STATUS_CD, 'X') AS ITEM_STATUS_CD,
	COALESCE(IT.ITEM_TYPE_CD, -999) AS ITEM_TYPE_CD,
	COALESCE(IT.VENDOR_NBR, -999) AS VENDOR_NBR,
	COALESCE(IT.VENDOR_NM, 'XXXX') AS VENDOR_NM,
	COALESCE(SCAN.SALE_TYPE_IND, -999) AS SALE_TYPE_IND,

    SUM(CASE WHEN SCAN.STORE_COMP_IND = 1 THEN SCAN.SCAN_RTL_AMT ELSE 0 END) AS SALE_EQ_UNIT_AMT,
    SUM(SCAN.SCAN_RTL_AMT) AS SALE_TOT_UNIT_AMT,    
    SUM(CASE WHEN SCAN.STORE_COMP_IND = 1 THEN SCAN.UNIT_QTY ELSE 0 END) AS SALE_EQ_UNIT_QTY,
	SUM(SCAN.UNIT_QTY) AS SALE_TOT_UNIT_QTY,
    DTS.VISIT_COMP_LY_DT, 
	DTS.VISIT_CAL_LY_DT, 
	DTS.VISIT_CY_DT
FROM ${src_schema_mx_spot_run}.${src_table_visit_scan_wmt} AS SCAN
INNER JOIN ${stg_schema}.${stg_table_dates} AS DTS ON SCAN.VISIT_DT = DTS.VISIT_COMP_LY_DT
LEFT  JOIN ${src_schema_mx_core_dim}.${src_table_item_dim} AS IT ON SCAN.SCAN_ID = IT.MDS_FAM_ID AND SCAN.OP_CMPNY_CD = IT.OP_CMPNY_CD AND IT.CURR_IND = 1 
WHERE SCAN.GEO_REGION_CD = ${region_code} AND SCAN.OP_CMPNY_CD = ${company_code} AND SCAN.DEPT_NBR NOT IN (49,50,72,75,99)
GROUP BY
    COALESCE(IT.ITEM_NBR,-999),
    COALESCE(IT.ITEM_DESC_1, 'XXXX'),
    COALESCE(IT.SIZE_DESC, 'XXXX'),
    COALESCE(IT.COLOR_DESC, 'XXXX'),
    COALESCE(IT.BRAND_ID,-999),
    COALESCE(IT.BRAND_NM, 'XXXX'),
    COALESCE(IT.UPC_NBR,-999),
    COALESCE(SCAN.CAT_NBR, -999),
    COALESCE(SCAN.CAT_DESC, 'XXXX'),
    COALESCE(SCAN.FINELINE_NBR, -999),
    COALESCE(SCAN.SCAN_ID, -999),
    COALESCE(SCAN.TRIBE_NBR,-999),
    COALESCE(SCAN.TRIBE_DESC,'XXXX'),
    COALESCE(SCAN.SQUAD_NBR,-999),
    COALESCE(SCAN.SQUAD_DESC,'XXXX'),
    COALESCE(SCAN.STORE_NBR,-999),
    COALESCE(SCAN.STORE_NM,'XXXX'),
	COALESCE(SCAN.BANNER_CD, 'XX'),
    COALESCE(SCAN.BANNER_DESC, 'XXXX'),
    COALESCE(SCAN.STORE_COMP_IND,-999),    
    COALESCE(SCAN.DEPT_DESC,'XXXX'),		
	COALESCE(SCAN.DEPT_NBR,-999),
	COALESCE(IT.MDS_FAM_ID, -999),
	COALESCE(IT.FINELINE_DESC, 'XXXX'),
	COALESCE(IT.ITEM_STATUS_CD, 'X'),
	COALESCE(IT.ITEM_TYPE_CD, -999),
	COALESCE(IT.VENDOR_NBR, -999),
	COALESCE(IT.VENDOR_NM, 'XXXX'),
	COALESCE(SCAN.SALE_TYPE_IND, -999),
    DTS.VISIT_COMP_LY_DT, 
	DTS.VISIT_CAL_LY_DT, 
	DTS.VISIT_CY_DT;

INSERT OVERWRITE TABLE ${stg_schema}.${stg_table_tot_ly_cal}
PARTITION (VISIT_CY_DT)
SELECT
    COALESCE(IT.ITEM_NBR,-999) AS ITEM_NBR,
    COALESCE(IT.ITEM_DESC_1, 'XXXX') AS ITEM_DESC,
    COALESCE(IT.SIZE_DESC, 'XXXX') AS SIZE_DESC,
    COALESCE(IT.COLOR_DESC, 'XXXX') AS COLOR_DESC,
    COALESCE(IT.BRAND_ID,-999) AS BRAND_ID,
    COALESCE(IT.BRAND_NM, 'XXXX') AS BRAND_NM,
    COALESCE(IT.UPC_NBR,-999) AS UPC_NBR,
    COALESCE(SCAN.CAT_NBR, -999) AS CAT_NBR,
    COALESCE(SCAN.CAT_DESC, 'XXXX') AS CAT_DESC,
    COALESCE(SCAN.FINELINE_NBR, -999) AS FINELINE_NBR,
    COALESCE(SCAN.SCAN_ID, -999) AS SCAN_ID,
    COALESCE(SCAN.TRIBE_NBR,-999) AS TRIBE_NBR,
    COALESCE(SCAN.TRIBE_DESC,'XXXX') AS TRIBE_DESC,
    COALESCE(SCAN.SQUAD_NBR,-999) AS SQUAD_NBR,
    COALESCE(SCAN.SQUAD_DESC,'XXXX') AS SQUAD_DESC,
    COALESCE(SCAN.STORE_NBR,-999) AS STORE_NBR,
    COALESCE(SCAN.STORE_NM,'XXXX') AS STORE_NM,
	COALESCE(SCAN.BANNER_CD, 'XX') AS BANNER_CD,
    COALESCE(SCAN.BANNER_DESC, 'XXXX') AS BANNER_DESC,
    COALESCE(SCAN.STORE_COMP_IND,-999) AS STORE_COMP_IND,    
    COALESCE(SCAN.DEPT_DESC,'XXXX') AS DEPT_DESC,
	COALESCE(SCAN.DEPT_NBR,-999) AS DEPT_NBR,
	COALESCE(IT.MDS_FAM_ID, -999) AS MDS_FAM_ID,
	COALESCE(IT.FINELINE_DESC, 'XXXX') AS FINELINE_DESC,
	COALESCE(IT.ITEM_STATUS_CD, 'X') AS ITEM_STATUS_CD,
	COALESCE(IT.ITEM_TYPE_CD, -999) AS ITEM_TYPE_CD,
	COALESCE(IT.VENDOR_NBR, -999) AS VENDOR_NBR,
	COALESCE(IT.VENDOR_NM, 'XXXX') AS VENDOR_NM,
	COALESCE(SCAN.SALE_TYPE_IND, -999) AS SALE_TYPE_IND,

    SUM(CASE WHEN SCAN.STORE_COMP_IND = 1 THEN SCAN.SCAN_RTL_AMT ELSE 0 END) AS SALE_EQ_UNIT_AMT,
    SUM(SCAN.SCAN_RTL_AMT) AS SALE_TOT_UNIT_AMT,    
    SUM(CASE WHEN SCAN.STORE_COMP_IND = 1 THEN SCAN.UNIT_QTY ELSE 0 END) AS SALE_EQ_UNIT_QTY,
	SUM(SCAN.UNIT_QTY) AS SALE_TOT_UNIT_QTY,
    DTS.VISIT_COMP_LY_DT, 
	DTS.VISIT_CAL_LY_DT, 
	DTS.VISIT_CY_DT
FROM ${src_schema_mx_spot_run}.${src_table_visit_scan_wmt} AS SCAN
INNER JOIN ${stg_schema}.${stg_table_dates} AS DTS ON SCAN.VISIT_DT = DTS.VISIT_CAL_LY_DT
LEFT  JOIN ${src_schema_mx_core_dim}.${src_table_item_dim} AS IT ON SCAN.SCAN_ID = IT.MDS_FAM_ID AND SCAN.OP_CMPNY_CD = IT.OP_CMPNY_CD AND IT.CURR_IND = 1 
WHERE SCAN.GEO_REGION_CD = ${region_code} AND SCAN.OP_CMPNY_CD = ${company_code} AND SCAN.DEPT_NBR NOT IN (49,50,72,75,99)
GROUP BY
    COALESCE(IT.ITEM_NBR,-999),
    COALESCE(IT.ITEM_DESC_1, 'XXXX'),
    COALESCE(IT.SIZE_DESC, 'XXXX'),
    COALESCE(IT.COLOR_DESC, 'XXXX'),
    COALESCE(IT.BRAND_ID,-999),
    COALESCE(IT.BRAND_NM, 'XXXX'),
    COALESCE(IT.UPC_NBR,-999),
    COALESCE(SCAN.CAT_NBR, -999),
    COALESCE(SCAN.CAT_DESC, 'XXXX'),
    COALESCE(SCAN.FINELINE_NBR, -999),
    COALESCE(SCAN.SCAN_ID, -999),
    COALESCE(SCAN.TRIBE_NBR,-999),
    COALESCE(SCAN.TRIBE_DESC,'XXXX'),
    COALESCE(SCAN.SQUAD_NBR,-999),
    COALESCE(SCAN.SQUAD_DESC,'XXXX'),
    COALESCE(SCAN.STORE_NBR,-999),
    COALESCE(SCAN.STORE_NM,'XXXX'),
	COALESCE(SCAN.BANNER_CD, 'XX'),
    COALESCE(SCAN.BANNER_DESC, 'XXXX'),
    COALESCE(SCAN.STORE_COMP_IND,-999),    
    COALESCE(SCAN.DEPT_DESC,'XXXX'),		
	COALESCE(SCAN.DEPT_NBR,-999),
	COALESCE(IT.MDS_FAM_ID, -999),
	COALESCE(IT.FINELINE_DESC, 'XXXX'),
	COALESCE(IT.ITEM_STATUS_CD, 'X'),
	COALESCE(IT.ITEM_TYPE_CD, -999),
	COALESCE(IT.VENDOR_NBR, -999),
	COALESCE(IT.VENDOR_NM, 'XXXX'),
	COALESCE(SCAN.SALE_TYPE_IND, -999),
    DTS.VISIT_COMP_LY_DT, 
	DTS.VISIT_CAL_LY_DT, 
	DTS.VISIT_CY_DT;

