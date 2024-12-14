﻿#==========================================================================================================================#
# FILE.......: ............................................................................................................#
# AUTHOR.....: ArBR 2021 [arcbrth@gmail.com................................................................................#
# DESCRIPTION: For WMT-MX .................................................................................................#
# PARAMETERS.: ............................................................................................................#
#...$hadoop_queue                  &HADOOP_QUEUE#                    intlprcincld..........................................#
#...$region_code                   &REGION_CODE#                     'MX'..................................................#
#......... . . . . . . . . . . . . . . . . . . . . . . . . . . .. . . . . . . . . . . . . . .  .. . . . . . . . . . . . . .#
#==========================================================================================================================#

DROP VIEW IF EXISTS mx_rep....._views.mx_reporting_wmt_tot_sales_trib;
CREATE VIEW mx_rep....._views.mx_reporting_wmt_tot_sales_trib
AS
SELECT 
     YEAR_NBR
    , MONTH_NBR
    , DAY_NBR
	, BANNER_CD
    , BANNER_DESC
    , CAT_NBR
    , CAT_DESC
    , TRIBE_NBR
    , TRIBE_DESC
    , SQUAD_NBR
    , SQUAD_DESC
    , DEPT_DESC
	, DEPT_NBR
	, SALE_TYPE_IND

	, SALE_CY_EQ_UNIT_AMT
	, SALE_CY_TOT_UNIT_AMT
	, SALE_CY_EQ_UNIT_QTY
	, SALE_CY_TOT_UNIT_QTY
	, SALE_COMP_LY_EQ_UNIT_AMT
	, SALE_COMP_LY_TOT_UNIT_AMT
	, SALE_COMP_LY_EQ_UNIT_QTY
	, SALE_COMP_LY_TOT_UNIT_QTY
	, SALE_CAL_LY_EQ_UNIT_AMT
	, SALE_CAL_LY_TOT_UNIT_AMT
	, SALE_CAL_LY_EQ_UNIT_QTY
	, SALE_CAL_LY_TOT_UNIT_QTY

    , SHIP_COST_CY_AMT
	, SHIP_COST_LY_AMT
    , SHIP_RTL_CY_AMT
	, SHIP_RTL_LY_AMT
    
    , RTN_COST_CY_AMT
	, RTN_COST_LY_AMT
	, RTN_RTL_CY_AMT
	, RTN_RTL_LY_AMT

	, DEC_CEDIS_CY_AMT
	, DEC_CEDIS_LY_AMT
	, THROW_AWAY_CY_AMT
	, THROW_AWAY_LY_AMT

	, OFC_INC_CY_AMT
	, PRICE_CHNG_TO_0_CY_AMT
	, DONATIONS_CY_AMT
	, DONATED_CY_AMT
	, LQDTNS_CY_AMT
	, ABSLT_CP_SHOP_CY_AMT
	, EVENT_5000_CY_AMT
	, MULTISAVING_CY_AMT
	, NBB_CY_AMT
	, OFC_SALES_CY_AMT

	, OFC_INC_LY_AMT
	, PRICE_CHNG_TO_0_LY_AMT
	, DONATIONS_LY_AMT
	, DONATED_LY_AMT
	, LQDTNS_LY_AMT
	, ABSLT_CP_SHOP_LY_AMT
	, EVENT_5000_LY_AMT
	, MULTISAVING_LY_AMT
	, NBB_LY_AMT
	, OFC_SALES_LY_AMT

	, ONHAND_COST_LY_AMT
	, ONHAND_COST_CY_AMT
	, INTRAN_COST_LY_AMT
	, INTRAN_COST_CY_AMT
	, INWH_COST_LY_AMT
	, INWH_COST_CY_AMT

	, CUST_LY_TOT_UNIT_QTY AS CUST_LY_QTY
	, CUST_CY_TOT_UNIT_QTY AS CUST_CY_QTY
	--, CUST_LY_EQ_UNIT_QTY
	--, CUST_CY_EQ_UNIT_QTY
	, TKT_LY_TOT_UNIT_QTY AS TKT_LY_QTY
	, TKT_CY_TOT_UNIT_QTY AS TKT_CY_QTY
	--, TKT_LY_EQ_UNIT_QTY
	--, TKT_CY_EQ_UNIT_QTY
	, TKT_SALE_LY_EQ_UNIT_AMT
	, TKT_SALE_CY_EQ_UNIT_AMT
	, TKT_SALE_LY_TOT_UNIT_AMT
	, TKT_SALE_CY_TOT_UNIT_AMT

	, VISIT_COMP_LY_DT
	, VISIT_CAL_LY_DT 
	, VISIT_CY_DT
FROM mx_reporting_tables.mx_reporting_wmt_tot_sales_trib;


DROP VIEW IF EXISTS mx_rep....._views.mx_reporting_wmt_tot_sales_trib_01;
CREATE VIEW mx_rep....._views.mx_reporting_wmt_tot_sales_trib_01
AS
SELECT 
     YEAR_NBR
    , MONTH_NBR
    , DAY_NBR
	, BANNER_CD
    , BANNER_DESC
    , CAT_NBR
    , CAT_DESC
    , TRIBE_NBR
    , TRIBE_DESC
    , SQUAD_NBR
    , SQUAD_DESC
    , DEPT_DESC
	, DEPT_NBR
	, SALE_TYPE_IND

	, SALE_CY_EQ_UNIT_AMT
	, SALE_CY_TOT_UNIT_AMT
	, SALE_CY_EQ_UNIT_QTY
	, SALE_CY_TOT_UNIT_QTY
	, SALE_COMP_LY_EQ_UNIT_AMT
	, SALE_COMP_LY_TOT_UNIT_AMT
	, SALE_COMP_LY_EQ_UNIT_QTY
	, SALE_COMP_LY_TOT_UNIT_QTY
	, SALE_CAL_LY_EQ_UNIT_AMT
	, SALE_CAL_LY_TOT_UNIT_AMT
	, SALE_CAL_LY_EQ_UNIT_QTY
	, SALE_CAL_LY_TOT_UNIT_QTY

    , SHIP_COST_CY_AMT
	, SHIP_COST_LY_AMT
    , SHIP_RTL_CY_AMT
	, SHIP_RTL_LY_AMT
    
    , RTN_COST_CY_AMT
	, RTN_COST_LY_AMT
	, RTN_RTL_CY_AMT
	, RTN_RTL_LY_AMT

	, DEC_CEDIS_CY_AMT
	, DEC_CEDIS_LY_AMT
	, THROW_AWAY_CY_AMT
	, THROW_AWAY_LY_AMT

	, OFC_INC_CY_AMT
	, PRICE_CHNG_TO_0_CY_AMT
	, DONATIONS_CY_AMT
	, DONATED_CY_AMT
	, LQDTNS_CY_AMT
	, ABSLT_CP_SHOP_CY_AMT
	, EVENT_5000_CY_AMT
	, MULTISAVING_CY_AMT
	, NBB_CY_AMT
	, OFC_SALES_CY_AMT

	, OFC_INC_LY_AMT
	, PRICE_CHNG_TO_0_LY_AMT
	, DONATIONS_LY_AMT
	, DONATED_LY_AMT
	, LQDTNS_LY_AMT
	, ABSLT_CP_SHOP_LY_AMT
	, EVENT_5000_LY_AMT
	, MULTISAVING_LY_AMT
	, NBB_LY_AMT
	, OFC_SALES_LY_AMT

	, ONHAND_COST_LY_AMT
	, ONHAND_COST_CY_AMT
	, INTRAN_COST_LY_AMT
	, INTRAN_COST_CY_AMT
	, INWH_COST_LY_AMT
	, INWH_COST_CY_AMT

	, CUST_LY_TOT_UNIT_QTY AS CUST_LY_QTY
	, CUST_CY_TOT_UNIT_QTY AS CUST_CY_QTY
	--, CUST_LY_EQ_UNIT_QTY
	--, CUST_CY_EQ_UNIT_QTY
	, TKT_LY_TOT_UNIT_QTY AS TKT_LY_QTY
	, TKT_CY_TOT_UNIT_QTY AS TKT_CY_QTY
	--, TKT_LY_EQ_UNIT_QTY
	--, TKT_CY_EQ_UNIT_QTY
	, TKT_SALE_LY_EQ_UNIT_AMT
	, TKT_SALE_CY_EQ_UNIT_AMT
	, TKT_SALE_LY_TOT_UNIT_AMT
	, TKT_SALE_CY_TOT_UNIT_AMT

	, VISIT_COMP_LY_DT
	, VISIT_CAL_LY_DT 
	, VISIT_CY_DT
FROM mx_reporting_tables.mx_reporting_wmt_tot_sales_trib
WHERE TRIBE_NBR=1;


DROP VIEW IF EXISTS mx_rep....._views.mx_reporting_wmt_tot_sales_trib_02;
CREATE VIEW mx_rep....._views.mx_reporting_wmt_tot_sales_trib_02
AS
SELECT 
     YEAR_NBR
    , MONTH_NBR
    , DAY_NBR
	, BANNER_CD
    , BANNER_DESC
    , CAT_NBR
    , CAT_DESC
    , TRIBE_NBR
    , TRIBE_DESC
    , SQUAD_NBR
    , SQUAD_DESC
    , DEPT_DESC
	, DEPT_NBR
	, SALE_TYPE_IND

	, SALE_CY_EQ_UNIT_AMT
	, SALE_CY_TOT_UNIT_AMT
	, SALE_CY_EQ_UNIT_QTY
	, SALE_CY_TOT_UNIT_QTY
	, SALE_COMP_LY_EQ_UNIT_AMT
	, SALE_COMP_LY_TOT_UNIT_AMT
	, SALE_COMP_LY_EQ_UNIT_QTY
	, SALE_COMP_LY_TOT_UNIT_QTY
	, SALE_CAL_LY_EQ_UNIT_AMT
	, SALE_CAL_LY_TOT_UNIT_AMT
	, SALE_CAL_LY_EQ_UNIT_QTY
	, SALE_CAL_LY_TOT_UNIT_QTY

    , SHIP_COST_CY_AMT
	, SHIP_COST_LY_AMT
    , SHIP_RTL_CY_AMT
	, SHIP_RTL_LY_AMT
    
    , RTN_COST_CY_AMT
	, RTN_COST_LY_AMT
	, RTN_RTL_CY_AMT
	, RTN_RTL_LY_AMT

	, DEC_CEDIS_CY_AMT
	, DEC_CEDIS_LY_AMT
	, THROW_AWAY_CY_AMT
	, THROW_AWAY_LY_AMT

	, OFC_INC_CY_AMT
	, PRICE_CHNG_TO_0_CY_AMT
	, DONATIONS_CY_AMT
	, DONATED_CY_AMT
	, LQDTNS_CY_AMT
	, ABSLT_CP_SHOP_CY_AMT
	, EVENT_5000_CY_AMT
	, MULTISAVING_CY_AMT
	, NBB_CY_AMT
	, OFC_SALES_CY_AMT

	, OFC_INC_LY_AMT
	, PRICE_CHNG_TO_0_LY_AMT
	, DONATIONS_LY_AMT
	, DONATED_LY_AMT
	, LQDTNS_LY_AMT
	, ABSLT_CP_SHOP_LY_AMT
	, EVENT_5000_LY_AMT
	, MULTISAVING_LY_AMT
	, NBB_LY_AMT
	, OFC_SALES_LY_AMT

	, ONHAND_COST_LY_AMT
	, ONHAND_COST_CY_AMT
	, INTRAN_COST_LY_AMT
	, INTRAN_COST_CY_AMT
	, INWH_COST_LY_AMT
	, INWH_COST_CY_AMT

	, CUST_LY_TOT_UNIT_QTY AS CUST_LY_QTY
	, CUST_CY_TOT_UNIT_QTY AS CUST_CY_QTY
	--, CUST_LY_EQ_UNIT_QTY
	--, CUST_CY_EQ_UNIT_QTY
	, TKT_LY_TOT_UNIT_QTY AS TKT_LY_QTY
	, TKT_CY_TOT_UNIT_QTY AS TKT_CY_QTY
	--, TKT_LY_EQ_UNIT_QTY
	--, TKT_CY_EQ_UNIT_QTY
	, TKT_SALE_LY_EQ_UNIT_AMT
	, TKT_SALE_CY_EQ_UNIT_AMT
	, TKT_SALE_LY_TOT_UNIT_AMT
	, TKT_SALE_CY_TOT_UNIT_AMT

	, VISIT_COMP_LY_DT
	, VISIT_CAL_LY_DT 
	, VISIT_CY_DT
FROM mx_reporting_tables.mx_reporting_wmt_tot_sales_trib
WHERE TRIBE_NBR=2;


DROP VIEW IF EXISTS mx_rep....._views.mx_reporting_wmt_tot_sales_trib_03;
CREATE VIEW mx_rep....._views.mx_reporting_wmt_tot_sales_trib_03
AS
SELECT 
     YEAR_NBR
    , MONTH_NBR
    , DAY_NBR
	, BANNER_CD
    , BANNER_DESC
    , CAT_NBR
    , CAT_DESC
    , TRIBE_NBR
    , TRIBE_DESC
    , SQUAD_NBR
    , SQUAD_DESC
    , DEPT_DESC
	, DEPT_NBR
	, SALE_TYPE_IND

	, SALE_CY_EQ_UNIT_AMT
	, SALE_CY_TOT_UNIT_AMT
	, SALE_CY_EQ_UNIT_QTY
	, SALE_CY_TOT_UNIT_QTY
	, SALE_COMP_LY_EQ_UNIT_AMT
	, SALE_COMP_LY_TOT_UNIT_AMT
	, SALE_COMP_LY_EQ_UNIT_QTY
	, SALE_COMP_LY_TOT_UNIT_QTY
	, SALE_CAL_LY_EQ_UNIT_AMT
	, SALE_CAL_LY_TOT_UNIT_AMT
	, SALE_CAL_LY_EQ_UNIT_QTY
	, SALE_CAL_LY_TOT_UNIT_QTY

    , SHIP_COST_CY_AMT
	, SHIP_COST_LY_AMT
    , SHIP_RTL_CY_AMT
	, SHIP_RTL_LY_AMT
    
    , RTN_COST_CY_AMT
	, RTN_COST_LY_AMT
	, RTN_RTL_CY_AMT
	, RTN_RTL_LY_AMT

	, DEC_CEDIS_CY_AMT
	, DEC_CEDIS_LY_AMT
	, THROW_AWAY_CY_AMT
	, THROW_AWAY_LY_AMT

	, OFC_INC_CY_AMT
	, PRICE_CHNG_TO_0_CY_AMT
	, DONATIONS_CY_AMT
	, DONATED_CY_AMT
	, LQDTNS_CY_AMT
	, ABSLT_CP_SHOP_CY_AMT
	, EVENT_5000_CY_AMT
	, MULTISAVING_CY_AMT
	, NBB_CY_AMT
	, OFC_SALES_CY_AMT

	, OFC_INC_LY_AMT
	, PRICE_CHNG_TO_0_LY_AMT
	, DONATIONS_LY_AMT
	, DONATED_LY_AMT
	, LQDTNS_LY_AMT
	, ABSLT_CP_SHOP_LY_AMT
	, EVENT_5000_LY_AMT
	, MULTISAVING_LY_AMT
	, NBB_LY_AMT
	, OFC_SALES_LY_AMT

	, ONHAND_COST_LY_AMT
	, ONHAND_COST_CY_AMT
	, INTRAN_COST_LY_AMT
	, INTRAN_COST_CY_AMT
	, INWH_COST_LY_AMT
	, INWH_COST_CY_AMT

	, CUST_LY_TOT_UNIT_QTY AS CUST_LY_QTY
	, CUST_CY_TOT_UNIT_QTY AS CUST_CY_QTY
	--, CUST_LY_EQ_UNIT_QTY
	--, CUST_CY_EQ_UNIT_QTY
	, TKT_LY_TOT_UNIT_QTY AS TKT_LY_QTY
	, TKT_CY_TOT_UNIT_QTY AS TKT_CY_QTY
	--, TKT_LY_EQ_UNIT_QTY
	--, TKT_CY_EQ_UNIT_QTY
	, TKT_SALE_LY_EQ_UNIT_AMT
	, TKT_SALE_CY_EQ_UNIT_AMT
	, TKT_SALE_LY_TOT_UNIT_AMT
	, TKT_SALE_CY_TOT_UNIT_AMT

	, VISIT_COMP_LY_DT
	, VISIT_CAL_LY_DT 
	, VISIT_CY_DT
FROM mx_reporting_tables.mx_reporting_wmt_tot_sales_trib
WHERE TRIBE_NBR=3;


DROP VIEW IF EXISTS mx_rep....._views.mx_reporting_wmt_tot_sales_trib_04;
CREATE VIEW mx_rep....._views.mx_reporting_wmt_tot_sales_trib_04
AS
SELECT 
     YEAR_NBR
    , MONTH_NBR
    , DAY_NBR
	, BANNER_CD
    , BANNER_DESC
    , CAT_NBR
    , CAT_DESC
    , TRIBE_NBR
    , TRIBE_DESC
    , SQUAD_NBR
    , SQUAD_DESC
    , DEPT_DESC
	, DEPT_NBR
	, SALE_TYPE_IND

	, SALE_CY_EQ_UNIT_AMT
	, SALE_CY_TOT_UNIT_AMT
	, SALE_CY_EQ_UNIT_QTY
	, SALE_CY_TOT_UNIT_QTY
	, SALE_COMP_LY_EQ_UNIT_AMT
	, SALE_COMP_LY_TOT_UNIT_AMT
	, SALE_COMP_LY_EQ_UNIT_QTY
	, SALE_COMP_LY_TOT_UNIT_QTY
	, SALE_CAL_LY_EQ_UNIT_AMT
	, SALE_CAL_LY_TOT_UNIT_AMT
	, SALE_CAL_LY_EQ_UNIT_QTY
	, SALE_CAL_LY_TOT_UNIT_QTY

    , SHIP_COST_CY_AMT
	, SHIP_COST_LY_AMT
    , SHIP_RTL_CY_AMT
	, SHIP_RTL_LY_AMT
    
    , RTN_COST_CY_AMT
	, RTN_COST_LY_AMT
	, RTN_RTL_CY_AMT
	, RTN_RTL_LY_AMT

	, DEC_CEDIS_CY_AMT
	, DEC_CEDIS_LY_AMT
	, THROW_AWAY_CY_AMT
	, THROW_AWAY_LY_AMT

	, OFC_INC_CY_AMT
	, PRICE_CHNG_TO_0_CY_AMT
	, DONATIONS_CY_AMT
	, DONATED_CY_AMT
	, LQDTNS_CY_AMT
	, ABSLT_CP_SHOP_CY_AMT
	, EVENT_5000_CY_AMT
	, MULTISAVING_CY_AMT
	, NBB_CY_AMT
	, OFC_SALES_CY_AMT

	, OFC_INC_LY_AMT
	, PRICE_CHNG_TO_0_LY_AMT
	, DONATIONS_LY_AMT
	, DONATED_LY_AMT
	, LQDTNS_LY_AMT
	, ABSLT_CP_SHOP_LY_AMT
	, EVENT_5000_LY_AMT
	, MULTISAVING_LY_AMT
	, NBB_LY_AMT
	, OFC_SALES_LY_AMT

	, ONHAND_COST_LY_AMT
	, ONHAND_COST_CY_AMT
	, INTRAN_COST_LY_AMT
	, INTRAN_COST_CY_AMT
	, INWH_COST_LY_AMT
	, INWH_COST_CY_AMT

	, CUST_LY_TOT_UNIT_QTY AS CUST_LY_QTY
	, CUST_CY_TOT_UNIT_QTY AS CUST_CY_QTY
	--, CUST_LY_EQ_UNIT_QTY
	--, CUST_CY_EQ_UNIT_QTY
	, TKT_LY_TOT_UNIT_QTY AS TKT_LY_QTY
	, TKT_CY_TOT_UNIT_QTY AS TKT_CY_QTY
	--, TKT_LY_EQ_UNIT_QTY
	--, TKT_CY_EQ_UNIT_QTY
	, TKT_SALE_LY_EQ_UNIT_AMT
	, TKT_SALE_CY_EQ_UNIT_AMT
	, TKT_SALE_LY_TOT_UNIT_AMT
	, TKT_SALE_CY_TOT_UNIT_AMT

	, VISIT_COMP_LY_DT
	, VISIT_CAL_LY_DT 
	, VISIT_CY_DT
FROM mx_reporting_tables.mx_reporting_wmt_tot_sales_trib
WHERE TRIBE_NBR=4;


DROP VIEW IF EXISTS mx_rep....._views.mx_reporting_wmt_tot_sales_trib_05;
CREATE VIEW mx_rep....._views.mx_reporting_wmt_tot_sales_trib_05
AS
SELECT 
     YEAR_NBR
    , MONTH_NBR
    , DAY_NBR
	, BANNER_CD
    , BANNER_DESC
    , CAT_NBR
    , CAT_DESC
    , TRIBE_NBR
    , TRIBE_DESC
    , SQUAD_NBR
    , SQUAD_DESC
    , DEPT_DESC
	, DEPT_NBR
	, SALE_TYPE_IND

	, SALE_CY_EQ_UNIT_AMT
	, SALE_CY_TOT_UNIT_AMT
	, SALE_CY_EQ_UNIT_QTY
	, SALE_CY_TOT_UNIT_QTY
	, SALE_COMP_LY_EQ_UNIT_AMT
	, SALE_COMP_LY_TOT_UNIT_AMT
	, SALE_COMP_LY_EQ_UNIT_QTY
	, SALE_COMP_LY_TOT_UNIT_QTY
	, SALE_CAL_LY_EQ_UNIT_AMT
	, SALE_CAL_LY_TOT_UNIT_AMT
	, SALE_CAL_LY_EQ_UNIT_QTY
	, SALE_CAL_LY_TOT_UNIT_QTY

    , SHIP_COST_CY_AMT
	, SHIP_COST_LY_AMT
    , SHIP_RTL_CY_AMT
	, SHIP_RTL_LY_AMT
    
    , RTN_COST_CY_AMT
	, RTN_COST_LY_AMT
	, RTN_RTL_CY_AMT
	, RTN_RTL_LY_AMT

	, DEC_CEDIS_CY_AMT
	, DEC_CEDIS_LY_AMT
	, THROW_AWAY_CY_AMT
	, THROW_AWAY_LY_AMT

	, OFC_INC_CY_AMT
	, PRICE_CHNG_TO_0_CY_AMT
	, DONATIONS_CY_AMT
	, DONATED_CY_AMT
	, LQDTNS_CY_AMT
	, ABSLT_CP_SHOP_CY_AMT
	, EVENT_5000_CY_AMT
	, MULTISAVING_CY_AMT
	, NBB_CY_AMT
	, OFC_SALES_CY_AMT

	, OFC_INC_LY_AMT
	, PRICE_CHNG_TO_0_LY_AMT
	, DONATIONS_LY_AMT
	, DONATED_LY_AMT
	, LQDTNS_LY_AMT
	, ABSLT_CP_SHOP_LY_AMT
	, EVENT_5000_LY_AMT
	, MULTISAVING_LY_AMT
	, NBB_LY_AMT
	, OFC_SALES_LY_AMT

	, ONHAND_COST_LY_AMT
	, ONHAND_COST_CY_AMT
	, INTRAN_COST_LY_AMT
	, INTRAN_COST_CY_AMT
	, INWH_COST_LY_AMT
	, INWH_COST_CY_AMT

	, CUST_LY_TOT_UNIT_QTY AS CUST_LY_QTY
	, CUST_CY_TOT_UNIT_QTY AS CUST_CY_QTY
	--, CUST_LY_EQ_UNIT_QTY
	--, CUST_CY_EQ_UNIT_QTY
	, TKT_LY_TOT_UNIT_QTY AS TKT_LY_QTY
	, TKT_CY_TOT_UNIT_QTY AS TKT_CY_QTY
	--, TKT_LY_EQ_UNIT_QTY
	--, TKT_CY_EQ_UNIT_QTY
	, TKT_SALE_LY_EQ_UNIT_AMT
	, TKT_SALE_CY_EQ_UNIT_AMT
	, TKT_SALE_LY_TOT_UNIT_AMT
	, TKT_SALE_CY_TOT_UNIT_AMT

	, VISIT_COMP_LY_DT
	, VISIT_CAL_LY_DT 
	, VISIT_CY_DT
FROM mx_reporting_tables.mx_reporting_wmt_tot_sales_trib
WHERE TRIBE_NBR=5;


DROP VIEW IF EXISTS mx_rep....._views.mx_reporting_wmt_tot_sales_trib_06;
CREATE VIEW mx_rep....._views.mx_reporting_wmt_tot_sales_trib_06
AS
SELECT 
     YEAR_NBR
    , MONTH_NBR
    , DAY_NBR
	, BANNER_CD
    , BANNER_DESC
    , CAT_NBR
    , CAT_DESC
    , TRIBE_NBR
    , TRIBE_DESC
    , SQUAD_NBR
    , SQUAD_DESC
    , DEPT_DESC
	, DEPT_NBR
	, SALE_TYPE_IND

	, SALE_CY_EQ_UNIT_AMT
	, SALE_CY_TOT_UNIT_AMT
	, SALE_CY_EQ_UNIT_QTY
	, SALE_CY_TOT_UNIT_QTY
	, SALE_COMP_LY_EQ_UNIT_AMT
	, SALE_COMP_LY_TOT_UNIT_AMT
	, SALE_COMP_LY_EQ_UNIT_QTY
	, SALE_COMP_LY_TOT_UNIT_QTY
	, SALE_CAL_LY_EQ_UNIT_AMT
	, SALE_CAL_LY_TOT_UNIT_AMT
	, SALE_CAL_LY_EQ_UNIT_QTY
	, SALE_CAL_LY_TOT_UNIT_QTY

    , SHIP_COST_CY_AMT
	, SHIP_COST_LY_AMT
    , SHIP_RTL_CY_AMT
	, SHIP_RTL_LY_AMT
    
    , RTN_COST_CY_AMT
	, RTN_COST_LY_AMT
	, RTN_RTL_CY_AMT
	, RTN_RTL_LY_AMT

	, DEC_CEDIS_CY_AMT
	, DEC_CEDIS_LY_AMT
	, THROW_AWAY_CY_AMT
	, THROW_AWAY_LY_AMT

	, OFC_INC_CY_AMT
	, PRICE_CHNG_TO_0_CY_AMT
	, DONATIONS_CY_AMT
	, DONATED_CY_AMT
	, LQDTNS_CY_AMT
	, ABSLT_CP_SHOP_CY_AMT
	, EVENT_5000_CY_AMT
	, MULTISAVING_CY_AMT
	, NBB_CY_AMT
	, OFC_SALES_CY_AMT

	, OFC_INC_LY_AMT
	, PRICE_CHNG_TO_0_LY_AMT
	, DONATIONS_LY_AMT
	, DONATED_LY_AMT
	, LQDTNS_LY_AMT
	, ABSLT_CP_SHOP_LY_AMT
	, EVENT_5000_LY_AMT
	, MULTISAVING_LY_AMT
	, NBB_LY_AMT
	, OFC_SALES_LY_AMT

	, ONHAND_COST_LY_AMT
	, ONHAND_COST_CY_AMT
	, INTRAN_COST_LY_AMT
	, INTRAN_COST_CY_AMT
	, INWH_COST_LY_AMT
	, INWH_COST_CY_AMT

	, CUST_LY_TOT_UNIT_QTY AS CUST_LY_QTY
	, CUST_CY_TOT_UNIT_QTY AS CUST_CY_QTY
	--, CUST_LY_EQ_UNIT_QTY
	--, CUST_CY_EQ_UNIT_QTY
	, TKT_LY_TOT_UNIT_QTY AS TKT_LY_QTY
	, TKT_CY_TOT_UNIT_QTY AS TKT_CY_QTY
	--, TKT_LY_EQ_UNIT_QTY
	--, TKT_CY_EQ_UNIT_QTY
	, TKT_SALE_LY_EQ_UNIT_AMT
	, TKT_SALE_CY_EQ_UNIT_AMT
	, TKT_SALE_LY_TOT_UNIT_AMT
	, TKT_SALE_CY_TOT_UNIT_AMT

	, VISIT_COMP_LY_DT
	, VISIT_CAL_LY_DT 
	, VISIT_CY_DT
FROM mx_reporting_tables.mx_reporting_wmt_tot_sales_trib
WHERE TRIBE_NBR=6;


DROP VIEW IF EXISTS mx_rep....._views.mx_reporting_wmt_tot_sales_trib_07;
CREATE VIEW mx_rep....._views.mx_reporting_wmt_tot_sales_trib_07
AS
SELECT 
     YEAR_NBR
    , MONTH_NBR
    , DAY_NBR
	, BANNER_CD
    , BANNER_DESC
    , CAT_NBR
    , CAT_DESC
    , TRIBE_NBR
    , TRIBE_DESC
    , SQUAD_NBR
    , SQUAD_DESC
    , DEPT_DESC
	, DEPT_NBR
	, SALE_TYPE_IND

	, SALE_CY_EQ_UNIT_AMT
	, SALE_CY_TOT_UNIT_AMT
	, SALE_CY_EQ_UNIT_QTY
	, SALE_CY_TOT_UNIT_QTY
	, SALE_COMP_LY_EQ_UNIT_AMT
	, SALE_COMP_LY_TOT_UNIT_AMT
	, SALE_COMP_LY_EQ_UNIT_QTY
	, SALE_COMP_LY_TOT_UNIT_QTY
	, SALE_CAL_LY_EQ_UNIT_AMT
	, SALE_CAL_LY_TOT_UNIT_AMT
	, SALE_CAL_LY_EQ_UNIT_QTY
	, SALE_CAL_LY_TOT_UNIT_QTY

    , SHIP_COST_CY_AMT
	, SHIP_COST_LY_AMT
    , SHIP_RTL_CY_AMT
	, SHIP_RTL_LY_AMT
    
    , RTN_COST_CY_AMT
	, RTN_COST_LY_AMT
	, RTN_RTL_CY_AMT
	, RTN_RTL_LY_AMT

	, DEC_CEDIS_CY_AMT
	, DEC_CEDIS_LY_AMT
	, THROW_AWAY_CY_AMT
	, THROW_AWAY_LY_AMT

	, OFC_INC_CY_AMT
	, PRICE_CHNG_TO_0_CY_AMT
	, DONATIONS_CY_AMT
	, DONATED_CY_AMT
	, LQDTNS_CY_AMT
	, ABSLT_CP_SHOP_CY_AMT
	, EVENT_5000_CY_AMT
	, MULTISAVING_CY_AMT
	, NBB_CY_AMT
	, OFC_SALES_CY_AMT

	, OFC_INC_LY_AMT
	, PRICE_CHNG_TO_0_LY_AMT
	, DONATIONS_LY_AMT
	, DONATED_LY_AMT
	, LQDTNS_LY_AMT
	, ABSLT_CP_SHOP_LY_AMT
	, EVENT_5000_LY_AMT
	, MULTISAVING_LY_AMT
	, NBB_LY_AMT
	, OFC_SALES_LY_AMT

	, ONHAND_COST_LY_AMT
	, ONHAND_COST_CY_AMT
	, INTRAN_COST_LY_AMT
	, INTRAN_COST_CY_AMT
	, INWH_COST_LY_AMT
	, INWH_COST_CY_AMT

	, CUST_LY_TOT_UNIT_QTY AS CUST_LY_QTY
	, CUST_CY_TOT_UNIT_QTY AS CUST_CY_QTY
	--, CUST_LY_EQ_UNIT_QTY
	--, CUST_CY_EQ_UNIT_QTY
	, TKT_LY_TOT_UNIT_QTY AS TKT_LY_QTY
	, TKT_CY_TOT_UNIT_QTY AS TKT_CY_QTY
	--, TKT_LY_EQ_UNIT_QTY
	--, TKT_CY_EQ_UNIT_QTY
	, TKT_SALE_LY_EQ_UNIT_AMT
	, TKT_SALE_CY_EQ_UNIT_AMT
	, TKT_SALE_LY_TOT_UNIT_AMT
	, TKT_SALE_CY_TOT_UNIT_AMT

	, VISIT_COMP_LY_DT
	, VISIT_CAL_LY_DT 
	, VISIT_CY_DT
FROM mx_reporting_tables.mx_reporting_wmt_tot_sales_trib
WHERE TRIBE_NBR=7;


DROP VIEW IF EXISTS mx_rep....._views.mx_reporting_wmt_tot_sales_trib_08;
CREATE VIEW mx_rep....._views.mx_reporting_wmt_tot_sales_trib_08
AS
SELECT 
     YEAR_NBR
    , MONTH_NBR
    , DAY_NBR
	, BANNER_CD
    , BANNER_DESC
    , CAT_NBR
    , CAT_DESC
    , TRIBE_NBR
    , TRIBE_DESC
    , SQUAD_NBR
    , SQUAD_DESC
    , DEPT_DESC
	, DEPT_NBR
	, SALE_TYPE_IND

	, SALE_CY_EQ_UNIT_AMT
	, SALE_CY_TOT_UNIT_AMT
	, SALE_CY_EQ_UNIT_QTY
	, SALE_CY_TOT_UNIT_QTY
	, SALE_COMP_LY_EQ_UNIT_AMT
	, SALE_COMP_LY_TOT_UNIT_AMT
	, SALE_COMP_LY_EQ_UNIT_QTY
	, SALE_COMP_LY_TOT_UNIT_QTY
	, SALE_CAL_LY_EQ_UNIT_AMT
	, SALE_CAL_LY_TOT_UNIT_AMT
	, SALE_CAL_LY_EQ_UNIT_QTY
	, SALE_CAL_LY_TOT_UNIT_QTY

    , SHIP_COST_CY_AMT
	, SHIP_COST_LY_AMT
    , SHIP_RTL_CY_AMT
	, SHIP_RTL_LY_AMT
    
    , RTN_COST_CY_AMT
	, RTN_COST_LY_AMT
	, RTN_RTL_CY_AMT
	, RTN_RTL_LY_AMT

	, DEC_CEDIS_CY_AMT
	, DEC_CEDIS_LY_AMT
	, THROW_AWAY_CY_AMT
	, THROW_AWAY_LY_AMT

	, OFC_INC_CY_AMT
	, PRICE_CHNG_TO_0_CY_AMT
	, DONATIONS_CY_AMT
	, DONATED_CY_AMT
	, LQDTNS_CY_AMT
	, ABSLT_CP_SHOP_CY_AMT
	, EVENT_5000_CY_AMT
	, MULTISAVING_CY_AMT
	, NBB_CY_AMT
	, OFC_SALES_CY_AMT

	, OFC_INC_LY_AMT
	, PRICE_CHNG_TO_0_LY_AMT
	, DONATIONS_LY_AMT
	, DONATED_LY_AMT
	, LQDTNS_LY_AMT
	, ABSLT_CP_SHOP_LY_AMT
	, EVENT_5000_LY_AMT
	, MULTISAVING_LY_AMT
	, NBB_LY_AMT
	, OFC_SALES_LY_AMT

	, ONHAND_COST_LY_AMT
	, ONHAND_COST_CY_AMT
	, INTRAN_COST_LY_AMT
	, INTRAN_COST_CY_AMT
	, INWH_COST_LY_AMT
	, INWH_COST_CY_AMT

	, CUST_LY_TOT_UNIT_QTY AS CUST_LY_QTY
	, CUST_CY_TOT_UNIT_QTY AS CUST_CY_QTY
	--, CUST_LY_EQ_UNIT_QTY
	--, CUST_CY_EQ_UNIT_QTY
	, TKT_LY_TOT_UNIT_QTY AS TKT_LY_QTY
	, TKT_CY_TOT_UNIT_QTY AS TKT_CY_QTY
	--, TKT_LY_EQ_UNIT_QTY
	--, TKT_CY_EQ_UNIT_QTY
	, TKT_SALE_LY_EQ_UNIT_AMT
	, TKT_SALE_CY_EQ_UNIT_AMT
	, TKT_SALE_LY_TOT_UNIT_AMT
	, TKT_SALE_CY_TOT_UNIT_AMT

	, VISIT_COMP_LY_DT
	, VISIT_CAL_LY_DT 
	, VISIT_CY_DT
FROM mx_reporting_tables.mx_reporting_wmt_tot_sales_trib
WHERE TRIBE_NBR=8;
