{{ config(
materialized='view'
) }}

SELECT DimensionDisplayValue as GeneralLedger,
     split_part(DimensionDisplayValue, '-',1) AS col1--AccountNumber
    , split_part(DimensionDisplayValue, '-', 2) AS col2 --Department
    , split_part(DimensionDisplayValue, '-', 3) AS col3 --CostCenter
    , split_part(DimensionDisplayValue, '-', 4) AS col4 --BusinessUnit
    , split_part(DimensionDisplayValue, '-', 5) AS col5 --interCompany
    , split_part(DimensionDisplayValue, '-', 6) AS col6 --Product
    , split_part(DimensionDisplayValue, '-', 7) AS col7 --ProductClass
    , split_part(DimensionDisplayValue, '-', 8) AS col8 --Project
    , split_part(DimensionDisplayValue, '-', 9) AS col9 --ProjectClass
FROM "UDP_STAGE_DEV"."DATASET_1011"."amcglobalbudgetregisterentrystaging"
