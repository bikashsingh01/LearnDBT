{{ config(
materialized='view'
) }}
    SELECT AccountDisplayValue AS GeneralLedger,
     split_part(AccountDisplayValue, '-',1) AS col1--AccountNumber
    , split_part(AccountDisplayValue, '-', 2) AS col2 --Department
    , split_part(AccountDisplayValue, '-', 3) AS col3 --CostCenter
    , split_part(AccountDisplayValue, '-', 4) AS col4 --BusinessUnit
    , split_part(AccountDisplayValue, '-', 5) AS col5 --interCompany
    , split_part(AccountDisplayValue, '-', 6) AS col6 --Product
    , split_part(AccountDisplayValue, '-', 7) AS col7 --ProductClass
    , split_part(AccountDisplayValue, '-', 8) AS col8 --Project
    , split_part(AccountDisplayValue, '-', 9) AS col9 --ProjectClass
from "UDP_STAGE_DEV"."DATASET_1011"."generaljournalaccountentryglobalstaging"




