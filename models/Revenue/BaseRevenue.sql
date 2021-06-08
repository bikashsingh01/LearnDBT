{{ config(
	materialized='table',
	pre_hook = spLogJobStart(1,"'dmr.spRevenueLoadD365'","'dmr'","'Revenue'","'Load'","'dataset_1011'","'D365'","'amc'"),
   	post_hook = fnClnBaseRevenue("'dataset_1011'"),
) }}
 
select 
                fnSplit.Column2 as rawDepartmentKey
                ,fnSplit.Column7 as rawProductGroupKey
                ,stg.LEDGERNAME as rawCompanyKey
                ,'Actual' as rawScenarioKey
                ,null as clnProductGroupKey
                ,null as clnScenarioKey
                ,null as clnLocationEntityKey
                ,cast(stg.AccountingDate as date) as rawDateKey
                ,Cmp.CompanyCurrency as rawCurrencykey
                ,null as clnCurrencyKey
                ,Sum((stg.ACCOUNTINGCURRENCYAMOUNT)*(-1)) as rawRevenue
                ,'dataset_1011' as udpDatasetKey
                ,'D365' udpSystemKey
                ,'amc' as udpEntityKey
                ,0 as udpIsDeleted
                ,'' as udpDeleteCode
                from "UDP_STAGE_DEV"."DATASET_1011"."generaljournalaccountentryglobalstaging" stg
                --cross apply  dmr.fnSplitInColumns(stg.AccountDisplayValue,'-') fnSplit 
                left join ({{fnSplitInColumns('"UDP_STAGE_DEV"."DATASET_1011"."generaljournalaccountentryglobalstaging"','AccountDisplayValue')}})
                 fnSplit on fnSplit.AccountDisplayValue=stg.AccountDisplayValue
                left join "UDP_STAGE_DEV"."DATASET_1011"."MAINACCOUNTSTAGING" MA on MA.MAINACCOUNTID = fnSplit.Column1 
                left join "UDP_TRANSFORM_DEV"."DMR"."baseCompany" Cmp on Cmp.CompanyKey = stg.LEDGERNAME
                where stg.AccountDisplayValue like '4%'
                and stg.AccountingDate<= getdate() and Year(stg.AccountingDate)>2017
                and stg.PostingLayer = 0
                group by fnSplit.Column2,fnSplit.Column7,stg.LEDGERNAME,cast(stg.AccountingDate as date),Cmp.CompanyCurrency
            union all
 
                select 
                fnSplit.Column2 as rawDepartmentKey
                ,fnSplit.Column7 as rawProductGroupKey
                ,stg.LegalEntityId as rawCompanyKey
                ,BUDGETMODELID as rawScenarioKey
                ,null as clnProductGroupKey
                ,null as clnScenarioKey
                ,null as clnLocationEntityKey
                ,cast(stg.DATE as Date) as rawDateKey
                ,Cmp.CompanyCurrency as rawCurrencykey
                ,null as clnCurrencyKey
                ,Sum((stg.ACCOUNTINGCURRENCYAMOUNT) * (-1)) as rawRevenue
                ,'dataset_1011' as udpDatasetKey
                ,'D365' udpSystemKey
                ,'amc' as udpEntityKey
                ,0 as udpIsDeleted
                ,'' as udpDeleteCode
                from "UDP_STAGE_DEV"."DATASET_1011"."amcglobalbudgetregisterentrystaging" stg
                 --cross apply dmr.fnSplitInColumns (stg.DIMENSIONDISPLAYVALUE,'-') fnSplit
                left join ({{fnSplitInColumns('"UDP_STAGE_DEV"."DATASET_1011"."amcglobalbudgetregisterentrystaging"','DIMENSIONDISPLAYVALUE')}}) fnSplit 
                on fnSplit.DIMENSIONDISPLAYVALUE=stg.DIMENSIONDISPLAYVALUE
                left join "UDP_STAGE_DEV"."DATASET_1011"."MAINACCOUNTSTAGING" MA on MA.MAINACCOUNTID = fnSplit.Column1 
                left join "UDP_TRANSFORM_DEV"."DMR"."baseCompany" Cmp on Cmp.CompanyKey = stg.LegalEntityId
                where stg.DIMENSIONDISPLAYVALUE like '4%' and BUDGETMODELID not like 'FCST%'
                and stg.Date <= getdate() and Year(stg.Date)>2017
                group by fnSplit.Column2,fnSplit.Column7,stg.LegalEntityId,BUDGETMODELID,cast(stg.DATE as Date),Cmp.CompanyCurrency
 