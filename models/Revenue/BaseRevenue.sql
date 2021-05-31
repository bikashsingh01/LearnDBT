    {{ config(
materialized='table'
) }}
    
    select 
				fnSplit.Col2 as rawDepartmentKey
				,fnSplit.Col7 as rawProductGroupKey
				,stg.LEDGERNAME as rawCompanyKey
				,'Actual' as rawScenarioKey
				,null as clnProductGroupKey
				,null as clnScenarioKey
				,null as clnLocationEntityKey
				--,cast(stg.AccountingDate as varchar(10)) as rawDateKey
                ,cast(stg.AccountingDate as Date) as rawDateKey
				,Cmp.CompanyCurrency as rawCurrencykey
				,null as clnCurrencyKey
				,Sum((stg.ACCOUNTINGCURRENCYAMOUNT)*(-1)) as rawRevenue
				,'dataset_1011' as udpDatasetKey
				,'D365' udpSystemKey
				,'amc' as udpEntityKey
				,0 as udpIsDeleted
				,'' as udpDeleteCode
				from "UDP_STAGE_DEV"."DATASET_1011"."generaljournalaccountentryglobalstaging" stg
			--	cross apply  dmr.fnSplitInColumnsActual(stg.AccountDisplayValue,'-') fnSplit 
				left join "UDP_TRANSFORM_DEV"."DMR"."FNSPLITINCOLUMNSACTUAL"  fnSplit on fnSplit.GeneralLedger=stg.AccountDisplayValue
				left join "UDP_STAGE_DEV"."DATASET_1011"."MAINACCOUNTSTAGING" MA on MA.MAINACCOUNTID = fnSplit.Col1 
				left join "UDP_TRANSFORM_DEV"."DMR"."baseCompany"  Cmp on Cmp.CompanyKey = stg.LEDGERNAME
				where stg.AccountDisplayValue like '4%'
				and stg.AccountingDate<= getdate() and Year(stg.AccountingDate)>2020
				and stg.PostingLayer = 0
				group by fnSplit.Col2,fnSplit.Col7,stg.LEDGERNAME,cast(stg.AccountingDate as Date) ,Cmp.CompanyCurrency
				union all

				select 
				fnSplit.Col2 as rawDepartmentKey
				,fnSplit.Col7 as rawProductGroupKey
				,stg.LegalEntityId as rawCompanyKey
				,BUDGETMODELID as rawScenarioKey
				,null as clnProductGroupKey
				,null as clnScenarioKey
				,null as clnLocationEntityKey
				,cast(stg.Date as Date) as rawDateKey
				,Cmp.CompanyCurrency as rawCurrencykey
				,null as clnCurrencyKey
				,Sum((stg.ACCOUNTINGCURRENCYAMOUNT) * (-1)) as rawRevenue
				,'dataset_1011' as udpDatasetKey
				,'D365' udpSystemKey
				,'amc' as udpEntityKey
				,0 as udpIsDeleted
				,'' as udpDeleteCode
				from "UDP_STAGE_DEV"."DATASET_1011"."amcglobalbudgetregisterentrystaging" stg
			--	  cross apply dmr.fnSplitInColumns (stg.DIMENSIONDISPLAYVALUE,'-') fnSplit UDP_TRANSFORM_DEV.DMR.FnSplitInColumnsBudget
				left join "UDP_TRANSFORM_DEV"."DMR"."FNSPLITINCOLUMNSBUDGET"  fnSplit on fnSplit.GeneralLedger=stg.DIMENSIONDISPLAYVALUE
				left join "UDP_STAGE_DEV"."DATASET_1011"."MAINACCOUNTSTAGING" MA on MA.MAINACCOUNTID = fnSplit.Col1 
				left join "UDP_TRANSFORM_DEV"."DMR"."baseCompany" Cmp on Cmp.CompanyKey = stg.LegalEntityId
				where stg.DIMENSIONDISPLAYVALUE like '4%' and BUDGETMODELID not like 'FCST%'
				and stg.Date <= getdate() and Year(stg.Date)>2017
				group by fnSplit.Col2,fnSplit.Col7,stg.LegalEntityId,BUDGETMODELID,cast(stg.Date as Date),Cmp.CompanyCurrency