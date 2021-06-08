/*
{{
  config(
    materialized='incremental',
    update_columns=['clnScenarioKey','clnLocationEntityKey','clnProductGroupKey','clnCurrencyKey']
  )
}}

/* Calling fnClnBaseRevenue for Updating Clean Columns */
--	{{fnClnBaseRevenue("'dataset_1011'")}};

{% macro fnClnBaseRevenue(DatasetKey) %}
--Step 1: clnScenarioKey populated from lkpD365Transformation
               UPDATE "UDP_TRANSFORM_DEV"."DMR"."BASEREVENUE"
                SET 
                    clnScenarioKey = lkp.targetValue 
                FROM "UDP_TRANSFORM_DEV"."DMR"."BASEREVENUE" base 
                join "UDP_TRANSFORM_DEV"."DMR"."lkpd365transformation" lkp
                ON lkp.sourceValue = base.rawScenarioKey
                WHERE base.udpDatasetKey = {{DatasetKey}};

--Step 2: clnlocationKey populated from lkpEntityCompanyDepartment and  lkpEntityCompany

                update "UDP_TRANSFORM_DEV"."DMR"."BASEREVENUE"
                set 
                    clnLocationEntityKey = coalesce( lkpECD.EntityKey,lkpEC.EntityKey) 
                from "UDP_TRANSFORM_DEV"."DMR"."BASEREVENUE"  base  
                left join "UDP_TRANSFORM_DEV"."DMR"."lkpentitycompanydepartment" lkpECD 
                on base.rawDepartmentKey = lkpECD.DepartmentCode and base.rawCompanyKey = lkpECD.CompanyCode
                left join "UDP_TRANSFORM_DEV"."DMR"."lkpentitycompany" lkpEC 
                on lkpEC.CompanyCode = base.rawCompanyKey
                where base.udpDatasetKey = {{DatasetKey}};

--Step 3: clnCurrencyKey populated from baseCurrency                
                update "UDP_TRANSFORM_DEV"."DMR"."BASEREVENUE"
                set 
                    clnCurrencyKey = cur.Currencykey                
                from "UDP_TRANSFORM_DEV"."DMR"."BASEREVENUE"  base
                left join "UDP_TRANSFORM_DEV"."DMR"."baseCurrency" cur
                on base.rawCurrencyKey = cur.SourceCurrency
                and cur.Targetcurrency ='USD'
                and base.rawDateKey between cur.Startdate and cur.Enddate
                where base.udpDatasetKey = {{DatasetKey}};

--Step 4: clnProductGroupKey populated from dmr.baseProductGroup

                update "UDP_TRANSFORM_DEV"."DMR"."BASEREVENUE"
                set 
                    clnProductGroupKey = PG.ProductGroupKey            
                from "UDP_TRANSFORM_DEV"."DMR"."BASEREVENUE"  base
                left join "UDP_TRANSFORM_DEV"."DMR"."baseProductGroup"  PG  
                on base.rawProductGroupKey = PG.D365ProductClassCode
                where base.udpDatasetKey = {{DatasetKey}};
            
{% endmacro %}

