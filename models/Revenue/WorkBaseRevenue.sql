/*
{{
  config(
    materialized='incremental',
    update_columns=['clnScenarioKey','clnLocationEntityKey','clnProductGroupKey','clnCurrencyKey']
  )
}}
*/

/* Calling fnClnBaseRevenue for Updating Clean Columns */
	{{fnClnBaseRevenue("'dataset_1011'")}};