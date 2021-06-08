{{ config(
	materialized='table',
    schema='DATAMART_DMR',

) }}
--Select * from  DMR.BASEREVENUE
Select * from  {{ref ('BaseRevenue')}}