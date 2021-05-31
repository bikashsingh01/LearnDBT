    {{ config(
materialized='table'
) }}
Select * from "UDP_TRANSFORM_DEV"."DMR"."BASEREVENUE"