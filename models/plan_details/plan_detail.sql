{{
  config(
    materialized = "table",
    alias = "plan_detail",
    tags = ["graphpad", "subscriptions","plan"]
  )
}}

with graphpad_plan_detail as (
SELECT
subscription_id,
  case when sb.planId in ( 1, 4 ) then 'Academic' when sb.planId in ( 2, 5 ) then 'Corporate' else 'Monthly' end as planSegment,
  case when sb.planId in ( 1, 2 ) then 'Group' else 'Personal' end as planType
FROM
{{ source('graphpad_db','subscriptions')}}--graphpad_db.public.subscriptions
)-- as-- sb on s.subscriptionid=sb.subscriptionid

select *
from graphpad_plan_detail


--include the snapgene data from sources
