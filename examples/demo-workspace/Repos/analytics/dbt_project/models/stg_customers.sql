{{ config(materialized='view') }}

select
    customer_id,
    case segment_code
        when 'E' then 'Enterprise'
        when 'M' then 'Mid-market'
        when 'S' then 'Small business'
        when 'C' then 'Consumer'
        when 'G' then 'Government'
        else 'Unknown'
    end as segment,
    case
        when state = 'NSW' then 'New South Wales'
        when state = 'QLD' then 'Queensland'
        when state = 'VIC' then 'Victoria'
        when state = 'SA' then 'South Australia'
        when state = 'WA' then 'Western Australia'
        when state = 'TAS' then 'Tasmania'
        when state = 'NT' then 'Northern Territory'
        when state = 'ACT' then 'ACT'
    end as state_name
from {{ ref('raw_customers') }}
