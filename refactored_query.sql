select * from {{ref{'stg_orders'}}}
where state != 'canceled' 
and extract(year from completed_at) < '2018' 
and email not like '%company.com' 
group by completed_at_date) 
 
),
 
order_items as (

select * from {{ref{'stg_order_items'}}} 

),

order_totals as ( 
select 
id, 
number, 
completed_at,  
completed_at::date as completed_at_date, 
sum(total) as net_rev,
sum(item_total) as gross_rev, 
count(id) as order_count

from orders
group by 1,2,3
),

orders_complete as (
select 
order_id, 
completed_at::date as completed_at_date, 
sum(quantity) as qty 
from order_items 
left join orders (using order_id)
where (is_cancelled_order = false OR is_pending_order != true) 
group by 1,2
),

joined as (
select 
 order_totals.completed_at_date, 
 order_totals.gross_rev,
 order_totals.net_rev, 
 orders_complete.qty, 
 order_totals.order_count as orders, 
 orders_complete.qty/a.distinct_orders as avg_unit_per_order, 
 order_totals.Gross_Rev/a.distinct_orders as aov_gross, 
 order_totals.Net_Rev/a.distinct_orders as aov_net
 from order_totals
 join orders_complete (using order_id)
where order_totals.net_rev >= 150000
order by completed_at_date desc
)