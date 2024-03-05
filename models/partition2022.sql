select
*
from {{ref('joins')}}
where date_part_year(order_date) = 2022