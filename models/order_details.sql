select 
orders.order_id,
orders.product_id,
orders.unit_price,
orders.quantity,
products.product_name,
products.supplier_id,
products.category_id,
orders.unit_price*orders.quantity as total, 
(products.unit_price * orders.quantity) - total as discount
from {{source('sources','order_details')}} as orders
left join {{source('sources','products')}} as products
on (orders.product_id = products.product_id)