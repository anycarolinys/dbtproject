with prod as (
    select 
    prod.product_name,
    prod.unit_price,
    prod.product_id,
    cat.category_name,
    sup.company_name
    from {{source('sources','products')}} as prod
    left join {{source('sources','suppliers')}} as sup
    on prod.supplier_id = sup.supplier_id
    left join {{source('sources','categories')}} as cat
    on prod.category_id = cat.category_id
), orddetai as (
    select 
    prod.*,
    order_det.order_id,
    order_det.quantity,
    order_det.discount
    from {{ref('order_details')}} as order_det
    left join prod
    on order_det.product_id = prod.product_id
), ordrs as (
    select
    ord.order_id,
    ord.order_date,
    ord.shipped_date,
    cust.company_name as customer_name, 
    emp.name as employee_name,
    emp.age,
    emp.length_of_service,
    ship.company_name as shipper_name
    from {{source('sources','orders')}} as ord
    left join {{ref('customers')}} as cust
    on ord.customer_id = cust.customer_id
    left join {{ref('employees')}} as emp
    on ord.employee_id = emp.employee_id
    left join {{source('sources','shippers')}} as ship
    on ord.ship_via = ship.shipper_id
), finaljoin as (
    select 
    orddetai.*,
    ordrs.order_date,
    ordrs.customer_name,
    ordrs.employee_name,
    ordrs.length_of_service
    from orddetai
    inner join ordrs
    on orddetai.order_id = ordrs.order_id
)


select * from finaljoin
-- select count(*) from {{ref('order_details')}}


