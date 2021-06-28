from .config import default_end_time

transform_dim_orders_sql = """
WITH stg_orders_with_row_number AS (
     SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY id ORDER BY event_time) AS rn
      FROM stg_orders
), earliest_orders AS (
    SELECT * FROM stg_orders_with_row_number WHERE rn = 1
)
UPDATE dim_orders
SET end_time = '{{ ts }}'
FROM earliest_orders
WHERE earliest_orders.id = dim_orders.order_id
AND '{{ ts }}' >= dim_orders.start_time AND '{{ ts }}' < dim_orders.end_time
AND (earliest_orders.status <> dim_orders.status);

WITH ordered_stg_orders as (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY id,status ORDER BY event_time) rn,
    LAST_VALUE(event_time) OVER(PARTITION BY id,status ORDER BY event_time) last_event_time
    FROM stg_orders
    order by id, event_time
), distinct_stg_orders as (
    select id, status, event_time, 
    event_time as start_time,
    last_event_time as end_time,
    ROW_NUMBER() OVER(PARTITION BY id ORDER BY event_time) rn
    from ordered_stg_orders where ordered_stg_orders.rn = 1
) ,new_records as (select 
    current_orders.id,
    current_orders.status,
    current_orders.event_time,
    current_orders.event_time as start_time,
    coalesce(next_orders.event_time, '%s') as end_time
from distinct_stg_orders current_orders left join distinct_stg_orders next_orders
on current_orders.id = next_orders.id and current_orders.rn = next_orders.rn -  1
) 
INSERT INTO dim_orders(order_id, status, event_time, processed_time, start_time, end_time)
SELECT id AS order_id,
    status,
    event_time,
    '{{ ts }}',
    start_time,
    end_time
FROM new_records
""" % default_end_time

transform_fact_orders_created_sql = """
INSERT INTO fact_orders_created(order_id, product_id, created_time, created_date_id, amount, total_price, processed_time)
SELECT stg_orders.id AS order_id,
    product_id,
    event_time as created_time,
    dim_dates.id as created_date_id,
    amount,
    total_price,
    '{{ ts }}'
FROM stg_orders
INNER JOIN dim_dates on dim_dates.datum = date(event_time)
ON CONFLICT(order_id) DO NOTHING
"""
