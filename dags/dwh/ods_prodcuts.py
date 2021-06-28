from .config import default_end_time

transform_dim_products_sql = """
UPDATE dim_products
    SET end_time = '{{ ts }}'
FROM stg_products
WHERE 
    stg_products.id = dim_products.id
    AND '{{ ts }}' >= dim_products.start_time AND '{{ ts }}' < dim_products.end_time
    AND (dim_products.title <> stg_products.title OR dim_products.category <> stg_products.category OR dim_products.price <> stg_products.price);

WITH sc as (
    SELECT * FROM dim_products
    WHERE '{{ ts }}' >= dim_products.start_time and '{{ ts }}' < dim_products.end_time
)
INSERT INTO dim_products(id, title, category, price, processed_time, start_time, end_time)
SELECT stg_products.id as id,
    stg_products.title,
    stg_products.category,
    stg_products.price,
    '{{ ts }}' AS processed_time,
    '{{ ts }}' AS start_time,
    '%s' AS end_time
FROM stg_products
WHERE stg_products.id NOT IN (select id from sc);
""" % default_end_time
