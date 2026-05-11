CREATE TABLE IF NOT EXISTS raw.olist_orders (
    order_id VARCHAR,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_purchase_timestamp VARCHAR,
    order_approved_at VARCHAR,
    order_delivered_carrier_date VARCHAR,
    order_delivered_customer_date VARCHAR,
    order_estimated_delivery_date VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.olist_customers (
    customer_id VARCHAR,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix VARCHAR,
    customer_city VARCHAR,
    customer_state VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.olist_order_items (
    order_id VARCHAR,
    order_item_id VARCHAR,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date VARCHAR,
    price VARCHAR,
    freight_value VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.olist_order_payments (
    order_id VARCHAR,
    payment_sequential VARCHAR,
    payment_type VARCHAR,
    payment_installments VARCHAR,
    payment_value VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.olist_order_reviews (
    review_id VARCHAR,
    order_id VARCHAR,
    review_score VARCHAR,
    review_comment_title VARCHAR,
    review_comment_message VARCHAR,
    review_creation_date VARCHAR,
    review_answer_timestamp VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.olist_geolocation (
    geolocation_zip_code_prefix VARCHAR,
    geolocation_lat VARCHAR,
    geolocation_lng VARCHAR,
    geolocation_city VARCHAR,
    geolocation_state VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.olist_products (
    product_id VARCHAR,
    product_category_name VARCHAR,
    product_name_lenght VARCHAR,
    product_description_lenght VARCHAR,
    product_photos_qty VARCHAR,
    product_weight_g VARCHAR,
    product_length_cm VARCHAR,
    product_height_cm VARCHAR,
    product_width_cm VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.olist_sellers (
    seller_id VARCHAR,
    seller_zip_code_prefix VARCHAR,
    seller_city VARCHAR,
    seller_state VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.product_category_name_translation (
    product_category_name VARCHAR,
    product_category_name_english VARCHAR
);

