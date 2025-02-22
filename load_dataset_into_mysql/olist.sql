-- CREATE DATABASE olist;
-- USE olist;

-- Tắt kiểm tra khóa ngoại để tránh lỗi khi DROP TABLE
SET FOREIGN_KEY_CHECKS = 0;

-- Xóa bảng theo thứ tự hợp lý (bảng con trước, bảng cha sau)
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS order_reviews;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS sellers;
DROP TABLE IF EXISTS geolocation;
DROP TABLE IF EXISTS product_category_name_translation;

-- Bật lại kiểm tra khóa ngoại
SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE product_category_name_translation (
    product_category_name varchar(64),
    product_category_name_english varchar(64),
    PRIMARY KEY (product_category_name)
);

CREATE TABLE geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city NVARCHAR(64),
    geolocation_state VARCHAR(64)
);

CREATE TABLE sellers (
    seller_id varchar(64),
    seller_zip_code_prefix INT,
    seller_city varchar(64),
    seller_state varchar(64),
    PRIMARY KEY (seller_id)
);

CREATE TABLE customers (
    customer_id varchar(64),
    customer_unique_id varchar(32),
    customer_zip_code_prefix INT,
    customer_city varchar(64),
    customer_state varchar(64),
    PRIMARY KEY (customer_id)
);

CREATE TABLE products (
    product_id varchar(64),
    product_category_name varchar(64),
    product_name_length INT,
    product_description_length FLOAT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT,
    PRIMARY KEY (product_id),
    FOREIGN KEY (product_category_name) REFERENCES product_category_name_translation(product_category_name)
);

CREATE TABLE orders (
    order_id varchar(64),
    customer_id varchar(64),
    order_status varchar(32),
    order_purchase_timestamp DATE,
    order_approved_at DATE,
    order_delivered_carrier_date DATE,
    order_delivered_customer_date DATE,
    order_estimated_delivery_date DATE,
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
    order_id varchar(64),
    order_item_id INT,
    product_id varchar(64),
    seller_id varchar(64),
    shipping_limit_date DATE,
    price FLOAT,
    freight_value FLOAT,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);

CREATE TABLE payments (
    order_id varchar(64),
    payment_sequential INT,
    payment_type varchar(32),
    payment_installments FLOAT,
    payment_value FLOAT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE order_reviews (
    review_id varchar(64),
    order_id varchar(64),
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATE,
    review_answer_timestamp DATE,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- SHOW VARIABLES LIKE 'secure_file_priv'
