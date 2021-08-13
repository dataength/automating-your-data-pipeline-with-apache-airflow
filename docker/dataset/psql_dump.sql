CREATE TABLE IF NOT EXISTS product (
    upc           VARCHAR(300),
    description   VARCHAR(300),
    manufacturer  VARCHAR(100),
    category      VARCHAR(100),
    sub_category  VARCHAR(100),
    product_size  VARCHAR(100)
);
COPY product FROM '/mnt/data/products.csv' DELIMITER ',' CSV;

CREATE TABLE IF NOT EXISTS store (
    store_id                 VARCHAR(100),
    store_name               VARCHAR(100),
    address_city_name        VARCHAR(300),
    address_state_prov_code  VARCHAR(2),
    msa_code                 VARCHAR(100),
    seg_value_name           VARCHAR(100),
    parking_space_qty        VARCHAR(100),
    sales_area_size_num      VARCHAR(100),
    avg_weekly_baskets       VARCHAR(100)
);
COPY store FROM '/mnt/data/stores.csv' DELIMITER ',' CSV;

CREATE TABLE IF NOT EXISTS transaction (
    week_end_date VARCHAR(40),
    store_num     VARCHAR(100),
    upc           VARCHAR(100),
    units         VARCHAR(100),
    visits        VARCHAR(100),
    hhs           VARCHAR(100),
    spend         VARCHAR(100),
    price         VARCHAR(100),
    base_price    VARCHAR(100),
    feature       VARCHAR(100),
    display       VARCHAR(100),
    tpr_only      VARCHAR(100)
);
COPY transaction FROM '/mnt/data/transactions.csv' DELIMITER ',' CSV;
