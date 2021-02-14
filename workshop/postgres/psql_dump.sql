CREATE TABLE IF NOT EXISTS product (
    upc           VARCHAR(300),
    description   VARCHAR(300),
    manufacturer  VARCHAR(100),
    category      VARCHAR(100),
    sub_category  VARCHAR(100),
    product_size  DECIMAL(38, 2)
);

INSERT INTO product VALUES ('12345', 'This is some product', 'test', 'xxxx', 'yyyyy', 12.22);

CREATE TABLE IF NOT EXISTS store (
    store_id                 INT,
    store_name               VARCHAR(100),
    address_city_name        VARCHAR(300),
    address_state_prov_code  VARCHAR(2),
    msa_code                 VARCHAR(100),
    seg_value_name           VARCHAR(100),
    parking_space_qty        DECIMAL(38, 2),
    sales_area_size_num      INT,
    avg_weekly_baskets       DECIMAL(38, 2)
);

CREATE TABLE IF NOT EXISTS transaction (
    week_end_date VARCHAR(40),
    store_num     INT,
    upc           VARCHAR(100),
    units         INT,
    visits        INT,
    hhs           INT,
    spend         DECIMAL(38, 2),
    price         DECIMAL(38, 2),
    base_price    DECIMAL(38, 2),
    feature       INT,
    display       INT,
    tpr_only      INT
);