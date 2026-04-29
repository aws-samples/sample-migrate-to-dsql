drop table if exists sales.orders;
drop table if exists sales.customers;
drop schema if exists sales;

create schema sales;

create table sales.customers
(
    id          uuid primary key,
    old_id      int,
    username    varchar(50),
    first_name  varchar(50),
    last_name   varchar(50)
);

create index async customers_oid on sales.customers (old_id);


create table sales.orders
(
    id  uuid primary key,
    old_id int,
    customer_id uuid,
    order_date date,
    order_timestamp timestamp,
    product_details text,
    quantity int,
    unit_cost numeric (6, 2),
    unit_weight real
);

create index async orders_oid on sales.orders (old_id);
create index async orders_customer on sales.orders (customer_id);
