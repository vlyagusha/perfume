begin;

drop table products;

drop table prices;

create table products
(
    id           serial,
    contractor   text                  not null,
    code         text                  not null,
    title        text                  not null,
    is_available boolean default false not null
);

alter table products
    owner to vlyagusha;

create unique index uq_products_code_title
    on products (code, title);

create index idx_products_title_trgm
    on products using gin (title gin_trgm_ops);

create table prices
(
    id         serial,
    code       text                    not null,
    title      text                    not null,
    price_usd  double precision,
    price_rub  double precision,
    updated_at timestamp default now() not null
);

alter table prices
    owner to vlyagusha;

create index idx_prices_code_title
    on prices (code, title);

create unique index uq_prices_price_usd
    on prices (code, title, price_usd, updated_at)
    where (price_usd IS NOT NULL);

create unique index uq_prices_price_rub
    on prices (code, title, price_rub, updated_at)
    where (price_rub IS NOT NULL);

create index idx_prices_title_trgm
    on prices using gin (title gin_trgm_ops);

commit;
