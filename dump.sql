begin;

create extension if not exists pg_trgm;

drop table if exists raw_prices;

create table raw_prices
(
    contractor   text not null,
    code         text not null,
    title        text not null,
    price_usd    double precision,
    price_rub    double precision,
    updated_at   timestamp default now() not null
);

alter table raw_prices owner to perfume_user;

create index idx_raw_prices_title_trgm on raw_prices using gin (title gin_trgm_ops);

commit;
