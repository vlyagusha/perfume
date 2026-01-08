begin;

drop table prices;

create table prices
(
    id           serial,
    contractor   text                    not null,
    code         text                    not null,
    title        text                    not null,
    price_usd    double precision,
    price_rub    double precision,
    updated_at   timestamp default now() not null
);

alter table prices
    owner to vlyagusha;

create index idx_prices_title_trgm
    on prices using gin (title gin_trgm_ops);

commit;
