BEGIN;

CREATE TABLE IF NOT EXISTS indx_prices_hyper (
  time TIMESTAMPTZ not null,
  indx_address varchar not null,
  price float
);

SELECT create_hypertable('indx_prices_hyper', 'time');

CREATE TABLE IF NOT EXISTS indx_descriptions (
  address varchar not null,
  description text not null
);

CREATE TABLE IF NOT EXISTS indx_eth_historical_price (
  day varchar not null,
  eth_price float not null
);

COMMIT;

