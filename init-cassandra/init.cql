CREATE KEYSPACE stockdata WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE stockdata.stocks (
    stock text,
    trade_id uuid,  
    price decimal,
    quantity int,
    trade_type text,  
    trade_date date,  
    trade_time time,  
    PRIMARY KEY (stock, trade_id)
);

CREATE TABLE stockdata.grouped_stocks (
    trade_type text PRIMARY KEY,
    avg_price decimal
);

CREATE TABLE stockdata.pivoted_stocks (
    stock text PRIMARY KEY,
    avg_price_buy decimal,
    avg_price_sell decimal
);

CREATE TABLE stockdata.ranked_stocks (
    stock text,
    trade_id text,
    price double,
    quantity int,
    trade_date date,
    trade_time time,
    trade_type text,
    rank int,
    PRIMARY KEY ((trade_type), rank)
);

CREATE TABLE stockdata.analytics_stocks (
    stock text,
    trade_id text,
    price double,
    quantity int,
    trade_date date,
    trade_time time,
    trade_type text,
    avg_price_overall double,
    PRIMARY KEY ((trade_type), trade_date)
);

CREATE TABLE stockdata.rollup_stocks (
    trade_date text,
    trade_type text,
    avg_price double,
    PRIMARY KEY (trade_date, trade_type)
);

