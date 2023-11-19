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
