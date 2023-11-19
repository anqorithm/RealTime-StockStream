CREATE KEYSPACE stockdata WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE stockdata.stocks (
    stock text,
    trade_id uuid,  -- UUID for unique identification of each trade
    price decimal,
    quantity int,
    trade_type text,  -- e.g., 'buy' or 'sell'
    trade_date date,  -- date of the trade
    trade_time time,  -- time of the trade
    PRIMARY KEY (stock, trade_id)
);
