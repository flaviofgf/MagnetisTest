CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.funnel
(
    user_id        int,
    timestamp      timestamp,
    evento         varchar(50),
    valor_simulado numeric(10, 2)
);