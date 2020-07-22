TRUNCATE raw.funnel;
COPY raw.funnel
    FROM '/data/funil.csv'
    DELIMITER ';'
    CSV HEADER QUOTE ''''
    ENCODING 'utf-8';