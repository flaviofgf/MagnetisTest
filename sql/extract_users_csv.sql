TRUNCATE raw.users;
COPY raw.users
    FROM '/data/usuarios.csv'
    DELIMITER ';'
    CSV HEADER QUOTE ''''
    ENCODING 'utf-8';