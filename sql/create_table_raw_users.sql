CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.users
(
    user_id             int,
    dados_pessoais      json,
    nivel_de_risco      int,
    objetivo            varchar(50),
    perfil_de_risco     varchar(50),
    fez_adicional       bool,
    fez_resgate_parcial bool,
    fez_resgate_total   bool,
    poupanca            numeric(10, 2),
    renda_fixa          numeric(10, 2),
    renda_variavel      numeric(10, 2)
);