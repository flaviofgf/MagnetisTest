CREATE TABLE IF NOT EXISTS public.users
(
    user_id                    int,
    genero                     varchar(50),
    estado_civil               varchar(50),
    idade                      int,
    nivel_de_risco             int,
    objetivo                   varchar(50),
    perfil_de_risco            varchar(50),
    fez_adicional              bool,
    fez_resgate_parcial        bool,
    fez_resgate_total          bool,
    flag_investidor_recorrente bool,
    poupanca                   numeric(10, 2),
    renda_fixa                 numeric(10, 2),
    renda_variavel             numeric(10, 2),
    investimentos_externos     numeric(10, 2)
);