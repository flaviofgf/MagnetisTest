CREATE TABLE IF NOT EXISTS public.funnel
(
    user_id               int,
    timestamp             timestamp,
    evento                varchar(50),
    valor_simulado        numeric(10, 2),
    primeiro_evento       bool,
    ultimo_evento         bool,
    ultimo_valor_simulado bool
);