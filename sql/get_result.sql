COPY (
    SELECT u.user_id,
           genero,
           estado_civil,
           idade,
           nivel_de_risco,
           objetivo,
           perfil_de_risco,
           to_char(f1.timestamp, 'yyyy-mm') AS homepage,
           f2.valor_simulado,
           flag_investidor_recorrente,
           investimentos_externos
    FROM public.users u
             LEFT JOIN funnel f1 ON u.user_id = f1.user_id AND f1.primeiro_evento = TRUE
             LEFT JOIN funnel f2 ON u.user_id = f2.user_id AND f2.ultimo_valor_simulado = TRUE
    )
    TO '/data/sql_result.csv'
    DELIMITER ';'
    CSV HEADER
    ENCODING 'utf-8';