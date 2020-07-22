TRUNCATE public.users;
INSERT INTO public.users
SELECT user_id,
       dados_pessoais ->> 'genero',
       CASE dados_pessoais ->> 'estado_civil'
           WHEN 'CASADO(A) COM BRASILEIRO(A) NATO(A)' THEN 'CASADO(A)'
           WHEN 'CASADO(A) COM BRASILEIRO(A) NATURALIZADO(A)' THEN 'CASADO(A)'
           WHEN 'UNIAO ESTAVEL' THEN 'CASADO(A)'
           ELSE dados_pessoais ->> 'estado_civil'
           END,
       (dados_pessoais ->> 'idade')::int,
       nivel_de_risco,
       objetivo,
       perfil_de_risco,
       fez_adicional,
       fez_resgate_parcial,
       fez_resgate_total,
       fez_adicional AND NOT (fez_resgate_parcial OR fez_resgate_total),
       poupanca,
       renda_fixa,
       renda_variavel,
       coalesce(poupanca, 0) + coalesce(renda_fixa, 0) + coalesce(renda_variavel, 0)
FROM raw.users;
