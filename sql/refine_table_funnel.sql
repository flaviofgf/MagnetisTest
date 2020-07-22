TRUNCATE public.funnel;
INSERT INTO public.funnel
SELECT *,
       1 = rank()
           OVER (PARTITION BY user_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
       1 = rank()
           OVER (PARTITION BY user_id ORDER BY timestamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
       valor_simulado IS NOT NULL AND 1 = rank()
                                          OVER (PARTITION BY user_id, valor_simulado IS NULL ORDER BY timestamp DESC, valor_simulado ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM raw.funnel;