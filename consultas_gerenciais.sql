--1. Valor das vendas agrupada por produto, tipo e categoria.
SELECT
    p.tb003_cod_tipo AS tipo_id, 
    p.tb003_cod_categoria AS categoria_id,
    SUM(v.tb011_quantidade) AS total_quantidade,
    SUM(v.tb011_valor) AS total_valor
FROM public.tb011_vendas v
JOIN public.tb003_produtos p ON v.tb011_cod_produto = p.tb003_cod_produto
WHERE 
	v.tb011_cod_cliente IS NULL
	AND v.tb011_cod_filial IS NULL
	AND v.tb011_cod_funcionario IS NULL
	AND v.tb011_cod_tempo IS NULL
GROUP BY p.tb003_cod_tipo, p.tb003_cod_categoria;
--2. Clientes que mais compraram na loja virtual com valor acumulado por período.
SELECT
	v.tb011_cod_cliente as cliente,
	v.tb011_valor as total
FROM public.tb011_vendas v
WHERE 
	v.tb011_quantidade IS NULL 
	AND v.tb011_cod_filial IS NULL
	AND v.tb011_cod_funcionario IS NULL
	AND v.tb011_cod_tempo IS NULL
--3. Volume das vendas por funcionário e localidade.
SELECT
	v.tb011_cod_funcionario as funcionario,
	v.tb011_cod_filial as loja,
	v.tb011_valor as total,
	v.tb011_quantidade as quantidade
FROM public.tb011_vendas v
WHERE 
	v.tb011_cod_produto IS NULL 
	AND v.tb011_cod_tempo IS NULL
	AND v.tb011_cod_cliente IS NULL
--4. Quantidade de atendimentos realizados por localidade permitindo uma visão hierárquica ao longo do tempo.
SELECT
	v.tb011_cod_filial as loja,
	v.tb011_quantidade as quantidade,
	v.tb011_cod_tempo
FROM public.tb011_vendas v
WHERE 
	v.tb011_valor IS NULL
	AND v.tb011_cod_produto IS NULL
	AND v.tb011_cod_cliente IS NULL
	AND v.tb011_cod_funcionario IS NULL
--5. Valor das últimas compras realizadas por cliente e tempo decorrido até a data atual
SELECT
    v.tb011_valor AS valor_compra,
    tp.tb007_tempo_cod,
    v.tb011_cod_cliente,
    CURRENT_DATE - tp.tb007_tempo_data AS dias_decorridos
FROM
    public.tb011_vendas v
INNER JOIN
    public.tb007_tempo tp ON v.tb011_cod_tempo = tp.tb007_tempo_cod
WHERE 
    v.tb011_quantidade IS NULL
    AND v.tb011_cod_produto IS NULL
    AND v.tb011_cod_filial IS NULL
    AND v.tb011_cod_funcionario IS NULL;
