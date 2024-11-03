--1. Valor das vendas agrupada por produto, tipo e categoria.
SELECT
    p.tb003_cod_tipo AS tipo_id, 
    t.tb002_descricao AS tipo_descricao,
    p.tb003_cod_categoria AS categoria_id,
    c.tb001_descricao AS categoria_descricao,
    SUM(v.tb011_quantidade) AS total_quantidade,
    SUM(v.tb011_valor) AS total_valor
FROM public.tb011_vendas v
JOIN public.tb003_produtos p ON v.tb011_cod_produto = p.tb003_cod_produto
JOIN public.tb001_categorias c ON p.tb003_cod_categoria = c.tb001_cod_categoria
JOIN public.tb002_tipos t ON p.tb003_cod_tipo = t.tb002_cod_tipo
WHERE 
    v.tb011_cod_cliente IS NULL
    AND v.tb011_cod_filial IS NULL
    AND v.tb011_cod_funcionario IS NULL
    AND v.tb011_cod_tempo IS NULL
GROUP BY p.tb003_cod_tipo, t.tb002_descricao, p.tb003_cod_categoria, c.tb001_descricao;

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
    v.tb011_cod_funcionario AS funcionario,
    v.tb011_cod_filial AS loja,
    fl.tb004_endereco AS localidade,
    v.tb011_valor AS total,
    v.tb011_quantidade AS quantidade
FROM public.tb011_vendas v
LEFT JOIN public.tb010_funcionario f ON v.tb011_cod_funcionario = f.tb010_cod_funcionario
LEFT JOIN public.tb004_filial fl ON v.tb011_cod_filial = fl.tb004_cod_filial
WHERE 
    v.tb011_cod_produto IS NULL 
    AND v.tb011_cod_tempo IS NULL
    AND v.tb011_cod_cliente IS NULL;
--4. Quantidade de atendimentos realizados por localidade permitindo uma visão hierárquica ao longo do tempo.
SELECT
    fl.tb004_endereco AS loja_nome,
    v.tb011_quantidade AS quantidade,
    t.tb007_tempo_data AS data_tempo
FROM public.tb011_vendas v
LEFT JOIN public.tb004_filial fl ON v.tb011_cod_filial = fl.tb004_cod_filial
LEFT JOIN public.tb007_tempo t ON v.tb011_cod_tempo = t.tb007_tempo_cod
WHERE 
    v.tb011_valor IS NULL
    AND v.tb011_cod_produto IS NULL
    AND v.tb011_cod_cliente IS NULL
    AND v.tb011_cod_funcionario IS NULL;
--5. Valor das últimas compras realizadas por cliente e tempo decorrido até a data atual
SELECT
    tp.tb007_tempo_data AS data_ultima_compra,
    v.tb011_valor AS valor_compra,
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
