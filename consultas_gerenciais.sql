--1. Valor das vendas agrupada por produto, tipo e categoria.
SELECT
    p.tb003_descricao AS produto,
    tp.tb002_descricao AS tipo,
    c.tb001_descricao AS categoria,
    SUM(v.tb011_valor * v.tb011_quantidade) AS total_vendas
FROM
    tb011_vendas v
JOIN
    tb003_produtos p ON v.tb011_cod_produto = p.tb003_cod_produto
JOIN
    tb002_tipos tp ON p.tb003_cod_tipo = tp.tb002_cod_tipo
JOIN
    tb001_categorias c ON p.tb003_cod_categoria = c.tb001_cod_categoria
GROUP BY
    p.tb003_descricao,
    tp.tb002_descricao,
    c.tb001_descricao
ORDER BY
    total_vendas DESC;
--2. Clientes que mais compraram na loja virtual com valor acumulado por período.
SELECT 
    c.tb009_cpf,
	c.tb009_nome_cliente,
    v.tb011_valor as total_comprado
FROM 
    tb011_vendas v
JOIN 
    tb009_cliente c ON v.tb011_cod_cliente = c.tb009_cpf
ORDER BY
    total_comprado DESC;

--3. Volume das vendas por funcionário e localidade.
SELECT
    f.tb010_nome_funcionario AS funcionario,
    l.tb004_endereco AS localidade,
    SUM(v.tb011_valor * v.tb011_quantidade) OVER (
        PARTITION BY f.tb010_nome_funcionario, l.tb004_endereco
    ) AS total_vendas
FROM
    tb011_vendas v
LEFT JOIN
    tb010_funcionario f ON v.tb011_cod_funcionario = f.tb010_cod_funcionario
LEFT JOIN
    tb004_filial l ON v.tb011_cod_filial = l.tb004_cod_filial
ORDER BY
    total_vendas DESC;
--4. Quantidade de atendimentos realizados por localidade permitindo uma visão hierárquica ao longo do tempo.
SELECT
    l.tb004_endereco AS localidade,
    t.tb007_tempo_ano AS ano,
    t.tb007_tempo_mes AS mes,
    t.tb007_tempo_dia AS dia,
    COUNT(*) OVER (
        PARTITION BY 
            l.tb004_endereco, 
            t.tb007_tempo_ano, 
            t.tb007_tempo_mes, 
            t.tb007_tempo_dia
    ) AS quantidade_atendimentos
FROM
    tb011_vendas v
LEFT JOIN
    tb004_filial l ON v.tb011_cod_filial = l.tb004_cod_filial
LEFT JOIN
    tb007_tempo t ON v.tb011_cod_tempo = t.tb007_tempo_cod
ORDER BY
    l.tb004_endereco,
    t.tb007_tempo_ano,
    t.tb007_tempo_mes,
    t.tb007_tempo_dia;
--5. Valor das últimas compras realizadas por cliente e tempo decorrido até a data atual
SELECT
    c.tb009_nome_cliente AS cliente,
    t.tb007_tempo_data AS data_ultima_compra,
    v.tb011_valor * v.tb011_quantidade AS valor_ultima_compra,
    CURRENT_DATE - t.tb007_tempo_data AS dias_desde_ultima_compra
FROM (
    SELECT
        v.tb011_cod_cliente,
        MAX(t.tb007_tempo_data) AS data_ultima_compra
    FROM
        tb011_vendas v
    JOIN
        tb007_tempo t ON v.tb011_cod_tempo = t.tb007_tempo_cod
    GROUP BY
        v.tb011_cod_cliente
) ultimas
JOIN
    tb011_vendas v ON v.tb011_cod_cliente = ultimas.tb011_cod_cliente
JOIN
    tb007_tempo t ON v.tb011_cod_tempo = t.tb007_tempo_cod AND t.tb007_tempo_data = ultimas.data_ultima_compra
JOIN
    tb009_cliente c ON v.tb011_cod_cliente = c.tb009_cpf
ORDER BY
    dias_desde_ultima_compra ASC;