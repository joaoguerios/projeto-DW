--1. Valor das vendas agrupada por produto, tipo e categoria.
SELECT
    p.tb003_cod_tipo AS tipo_id, 
    p.tb003_cod_categoria AS categoria_id,
    SUM(v.tb011_quantidade) AS total_quantidade,
    SUM(v.tb011_valor) AS total_valor
FROM public.tb011_vendas v
JOIN public.tb003_produtos p ON v.tb011_cod_produto = p.tb003_cod_produto
GROUP BY p.tb003_cod_tipo, p.tb003_cod_categoria;
--2. Clientes que mais compraram na loja virtual com valor acumulado por período.
SELECT 
    c.tb009_cpf,
    c.tb009_nome_cliente,
    SUM(v.tb011_quantidade) AS total_quantidade_produtos,  
    SUM(v.tb011_valor) AS total_comprado_valor            
FROM 
    tb011_vendas v
JOIN 
    tb009_cliente c ON v.tb011_cod_cliente = c.tb009_cpf
GROUP BY 
    c.tb009_cpf, c.tb009_nome_cliente                
ORDER BY
    total_comprado_valor DESC;                              
-- ou 
SELECT 
    c.tb009_nome_cliente,
    c.tb009_cpf,
    SUM(v.tb011_valor) AS valor_total,
    DATE_TRUNC('month', t.tb007_tempo_data) AS periodo
FROM 
    public.tb011_vendas v
JOIN 
    public.tb009_cliente c ON v.tb011_cod_cliente = c.tb009_cpf
JOIN 
    public.tb007_tempo t ON v.tb011_cod_tempo = t.tb007_tempo_cod
GROUP BY 
    c.tb009_nome_cliente, c.tb009_cpf, periodo
ORDER BY 
    valor_total DESC;

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
WITH UltimasCompras AS (
    SELECT 
        c.tb009_nome_cliente,
        c.tb009_cpf,
        v.tb011_valor,
        t.tb007_tempo_data,
        ROW_NUMBER() OVER (PARTITION BY c.tb009_cpf ORDER BY t.tb007_tempo_data DESC) AS rn
    FROM 
        public.tb011_vendas v
    JOIN 
        public.tb009_cliente c ON v.tb011_cod_cliente = c.tb009_cpf
    JOIN 
        public.tb007_tempo t ON v.tb011_cod_tempo = t.tb007_tempo_cod
)
SELECT 
    tb009_nome_cliente,
    tb009_cpf,
    tb011_valor,
    CURRENT_DATE - tb007_tempo_data AS dias_decorridos
FROM 
    UltimasCompras
WHERE 
    rn = 1
ORDER BY 
    tb009_nome_cliente;
