import pyodbc
import psycopg2
from datetime import datetime, timedelta

# Função para determinar o tipo com base na categoria


def definir_tipo(categoria):
    if categoria in [1, 2]:
        return 1
    elif categoria in [3, 4, 5]:
        return 2
    elif categoria in [6, 7, 8]:
        return 3
    else:
        return None  # ou outro valor padrão


def transfer_data(sql_cursor, pg_cursor, select_query, insert_query, process_row=None):
    sql_cursor.execute(select_query)
    rows_sql = sql_cursor.fetchall()

    for row in rows_sql:
        if process_row:
            row = process_row(row)
        pg_cursor.execute(insert_query, row)


# Conexão com SQL Server
sql_conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=ADS;'
    'UID=user_novo;'
    'PWD=123;'
)
cursor_sql = sql_conn.cursor()

# Conexão com PostgreSQL
pg_conn = psycopg2.connect(
    host="localhost",
    database="Dw",
    user="postgres",
    password="#"
)
cursor_pg = pg_conn.cursor()

# ETL para tb007_tempo (Dimensão Tempo) - Gerando datas

insert_tempo = """
INSERT INTO public.tb007_tempo (tb007_tempo_cod, tb007_tempo_data, tb007_tempo_dia, tb007_tempo_mes, tb007_tempo_ano)
VALUES (%s, %s, %s, %s, %s)
"""

start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 12, 31)
delta = timedelta(days=1)

current_date = start_date
while current_date <= end_date:
    tempo_cod = int(current_date.strftime('%Y%m%d'))
    data = current_date.date()
    dia = current_date.day
    mes = current_date.month
    ano = current_date.year
    cursor_pg.execute(insert_tempo, (tempo_cod, data, dia, mes, ano))
    current_date += delta

# ETL para tb013_categorias -> tb001_categorias
select_categorias = "SELECT tb013_cod_categoria, tb013_descricao FROM ADS.dbo.tb013_categorias"
insert_categorias = """
INSERT INTO public.tb001_categorias (tb001_cod_categoria, tb001_descricao)
VALUES (%s, %s)
"""
transfer_data(cursor_sql, cursor_pg, select_categorias, insert_categorias)

# Extração e Transformação dos Dados
cursor_sql_server = sql_conn.cursor()
cursor_sql_server.execute(
    "SELECT tb012_cod_produto, tb013_cod_categoria, tb012_descricao FROM tb012_produtos")

# Preparar inserção no PostgreSQL
cursor_pg = pg_conn.cursor()
for tb012_cod_produto, tb013_cod_categoria, tb012_descricao in cursor_sql_server:
    tb003_cod_tipo = definir_tipo(tb013_cod_categoria)

    # Verifica se o tipo é válido
    if tb003_cod_tipo is not None:
        cursor_pg.execute(
            """
            INSERT INTO tb003_produtos (tb003_cod_produto, tb003_cod_categoria, tb003_descricao, tb003_cod_tipo) 
            VALUES (%s, %s, %s, %s)
            """,
            (tb012_cod_produto, tb013_cod_categoria,
             tb012_descricao, tb003_cod_tipo)
        )

# ETL para tb004_lojas -> tb004_filial
select_filial = """
SELECT 
    l.tb004_cod_loja,
    l.tb004_inscricao_estadual,
    u.tb001_nome_estado + ', ' + c.tb002_nome_cidade + ', ' + e.tb003_bairro + ', ' + e.tb003_nome_rua + ' ' + ISNULL(e.tb003_complemento, '') AS endereco_completo,
    l.tb004_cnpj_loja
FROM 
    ADS.dbo.tb004_lojas l
JOIN 
    ADS.dbo.tb003_enderecos e ON l.tb003_cod_endereco = e.tb003_cod_endereco
JOIN 
    ADS.dbo.tb002_cidades c ON e.tb002_cod_cidade = c.tb002_cod_cidade AND e.tb001_sigla_uf = c.tb001_sigla_uf
JOIN 
    ADS.dbo.tb001_uf u ON e.tb001_sigla_uf = u.tb001_sigla_uf
"""
insert_filial = """
INSERT INTO public.tb004_filial (tb004_cod_filial, tb004_inscricao_estadual, tb004_endereco, tb004_cnpj_filial)
VALUES (%s, %s, %s, %s)
"""


def process_filial_row(row):
    cod_filial = row[0]
    inscricao_estadual = row[1]
    endereco_completo = row[2]
    cnpj_filial = row[3]
    return (cod_filial, inscricao_estadual, endereco_completo, cnpj_filial)


transfer_data(cursor_sql, cursor_pg, select_filial,
              insert_filial, process_filial_row)

# ETL para clientes
select_clientes = """
SELECT tb010_cpf, tb010_nome
FROM ADS.dbo.tb010_clientes

UNION ALL

SELECT ca.tb010_cpf, ca.tb010_nome
FROM ADS.dbo.tb010_clientes_antigos ca
WHERE NOT EXISTS (
    SELECT 1
    FROM ADS.dbo.tb010_clientes c
    WHERE c.tb010_cpf = ca.tb010_cpf
)
"""
insert_clientes = """
INSERT INTO public.tb009_cliente (tb009_cpf, tb009_nome_cliente)
VALUES (%s, %s)
"""
transfer_data(cursor_sql, cursor_pg, select_clientes, insert_clientes)

select_funcionarios = """
SELECT 
    f.tb005_matricula, 
    f.tb005_nome_completo, 
    f.tb005_CPF
FROM ADS.dbo.tb005_funcionarios f
"""
insert_funcionarios = """
INSERT INTO public.tb010_funcionario (tb010_cod_funcionario, tb010_nome_funcionario, tb010_cpf)
VALUES (%s, %s, %s)
"""


def process_funcionario_row(row):
    cod_funcionario = row[0]
    nome_funcionario = row[1]
    cpf = row[2]
    return (cod_funcionario, nome_funcionario, cpf)


transfer_data(cursor_sql, cursor_pg, select_funcionarios,
              insert_funcionarios, process_funcionario_row)

# ETL para tb011_vendas
select_vendas = """
SELECT
    v.tb010_012_quantidade,
    v.tb010_012_valor_unitario,
    v.tb012_cod_produto,
    f.tb004_cod_loja,
    v.tb010_012_data,
    v.tb010_cpf,
    v.tb005_matricula
FROM ADS.dbo.tb010_012_vendas v
JOIN ADS.dbo.tb005_funcionarios f ON v.tb005_matricula = f.tb005_matricula
"""

insert_vendas = """
INSERT INTO public.tb011_vendas (
    tb011_quantidade,
    tb011_valor,
    tb011_cod_produto,
    tb011_cod_filial,
    tb011_cod_tempo,
    tb011_cod_cliente,
    tb011_cod_funcionario
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


def process_venda_row(row):
    quantidade = row[0]
    valor = row[1]
    cod_produto = row[2]
    cod_filial = row[3]
    data_venda = row[4]
    cpf_cliente = row[5]
    cod_funcionario = row[6]

    # Gerar o código de tempo (tb007_tempo_cod) a partir da data_venda
    if isinstance(data_venda, datetime):
        data_venda = data_venda.date()
    tempo_cod = int(data_venda.strftime('%Y%m%d'))  # Formato YYYYMMDD

    return (quantidade, valor, cod_produto, cod_filial, tempo_cod, cpf_cliente, cod_funcionario)


transfer_data(cursor_sql, cursor_pg, select_vendas,
              insert_vendas, process_venda_row)

# Confirma as inserções e fecha conexões
pg_conn.commit()
cursor_sql.close()
sql_conn.close()
cursor_pg.close()
pg_conn.close()
