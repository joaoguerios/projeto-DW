CREATE TABLE tb001_categorias (
    tb001_cod_categoria INT PRIMARY KEY,
    tb001_descricao VARCHAR(255) NOT NULL
);

CREATE TABLE tb002_tipos (
    tb002_cod_tipo INT PRIMARY KEY,
    tb002_descricao VARCHAR(255) NOT NULL
);

INSERT INTO tb002_tipos (tb002_cod_tipo,tb002_descricao)
VALUES
    (1,'Alimento'),
    (2,'Eletrodoméstico'),
    (3,'Vestuário');

CREATE TABLE tb003_produtos (
    tb003_cod_produto INT PRIMARY KEY,
    tb003_cod_categoria INT NOT NULL REFERENCES tb001_categorias(tb001_cod_categoria),
    tb003_descricao VARCHAR(255) NOT NULL,
    tb003_cod_tipo INT NOT NULL REFERENCES tb002_tipos(tb002_cod_tipo)
);

CREATE TABLE tb004_filial (
    tb004_cod_filial INT PRIMARY KEY,
    tb004_inscricao_estadual VARCHAR(20) NULL,
    tb004_endereco VARCHAR(255) NULL,
    tb004_cnpj_filial VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE tb007_tempo (
    tb007_tempo_cod INT PRIMARY KEY,
    tb007_tempo_data DATE NOT NULL,
    tb007_tempo_dia INT NOT NULL,
    tb007_tempo_mes INT NOT NULL,
    tb007_tempo_ano INT NOT NULL
);

CREATE TABLE tb009_cliente (
    tb009_cpf VARCHAR(11) NOT NULL PRIMARY KEY,
    tb009_nome_cliente VARCHAR(100) NOT NULL
);

CREATE TABLE tb010_funcionario (
    tb010_cod_funcionario INT PRIMARY KEY,
    tb010_nome_funcionario VARCHAR(100) NOT NULL,
    tb010_cpf VARCHAR(17) NOT NULL UNIQUE
);

CREATE TABLE tb011_vendas (
    tb011_quantidade INT NOT NULL,
    tb011_valor NUMERIC(10, 2) NOT NULL,
    tb011_cod_produto INT REFERENCES tb003_produtos(tb003_cod_produto),
    tb011_cod_filial INT REFERENCES tb004_filial(tb004_cod_filial),
    tb011_cod_tempo INT REFERENCES tb007_tempo(tb007_tempo_cod),
    tb011_cod_cliente VARCHAR(20) REFERENCES tb009_cliente(tb009_cpf), 
    tb011_cod_funcionario INT REFERENCES tb010_funcionario(tb010_cod_funcionario) 
);