CREATE TABLE IF NOT EXISTS Produtos (
    id_produto INT PRIMARY KEY,
    produto VARCHAR(255),
    marca VARCHAR(255),
    custo_de_compra FLOAT,
    valor_de_venda FLOAT,
    cor VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Clientes (
    id_cliente INT PRIMARY KEY,
    primeiro_nome VARCHAR(255),
    sobrenome VARCHAR(255),
    email VARCHAR(255),
    genero VARCHAR(255),
    num_filhos INT,
    data_de_nascimento DATE
);

CREATE TABLE IF NOT EXISTS Vendas (
    cod_produto INT,
    data_emissao DATE,
    loja VARCHAR(255),
    cod_cliente INT,
    quantidade INT,
    forma_de_pagamento VARCHAR(255),
    id_pedido SERIAL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS itens_pedidos (
    id_ped_item SERIAL PRIMARY KEY,
    cod_produto INT,
    valor_de_venda FLOAT
);
