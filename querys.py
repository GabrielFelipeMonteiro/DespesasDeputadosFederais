# Função que gera a tabela de despesas no PostegreSQL
createTableDespesas = """
        CREATE TABLE IF NOT EXISTS public.despesas (
        txNomeParlamentar VARCHAR(250), ideCadastro VARCHAR(50), sgUF VARCHAR(5),
        sgPartido VARCHAR(5), txtDescricao VARCHAR(250), txtDescricaoEspecificacao VARCHAR(250),
        txtFornecedor VARCHAR(250), txtCNPJCPF VARCHAR(250), txtNumero VARCHAR(50), indTipoDocumento INTEGER,
        datEmissao VARCHAR(250), vlrDocumento NUMERIC(10, 2), vlrGlosa NUMERIC(10, 2),
        vlrLiquido NUMERIC(10, 2), numMes INTEGER, numAno INTEGER,
        numParcela INTEGER, txtPassageiro VARCHAR(250), txtTrecho VARCHAR(250),
        numLote INTEGER, ideDocumento INTEGER , urlDocumento VARCHAR(250)
    );
"""
