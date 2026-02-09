# Desafio Técnico: Data Engineer

Bem-vindo(a) ao desafio técnico para a vaga de Engenharia de Dados! O objetivo deste case é avaliar suas habilidades em construção de pipelines de dados, modelagem, qualidade de código e arquitetura de soluções.

## Contexto do Negócio

O time de Inteligência de Mercado precisa analisar a evolução do ambiente de negócios na cidade de São Paulo. Para isso, foi solicitado um estudo sobre a abertura e manutenção de empresas utilizando os **Dados Abertos do CNPJ da Receita Federal**.

A demanda específica para este MVP é analisar o cenário referente a **Dezembro de 2025** (ou o mês mais recente disponível, caso este ainda não tenha sido publicado), respondendo à seguinte pergunta de negócio:

> "Quantas filiais vs. matrizes com Situação Cadastral 'Ativa' existem na cidade de São Paulo?"

## O Desafio

Como Data Engineer, sua missão é construir uma pipeline de dados automatizada que faça a ingestão, processamento e disponibilização desses dados para consumo analítico.

### Fonte de Dados
* **Origem:** [Dados Abertos CNPJ - Receita Federal](https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9)
* **Foco:** Dados de Estabelecimentos e Municípios (necessário para filtrar "São Paulo").

### Requisitos Funcionais

1.  **Pipeline Parametrizável:** A pipeline deve aceitar uma data (mês/ano) como parâmetro de entrada para execução.
2.  **Ingestão Automatizada:** O script deve identificar o arquivo correto na URL base e realizar o download automático.
3.  **Processamento (ETL/ELT):**
    * Filtrar apenas estabelecimentos da cidade de **São Paulo**.
    * Filtrar apenas estabelecimentos com Situação Cadastral **Ativa**.
    * Segregar o que é **Matriz**.
4.  **Visualização:** Gerar uma saída (pode ser um gráfico simples em Python/Notebook, um print de um dashboard ou uma tabela agregada final) que mostre a contagem solicitada pelo time de negócios.

### Requisitos Técnicos (Non-Functional)

* **Arquitetura Medallion:** Organize o armazenamento dos dados seguindo boas práticas de Data Lake (ex: Camadas Bronze/Landing, Silver/Processing, Gold/Curated).
* **Idempotência:** A pipeline deve poder ser reexecutada sem duplicar dados ou gerar inconsistências.
* **Reprodutibilidade:** O projeto deve conter instruções claras de como rodar em outro ambiente.

## Diferenciais (Pontos Extras) 🌟

A utilização das tecnologias abaixo será considerada um grande diferencial na avaliação:

* **Docker / Docker Compose:** Para orquestrar o ambiente.
* **Airflow:** Para orquestração das tarefas da pipeline.
* **MinIO (S3 Compatible):** Para simular o Data Lake localmente.
* **Apache Spark (PySpark):** Para processamento distribuído dos dados.

## O que será avaliado?

1.  **Qualidade do Código:** Clareza, modularidade e boas práticas (PEP8, Clean Code).
2.  **Arquitetura:** Escolha das ferramentas e organização das pastas/camadas.
3.  **Lógica de Dados:** Como você cruzou os dados de município com estabelecimentos? Como tratou arquivos grandes?
4.  **Documentação:** O `README` explica como rodar o projeto? Explica as decisões arquiteturais tomadas?

## Entrega

* Disponibilize o código em um repositório público (GitHub/GitLab).
* Envie o link do repositório para o recrutador responsável.
* Inclua um arquivo `README.md` com as instruções de execução.

---
**Boa sorte!**