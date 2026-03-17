# 🚀 Transformação de Dados com dbt (Data Build Tool)

Este repositório é o núcleo da camada de transformação de dados (o "T" do ELT). Ele contém todos os modelos em SQL e configurações do **dbt**, responsáveis por pegar os dados brutos do nosso Data Warehouse, limpá-los, testá-los e prepará-los para o consumo das ferramentas de Business Intelligence.

## 📁 Estrutura do Projeto

Nossa arquitetura de dados segue as melhores práticas de modelagem, dividida nas seguintes camadas principais:

* **`modelos_dbt/`**: Contém os pacotes e modelos base importados de outros repositórios de infraestrutura (sincronizados via Git Subtree).
* **`models/staging/`**: Modelos de visão (views) que fazem a limpeza inicial, padronização de nomes de colunas e tipagem de dados diretamente das fontes brutas.
* **`models/marts/`**: Modelos de negócio finais (tabelas materializadas) otimizados para performance e leitura pelas ferramentas de BI.
* **`tests/`**: Testes customizados de integridade para garantir a qualidade (Data Quality) e a confiabilidade das métricas.

## ⚙️ Como executar este projeto localmente

Para rodar os modelos na sua máquina, siga os passos abaixo:

1. Clone este repositório:
   ```bash
   git clone [https://github.com/rdsmelobsb/dbt_models.git](https://github.com/rdsmelobsb/dbt_models.git)
   ```

2. Instale o dbt core e o adaptador do seu banco de dados (recomenda-se o uso de ambiente virtual em Python):
   ```bash
   pip install dbt-core dbt-[nome-do-adaptador]
   ```

3. Configure o seu arquivo `profiles.yml` (geralmente localizado em `~/.dbt/`) com as credenciais de acesso ao Data Warehouse.

4. Verifique a conexão com o banco:
   ```bash
   dbt debug
   ```
   
5. Execute os modelos:
   ```bash
   dbt run
