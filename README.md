# Engenharia
#### Teste para Pessoa Engenheira de dados

## Configurações
Utilizando o **Docker** como máquina virtual decidi utilizar o **Docker Compose**
para configurar cada um dos serviços necessários para o desafio de forma clusterizada
e conectada simulando mais ou menos como seria em um ambiente cloud.

No [docker-compose.yml](https://github.com/flaviofgf/magnetis_test/blob/master/docker-compose.yml) tem as configurações de cada serviço sendo eles:
+ **airflow_db**: banco de dados **PostgreSQL** para o **Airflow**.
+ **postgres**: banco de dados **PostgreSQL** para a resolução em SQL.
+ **airflow**: faz referência ao **[Dockerfile](https://github.com/flaviofgf/magnetis_test/blob/master/docker/airflow/Dockerfile)** 
 da pasta **[docker/airflow](https://github.com/flaviofgf/magnetis_test/tree/master/docker/airflow)**.
+ **spark**: faz referência ao **[Dockerfile](https://github.com/flaviofgf/magnetis_test/blob/master/docker/spark/Dockerfile)**
 da **[docker/spark](https://github.com/flaviofgf/magnetis_test/tree/master/docker/spark)**.

Todas as imagens foram utilizadas na versão **latest** sendo até o momento:
+ **postgres**: 12.3
+ **airflow**: 1.10.9
    + **python**: 3.7.6
+ **spark**: 3.0.0
    + **python**: 3.8.3

## Estruturação das pastas
+ **[dags](https://github.com/flaviofgf/magnetis_test/tree/master/dags)**:
contém as dags
+ **[data](https://github.com/flaviofgf/magnetis_test/tree/master/data)**:
contém os csvs do desafio e onde salvará os csvs de resultado
  + **[refined_zone](https://github.com/flaviofgf/magnetis_test/tree/master/data/refined_zone)**:
  onde salvará os csvs refinados do pyspark
+ **[docker](https://github.com/flaviofgf/magnetis_test/tree/master/docker)**:
contém o **Dockerfile** e **requirements.txt** dos seguintes serviços.
    + **[airflow](https://github.com/flaviofgf/magnetis_test/tree/master/docker/airflow)**
    + **[spark](https://github.com/flaviofgf/magnetis_test/tree/master/docker/spark)**
+ **[notebook](https://github.com/flaviofgf/magnetis_test/tree/master/notebook)**:
contém a resolução em **PySpark Notebook**.
+ **[sql](https://github.com/flaviofgf/magnetis_test/tree/master/sql)**:
contém as consultas da resolução em **SQL**.
+ **[work](https://github.com/flaviofgf/magnetis_test/tree/master/work)**:
contém os scripts python da resolução em **PySpark**.
    + **[etl](https://github.com/flaviofgf/magnetis_test/tree/master/work/etl)**:
    com o script python com a classe para tratar os dados.

## Considerações
Com a ajuda do **Docker Compose** consegui separar os serviços e mantê-los numa mesma
rede sem grandes configurações.
Comecei fazendo a resolução do problema no **[notebook](https://github.com/flaviofgf/magnetis_test/blob/master/notebook/etl.ipynb)**
o que foi tranquilo, minha maior dificuldade foi passar para a dag do **Airflow**.

Não conseguia conectar o **Airflow** do container dele ao **Spark** de outro container,
após muitas tentativas consegui achar uma solução, talvez não a que eu queria de início,
mas que ainda simulasse um pouco como seria em um ambiente cloud.

Usando o **DockerOperator** do **Airflow** fiz com que ele criasse um container e iniciasse
o script em python, assim mantendo a ideia de tarefas isoladas e independentes entre si.

Após isso fiz a resolução do problema em **SQL**, que ficou com a dag mais da forma que eu
gostaria, onde mostra o passo a passo de cada fase da tabela.

Ao comparar os resultados percebi um erro que tinha passado desapercebido, o solicitado 
é o último valor simulado, e eu estava trazendo o último registro que, em alguns casos,
tinha valor simulado nulo.

Sendo assim retornei e corrigi todas as resoluções anteriores.

Em todas as resoluções faço a criação de três tabelas: 
+ **users**: tabela de usuários
+ **funnel**: tabela de funil
+ **result**: tabela de resultado

Na criação da tabela users eu já adiciono os dados que eu precisaria para a tabela
de resultado, flag_investidor_recorrente e investimentos_externos. Além disso já abro em
colunas o conteúdo do json, como também já deixo simplificado a coluna estado_civil.

Na criação da tabela funnel eu já adiciono colunas com a ordem dos eventos
e do valor simulado por usuário, e duas flags se é primeiro_evento e se é 
ultimo_valor_simulado
 
Portanto assim a consulta para gerar a tabela de resultado se torna bastante
simples já que apenas preciso juntar as duas tabelas e colocar a flag que preciso
um para pegar a data do primeiro evento e outra para pegar o último valor simulado.

## Instalação
+ Requisitos:
    + **Docker**: última versão (atualmente v19.03.8)
    + **Git**: última versão

1. Instalar/Atualizar o **Docker**;
1. Clonar este repositório em uma pasta local: `git clone https://github.com/flaviofgf/magnetis_test.git`
1. Editar a variável de ambiente do [Dockerfile](https://github.com/flaviofgf/magnetis_test/blob/master/docker/airflow/Dockerfile)
do **Airflow** [linha 9](https://github.com/flaviofgf/magnetis_test/blob/ea921141d206033bfba08a74d1c1eadbf3be7771/docker/airflow/Dockerfile#L9):
 `ENV HOST_MAIN_PATH 'D:/Projects/magnetis_test'` colocando o caminho absoluto do projeto na sua máquina
1. No console ir a pasta do projeto e subir o **Docker Compose**: `docker-compose -f docker-compose.yml up -d --build`
1. Aguarde a mensagem: `'Compose: magnetis_test' has been deployed successfully.`
1. Ainda no console digite: `docker-compose logs spark` e acesse o link do **Jupyter Notebook** que comece por
`http://127.0.0.1:8888/`
1. Assim já poderá acessar as seguintes urls:
    + **Jupyter**: [http://127.0.0.1:8888/](http://127.0.0.1:8888/)
    + **Airflow**: [http://127.0.0.1:8080/](http://127.0.0.1:8080/)

## Execução
1. Jupyter: 
    1. Acesse o [Jupyter](http://127.0.0.1:8888/) e acesse [notebook/etl.ipynb](http://127.0.0.1:8888/notebooks/notebook/etl.ipynb);
    1. Poderá na barra de menu ir em `Cell/Run All` ou teclar `Shift + Enter` nas células e ver seus resultados;
    1. O arquivo csv com o resultado estará em `data/ipynb_result.csv`.
1. PySpark:
    1. Acesso o [Airflow](http://127.0.0.1:8080/);
    1. Ligue a dag **etl_DAG** (ao ligar já entrará em execução)
    1. O arquivo csv com o resultado estará em `data/result.csv`.
1. SQL:
    1. Acesso o [Airflow](http://127.0.0.1:8080/);
    1. Ligue a dag **sql_DAG** (ao ligar já entrará em execução)
    1. O arquivo csv com o resultado estará em `data/sql_result.csv`.

Qualquer dúvida estou inteiramente à disposição.
