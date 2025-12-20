mvp_Engenharia_de_Dados_40530010057_20250_02

Objetivo:
Analisar a evolução ofensiva ao longo do tempo na NBA, por meio de métricas estatísticas e individuais, avaliando eficiência, tempo em quadra, idade, e atuações de destaque no mata-mata.

Para isso, o trabalho buscará responder as seguintes perguntas:

Existe correlação entre tempo em quadra e eficiência dos jogadores?

Como foi a evolução das métricas ofensivas ao longo do tempo na NBA?

Quais jogadores mais se destacam no mata mata?

Quais são as melhores atuações no mata mata?

Existe correlação entre idade dos jogadores e desempenho nos Playoffs?

Quais são os principais fatores estatísticos que influenciam o sucesso nos Playoffs?


# Coleta e Modelagem

O dataset foi retirado da URL: https://www.kaggle.com/datasets/jacobbaruch/basketball-players-stats-per-season-49-leagues

O mesmo possui estatísticas diversas sobre jogadores de várias ligas, desde a temporada 1999 - 2000 até 2019 - 2020. O tamanho do arquivo é de 9.44 MB.

O modelo utilizado é flat por cada conceito, ou seja, foi utilizada apenas uma tabela.

# Preparação

Foi criada uma aba de preparação no databricks para a criação dos schemas e catálogos do projeto. Manualmente, foi realizado o upload do CSV do dataset para o volume "basquete:"
<img width="1117" height="253" alt="image" src="https://github.com/user-attachments/assets/cba0ff84-ba08-4f99-8ebe-527a8e5e0c54" />


# Bronze
Nessa etapa, o conteúdo do CSV foi transformado em data frame e posteriormente transformado em tabela delta, dentro do schema bronze. Foi criado um Catálogo de Dados com essa tabela, com os domínios e a descrição de cada coluna.

<img width="498" height="708" alt="image" src="https://github.com/user-attachments/assets/66fea0c6-b637-4e34-a8c9-0e4e04a28337" />
<img width="485" height="504" alt="image" src="https://github.com/user-attachments/assets/4dc78e18-24b9-4176-a337-ec38b015e54d" />

# Prata

A partir da tabela bronze, foram utilizadas algumas condições para filtrar linhas específicas e relevantes para o projeto. A condição de um mínimo de 100 minutos para cada jogador visa evitar números muito altos de porcentagem causados por poucas tentativas e nivelar a consistência das atuações, para que o projeto analise o topo de jogadores em atividade. 

Condições:
* Jogadores da NBA
* Apenas durante os playoffs
* Mais de 100 minutos em quadra

Além disso, apenas essas colunas foram selecionadas:
<img width="857" height="94" alt="image" src="https://github.com/user-attachments/assets/f84fd347-dfbe-4edd-a76d-42e1ae371673" />

Foram criadas novas colunas por um processo de feature engineering e criada a tabela prata:
<img width="763" height="432" alt="image" src="https://github.com/user-attachments/assets/19e4a27d-dbed-425d-b112-ede6b3ee0484" />

Foi verificado que não há valores nulos na tabela prata, o que se dá pela condição de um mínimo de 100 minutos jogados por cada jogador na temporada.

# Ouro

Foram criadas tabelas na camada ouro para responder cada pergunta:

**Existe correlação entre tempo em quadra e eficiência dos jogadores?**

Escolhidas apenas as medidas de minutos em quadra, porcentagens de acerto e pontos por minuto, para determinar uma possível correlaçãp.

**Como foi a evolução das métricas ofensivas ao longo do tempo na NBA?**

Escolhidas apenas as medidas de temporada, porcentagens de acerto e pontos por minuto, para determinar se houve uma tendência geral.

**Quais jogadores mais se destacam no mata mata?**

Foram somados os valores ofensivos históricos e somados por jogador, para determinar quais possuem os melhores números.

**Quais são as melhores atuações no mata mata?**

Escolhidas apenas as medidas de temporada, jogador, porcentagens de acerto, pontos por minuto e minutagem, para verificar as maiores atuações específicas em temporadas de acordo com as métricas criadas.

**Existe correlação entre idade dos jogadores e desempenho nos Playoffs?**

Primeiramente, foi criada uma tabela com a comparação entre a performance anual dos jogadores de acordo com as métricas de porcentagem de acerto e pontos por minuto. Posteriormente, foi criada uma tabela para somar as vezes em que a performance melhorou ou piorou em um atributo para cada idade: a ideia é identificar quais idades possuem maiores picos de melhoria ou piora.
