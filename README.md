# 📊 MVP - Engenharia de Dados - Performance Ofensiva de Jogadores de Basquete

Nome: Murillo Andrade

Matrícula: 4052025000833

## 📓 Glossário e Observações

* BPSS = Nome abreviado do Dataset utilizado, Basketball Players Stats per Season - 49 Leagues. Utilizei essa abreviação para salvar as tabelas nos níveis prata e bronze no Databricks.
* Cestas = Em inglês, Field Goals. Se tratam de todos os arremessos convertidos durante o jogo. Ao converter uma cesta antes da linha de 3 pontos, que é mais difícil, o arremesso converte 3 pontos para a equipe. Quando não for especificado que são cestas de 3 pontos, o atributo se refere à todas as cestas no geral, excluindo lances livres.
* Lances livres = Em inglês, Free Throws. O jogador que sofrer uma falta enquanto ataca tem a oportunidade de fazer arremessos que valem 1 ponto cada de uma posição próxima à cesta, sem marcação adversária.
* Limitações = o dataset não possui distinção de posição dos jogadores. Além disso, a qualidade defensiva dos times adversários não é levada em consideração.
* Temporada Regular e Mata-Mata (ou Playoffs): O dataset divide a temporada da NBA em temporada regular, em que os times de cada conferência (Leste e Oeste) competem para ficar no top 8 times com mais pontos, para classificarem para a temporada de mata-mata, onde os melhores times se enfrentam em jogos eliminatórios até sobrar o campeão. O dataset discrimina os dois tipos de temporada, e nesse projeto utilizei apenas as fase de playoffs.

## 🧠 Objetivos:
O desempenho dos jogadores na NBA é resultado de uma combinação de diversos fatores, que são colocados à prova nos jogos de maior pressão competitiva, como os Playoffs. O estilo predominante da liga está sempre em evolução, vide as equipes de Rockets e Warriors protagonizando um foco maior nas cestas de 3 pontos na metade da década de 2010. Nesse contexto, a análise estatística aplicada permite identificar padrões de desempenho, comparar atuações em diferentes fases da carreira e tentar identificar fatores que mais contribuem para o sucesso no mata-mata. O objetivo do projeto é analisar a evolução ofensiva ao longo do tempo na NBA, por meio de métricas estatísticas e individuais, avaliando eficiência, tempo em quadra, idade, e atuações de destaque no mata-mata.

Para isso, o trabalho buscará responder as seguintes perguntas:

1) Existe correlação entre tempo em quadra e eficiência dos jogadores?

2) Como foi a evolução das métricas ofensivas ao longo do tempo na NBA?

3) Quais jogadores mais se destacam no mata mata?

4) Quais são as melhores atuações no mata mata?

5) Existe correlação entre idade dos jogadores e desempenho nos Playoffs?

6) Quais são os principais fatores estatísticos que influenciam o sucesso nos Playoffs?


## 📚 Coleta e Modelagem

O dataset foi retirado da URL: https://www.kaggle.com/datasets/jacobbaruch/basketball-players-stats-per-season-49-leagues

O mesmo possui estatísticas diversas sobre jogadores de várias ligas, desde a temporada 1999 - 2000 até 2019 - 2020. O tamanho do arquivo é de 9.44 MB.

O modelo utilizado é flat por cada conceito, ou seja, foi utilizada apenas uma tabela.

## 📝 Preparação

Foi criada uma aba de preparação no databricks para a criação dos schemas e catálogos do projeto. Manualmente, foi realizado o upload do CSV do dataset para o volume "basquete:"
<img width="1117" height="253" alt="image" src="https://github.com/user-attachments/assets/cba0ff84-ba08-4f99-8ebe-527a8e5e0c54" />


## 🥉 Bronze
Nessa etapa, o conteúdo do CSV foi transformado em data frame e posteriormente transformado em tabela delta, dentro do schema bronze. Foi criado um Catálogo de Dados com essa tabela, com os domínios e a descrição de cada coluna.

<img width="498" height="708" alt="image" src="https://github.com/user-attachments/assets/66fea0c6-b637-4e34-a8c9-0e4e04a28337" />
<img width="485" height="504" alt="image" src="https://github.com/user-attachments/assets/4dc78e18-24b9-4176-a337-ec38b015e54d" />

## 🥈 Prata

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

## 🥇 Ouro

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

## 🔍 Análise

# Qualidade de Dados

O conjunto de dados utilizado não apresenta problemas de qualidade que comprometam a análise, especialmente no que se refere aos atributos selecionados para o estudo. Durante o processo de feature engineering, algumas métricas derivadas podem assumir valores nulos em situações onde não há tentativas registradas (por exemplo, Shaquille O'Neal não tentava cestas de 3 pontos). Nesses casos, os valores serão tratados e convertidos para zero, de forma a manter a consistência dos dados e evitar vieses decorrentes de valores ausentes. Posteriormente, as métricas criadas que apresentarem valor igual a zero não serão consideradas nas análises finais, já que as consultas possuem condições que impedem que o valor seja igual a zero.

# Análise das Perguntas
