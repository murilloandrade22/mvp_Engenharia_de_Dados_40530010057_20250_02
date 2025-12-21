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

Inicialmente, foi criado um catálogo denominado MVP, responsável por organizar e centralizar os objetos de dados utilizados no projeto. Em seguida, definiu-se um schema de staging e um volume basquete, destinados ao armazenamento dos dados brutos provenientes do arquivo CSV, garantindo a separação entre dados de origem e dados processados. Manualmente, foi realizado o upload do CSV do dataset para esse volume:
<img width="1117" height="253" alt="image" src="https://github.com/user-attachments/assets/cba0ff84-ba08-4f99-8ebe-527a8e5e0c54" /> 

Posteriormente, foram criados os schemas bronze, prata e ouro, que serão utilizados nas etapas subsequentes do data lake, permitindo a evolução dos dados desde o nível bruto, passando por tratamentos e enriquecimentos, até camadas analíticas prontas para consumo e análise.

## 🥉 Bronze
Nessa etapa, o conteúdo do CSV foi transformado em data frame e posteriormente transformado em tabela delta, dentro do schema bronze. Os dados foram inicialmente carregados a partir do arquivo CSV armazenado no volume de staging, utilizando o método de leitura do Apache Spark com inferência automática de esquema e reconhecimento do cabeçalho. Em seguida, o DataFrame resultante foi persistido no formato Delta, utilizando sobrescrita controlada, e salvo como uma tabela estruturada. Esse mesmo procedimento foi adotado posteriormente para o armazenamento das tabelas nas camadas prata e ouro.

Foi criado um Catálogo de Dados com essa tabela, com os domínios, a descrição de cada coluna, exemplos se for uma medida categórica e o range se for uma medida numérica:

<img width="633" height="727" alt="image" src="https://github.com/user-attachments/assets/c0b18aa0-0e5d-44b1-8f67-5e13005d2a32" />
<img width="732" height="774" alt="image" src="https://github.com/user-attachments/assets/3ffe7f45-ba95-4fee-9d6f-abdfd6e4d518" />
<img width="622" height="433" alt="image" src="https://github.com/user-attachments/assets/6256196e-e169-44ae-b475-3318ae2957a0" />
<img width="718" height="799" alt="image" src="https://github.com/user-attachments/assets/db0ca9a2-b6c5-47e5-9482-f8eadf14af24" />
<img width="714" height="532" alt="image" src="https://github.com/user-attachments/assets/8201c3a2-6b63-4305-a531-7bb6278e100d" />

## 🥈 Prata

A partir da tabela bronze, foram utilizadas algumas condições para filtrar linhas específicas e relevantes para o projeto. A condição de um mínimo de 100 minutos para cada jogador visa evitar números muito altos de porcentagem causados por poucas tentativas e nivelar a consistência das atuações, para que o projeto analise o topo de jogadores em atividade. 

Condições:
* Jogadores da NBA
* Apenas durante os playoffs
* Mais de 100 minutos em quadra

Além disso, apenas essas colunas foram selecionadas:
<img width="857" height="94" alt="image" src="https://github.com/user-attachments/assets/f84fd347-dfbe-4edd-a76d-42e1ae371673" />

Foram criadas novas colunas por um processo de feature engineering e criada a tabela prata:
<img width="664" height="529" alt="image" src="https://github.com/user-attachments/assets/2c0d662b-097e-4209-b9e1-29fa166b2dce" />


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

## 1) Existe correlação entre tempo em quadra e eficiência dos jogadores?
**Correlação entre porcentagens de acerto e minutagem em quadra**
| correlacao_fg_pct_min    | correlacao_3p_pct_min | correlacao_ft_pct_min | correlacao_pts_per_min_min |
| ------------------------ | --------------------- | --------------------- | -------------------------- |
|   0.17332496241421205    | 0.014819217559017831  |  0.003299391977271271 |    0.23883546680802328     |

Foi feita a correlação entre as porcentagens de acerto e os minutos em quadra. Os dados mostram baixa correlação com porcentagem de arremessos em quadra e com pontos por minuto e sem correlação com porcentagem de arremessos de 3 pontos e de lances livres. Há fatores como nível de qualidade de jogadores, posição de cada jogador e times no qual o jogador atuou contra ou estava atuando que podem ser investigados posteriormente para verificar uma correlação.

## 2) Como foi a evolução das métricas ofensivas ao longo do tempo na NBA?

**Média da Porcentagem de Acertos de Arremessos em quadra X Temporada**
<img width="885" height="500" alt="image" src="https://github.com/user-attachments/assets/73a083d2-cc14-475f-871a-da626d6d489f" />

Apesar de sempre ter uma variação anual, a tendência é da média de acertos de arremessos estar aumentando no geral.

**Média da Porcentagem de Acertos de Arremessos de 3 pontos X Temporada**
<img width="885" height="500" alt="image" src="https://github.com/user-attachments/assets/d801503a-5c21-49ec-9e05-00ba01c3dd83" />

Por ser uma medida com mais risco, as variações entre cada ano são maiores do que as de arremessos no geral. Também possui uma tendência positiva, mas apesar do estilo atual da NBA demandar mais cestas de 3, a quantidade de erros pode ser decisiva.

**Média da Porcentagem de Acertos de Lances Livres X Temporada**
<img width="885" height="500" alt="image" src="https://github.com/user-attachments/assets/db3453ba-dc80-43ea-a6c7-60e178191ec2" />

Varia bastante e tem uma maior evolução a partir de 2015. Como se trata de um fundamento que defensores também precisam realizar com frequência, verificar a diferença de efetividade entre as posições pode ser interessante em um estudo futuro.

**Média de Pontos por Minuto X Temporada**
<img width="885" height="500" alt="image" src="https://github.com/user-attachments/assets/fdfa9d77-f4c8-4663-a31c-74a78f0412a9" />

O gráfico mostra uma constante evolução. Isso mostra que os jogadores com maior tempo nos playoffs estão pontuando cada vez mais nessa fase decisiva. Seria interessante verificar se houve uma mudança em relação à defesa dos times, talvez haja uma correlação.

## 3) Quais jogadores mais se destacam no mata mata?

**Top 3 jogadores de acordo com cada métrica entre a temporada de 1999 - 2000 até 2019 - 2020**
  
| Player          | Métrica | Tipo de Métrica              |
|-----------------|---------|------------------------------|
| LeBron James    | 10811.7 | Minutos Jogados              |
| Tim Duncan      | 8263.3  | Minutos Jogados              |
| Kobe Bryant     | 7971.7  | Minutos Jogados              |
| LeBron James    | 2671    | Cestas Marcadas              |
| Kobe Bryant     | 1901    | Cestas Marcadas              |
| Tim Duncan      | 1758    | Cestas Marcadas              |
| Stephen Curry   | 470     | Cestas de 3 Marcadas         |
| LeBron James    | 414     | Cestas de 3 Marcadas         |
| Ray Allen       | 376     | Cestas de 3 Marcadas         |
| LeBron James    | 1735    | Lances Livres Marcados       |
| Kobe Bryant     | 1235    | Lances Livres Marcados       |
| Dirk Nowitzki   | 1074    | Lances Livres Marcados       |
| LeBron James    | 7491    | Pontos Marcados              |
| Kobe Bryant     | 5312    | Pontos Marcados              |
| Tim Duncan      | 4591    | Pontos Marcados              |

A tabela evidencia a longevidade e domínio de LeBron James, Tim Duncan e Kobe Bryant em métricas acumuladas. Como se tratam dos jogadores com maior minutagem, aparecem em outras medidas com frequência. Principalmente Lebron James, que lidera quase todas as estatísticas. Em relação ao arremesso de três pontos, observa-se a presença de Stephen Curry, cuja presença simboliza a mudança da NBA para maior foco nos arremessos de 3 pontos.

## 4) Quais são as maiores atuações no mata mata?

**Maiores métricas em uma temporada**
| player                  | season       | metrica | tipo_metrica                              |
|-------------------------|--------------|---------|-------------------------------------------|
| Chris Andersen          | 2012 - 2013  | 0.8     | Porcentagem de Cestas Convertidas          |
| Andris Biedrins         | 2006 - 2007  | 0.7     | Porcentagem de Cestas Convertidas          |
| DeAndre Jordan          | 2013 - 2014  | 0.7     | Porcentagem de Cestas Convertidas          |
| Nazr Mohammed           | 2004 - 2005  | 1.0     | Porcentagem de Cestas de 3 Convertidas     |
| Tim Duncan              | 2000 - 2001  | 1.0     | Porcentagem de Cestas de 3 Convertidas     |
| Rasheed Wallace         | 1999 - 2000  | 0.6     | Porcentagem de Cestas de 3 Convertidas     |
| Shane Battier           | 2008 - 2009  | 1.0     | Porcentagem de Lances Livres Convertidos   |
| Stephen Curry           | 2017 - 2018  | 1.0     | Porcentagem de Lances Livres Convertidos   |
| Ray Allen               | 2008 - 2009  | 0.9     | Porcentagem de Lances Livres Convertidos   |
| Donovan Mitchell        | 2019 - 2020  | 1.0     | Pontos por Minuto                          |
| Giannis Antetokounmpo   | 2019 - 2020  | 0.9     | Pontos por Minuto                          |
| Kevin Durant            | 2018 - 2019  | 0.9     | Pontos por Minuto                          |

A tabela mostra desempenhos ofensivos de alta eficiência em diferentes temporadas da NBA, com valores máximos concentrados em métricas de aproveitamento, como arremessos de três pontos e lances livres. Independentemente do tempo em quadra, essas métricas denotam um grande impacto em quadra.

## 5) Qxiste correlação entre idade dos jogadores e desempenho nos Playoffs?

**Variação de melhoria de cestas de acordo com a idade do jogador**
<img width="885" height="500" alt="image" src="https://github.com/user-attachments/assets/16456095-3ed5-4e4b-90af-06f16f9f26e8" />

O gráfico mostra uma grande variabilidade nas idades mais jovens, com picos elevados de melhoria no início da carreira, intercalados por quedas abruptas, o que sugere instabilidade e influência de fatores como adaptação à liga e volume de tentativas. A partir da faixa dos 30 anos, a melhoria tende a se estabilizar em níveis mais moderados, com menor amplitude de variação, indicando maior consistência no desempenho. Em idades mais avançadas, ainda ocorrem picos pontuais, sugerindo que experiência e tomada de decisão podem compensar possíveis limitações físicas, mas não há um padrão crescente sustentado ao longo do tempo.

**Variação de melhoria de cestas de 3 pontos de acordo com a idade do jogador**
<img width="885" height="500" alt="image" src="https://github.com/user-attachments/assets/0ab04adf-2009-4b5e-b47f-3001844e6828" />

Novamente, há uma alta volatilidade em relação aos mais jovens. O que pode ser interessante é que ao longo do tempo a necessidade das tentativas de 3 pontos aumentou, então é possível realizar essa comparação de acordo com diferentes períodos.

**Variação de melhoria de lances livres de acordo com a idade do jogador**
<img width="885" height="500" alt="image" src="https://github.com/user-attachments/assets/08dff743-ffce-4c72-b1ae-fbaf486e9bb0" />

A medida é a com menor variabilidade, indicando que idades mais avançadas conseguem acompanhar os valores dos mais jovens.

**Variação de melhoria de pontos por minuto de acordo com a idade do jogador**
<img width="885" height="500" alt="image" src="https://github.com/user-attachments/assets/8ef80517-68a4-4a99-b467-2ae8e22f91b7" />

O gráfico mostra um alto pico aos 23 anos e uma clara queda após os 28 anos. Com isso, os jogadores mais experientes não conseguem replicar as performances de antes, e precisam equilibrar o tempo em quadra.

# Autoavaliação

A execução deste trabalho permitiu atingir com sucesso a maior parte dos objetivos propostos, viabilizando uma análise consistente do desempenho de jogadores da NBA a partir de métricas estatísticas de basquete. Foi possível investigar as cinco primeiras perguntas realizadas e identificar relações entre idade dos jogadores, evolução das estatísticas ao longo dos anos e observar atuações individuais para identificar longevidade e desempenho. A análise dos principais fatores estatísticos que influenciam diretamente o sucesso nos Playoffs não pôde ser plenamente realizada, uma vez que o conjunto de dados disponível não contempla informações suficientes para caracterizar sucesso coletivo ou resultado das séries, além da posição de cada jogadore e de métricas defensivas. Essas limitações foram explicitadas ao longo das análises, juntamente com restrições metodológicas que podem ser ampliadas e refinadas em estudos futuros, por meio da incorporação de novos atributos e fontes de dados.
