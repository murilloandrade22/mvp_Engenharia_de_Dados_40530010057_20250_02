# mvp_Engenharia_de_Dados_40530010057_20250_02

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

O mesmo possui estatísticas diversas sobre jogadores de várias ligas, desde a temporada 1999 - 2000 até 2019 - 2020.

O modelo utilizado é flat por cada conceito, ou seja, foi utilizada apenas uma tabela.

# Preparação

Foi criada uma aba de preparação no databricks para a criação dos schemas e catálogos do projeto. Manualmente, foi realizado o upload do CSV do dataset para o volume "basquete:"
<img width="1117" height="253" alt="image" src="https://github.com/user-attachments/assets/cba0ff84-ba08-4f99-8ebe-527a8e5e0c54" />


# Bronze
Nessa etapa, o conteúdo do CSV foi transformado em data frame e posteriormente transformado em tabela delta, dentro do schema bronze. Foi criado um Catálogo de Dados com essa tabela, com os domínios e a descrição de cada coluna.

<img width="498" height="708" alt="image" src="https://github.com/user-attachments/assets/66fea0c6-b637-4e34-a8c9-0e4e04a28337" />
<img width="485" height="504" alt="image" src="https://github.com/user-attachments/assets/4dc78e18-24b9-4176-a337-ec38b015e54d" />

# Prata

A partir da tabela bronze, foram utilizadas algumas condições para filtrar linhas específicas e relevantes para o projeto.

Condições:
* Jogadores da NBA
* Apenas durante os playoffs
* Mais de 100 minutos em quadra

Além disso, apenas essas colunas foram selecionadas:
<img width="857" height="94" alt="image" src="https://github.com/user-attachments/assets/f84fd347-dfbe-4edd-a76d-42e1ae371673" />
