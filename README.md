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

<img width="612" height="610" alt="image" src="https://github.com/user-attachments/assets/09cea05a-1bd3-469c-b87e-038f68858b83" />
<img width="634" height="714" alt="image" src="https://github.com/user-attachments/assets/081b4959-d60b-432d-aa4a-e120abd4c535" />
<img width="600" height="729" alt="image" src="https://github.com/user-attachments/assets/49f19c5a-5ac7-4d77-9dec-b38651ca1756" />
<img width="628" height="158" alt="image" src="https://github.com/user-attachments/assets/9a27fc5d-0cbe-49d8-b4ff-e41f3fbb5e7e" />

# Prata



