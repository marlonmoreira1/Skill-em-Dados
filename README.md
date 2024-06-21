# Skill em Dados
## 1. Descrição
#### Definição do Problema
A ideia para este projeto surgiu da falta de direcionamento sobre o que estudar e quais habilidades desenvolver, por parte de muitos iniciantes na área de dados, incluindo colegas de faculdade e cursos. A volumosa quantidade de ferramentas e materiais didáticos disponíveis pode acabar gerando uma sobrecarga no aluno por não haver um caminho definido para seguir.
Objetivo do Projeto
O objetivo deste projeto é contribuir com a formação de iniciantes que almejam entrar na área de dados, fornecendo uma visão baseada em dados sobre as habilidades e conhecimentos mais demandados pelo mercado. Através da coleta e análise de vagas de emprego/estágio, o projeto visa responder à pergunta: “Como se tornar um profissional de dados no Brasil?” Para isso, são identificados as ferramentas, tecnologias, soft skills, metodologias, formações acadêmicas e outros requisitos mais frequentemente requeridos nas vagas. Assim, criei um guia no formato de dashboard, no qual é possível visualizar, de modo ágil, as demandas do que estudar e se capacitar para ingressar no mercado de trabalho. Ao fornecer dados concretos e atualizados sobre as exigências mercadológicas, este projeto ajuda a reduzir a incerteza que muitos iniciantes enfrentam, oferecendo um caminho mais claro para o desenvolvimento de suas carreiras na área de dados.
Perguntas a Serem Respondidas
1.	Quais são as ferramentas e tecnologias mais demandadas no mercado de trabalho na área de dados?
2.	Quais são as soft skills e metodologias ágeis mais requisitadas?
3.	Qual é a formação acadêmica mais requerida nas vagas?
4.	Qual a proporção das vagas de trabalho remoto e presencial?
5.	Qual a senioridade e tipo de contrato das vagas?
6.	Como as vagas estão distribuídas geograficamente no Brasil?

[Link Para o Dashboard](https://app.powerbi.com/view?r=eyJrIjoiMzZlMWIxNzEtZmU1YS00YTNlLWJlMWItNjQzMTNhMTA0NTIwIiwidCI6ImMzODRkN2Y5LTdhNDEtNDZiOS04ZTRjLWQzOTJlMGU4Zjc4OSJ9)

#### Público-alvo do Projeto
Este projeto é destinado a pessoas que estão no início da carreira profissional na área de dados. Além disso, também pode oferecer valor significativo para outros públicos, tais como:
*	Profissionais da área: Interessados em se atualizar, obtendo uma visão das demandas do mercado.
*	Recrutadores: Buscando entender as tendências e necessidades do mercado para melhorar seus processos de recrutamento e seleção.
*	Empresas de Tecnologia: Planejando treinamento e desenvolvimento de equipes com base nas competências mais requisitadas.
*	Instituições de Ensino: Ajustando currículos e programas de ensino para atender às demandas atuais do mercado de trabalho.
## 2. Busca dos Dados
#### Fonte de Dados
*	API: Google Jobs via SerpAPI
*	Formato: JSON
*	Frequência de Coleta: Diariamente
#### Descrição dos Dados
Os dados coletados incluem informações detalhadas sobre vagas de emprego na área de dados. As colunas e suas respectivas descrições são as seguintes:
*	company_name: O nome da empresa que está oferecendo a vaga.
*	via: A fonte ou plataforma através da qual a vaga foi postada.
*	job_id: Um identificador único para cada vaga de emprego/estágio.
*	date: A data em que a vaga foi postada.
*	xp: Nível de experiência requerida para a vaga.
*	new_title: O cargo específico da vaga de emprego/estágio.
*	estado: O estado brasileiro onde a vaga está localizada.
*	cidade: A cidade brasileira onde a vaga está localizada.
*	is_remote: Indica se a vaga é trabalho remoto ou não.
*	hard_skills: As habilidades técnicas exigidas para a vaga.
*	complemento: Competências ou tecnologias mais citadas nas vagas.
*	soft_skills: As habilidades interpessoais e comportamentais requeridas.
*	graduacoes: Os requisitos de formação acadêmica para a vaga.
*	metodologia_trabalho: As metodologias de trabalho preferidas ou exigidas (como metodologias ágeis).
*	tipo_contrato: O tipo de contrato oferecido (por exemplo, CLT ou PJ).
#### Limitação na Coleta de Dados
Devido às restrições do plano gratuito da SerpAPI, o projeto enfrenta limitações na quantidade de dados que podem ser coletados diariamente. O plano gratuito permite apenas 100 requisições mensais. Portanto, apesar da coleta diária, o projeto consegue capturar entre 50 a 60 vagas por dia. Essa limitação deve ser considerada ao interpretar e utilizar os dados coletados, garantindo uma análise consciente das tendências do mercado de trabalho na área de dados.




## 3. Definição da Arquitetura
O projeto utiliza a seguinte arquitetura para coleta, transformação, armazenamento e visualização dos dados obtidos da API da SerpAPI:

![arquitetura-projeto](https://github.com/marlonmoreira1/dadossobredados/assets/71144665/a45d409a-e613-45e0-9e89-e1a4f10dfb6c)

#### Apache Airflow
O Apache Airflow desempenha um papel central na arquitetura do projeto, sendo utilizado para orquestrar o processo de ETL (Extração, Transformação e Carga) dos dados. A DAG (Directed Acyclic Graph) no Airflow é estruturada utilizando operadores Python (PythonOperator) para executar as tarefas de Extrair, Transformar e Carregar de forma automatizada e sequencial.
#### BigQuery
Os dados transformados são armazenados no Google BigQuery. Essa escolha se deve ao nível generoso de armazenamento gratuito que o BigQuery oferece, permitindo até 10GB por mês, o que é adequado para o volume atual de dados do projeto.
#### Power BI
O Power BI foi selecionado como a ferramenta de visualização principal devido às seguintes razões estratégicas:
*	Conectividade com a Fonte de Dados: O Power BI permite conectar-se a várias fontes de dados, incluindo o Google BigQuery, facilitando a integração e análise de dados.
*	Publicação na Web Gratuita: A capacidade de publicar o dashboard como uma página web de forma gratuita e simples, com a possibilidade de atualização diária, facilita o compartilhamento do projeto.
*	Robustez na Visualização de Dados: O Power BI oferece recursos avançados de visualização que permitem criar dashboards e relatórios altamente personalizados, proporcionando uma análise detalhada das tendências e demandas do mercado de trabalho na área de dados, de maneira intuitiva e informativa.
  
Esses atributos do Power BI foram determinantes na escolha da ferramenta para suportar a visualização dos dados coletados e transformados no projeto.
#### Python
O Python desempenha um papel fundamental na maioria das etapas do projeto, a saber:
*	Coleta de Dados: Utilizado para realizar chamadas à API da SerpAPI e extrair os dados no formato JSON.
*	Transformação de Dados: Utilizado para manipular e transformar os dados brutos coletados em uma estrutura de dados tabular que é a adequada para o armazenamento e a análise.
*	Integração com Apache Airflow: Utilizado na definição de operadores e tarefas na DAG do Apache Airflow, permitindo a automação e a orquestração das etapas de ETL.
*	Integração com Google BigQuery: Utilizado para carregar os dados transformados no BigQuery.
  
O Python foi escolhido devido à sua versatilidade, ampla gama de bibliotecas para manipulação de dados e integração com outras ferramentas essenciais do projeto.

## Conclusão
Este projeto pode fornecer orientações valiosas para iniciantes na área de dados e contribuir para uma compreensão das tendências do mercado de trabalho em dados no Brasil. Os recursos foram projetados de modo a oferecer uma visão objetiva da dinâmica mercadológica, auxiliando na jornada dos iniciantes e interessados na área de dados.
Conheça os detalhes de cada etapa do projeto no artigo publicado no Medium:
*	Processo de ETL e Automação
*	Construção do Dashboard
*	Deploy e Automação da Atualização
*	Insights e Uso do Aplicativo (Dashboard)

[Medium em Breve](www.pspojdsa.com)
