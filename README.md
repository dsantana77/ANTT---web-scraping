# ANTT---web-scraping
WEB SCRAPING COM PYTHON - monitoramento de mercado de transporte terrestre

### Obs: 
A pasta contem script python que executa todas as atividades propostas:
  + Web Scraping
  + cria pasta e salva arquivos (os arquivos salvos não são subcritos se script for executado pela segunda vez)
  + concatena os arquivos salvos ver mais explicações abaixo
  + filtra os ultimos 3 meses e salva em .csv e .parquet

    adicional: a questão de encode está prevista

### Justificativa
O monitoramento de mercados é essencial para que os operadores mantenham sua competitividade,
aumentando sua participação no mercado. Isso inclui o estudo das movimentações dos concorrentes,
como a aquisição de novos mercados e trechos, além do crescimento e retenção de clientes. Para
facilitar a análise desses dados, a ANTT disponibiliza um conjunto de dados abertos sobre o mercado
rodoviário. Entre esses dados estão:

  • Histórico de Linhas
  • Pontos do Esquema Operacional
  • Serviços Paralisados
  • Empresas, Linhas e Seções
  • Horários

### Fonte
Os dados podem ser acessados pela seguinte URL: Gerenciamento de Autorizações - Conjuntos de
dados - Portal de Dados Abertos ANTT (https://dados.antt.gov.br/dataset/gerenciamento-de-
autorizacoes).

### Objetivo
Desenvolver um script em Python que acesse a página de gerenciamento de autorizações
da ANTT, verifique se há novas atualizações (novas bases de dados disponíveis) e realizar o download
das bases no formato CSV.

### Orientações Gerais:
### Passo 1 - scraping
• Criado um script em Python que seja executado diariamente e acesse a página de gerenciamento
de autorizações da ANTT para identificar e baixar novas bases de dados.
• Focamos apenas nas bases "Horários" e "Empresas, Linhas e Seções".
• O script salva os arquivos em formato CSV em uma pasta no mesmo diretório onde é
executado.
• Rodando pela segunda vez o script baixa e processa apenas os arquivos que ainda não foram baixados.

### Passo 2 - Concatenação e Engineering
• Após o download, cada novo arquivo deve será concatenado em um único arquivo histórico pra
cada base (**"Horários" e “Empresas, Linhas e Seções”**)
o Para as bases "Horários", os arquivos são concatenados em um único arquivo
chamado **horarios_historico**, que será salvo no formato csv e parquet.
o Para as bases "Empresas, Linhas e Seções", os arquivos devem ser concatenados em
um único arquivo chamado **empresas_e_secoes_historico**, que será salvo no formato
csv e parquet.

• Durante a concatenação, são adicionadas três colunas:
o data_download (data em que o download foi feito);
o data_competencia (mês e ano a que a base pertence, no formato YYYY-MM-DD); e
o fonte (nome do arquivo original).
