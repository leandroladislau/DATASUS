# Importando pacotes ------------------------------------------------------

library(read.dbc)
library(dplyr)
library(DBI)
library(RMySQL)
library(stringi)
library(data.table)

setwd("your_folder")

# Conexão MySQL -----------------------------------------------------------

drv = dbDriver("MySQL")

con = dbConnect(drv, user = "root", password = "password",
                dbname = "db_name", host = "IP")

dbListTables(con)

# Verificar os arquivos descompactados do DATASUS -------------------------

arquivos <- as.vector(list.files(pattern = "dbc"))
arquivos

# Loop simples para salvar no MySQL

for (i in seq_along(arquivos)) {
  df <- read.dbc(arquivos[i])
  dbWriteTable(con, "table", df, append = TRUE, row.names = FALSE)
  print(arquivos[i])
}
