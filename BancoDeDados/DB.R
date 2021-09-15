# Importando pacotes ------------------------------------------------------

library(read.dbc)
library(dplyr)
library(lubridate)
library(tidyr)
library(forcats)
library(ggplot2)
library(DBI)
library(RMySQL)
library(readr)
library(stringi)
library(svMisc)
library(data.table)

# Definir pasta de trabalho -----------------------------------------------

setwd("E:/DataSience/PROJETOS/DATASUS/SIASUS")

# Conexão MySQL -----------------------------------------------------------

drv = dbDriver("MySQL")

con = dbConnect(drv, user = "user", password = "passwd",
                dbname = "name", host = "localhost")

dbListTables(con)
dbWriteTable(con, "table", df, overwrite = TRUE)

# Verificar os arquivos descompactados do DATASUS -------------------------

arquivos <- as.vector(list.files(pattern = "dbc"))
arquivos

# Loop para trasformar str de bytes para utf-8 e salvar no MySQL

for (i in seq_along(arquivos)) {
  df <- read.dbc(arquivos[i])
  df <- as_tibble(df)
  var1 <- as.vector(df$AP_CNSPCN)
  var2 <- as.vector(df$AQ_ESQU_P1) 
  var3 <-  as.vector(df$AQ_ESQU_P2)
  var1 <- stri_enc_toutf8(var1, is_unknown_8bit = TRUE, validate = TRUE)
  var2 <- stri_enc_toutf8(var2, is_unknown_8bit = TRUE, validate = TRUE)
  var3 <- stri_enc_toutf8(var3, is_unknown_8bit = TRUE, validate = TRUE)
  df$AP_CNSPCN <- NULL
  df$AQ_ESQU_P1 <- NULL
  df$AQ_ESQU_P2 <- NULL
  df$AP_CNSPCN <- var1
  df$AQ_ESQU_P1 <- var2
  df$AQ_ESQU_P2 <- var3
  fwrite(df, "temp.csv")
  df <- fread("temp.csv", encoding = "UTF-8")
  dbWriteTable(con, "table", df, append = TRUE, row.names = TRUE)
  print(arquivos[i])
}

dbDisconnect(con)