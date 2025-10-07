# comexstat_ncm

# creating loop for download files
anos <- as.integer(lubridate::year(lubridate::today())-5):
  as.integer(lubridate::year(lubridate::today()))
EXP <- vector(mode = 'list', length = length(anos))
IMP <- vector(mode = 'list', length = length(anos))

for(i in seq_along(anos)){
  
  # Tentativa com controle de erros para exportação
  sucesso_exp <- FALSE
  while(!sucesso_exp) {
    tryCatch({
      link_file_exp <- paste0("https://balanca.economia.gov.br/balanca/bd/",
                              "comexstat-bd/ncm/EXP_", anos[i], ".csv")
      EXP[[i]] <- readr::read_csv2(link_file_exp)
      sucesso_exp <- TRUE  # Se sucesso, sai do loop
    }, error = function(e) {
      cat("Erro ao baixar o arquivo de exportação do ano", anos[i],
          "- Tentando novamente...\n")
      Sys.sleep(5)  # Aguarda 5 segundos antes de tentar novamente
    })
  }
  
  # Tentativa com controle de erros para importação
  sucesso_imp <- FALSE
  while(!sucesso_imp) {
    tryCatch({
      link_file_imp <- paste0("https://balanca.economia.gov.br/balanca/",
                              "bd/comexstat-bd/ncm/IMP_", anos[i], ".csv")
      IMP[[i]] <- readr::read_csv2(link_file_imp)
      sucesso_imp <- TRUE  # Se sucesso, sai do loop
    }, error = function(e) {
      cat("Erro ao baixar o arquivo de importação do ano", anos[i],
          "- Tentando novamente...\n")
      Sys.sleep(5)  # Aguarda 5 segundos antes de tentar novamente
    })
  }
}

# unifying all data

EXP <-
  EXP |> dplyr::bind_rows() |>
  dplyr::mutate(VL_FRETE = NA) |>
  dplyr::mutate(VL_SEGURO = NA) |>
  dplyr::mutate(tipo_transacao = "Exportação")

IMP <-
  IMP |> dplyr::bind_rows() |>
  dplyr::mutate(tipo_transacao = "Importação")

comercio_exterior_ncm <- EXP |> dplyr::bind_rows(IMP)

#comercio_exterior_ncm <- comercio_exterior_ncm |> dplyr::select(!"<!DOCTYPE html>")

# transform column date

comercio_exterior_ncm <- 
  comercio_exterior_ncm |> 
  tidyr::unite(competencia, "CO_MES", "CO_ANO", sep = "-") |> 
  dplyr::mutate(competencia = stringr::str_c("01-", competencia)) |>
  dplyr::mutate(competencia = lubridate::dmy(competencia)) |>
  dplyr::rename(SG_UF = SG_UF_NCM)

# downloading and applying decoder 

link_decodificador <-
  "https://balanca.economia.gov.br/balanca/bd/tabelas/TABELAS_AUXILIARES.xlsx"

curl::curl_download(link_decodificador,
                    paste0(getwd(), "/decodificador_comexstat.xlsx"))

# preparing a decoder list

nomes_atraduzir <- readxl::excel_sheets("decodificador_comexstat.xlsx")

lista_tradutor <- vector(mode = 'list',
                         length = length(nomes_atraduzir)-1)


for(l in seq_along(lista_tradutor)){
  lista_tradutor[[l]] <-
    readxl::read_excel("decodificador_comexstat.xlsx", as.character(l)) |> #dplyr::glimpse()
    dplyr::select(c(contains("CO_NCM"), !contains(c("_ESP", "_ING", "CO_"))) | # 1 to 9
                    c(contains(c("CO_PAIS", "CO_PAIS_ISOA3"))) | # 10 11 # 12
                    c(contains("CO_MUN_GEO")) |
                    contains("CO_VIA") |
                    contains("CO_URF")
    )
}

# finding common names to decode

nomes_atraduzir <- vector(mode = 'list',
                          length = length(lista_tradutor)) 

for(l in seq_along(nomes_atraduzir)){
  nomes_atraduzir[[l]] <-
    dplyr::intersect(names(comercio_exterior_ncm), names(lista_tradutor[[l]]))
}

nomes_atraduzir <- nomes_atraduzir |> paste()

# Decoding data part by part to be able to safe in a list

compilado_traduzido <- vector(mode = 'list',
                              length = length(nomes_atraduzir))


for(l in seq_along(lista_tradutor)){
  tryCatch({
    compilado_traduzido[[l]] <-
      comercio_exterior_ncm[nomes_atraduzir[l]] |>
      dplyr::left_join(lista_tradutor[[l]], multiple = "first") |>
      dplyr::select(!contains(c("CO_NCM", "CO_PAIS", "CO_MUN_GEO", "CO_VIA",
                                "CO_URF")))},
    error = function(err) { warning("file could not be join") })
}

# Unifying and selecting every decoded data into one file

#compilado_traduzido[[13]] <- NULL #não sei pq era necessario

compilado_traduzido <-
  compilado_traduzido |> purrr::list_cbind()

# Unifying with decode part

comercio_exterior_ncm <- comercio_exterior_ncm |>
  dplyr::bind_cols(compilado_traduzido)

# selecting variables

nomes_semremocao <- comercio_exterior_ncm |> names()

nomes_comremocao <- c("competencia", "QT_ESTAT", "KG_LIQUIDO", "VL_FOB",
                      "VL_FRETE", "tipo_transacao", "NO_NCM_POR...14",
                      "NO_SH4_POR", "NO_ISIC_SECAO...33", "NO_CUCI_SEC",
                      "SG_UNID", "NO_PAIS", "NO_VIA", "NO_URF", "NO_UF",
                      "NO_REGIAO")

comercio_exterior_ncm <- comercio_exterior_ncm |>
  dplyr::select(nomes_comremocao)

# creating a column with positive and negative numbers

comercio_exterior_ncm <- comercio_exterior_ncm |>
  dplyr::mutate(VL_FOB_BIVALENTE = dplyr::case_when(
    tipo_transacao == "Exportação" ~ VL_FOB * 1,
    tipo_transacao == "Importação" ~ VL_FOB * -1
  ))

comercio_exterior_ncm |> dplyr::glimpse()

# Using the particular produce decoder to adding more information

compilado_decodificador_endereço <-
  paste0("https://github.com/WillianDambros/data_source/raw/refs/",
         "heads/main/compilado_decodificador.xlsx")

decodificador_endereco <- paste0(getwd(), "/compilado_decodificador.xlsx")

curl::curl_download(compilado_decodificador_endereço,
                    decodificador_endereco)

"compilado_decodificador.xlsx" |> readxl::excel_sheets()

geo_estados <- 
  readxl::read_excel("compilado_decodificador.xlsx",
                     sheet =  "geo_estados") |>
  dplyr::select(-Região_estados)

#setdiff(comercio_exterior_ncm$NO_UF, geo_estados$ESTADO)
#setdiff(geo_estados$ESTADO, comercio_exterior_ncm$NO_UF)

comercio_exterior_ncm <- comercio_exterior_ncm |>
  dplyr::left_join(geo_estados,
                   by = dplyr::join_by(NO_UF == ESTADO))

geo_paises <- 
  readxl::read_excel("compilado_decodificador.xlsx",
                     sheet =  "geo_paises")

#setdiff(comercio_exterior_ncm$NO_PAIS, geo_paises$`País ou território`)
#setdiff(geo_paises$`País ou território`, comercio_exterior_ncm$NO_PAIS)

comercio_exterior_ncm <- comercio_exterior_ncm |>
  dplyr::left_join(geo_paises,
                   by = dplyr::join_by(NO_PAIS == `País ou território`))

# Writing file

#nome_arquivo_csv <- "comexstat_estado_ncm"

#caminho_arquivo <- paste0("D:/comexstat_temporario","/",nome_arquivo_csv, ".txt")

#readr::write_csv2(comercio_exterior_ncm, caminho_arquivo)

###################   wrinting in postgresql
comercio_exterior_ncm |> dplyr::glimpse()

comercio_exterior_ncm$competencia |> unique()
#estabelecendo conexao

source("X:/POWER BI/NOVOCAGED/conexao.R")

RPostgres::dbListTables(conexao)

schema_name <- "comexstat"

table_name <- "comexstat_ncm_estado"

DBI::dbSendQuery(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

RPostgres::dbWriteTable(conexao,
                        name = DBI::Id(schema = schema_name,
                                       table = table_name),
                        value = comercio_exterior_ncm,
                        row.names = FALSE, overwrite = TRUE)

RPostgres::dbDisconnect(conexao)
