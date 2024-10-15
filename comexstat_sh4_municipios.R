# comexstat_sh4 comércio exterior por municípios #

# loop to download files

anos <- as.integer(lubridate::year(lubridate::today())-4):
  as.integer(lubridate::year(lubridate::today()))
EXP <- vector(mode = 'list', length = length(anos))
IMP <- vector(mode = 'list', length = length(anos))

for(i in seq_along(anos)){
  
  
  # tentativa com controle de erros exportação
  sucesso_exp <- FALSE
  while(!sucesso_exp) {
    tryCatch({
      link_file_exp <- paste0("https://balanca.economia.gov.br/balanca/bd/",
                              "comexstat-bd/mun/EXP_",anos[i],"_MUN", ".csv")
      EXP[[i]] <- readr::read_csv2(link_file_exp,
                                   col_types = readr::cols(
                                     "KG_LIQUIDO" = readr::col_double(),
                                     "VL_FOB" = readr::col_double(),
                                     .default = readr::col_character()
                                     ))
      sucesso_exp <- TRUE # Se sucesso, sai do loop
    }, error = function(e) {
      cat("Erro ao baixar o arquivo exportação do ano", anos[i],
          "- Tentando novamente...\n")
      Sys.sleep(5) # Aguarda 5 segundos antes de tentar novamente
    })
  }
  
  # tentativa com controle de erros para importação
  sucesso_imp <- FALSE
  while (!sucesso_imp) {
    tryCatch({
      link_file_imp <- 
        paste0("https://balanca.economia.gov.br/balanca/",
               "bd/comexstat-bd/mun/IMP_", anos[i], "_MUN", ".csv")
      
      IMP[[i]] <- readr::read_csv2(link_file_imp,
                                   col_types = readr::cols(
                                     "KG_LIQUIDO" = readr::col_double(),
                                     "VL_FOB" = readr::col_double(),
                                     .default = readr::col_character()
                                   ))
      sucesso_imp <- TRUE # Se sucesso, sai do loop
    }, error = function(e) {
      cat("Erro ao baixar o arquivo de importação do ano", anos[i],
          "- Tentando novamente...\n")
      Sys.sleep(5) # Aguarda 5 segundos antes de tentar novamente
    })
  }
}

# unifying data

EXP <-
  EXP |> dplyr::bind_rows() |>
  dplyr::mutate(tipo_transacao = "Exportação")

IMP <-
  IMP |> dplyr::bind_rows() |>
  dplyr::mutate(tipo_transacao = "Importação")

comercio_exterior_sh4 <- EXP |> dplyr::bind_rows(IMP)

# transform column date and renaming

comercio_exterior_sh4 <- 
  comercio_exterior_sh4 |> 
  tidyr::unite(competencia, "CO_MES", "CO_ANO", sep = "-") |> 
  dplyr::mutate(competencia = stringr::str_c("01-", competencia)) |>
  dplyr::mutate(competencia = lubridate::dmy(competencia)) |>
  dplyr::rename(CO_SH4 = SH4, SG_UF = SG_UF_MUN, CO_MUN_GEO = CO_MUN)

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
    dplyr::select(c(contains("CO_SH4"),
                    !contains(c("_ESP", "_ING", "CO_", "_NCM_POR", "SH6"))) | # 1 to 9
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
    dplyr::intersect(names(comercio_exterior_sh4), names(lista_tradutor[[l]]))
}

nomes_atraduzir <- nomes_atraduzir |> paste()

nomes_atraduzir[[13]] <- "CO_MUN_GEO"  # fixing bug

# Join all decode into list and isolating only the decode part

compilado_traduzido <- vector(mode = 'list',
                              length = length(nomes_atraduzir))

for(l in seq_along(lista_tradutor)){
  tryCatch({
    compilado_traduzido[[l]] <-
      comercio_exterior_sh4[nomes_atraduzir[l]] |>
      dplyr::left_join(lista_tradutor[[l]], multiple = "first") |>
      dplyr::select(!contains(c("CO_SH4", "CO_PAIS", "SG_UF", "CO_MUN_GEO")))
  },
  error = function(err) { warning("file could not be join") })
}

# Unifying and selecting every decoded data into one file

compilado_traduzido <-
  compilado_traduzido |> purrr::list_cbind()

# Unifying main file with decode part

comercio_exterior_sh4 <- comercio_exterior_sh4 |>
  dplyr::bind_cols(compilado_traduzido)

# creating a column with positive and negative numbers

comercio_exterior_sh4 <- comercio_exterior_sh4 |>
  dplyr::mutate(VL_FOB_BIVALENTE = dplyr::case_when(
    tipo_transacao == "Exportação" ~ VL_FOB * 1,
    tipo_transacao == "Importação" ~ VL_FOB * -1
  ))

comercio_exterior_sh4 |> dplyr::glimpse()


compilado_decodificador_endereço <-
  paste0("https://github.com/WillianDambros/data_source/raw/refs/",
         "heads/main/compilado_decodificador.xlsx")

decodificador_endereco <- paste0(getwd(), "/compilado_decodificador.xlsx")

curl::curl_download(compilado_decodificador_endereço,
                    decodificador_endereco)

"compilado_decodificador.xlsx" |> readxl::excel_sheets()

# compilado códigos de Latitude e Longitude para munícipios de MT


territorialidade_municipios_mt <-
  readxl::read_excel(decodificador_endereco,
                     sheet = "territorialidade_municipios_mt") |> 
  dplyr::select(territorio_municipio_codigo_7d,
                territorio_latitude,
                territorio_longitude,
                rpseplan10340_munícipio_polo_decodificado,
                rpseplan10340_regiao_decodificado) |>
  dplyr::mutate(
    territorio_latitude = as.numeric(territorio_latitude),
    territorio_longitude = as.numeric(territorio_longitude)
  )

territorialidade_municipios_mt |>dplyr::glimpse()

comercio_exterior_sh4 <- comercio_exterior_sh4 |>
  dplyr::left_join(territorialidade_municipios_mt,
                   by = dplyr::join_by(CO_MUN_GEO ==
                                         territorio_municipio_codigo_7d))


geo_paises <- 
  readxl::read_excel("compilado_decodificador.xlsx",
                     sheet =  "geo_paises")

# buscar como contemplar todos paises, fonte oficial

comercio_exterior_sh4 <- comercio_exterior_sh4 |>
  dplyr::left_join(geo_paises, 
                   by = dplyr::join_by(NO_PAIS == `País ou território`))

# Writing file

#nome_arquivo_csv <- "comercio_exterior_sh4"

#caminho_arquivo <- paste0(getwd(),"/",nome_arquivo_csv, ".txt")

#readr::write_csv2(comercio_exterior_sh4,
#                  caminho_arquivo)

# writing PostgreSQL

conexao <- RPostgres::dbConnect(RPostgres::Postgres(),
                                dbname = "###########",
                                host = "#############",
                                port = "#############",
                                user = "##############",
                                password = "#############")

RPostgres::dbListTables(conexao)

schema_name <- "comexstat"

table_name <- "comexstat_sh4_municipios"

DBI::dbSendQuery(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

RPostgres::dbWriteTable(conexao,
                        name = DBI::Id(schema = schema_name,table = table_name),
                        value = comercio_exterior_sh4,
                        row.names = FALSE, overwrite = TRUE)

RPostgres::dbDisconnect(conexao)
