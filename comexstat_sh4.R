# comexstat_sh4 comércio exterior por municípios #

setwd("Z:/rstudio/comexstat/comexstat_sh4")

# loop to download files

anos <- as.integer(lubridate::year(lubridate::today())-4):
  as.integer(lubridate::year(lubridate::today()))
EXP <- vector(mode = 'list', length = length(anos))
IMP <- vector(mode = 'list', length = length(anos))

for(i in seq_along(anos)){
  
  link_file_exp <-
    paste0("https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/EXP_",
           anos[i],"_MUN", ".csv")
  
  EXP[[i]] <- readr::read_csv2(link_file_exp,
                               col_types = readr::cols(
                                 "KG_LIQUIDO" = readr::col_double(),
                                 "VL_FOB" = readr::col_double(),
                                 .default = readr::col_character()
                               ))
  
  link_file_imp <-
    paste0("https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/IMP_",
           anos[i], "_MUN", ".csv")
  
  IMP[[i]] <- readr::read_csv2(link_file_imp,
                               col_types = readr::cols(
                                 "KG_LIQUIDO" = readr::col_double(),
                                 "VL_FOB" = readr::col_double(),
                                 .default = readr::col_character()
                               ))
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

# Writing file

nome_arquivo_csv <- "comercio_exterior_sh4"

caminho_arquivo <- paste0(getwd(),"/",nome_arquivo_csv, ".txt")

readr::write_csv2(comercio_exterior_sh4,
                  caminho_arquivo)
