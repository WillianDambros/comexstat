# comexstat_ncm
# creating loop for download files
setwd("Z:/rstudio/comexstat/comexstat_ncm")

anos <- as.integer(lubridate::year(lubridate::today())-4):
  as.integer(lubridate::year(lubridate::today()))
EXP <- vector(mode = 'list', length = length(anos))
IMP <- vector(mode = 'list', length = length(anos))

for(i in seq_along(anos)){
  
  link_file_exp <-
    paste0("https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_",
           anos[i], ".csv")
  
  EXP[[i]] <- readr::read_csv2(link_file_exp)
  
  link_file_imp <-
    paste0("https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_",
           anos[i], ".csv")
  
  IMP[[i]] <- readr::read_csv2(link_file_imp)
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

# transform column date

comercio_exterior_ncm <- 
  comercio_exterior_ncm |> 
  tidyr::unite(competencia, "CO_MES", "CO_ANO", sep = "-") |> 
  dplyr::mutate(competencia = stringr::str_c("01-", competencia)) |>
  dplyr::mutate(competencia = lubridate::dmy(competencia))

# downloading and applying decoder 

link_decodificador <-
  "https://balanca.economia.gov.br/balanca/bd/tabelas/TABELAS_AUXILIARES.xlsx"

curl::curl_download(link_decodificador,
                    paste0(getwd(), "/decodificador_comexstat.xlsx"))

# preparing a decoder list

nomes_atraduzir<- readxl::excel_sheets("decodificador_comexstat.xlsx")

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

compilado_traduzido <-
  compilado_traduzido |> purrr::list_cbind()

# Unifying with decode part

comercio_exterior_ncm <- comercio_exterior_ncm |>
  dplyr::bind_cols(compilado_traduzido)

# selecting variables

nomes_semremocao <- comercio_exterior_ncm |> names()

nomes_comremocao <- c("competencia", "CO_NCM", "CO_UNID", "CO_PAIS",
                      "SG_UF_NCM", "CO_VIA", "CO_URF", "QT_ESTAT", "KG_LIQUIDO",
                      "VL_FOB", "tipo_transacao", "NO_NCM_POR...14",
                      "NO_SEC_POR", "NO_CUCI_SEC","NO_CGCE_N2", "NO_ISIC_GRUPO",
                      "NO_UNID","SG_UNID", "NO_PAIS",
                      "NO_BLOCO")

comercio_exterior_ncm <- comercio_exterior_ncm |>
  dplyr::select(nomes_comremocao)

# Writing file

nome_arquivo_csv <- "comercio_exterior_ncm"

caminho_arquivo <- paste0(getwd(),"/",nome_arquivo_csv, ".txt")

readr::write_csv2(comercio_exterior_ncm,
                  caminho_arquivo)
