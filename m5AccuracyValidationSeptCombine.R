setwd("m5AV")

library(tidyverse)
library(stringr)
#sample_submission %>% filter( grepl("evaluation", sample_submission$id) ) %>% dim()


#for
cleanFiles <- function(path, city, store){
  #read in files in the path
  dirDat <- list.files(path , 
                       pattern =  paste0("^(",city,")Data(",store,")(Hobbies|Household|Food)CleanTT_hts_aggts_forecast_ts_f(\\.csv)$"#_(","\\d\\.\\d+","), 
                       )
  )
  
  k = purrr::pmap_dfc(.f = read_csv, .l = list(dirDat ) )
  
  nm <- colnames(k)
  
  colnames(k) <- if_else( 
    grepl( "^HOBB_\\d_\\d{3}|^HOUS_\\d_\\d{3}|^FOOD_\\d_\\d{3}", nm), 
    stringr::str_c( str_sub(nm, 1, 10), str_sub(nm, 12),store,"validation", sep = "_"   ) , nm
    #if_else( grepl( "^HOBB_\\d__\\d\\.\\d+|^HOUS_\\d__\\d\\.\\d+|^FOOD_\\d__\\d\\.\\d+", nm), #\\d+{2,3}
     #        stringr::str_c( str_sub(nm, 1, 6), store, str_sub(nm, 9), "validation", sep = "_"), 
      #       stringr::str_c( str_sub(nm, 1, 5), store, 
       #                      str_sub(nm, 7), "validation", sep = "_"   ) 
    #)
  )
  k
}

#CA_1#####

CA_1 <- cleanFiles(path = ".",city = "CA",store = "CA_1")
head( CA_1 )

CA_1_t <- t(CA_1) %>% as.data.frame()
CA_1_tt <- subset(CA_1_t, !grepl("Total",rownames(CA_1_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( CA_1_tt) <- c("id", str_c("F", 1:28 ) )

#CA_2####
CA_2 <- cleanFiles(path = ".",city = "CA",store = "CA_2")
head( CA_2 ) 
#View( CA_2 )

CA_2_t <- t(CA_2) %>% as.data.frame()
CA_2_tt <- subset(CA_2_t, !grepl("Total",rownames(CA_2_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( CA_2_tt) <- c("id", str_c("F", 1:28 ) )
#
#CA_3#####
CA_3 <- cleanFiles(path = ".",city = "CA",store = "CA_3")
head( CA_3 ) 
#View( CA_3 )

CA_3_t <- t(CA_3) %>% as.data.frame()
CA_3_tt <- subset(CA_3_t, !grepl("Total",rownames(CA_3_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( CA_3_tt) <- c("id", str_c("F", 1:28 ) )

#CA_4####
CA_4 <- cleanFiles(path = ".",city = "CA",store = "CA_4")
head( CA_4 ) 
#View( CA_4 )

CA_4_t <- t(CA_4) %>% as.data.frame()
CA_4_tt <- subset(CA_4_t, !grepl("Total",rownames(CA_4_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( CA_4_tt) <- c("id", str_c("F", 1:28 ) )
#

#TX_1####
TX_1 <- cleanFiles(path = ".",city = "TX",store = "TX_1")
head( TX_1 ) 
#View( TX_1 )

TX_1_t <- t(TX_1) %>% as.data.frame()
TX_1_tt <- subset(TX_1_t, !grepl("Total",rownames(TX_1_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( TX_1_tt) <- c("id", str_c("F", 1:28 ) )

#TX_2#####
TX_2 <- cleanFiles(path = ".",city = "TX",store = "TX_2")
head( TX_2 ) 
#View( TX_2 )

TX_2_t <- t(TX_2) %>% as.data.frame()
TX_2_tt <- subset(TX_2_t, !grepl("Total",rownames(TX_2_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( TX_2_tt) <- c("id", str_c("F", 1:28 ) )

#TX_3####
TX_3 <- cleanFiles(path = ".",city = "TX",store = "TX_3")
head( TX_3 ) 
#View( TX_3 )

TX_3_t <- t(TX_3) %>% as.data.frame()
TX_3_tt <- subset(TX_3_t, !grepl("Total",rownames(TX_3_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( TX_3_tt) <- c("id", str_c("F", 1:28 ) )


#WI_1####
WI_1 <- cleanFiles(path = ".",city = "WI",store = "WI_1")
head( WI_1 ) 
#View( WI_1 )

WI_1_t <- t(WI_1) %>% as.data.frame()
WI_1_tt <- subset(WI_1_t, !grepl("Total",rownames(WI_1_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( WI_1_tt) <- c("id", str_c("F", 1:28 ) )

#WI_2####
WI_2 <- cleanFiles(path = ".",city = "WI",store = "WI_2")
head( WI_2 ) 
#View( WI_2 )

WI_2_t <- t(WI_2) %>% as.data.frame()
WI_2_tt <- subset(WI_2_t, !grepl("Total",rownames(WI_2_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( WI_2_tt) <- c("id", str_c("F", 1:28 ) )

#WI_3####

WI_3 <- cleanFiles(path = ".",city = "WI",store = "WI_3")
head( WI_3 ) 
#View( WI_3 )

WI_3_t <- t(WI_3) %>% as.data.frame()
WI_3_tt <- subset(WI_3_t, !grepl("Total",rownames(WI_3_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( WI_3_tt) <- c("id", str_c("F", 1:28 ) )



#bind####
store_data_validation <- 
  bind_rows(CA_1_tt,CA_2_tt,CA_3_tt,CA_4_tt,TX_1_tt,TX_2_tt,TX_3_tt,WI_1_tt,WI_2_tt,WI_3_tt)

head(store_data_validation[,1:2])
tail(store_data_validation[,1:2])

stores_validation <- store_data_validation %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation[,1:2])

#write
write_csv(stores_validation,"stores_validation.csv")




