setwd("projects/m5/m5Unc/")

list.files()

library(tidyverse)
# BaseFiles1 ------------------------
#Import Function -----------------------------------------------------------------------

cleanFiles <- function(path, city, store){
  #read in files in the path
  dirDat <- list.files(".", 
                       pattern =  paste0(
                         "^(",city,")Data(",store,")(Hobbies|Household|Food)CleanTT_hts_aggts_forecast_ts_f_(\\d+\\.\\d+|\\d+)(\\.csv)$"
                       ))
  
  k = purrr::pmap_dfc(.f = read_csv, .l = list(dirDat ) )
  
  nm <- colnames(k)
  
  colnames(k) <- if_else( 
    grepl( "^HOBB_\\d_\\d{3}|^HOUS_\\d_\\d{3}|^FOOD_\\d_\\d{3}", nm), 
    stringr::str_c( str_sub(nm, 1, 10), store, str_sub(nm, 12),"validation", sep = "_"   ) , nm
  )
  k <- k %>% select(contains(store))
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

stores_validation1 <- store_data_validation %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation1[,1:2])

#write
write_csv(stores_validation1,"D:/datas/proj/stores_validation_base1.csv")
#
######DEPTOTALX -----------------------------------

store_data_validation_3 <- 
  bind_cols(CA_1,CA_2,CA_3,CA_4,TX_1,TX_2,TX_3,WI_1,WI_2,WI_3)

(store_data_validation_3[c(1:5),c(1)])

names(store_data_validation_3) %>% head()

names(store_data_validation_3) <-gsub("_0.25_","_0.250_",names(store_data_validation_3), fixed = TRUE)
names(store_data_validation_3) <-gsub("_0.5_","_0.500_",names(store_data_validation_3), fixed = TRUE)
names(store_data_validation_3) <-gsub("_0.75_","_0.750_",names(store_data_validation_3), fixed = TRUE)

names(store_data_validation_3) <- str_c(
  str_sub(names(store_data_validation_3),1,11),
  str_sub(names(store_data_validation_3),17),
  str_sub(names(store_data_validation_3),12,16)
)
head(names(store_data_validation_3))

library(hts)

store_data_validation_3_agg <- aggts(
  hts(store_data_validation_3, characters = c(11,16,5)),levels = c(2)
) 

View(colnames(store_data_validation_3_agg))

colnames(store_data_validation_3_agg) <- str_c(
  str_sub(colnames(store_data_validation_3_agg),1,11), "_","X","_",
  str_sub(colnames(store_data_validation_3_agg),12)
)

View(store_data_validation_3_agg)
#
store_data_validation_3_agg_t <- t(store_data_validation_3_agg) %>% as.data.frame()
store_data_validation_3_agg_tt <- subset(store_data_validation_3_agg_t, 
                                         !grepl("Total",rownames(store_data_validation_3_agg_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( store_data_validation_3_agg_tt) <- c("id", str_c("F", 1:28 ) )
#
stores_validation12 <- store_data_validation_3_agg_tt %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation12[,1:2])
#write
write_csv(stores_validation12,"D:/datas/proj/stores_validation_base12.csv")
#

#####CITYTOTALDEPTOTAL-----------------------------------

store_data_validation_3 <- 
  bind_cols(CA_1,CA_2,CA_3,CA_4,TX_1,TX_2,TX_3,WI_1,WI_2,WI_3)

(store_data_validation_3[c(1:5),c(1)])

names(store_data_validation_3) %>% head()

names(store_data_validation_3) <-gsub("_0.25_","_0.250_",names(store_data_validation_3), fixed = TRUE)
names(store_data_validation_3) <-gsub("_0.5_","_0.500_",names(store_data_validation_3), fixed = TRUE)
names(store_data_validation_3) <-gsub("_0.75_","_0.750_",names(store_data_validation_3), fixed = TRUE)

names(store_data_validation_3) <- str_c(
  str_sub(names(store_data_validation_3),12,13),
  str_sub(names(store_data_validation_3),1,11),
  str_sub(names(store_data_validation_3),17),
  str_sub(names(store_data_validation_3),14,16)
)
head(names(store_data_validation_3))

library(hts)

store_data_validation_3_agg <- aggts(
  hts(store_data_validation_3, characters = c(2,11,16,3)),levels = c(3)
) 

View(colnames(store_data_validation_3_agg))

colnames(store_data_validation_3_agg) <- str_c(
  str_sub(colnames(store_data_validation_3_agg),1,2), "_",
  str_sub(colnames(store_data_validation_3_agg),3)
)

View(store_data_validation_3_agg)
#
store_data_validation_3_agg_t <- t(store_data_validation_3_agg) %>% as.data.frame()
store_data_validation_3_agg_tt <- subset(store_data_validation_3_agg_t, 
                                         !grepl("Total",rownames(store_data_validation_3_agg_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( store_data_validation_3_agg_tt) <- c("id", str_c("F", 1:28 ) )
#
stores_validation13 <- store_data_validation_3_agg_tt %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation13[,1:2])
#write
write_csv(stores_validation13,"D:/datas/proj/stores_validation_base13.csv")
#
#####CITYTOTALDEPTOTALALSO-----------------------------------

store_data_validation_3 <- 
  bind_cols(CA_1,CA_2,CA_3,CA_4,TX_1,TX_2,TX_3,WI_1,WI_2,WI_3)

(store_data_validation_3[c(1:5),c(1)])

names(store_data_validation_3) %>% head()

names(store_data_validation_3) <-gsub("_0.25_","_0.250_",names(store_data_validation_3), fixed = TRUE)
names(store_data_validation_3) <-gsub("_0.5_","_0.500_",names(store_data_validation_3), fixed = TRUE)
names(store_data_validation_3) <-gsub("_0.75_","_0.750_",names(store_data_validation_3), fixed = TRUE)

names(store_data_validation_3) <- str_c(
  str_sub(names(store_data_validation_3),1,6),
  str_sub(names(store_data_validation_3),17),
  str_sub(names(store_data_validation_3),7,16)
)
head(names(store_data_validation_3))

library(hts)

store_data_validation_3_agg <- aggts(
  hts(store_data_validation_3, characters = c(6,16,10)),levels = c(2)
) 

View(colnames(store_data_validation_3_agg))

colnames(store_data_validation_3_agg) <- str_c(
  str_sub(colnames(store_data_validation_3_agg),1,6), "_",
  str_sub(colnames(store_data_validation_3_agg),7)
)

View(store_data_validation_3_agg)
#
store_data_validation_3_agg_t <- t(store_data_validation_3_agg) %>% as.data.frame()
store_data_validation_3_agg_tt <- subset(store_data_validation_3_agg_t, 
                                         !grepl("Total",rownames(store_data_validation_3_agg_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( store_data_validation_3_agg_tt) <- c("id", str_c("F", 1:28 ) )
#
stores_validation14 <- store_data_validation_3_agg_tt %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation14[,1:2])
#write
write_csv(stores_validation14,"D:/datas/proj/stores_validation_base14.csv")
#
#ABOVEALSO------------------------
store_data_validation_3 <- 
  bind_cols(CA_1,CA_2,CA_3,CA_4,TX_1,TX_2,TX_3,WI_1,WI_2,WI_3)

(store_data_validation_3[c(1:5),c(1)])

names(store_data_validation_3) %>% head()

names(store_data_validation_3) <-gsub("_0.25_","_0.250_",names(store_data_validation_3), fixed = TRUE)
names(store_data_validation_3) <-gsub("_0.5_","_0.500_",names(store_data_validation_3), fixed = TRUE)
names(store_data_validation_3) <-gsub("_0.75_","_0.750_",names(store_data_validation_3), fixed = TRUE)

names(store_data_validation_3) <- str_c(
  str_sub(names(store_data_validation_3),1,6),
  str_sub(names(store_data_validation_3),17),
  str_sub(names(store_data_validation_3),7,16)
)
head(names(store_data_validation_3))

library(hts)

store_data_validation_3_agg <- aggts(
  hts(store_data_validation_3, characters = c(6,16,10)),levels = c(2)
) 

View(colnames(store_data_validation_3_agg))

colnames(store_data_validation_3_agg) <- str_c(
  str_sub(colnames(store_data_validation_3_agg),1,6), "_","X","_",
  str_sub(colnames(store_data_validation_3_agg),7)
)

View(store_data_validation_3_agg)
#
store_data_validation_3_agg_t <- t(store_data_validation_3_agg) %>% as.data.frame()
store_data_validation_3_agg_tt <- subset(store_data_validation_3_agg_t, 
                                         !grepl("Total",rownames(store_data_validation_3_agg_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( store_data_validation_3_agg_tt) <- c("id", str_c("F", 1:28 ) )
#
stores_validation15 <- store_data_validation_3_agg_tt %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation15[,1:2])
#write
write_csv(stores_validation15,"D:/datas/proj/stores_validation_base15.csv")
#
#
# BaseFiles2 ----------------------------------------------

cleanFiles <- function(path, city, store){
  #read in files in the path
  dirDat <- list.files(".", 
                       pattern =  paste0(
                         "^(",city,")Data(",store,")(Hobbies|Household|Food)CleanTT_hts_aggts_forecast_ts_f_(\\d+\\.\\d+|\\d+)(\\.csv)$"
                         #_(","\\d\\.\\d+","), 
                       ))
  
  k = purrr::pmap_dfc(.f = read_csv, .l = list(dirDat ) )
  
  nm <- colnames(k)
  
  colnames(k) <- if_else( 
    grepl("^HOBB_\\d__\\d\\.\\d+|^HOUS_\\d__\\d\\.\\d+|^FOOD_\\d__\\d\\.\\d+", nm), 
    stringr::str_c( store, nm, "validation", sep = "_"   ) , nm
  )
  k <- k %>% select(contains(store))
}


#CA_1#####

CA_1 <- cleanFiles(path = ".",city = "CA",store = "CA_1")
head( CA_1 )

CA_1_t <- t(CA_1) %>% as.data.frame()
CA_1_tt <- subset(CA_1_t, !grepl("Total",rownames(CA_1_t))) %>% 
  rownames_to_column(var = "id") %>% 
  mutate(id = str_replace(id,"__","_")) %>% 
  as_tibble()
colnames( CA_1_tt) <- c("id", str_c("F", 1:28 ) )

#CA_2####
CA_2 <- cleanFiles(path = ".",city = "CA",store = "CA_2")
head( CA_2 ) 
#View( CA_2 )

CA_2_t <- t(CA_2) %>% as.data.frame()
CA_2_tt <- subset(CA_2_t, !grepl("Total",rownames(CA_2_t))) %>% 
  rownames_to_column(var = "id") %>% 
  mutate(id = str_replace(id,"__","_")) %>% 
  as_tibble()
colnames( CA_2_tt) <- c("id", str_c("F", 1:28 ) )
#
#CA_3#####
CA_3 <- cleanFiles(path = ".",city = "CA",store = "CA_3")
head( CA_3 ) 
#View( CA_3 )

CA_3_t <- t(CA_3) %>% as.data.frame()
CA_3_tt <- subset(CA_3_t, !grepl("Total",rownames(CA_3_t))) %>% 
  rownames_to_column(var = "id") %>% 
  mutate(id = str_replace(id,"__","_")) %>% 
  as_tibble()
colnames( CA_3_tt) <- c("id", str_c("F", 1:28 ) )

#CA_4####
CA_4 <- cleanFiles(path = ".",city = "CA",store = "CA_4")
head( CA_4 ) 
#View( CA_4 )

CA_4_t <- t(CA_4) %>% as.data.frame()
CA_4_tt <- subset(CA_4_t, !grepl("Total",rownames(CA_4_t))) %>% 
  rownames_to_column(var = "id") %>% 
  mutate(id = str_replace(id,"__","_")) %>% 
  as_tibble()
colnames( CA_4_tt) <- c("id", str_c("F", 1:28 ) )
#

#TX_1####
TX_1 <- cleanFiles(path = ".",city = "TX",store = "TX_1")
head( TX_1 ) 
#View( TX_1 )

TX_1_t <- t(TX_1) %>% as.data.frame()
TX_1_tt <- subset(TX_1_t, !grepl("Total",rownames(TX_1_t))) %>% 
  rownames_to_column(var = "id") %>% 
  mutate(id = str_replace(id,"__","_")) %>% 
  as_tibble()
colnames( TX_1_tt) <- c("id", str_c("F", 1:28 ) )

#TX_2#####
TX_2 <- cleanFiles(path = ".",city = "TX",store = "TX_2")
head( TX_2 ) 
#View( TX_2 )

TX_2_t <- t(TX_2) %>% as.data.frame()
TX_2_tt <- subset(TX_2_t, !grepl("Total",rownames(TX_2_t))) %>% 
  rownames_to_column(var = "id") %>% 
  mutate(id = str_replace(id,"__","_")) %>% 
  as_tibble()
colnames( TX_2_tt) <- c("id", str_c("F", 1:28 ) )

#TX_3####
TX_3 <- cleanFiles(path = ".",city = "TX",store = "TX_3")
head( TX_3 ) 
#View( TX_3 )

TX_3_t <- t(TX_3) %>% as.data.frame()
TX_3_tt <- subset(TX_3_t, !grepl("Total",rownames(TX_3_t))) %>% 
  rownames_to_column(var = "id") %>% 
  mutate(id = str_replace(id,"__","_")) %>% 
  as_tibble()
colnames( TX_3_tt) <- c("id", str_c("F", 1:28 ) )


#WI_1####
WI_1 <- cleanFiles(path = ".",city = "WI",store = "WI_1")
head( WI_1 ) 
#View( WI_1 )

WI_1_t <- t(WI_1) %>% as.data.frame()
WI_1_tt <- subset(WI_1_t, !grepl("Total",rownames(WI_1_t))) %>% 
  rownames_to_column(var = "id") %>% 
  mutate(id = str_replace(id,"__","_")) %>% 
  as_tibble()
colnames( WI_1_tt) <- c("id", str_c("F", 1:28 ) )

#WI_2####
WI_2 <- cleanFiles(path = ".",city = "WI",store = "WI_2")
head( WI_2 ) 
#View( WI_2 )

WI_2_t <- t(WI_2) %>% as.data.frame()
WI_2_tt <- subset(WI_2_t, !grepl("Total",rownames(WI_2_t))) %>% 
  rownames_to_column(var = "id") %>% 
  mutate(id = str_replace(id,"__","_")) %>% 
  as_tibble()
colnames( WI_2_tt) <- c("id", str_c("F", 1:28 ) )

#WI_3####

WI_3 <- cleanFiles(path = ".",city = "WI",store = "WI_3")
head( WI_3 ) 
#View( WI_3 )

WI_3_t <- t(WI_3) %>% as.data.frame()
WI_3_tt <- subset(WI_3_t, !grepl("Total",rownames(WI_3_t))) %>% 
  rownames_to_column(var = "id") %>% 
  mutate(id = str_replace(id,"__","_")) %>% 
  as_tibble()
colnames( WI_3_tt) <- c("id", str_c("F", 1:28 ) )



#bind####
store_data_validation <- 
  bind_rows(CA_1_tt,CA_2_tt,CA_3_tt,CA_4_tt,TX_1_tt,TX_2_tt,TX_3_tt,WI_1_tt,WI_2_tt,WI_3_tt)

head(store_data_validation[,1:2])
tail(store_data_validation[,1:2])

stores_validation2 <- store_data_validation %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation2[,1:2])

#write
write_csv(stores_validation2,"D:/datas/proj/stores_validation_base2.csv")

######CITYDEPTOTALALLr-----------------------------------

store_data_validation_3 <- 
  bind_cols(CA_1,CA_2,CA_3,CA_4,TX_1,TX_2,TX_3,WI_1,WI_2,WI_3)

store_data_validation_3 <- store_data_validation_3 %>% 
  select(contains("__"))

#View(store_data_validation_3[c(1:5),c(1:5)])
(store_data_validation_3[c(1:5),c(1)])

names(store_data_validation_3) %>% head()

names(store_data_validation_3) <-gsub("_0.25_","_0.250_",names(store_data_validation_3), fixed = TRUE)
names(store_data_validation_3) <-gsub("_0.5_","_0.500_",names(store_data_validation_3), fixed = TRUE)
names(store_data_validation_3) <-gsub("_0.75_","_0.750_",names(store_data_validation_3), fixed = TRUE)

names(store_data_validation_3) <- str_c(
  str_sub(names(store_data_validation_3),1,2),
  str_sub(names(store_data_validation_3),6,13),
  str_sub(names(store_data_validation_3),14),
  str_sub(names(store_data_validation_3),3,5)
)

head(names(store_data_validation_3))

library(hts)

store_data_validation_3_agg <- aggts(
  hts(store_data_validation_3, characters = c(2,8,16,3)),levels = c(3)
) 

View(colnames(store_data_validation_3_agg))

colnames(store_data_validation_3_agg) <- str_c(
  str_sub(colnames(store_data_validation_3_agg),1,2), "_",
  str_sub(colnames(store_data_validation_3_agg),3)
)

View(store_data_validation_3_agg)
#
store_data_validation_3_agg_t <- t(store_data_validation_3_agg) %>% as.data.frame()
store_data_validation_3_agg_tt <- subset(store_data_validation_3_agg_t, 
                                         !grepl("Total",rownames(store_data_validation_3_agg_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( store_data_validation_3_agg_tt) <- c("id", str_c("F", 1:28 ) )
#
stores_validation11 <- store_data_validation_3_agg_tt %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation11[,1:2])
#write
write_csv(stores_validation11,"D:/datas/proj/stores_validation_base11.csv")
#
#GrandTotalsPErCategory---------------------------

list.files()
#prod = "Hobbies","Food", "FOOD"
cleanFiles <- function(path, city, store, prod){
  #read in files in the path
  dirDat <- list.files(".", 
                       pattern =  paste0(
                         "^(",city,")Data(",store,")(",prod,")CleanTT_hts_aggts_forecast_ts_f_(\\d+\\.\\d+|\\d+)(\\.csv)$"
                       ))
  k = purrr::pmap_dfc(.f = read_csv, .l = list(dirDat )
  ) %>% 
    select(contains("Total"))
  nm <- as.vector( colnames(k) )
  nm1 <-  if (prod == "Hobbies"){
    str_replace(nm, "Total", "HOBB")}
  else if ( prod == "Household"){
    str_replace(nm, "Total", "HOUS")}
  else { str_replace_all(nm, "Total", "FOOD")}
  
  colnames(k) <- stringr::str_c( store, nm1, "validation", sep = "_"   )
  k
}

#CA_1#####
CA_11 <- cleanFiles(path = ".",city = "CA",store = "CA_1", prod = "Hobbies")
CA_12 <- cleanFiles(path = ".",city = "CA",store = "CA_1", prod = "Household")
CA_13 <- cleanFiles(path = ".",city = "CA",store = "CA_1", prod = "Food")
CA_1 <- cbind.data.frame(CA_11, CA_12, CA_13)
head( CA_1 )

CA_1_t <- t(CA_1) %>% as.data.frame()
CA_1_tt <- subset(CA_1_t, !grepl("Total",rownames(CA_1_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( CA_1_tt) <- c("id", str_c("F", 1:28 ) )

#CA_2####
CA_21 <- cleanFiles(path = ".",city = "CA",store = "CA_2", prod = "Hobbies")
CA_22 <- cleanFiles(path = ".",city = "CA",store = "CA_2", prod = "Household")
CA_23 <- cleanFiles(path = ".",city = "CA",store = "CA_2", prod = "Food")
CA_2 <- cbind.data.frame(CA_21, CA_22, CA_23)

head( CA_2 ) 
#View( CA_2 )

CA_2_t <- t(CA_2) %>% as.data.frame()
CA_2_tt <- subset(CA_2_t, !grepl("Total",rownames(CA_2_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( CA_2_tt) <- c("id", str_c("F", 1:28 ) )
#
#CA_3#####
CA_31 <- cleanFiles(path = ".",city = "CA",store = "CA_3", prod = "Hobbies")
CA_32 <- cleanFiles(path = ".",city = "CA",store = "CA_3", prod = "Household")
CA_33 <- cleanFiles(path = ".",city = "CA",store = "CA_3", prod = "Food")
CA_3 <- cbind.data.frame(CA_31, CA_32, CA_33)

head( CA_3 ) 
#View( CA_3 )

CA_3_t <- t(CA_3) %>% as.data.frame()
CA_3_tt <- subset(CA_3_t, !grepl("Total",rownames(CA_3_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( CA_3_tt) <- c("id", str_c("F", 1:28 ) )

#CA_4####
CA_41 <- cleanFiles(path = ".",city = "CA",store = "CA_4", prod = "Hobbies")
CA_42 <- cleanFiles(path = ".",city = "CA",store = "CA_4", prod = "Household")
CA_43 <- cleanFiles(path = ".",city = "CA",store = "CA_4", prod = "Food")
CA_4 <- cbind.data.frame(CA_41, CA_42, CA_43)
head( CA_4 ) 
#View( CA_4 )

CA_4_t <- t(CA_4) %>% as.data.frame()
CA_4_tt <- subset(CA_4_t, !grepl("Total",rownames(CA_4_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( CA_4_tt) <- c("id", str_c("F", 1:28 ) )
#

#TX_1####
TX_11 <- cleanFiles(path = ".",city = "TX",store = "TX_1", prod = "Hobbies")
TX_12 <- cleanFiles(path = ".",city = "TX",store = "TX_1", prod = "Household")
TX_13 <- cleanFiles(path = ".",city = "TX",store = "TX_1", prod = "Food")
TX_1 <- cbind.data.frame(TX_11, TX_12, TX_13)

head( TX_1 ) 
#View( TX_1 )

TX_1_t <- t(TX_1) %>% as.data.frame()
TX_1_tt <- subset(TX_1_t, !grepl("Total",rownames(TX_1_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( TX_1_tt) <- c("id", str_c("F", 1:28 ) )

#TX_2#####
TX_21 <- cleanFiles(path = ".",city = "TX",store = "TX_2", prod = "Hobbies")
TX_22 <- cleanFiles(path = ".",city = "TX",store = "TX_2", prod = "Household")
TX_23 <- cleanFiles(path = ".",city = "TX",store = "TX_2", prod = "Food")
TX_2 <- cbind.data.frame(TX_21, TX_22, TX_23)

head( TX_2 ) 
#View( TX_2 )

TX_2_t <- t(TX_2) %>% as.data.frame()
TX_2_tt <- subset(TX_2_t, !grepl("Total",rownames(TX_2_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( TX_2_tt) <- c("id", str_c("F", 1:28 ) )

#TX_3####
TX_31 <- cleanFiles(path = ".",city = "TX",store = "TX_3", prod = "Hobbies")
TX_32 <- cleanFiles(path = ".",city = "TX",store = "TX_3", prod = "Household")
TX_33 <- cleanFiles(path = ".",city = "TX",store = "TX_3", prod = "Food")
TX_3 <- cbind.data.frame(TX_31, TX_32, TX_33)

head( TX_3 ) 
#View( TX_3 )

TX_3_t <- t(TX_3) %>% as.data.frame()
TX_3_tt <- subset(TX_3_t, !grepl("Total",rownames(TX_3_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( TX_3_tt) <- c("id", str_c("F", 1:28 ) )


#WI_1####
WI_11 <- cleanFiles(path = ".",city = "WI",store = "WI_1", prod = "Hobbies")
WI_12 <- cleanFiles(path = ".",city = "WI",store = "WI_1", prod = "Household")
WI_13 <- cleanFiles(path = ".",city = "WI",store = "WI_1", prod = "Food")
WI_1 <- cbind.data.frame(WI_11, WI_12, WI_13)

head( WI_1 ) 
#View( WI_1 )

WI_1_t <- t(WI_1) %>% as.data.frame()
WI_1_tt <- subset(WI_1_t, !grepl("Total",rownames(WI_1_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( WI_1_tt) <- c("id", str_c("F", 1:28 ) )

#WI_2####
WI_21 <- cleanFiles(path = ".",city = "WI",store = "WI_2", prod = "Hobbies")
WI_22 <- cleanFiles(path = ".",city = "WI",store = "WI_2", prod = "Household")
WI_23 <- cleanFiles(path = ".",city = "WI",store = "WI_2", prod = "Food")
WI_2 <- cbind.data.frame(WI_21, WI_22, WI_23)

head( WI_2 ) 
#View( WI_2 )

WI_2_t <- t(WI_2) %>% as.data.frame()
WI_2_tt <- subset(WI_2_t, !grepl("Total",rownames(WI_2_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( WI_2_tt) <- c("id", str_c("F", 1:28 ) )

#WI_3####

WI_31 <- cleanFiles(path = ".",city = "WI",store = "WI_3", prod = "Hobbies")
WI_32 <- cleanFiles(path = ".",city = "WI",store = "WI_3", prod = "Household")
WI_33 <- cleanFiles(path = ".",city = "WI",store = "WI_3", prod = "Food")
WI_3 <- cbind.data.frame(WI_31, WI_32, WI_33)

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

stores_validation3 <- store_data_validation %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation3[,1:2])

#write
write_csv(stores_validation3,"D:/datas/proj/stores_validation_base3.csv")

#CITYDEPTOTAL11111111r-----------------------------------

store_data_validation_3 <- 
  bind_cols(CA_1,CA_2,CA_3,CA_4,TX_1,TX_2,TX_3,WI_1,WI_2,WI_3)

View(store_data_validation_3)

names(store_data_validation_3) <-gsub("_0.25_","_0.250_",names(store_data_validation_3))
names(store_data_validation_3) <-gsub("_0.5_","_0.500_",names(store_data_validation_3))
names(store_data_validation_3) <-gsub("_0.75_","_0.750_",names(store_data_validation_3))

names(store_data_validation_3) <- str_c(
  str_sub(names(store_data_validation_3),1,2),
  str_sub(names(store_data_validation_3),6,10),
  str_sub(names(store_data_validation_3),11,26),
  str_sub(names(store_data_validation_3),3,5)
)

head(names(store_data_validation_3))

library(hts)

store_data_validation_3_agg <- aggts(
  hts(store_data_validation_3, characters = c(2,5,16,3)),levels = c(3)
) 

View(colnames(store_data_validation_3_agg))

colnames(store_data_validation_3_agg) <- str_c(
  str_sub(colnames(store_data_validation_3_agg),1,2), "_",
  str_sub(colnames(store_data_validation_3_agg),3)
)

View(store_data_validation_3_agg)
#
store_data_validation_3_agg_t <- t(store_data_validation_3_agg) %>% as.data.frame()
store_data_validation_3_agg_tt <- subset(store_data_validation_3_agg_t, 
                                         !grepl("Total",rownames(store_data_validation_3_agg_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( store_data_validation_3_agg_tt) <- c("id", str_c("F", 1:28 ) )
#
stores_validation4 <- store_data_validation_3_agg_tt %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation4[,1:2])
#write
write_csv(stores_validation4,"D:/datas/proj/stores_validation_base4.csv")
#
#CITYDEPTOTALr-----------------------------------

store_data_validation_3 <- 
  bind_cols(CA_1,CA_2,CA_3,CA_4,TX_1,TX_2,TX_3,WI_1,WI_2,WI_3)

View(store_data_validation_3)

names(store_data_validation_3) <-gsub("_0.25_","_0.250_",names(store_data_validation_3))
names(store_data_validation_3) <-gsub("_0.5_","_0.500_",names(store_data_validation_3))
names(store_data_validation_3) <-gsub("_0.75_","_0.750_",names(store_data_validation_3))

names(store_data_validation_3) <- str_c(
  str_sub(names(store_data_validation_3),1,2),
  str_sub(names(store_data_validation_3),11,26),
  str_sub(names(store_data_validation_3),6,10),
  str_sub(names(store_data_validation_3),3,5)
)

head(names(store_data_validation_3))

library(hts)

store_data_validation_3_agg <- aggts(
  hts(store_data_validation_3, characters = c(2,16,5,3)),levels = c(2)
)

View(colnames(store_data_validation_3_agg))

colnames(store_data_validation_3_agg) <- str_c(
  str_sub(colnames(store_data_validation_3_agg),1,2), "_","X","_",
  str_sub(colnames(store_data_validation_3_agg),3)
)

View(store_data_validation_3_agg)
#
store_data_validation_3_agg_t <- t(store_data_validation_3_agg) %>% as.data.frame()
store_data_validation_3_agg_tt <- subset(store_data_validation_3_agg_t, 
                                         !grepl("Total",rownames(store_data_validation_3_agg_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( store_data_validation_3_agg_tt) <- c("id", str_c("F", 1:28 ) )
#
stores_validation5 <- store_data_validation_3_agg_tt %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation5[,1:2])
#write
write_csv(stores_validation5,"D:/datas/proj/stores_validation_base5.csv")
#
#DEPARTMENT-------------

store_data_validation_3 <- 
  bind_cols(CA_1,CA_2,CA_3,CA_4,TX_1,TX_2,TX_3,WI_1,WI_2,WI_3)

View(store_data_validation_3)

names(store_data_validation_3) <-gsub("_0.25_","_0.250_",names(store_data_validation_3))
names(store_data_validation_3) <-gsub("_0.5_","_0.500_",names(store_data_validation_3))
names(store_data_validation_3) <-gsub("_0.75_","_0.750_",names(store_data_validation_3))

names(store_data_validation_3) <- str_c(
  str_sub(names(store_data_validation_3),6,10),
  str_sub(names(store_data_validation_3),11,26),
  str_sub(names(store_data_validation_3),3,5),
  str_sub(names(store_data_validation_3),1,2)
)

head(names(store_data_validation_3))

library(hts)

store_data_validation_3_agg <- aggts(
  hts(store_data_validation_3, characters = c(5,16,3,2)),levels = c(2)
) 

View(colnames(store_data_validation_3_agg))

View(store_data_validation_3_agg)
#
store_data_validation_3_agg_t <- t(store_data_validation_3_agg) %>% as.data.frame()
store_data_validation_3_agg_tt <- subset(store_data_validation_3_agg_t, 
                                         !grepl("Total",rownames(store_data_validation_3_agg_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( store_data_validation_3_agg_tt) <- c("id", str_c("F", 1:28 ) )
#

View(store_data_validation_3_agg_tt)

store_data_validation_3_agg_tt$id <- str_c(
  str_sub(store_data_validation_3_agg_tt$id,1,5),"X","_",str_sub(store_data_validation_3_agg_tt$id,6)
)

#
stores_validation6 <- store_data_validation_3_agg_tt %>% as_tibble() %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation6[,1:2])
#write
write_csv(stores_validation6,"D:/datas/proj/stores_validation_base6.csv")
#STOREDEPTOTALr-----------------------------------

store_data_validation_3 <- 
  bind_cols(CA_1,CA_2,CA_3,CA_4,TX_1,TX_2,TX_3,WI_1,WI_2,WI_3)

View(store_data_validation_3)

names(store_data_validation_3) <-gsub("_0.25_","_0.250_",names(store_data_validation_3))
names(store_data_validation_3) <-gsub("_0.5_","_0.500_",names(store_data_validation_3))
names(store_data_validation_3) <-gsub("_0.75_","_0.750_",names(store_data_validation_3))

names(store_data_validation_3) <- str_c(
  str_sub(names(store_data_validation_3),1,5),
  str_sub(names(store_data_validation_3),11,26),
  str_sub(names(store_data_validation_3),6,10)
)

head(names(store_data_validation_3))

library(hts)

store_data_validation_3_agg <- aggts(
  hts(store_data_validation_3, characters = c(5,16,5)),levels = c(2)
) 

View(colnames(store_data_validation_3_agg))

colnames(store_data_validation_3_agg) <- str_c(
  str_sub(colnames(store_data_validation_3_agg),1,5), "_","X","_",
  str_sub(colnames(store_data_validation_3_agg),6)
)

View(store_data_validation_3_agg)
#
store_data_validation_3_agg_t <- t(store_data_validation_3_agg) %>% as.data.frame()
store_data_validation_3_agg_tt <- subset(store_data_validation_3_agg_t, 
                                         !grepl("Total",rownames(store_data_validation_3_agg_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( store_data_validation_3_agg_tt) <- c("id", str_c("F", 1:28 ) )
#
stores_validation7 <- store_data_validation_3_agg_tt %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation7[,1:2])
#write
write_csv(stores_validation7,"D:/datas/proj/stores_validation_base7.csv")
#

#LEVEL-----------------------------------

store_data_validation_3 <- 
  bind_cols(CA_1,CA_2,CA_3,CA_4,TX_1,TX_2,TX_3,WI_1,WI_2,WI_3)

View(store_data_validation_3)

names(store_data_validation_3) <-gsub("_0.25_","_0.250_",names(store_data_validation_3))
names(store_data_validation_3) <-gsub("_0.5_","_0.500_",names(store_data_validation_3))
names(store_data_validation_3) <-gsub("_0.75_","_0.750_",names(store_data_validation_3))

names(store_data_validation_3) <- str_c(
  str_sub(names(store_data_validation_3),11,26),
  str_sub(names(store_data_validation_3),6,10),
  str_sub(names(store_data_validation_3),3,5),
  str_sub(names(store_data_validation_3),1,2)
)

head(names(store_data_validation_3))

library(hts)

store_data_validation_3_agg <- aggts(
  hts(store_data_validation_3, characters = c(16,5,3,2)),levels = c(1)
)

View(colnames(store_data_validation_3_agg))

colnames(store_data_validation_3_agg) <- str_c("Total","_","X","_",
                                               str_sub(colnames(store_data_validation_3_agg),1)
)

View(store_data_validation_3_agg)
#
store_data_validation_3_agg_t <- t(store_data_validation_3_agg) %>% as.data.frame()
store_data_validation_3_agg_tt <- subset(store_data_validation_3_agg_t, 
                                         grepl("Total",rownames(store_data_validation_3_agg_t))) %>% 
  rownames_to_column(var = "id") %>% as_tibble()
colnames( store_data_validation_3_agg_tt) <- c("id", str_c("F", 1:28 ) )
#
stores_validation8 <- store_data_validation_3_agg_tt %>% 
  mutate_if(is.character, stringr::str_replace, pattern = "__", replacement = "_" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOBB", replacement = "HOBBIES" ) %>% 
  mutate_if(is.character, stringr::str_replace_all, pattern = "HOUS", replacement = "HOUSEHOLD" ) %>%
  mutate_if(is.character, stringr::str_replace_all, pattern = "FOOD", replacement = "FOODS" )

head(stores_validation8[,1:2])
#write
write_csv(stores_validation8,"D:/datas/proj/stores_validation_base8.csv")
#

##------------------------
#View(stores_validation1)
FullData1 <- 
  bind_rows(
    stores_validation1, stores_validation2, stores_validation3, stores_validation4,
    stores_validation5, stores_validation6, stores_validation7, stores_validation8,
    stores_validation11, stores_validation12, 
    stores_validation13, #stores_validation14, 
    stores_validation15
  )

dim(FullData1)

write_csv(FullData1,"D:/datas/proj/stores_validation.csv")

#


