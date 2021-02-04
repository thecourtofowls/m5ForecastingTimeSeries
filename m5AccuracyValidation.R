library(doParallel)
cl <- makePSOCKcluster(parallel::detectCores() - 1)
registerDoParallel(cl)

library(tidyverse)
library(magrittr)
library(readr)

dir.create("m5A")

# CalendaImport -----------------------------------------------------------


calendar <- read_csv("m5-forecasting-accuracy/calendar.csv")
head(calendar)

# SellPriceImport ---------------------------------------------------------
library(readr)
sell_prices <- read_csv("m5-forecasting-accuracy/sell_prices.csv", 
                        col_types = cols(store_id = col_factor(levels = c("CA_1", 
                                                                          "CA_2", "CA_3", "CA_4", "TX_1", "TX_2", 
                                                                          "TX_3", "WI_1", "WI_2", "WI_3")),
                                         wm_yr_wk = col_factor(levels = as.character(11101:11621))))

View(sell_prices)
tibble::glimpse(sell_prices)
# importSalesTrain --------------------------------------------------------
sales_train_validation <- read_csv("m5-forecasting-accuracy/sales_train_validation.csv", 
                                   col_types = cols(cat_id = col_factor(levels = c("FOODS", 
                                                                                   "HOBBIES", "HOUSEHOLD")
                                   ), 
                                   state_id = col_factor(levels = c("CA", "TX", "WI")), 
                                   store_id = col_factor(levels = c("CA_1", "CA_2", "CA_3", 
                                                                    "CA_4", "TX_1", "TX_2", "TX_3", 
                                                                    "WI_1", "WI_2", "WI_3"))
                                   )
)
sales_train_validation %>% dim()
View(sales_train_validation)

# CA ----------------------------------------------------------------------

####### take CA state data, group by store_id, group by category

CAData <- sales_train_validation %>% filter(state_id == "CA") %>% 
  select(-state_id)
head(CAData[1:5])

#CA_1####################
#store CA_1

CADataCA_1 <- CAData %>% filter(store_id == "CA_1") %>% 
  select(-store_id)
head(CADataCA_1[1:4])

#CA_1HOBBIES##############################
CADataCA_1Hobbies <- CADataCA_1 %>% 
  filter(cat_id == "HOBBIES") %>% select(-cat_id)
head(CADataCA_1Hobbies[1:3])

#CA_1HOBBIEShts###########################

#clean HOBS CA_1
cleanCAHOBS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

CADataCA_1HobbiesClean <- cleanCAHOBS(CADataCA_1Hobbies) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(CADataCA_1HobbiesClean)

CADataCA_1HobbiesCleanT <- t(CADataCA_1HobbiesClean[,-1914])
colnames(CADataCA_1HobbiesCleanT) <- c(CADataCA_1HobbiesClean$groupS)#as.list
CADataCA_1HobbiesCleanTT <- as_tibble(CADataCA_1HobbiesCleanT)

head(CADataCA_1HobbiesCleanTT)

cal_CA <- calendar %>% select(date, wm_yr_wk)
head(cal_CA)

varCA <- c("date", "Snap_CA",#"Snap_TX", "Snap_WI",
           "SuperBowl", "Sporting", "ValentinesDay", "Cultural", "PresidentsDay", 
           "National", "LentStart", "Religious", "LentWeek2", "StPatricksDay", 
           "Purim End", "OrthodoxEaster", "Easter", "Pesach End", "Cinco De Mayo", 
           "Mother's day", "MemorialDay", "NBAFinalsStart", "NBAFinalsEnd", "Father's day", 
           "IndependenceDay", "Ramadan starts", "Eid al-Fitr", "LaborDay", "ColumbusDay", 
           "Halloween", "EidAlAdha", "VeteransDay", "Thanksgiving", "Christmas", 
           "Chanukah End", "NewYear", "OrthodoxChristmas", "MartinLutherKingDay")
calendar1_CA <- calendar1 %>% 
  select(!!varCA)

#CA_1
sell_CA_1 <- sell_prices %>% filter(store_id == "CA_1") %>% 
  filter(grepl('HOBB', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_CA_1)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_CA_sell_CA_1 <- merge(cal_CA, sell_CA_1, by = "wm_yr_wk", all = TRUE) 

cal_CA_sell_CA_1_clean <-  cleanSell(cal_CA_sell_CA_1) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% 
  select(-date)

cal_CA_sell_CA_1_clean[is.na(cal_CA_sell_CA_1_clean)] = 0
cal_CA_sell_CA_1_clean %<>%  as.matrix()

cal_CA_sell_CA_1_clean_train_reg <- cal_CA_sell_CA_1_clean[1:1913, ]
cal_CA_sell_CA_1_clean_forcast_reg <- cal_CA_sell_CA_1_clean[1914:1941,]

head(cal_CA_sell_CA_1_clean_train_reg)

library(hts)
cal_CA_sell_CA_1_clean_train_reg_agg <- aggts(
  hts(cal_CA_sell_CA_1_clean_train_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)

cal_CA_sell_CA_1_clean_forcast_reg_agg <- aggts(
  hts(cal_CA_sell_CA_1_clean_forcast_reg, 
      characters = c(10,3)),levels = c(0,1,2)
) 

CADataCA_1HobbiesCleanTT_hts <- hts(CADataCA_1HobbiesCleanTT, 
                                    characters = c(7,3)) #convert to hts object

h <- 28
CADataCA_1HobbiesCleanTT_hts_aggts <- aggts(CADataCA_1HobbiesCleanTT_hts, 
                                            levels = c(0,1,2))
n <- nrow(CADataCA_1HobbiesCleanTT_hts_aggts)
p <- ncol(CADataCA_1HobbiesCleanTT_hts_aggts)

CADataCA_1HobbiesCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
CADataCA_1HobbiesCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(CADataCA_1HobbiesCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    CADataCA_1HobbiesCleanTT_hts_aggts[,i], 
    as.character(
      CADataCA_1HobbiesCleanTT_hts_aggts[,i][cumsum(CADataCA_1HobbiesCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  CADataCA_1HobbiesCleanTT_hts_aggts_fit <- auto.arima( 
    CADataCA_1HobbiesCleanTT_hts_aggts[l:k,i],
    xreg = cal_CA_sell_CA_1_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  CADataCA_1HobbiesCleanTT_hts_aggts_forecast[, i] <- forecast(
    CADataCA_1HobbiesCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_CA_sell_CA_1_clean_forcast_reg_agg[,i]
  )$mean
  #CADataCA_1HobbiesCleanTT_hts_aggts_resid[l:k, i] <-  
  # CADataCA_1HobbiesCleanTT_hts_aggts[l:k, i] - fitted(CADataCA_1HobbiesCleanTT_hts_aggts_fit) 
}

CADataCA_1HobbiesCleanTT_hts_aggts_forecast[!is.finite(CADataCA_1HobbiesCleanTT_hts_aggts_forecast)] <- 0
CADataCA_1HobbiesCleanTT_hts_aggts_forecast_ts <- ts(
  CADataCA_1HobbiesCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

CADataCA_1HobbiesCleanTT_hts_aggts_forecast_ts_f <- combinef(
  CADataCA_1HobbiesCleanTT_hts_aggts_forecast_ts,
  get_nodes(CADataCA_1HobbiesCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

colnames(CADataCA_1HobbiesCleanTT_hts_aggts_forecast_ts_f) <- 
  c(colnames(CADataCA_1HobbiesCleanTT_hts_aggts)[c(-1,-2,-3)])

CADataCA_1HobbiesCleanTT_hts_aggts_forecast_ts_f %>% View()

write_csv(as.data.frame(CADataCA_1HobbiesCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/CADataCA_1HobbiesCleanTT_hts_aggts_forecast_ts_f.csv") 

Sys.time()

#####################################################enddssssssss

CADataCA_1Hobbies_Depart1 <- CADataCA_1Hobbies %>% 
  filter(dept_id == "HOBBIES_1") %>% select(-dept_id)
head(CADataCA_1Hobbies_Depart1[1:3])

CADataCA_1Hobbies_Depart2 <- CADataCA_1Hobbies %>% 
  filter(dept_id == "HOBBIES_2") %>% select(-dept_id)
head(CADataCA_1Hobbies_Depart2[1:3])

#CA_1HOUSE#####################################################

CADataCA_1Household <- CADataCA_1 %>% 
  filter(cat_id == "HOUSEHOLD") %>% select(-cat_id)
head(CADataCA_1Household[1:3])

#CA_1HOUSEhts###########################
#clean HOUS CA_1
cleanCAHOUS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

CADataCA_1HouseholdClean <- cleanCAHOUS(CADataCA_1Household) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(CADataCA_1HouseholdClean)

CADataCA_1HouseholdCleanT <- t(CADataCA_1HouseholdClean[,-1914])
colnames(CADataCA_1HouseholdCleanT) <- c(CADataCA_1HouseholdClean$groupS)
CADataCA_1HouseholdCleanTT <- as_tibble(CADataCA_1HouseholdCleanT)

head(CADataCA_1HouseholdCleanTT)

cal_CA <- calendar %>% select(date, wm_yr_wk)
head(cal_CA)

#CA_1
sell_CA_1 <- sell_prices %>% filter(store_id == "CA_1") %>% 
  filter(grepl('HOUS', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_CA_1)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_CA_sell_CA_1 <- merge(cal_CA, sell_CA_1, by = "wm_yr_wk", all = TRUE) 

cal_CA_sell_CA_1_clean <-  cleanSell(cal_CA_sell_CA_1) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_CA_sell_CA_1_clean[is.na(cal_CA_sell_CA_1_clean)] = 0
cal_CA_sell_CA_1_clean %<>%  as.matrix()

cal_CA_sell_CA_1_clean_train_reg <- cal_CA_sell_CA_1_clean[1:1913, ]
cal_CA_sell_CA_1_clean_forcast_reg <- cal_CA_sell_CA_1_clean[1914:1941,]


library(hts)
cal_CA_sell_CA_1_clean_train_reg_agg <- aggts(
  hts(cal_CA_sell_CA_1_clean_train_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)
cal_CA_sell_CA_1_clean_forcast_reg_agg <- aggts(
  hts(cal_CA_sell_CA_1_clean_forcast_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)

CADataCA_1HouseholdCleanTT_hts <- hts(CADataCA_1HouseholdCleanTT, 
                                      characters = c(7,3)) #convert to hts object

h <- 28
CADataCA_1HouseholdCleanTT_hts_aggts <- aggts(CADataCA_1HouseholdCleanTT_hts, 
                                              levels = c(0,1,2))
n <- nrow(CADataCA_1HouseholdCleanTT_hts_aggts)
p <- ncol(CADataCA_1HouseholdCleanTT_hts_aggts)

CADataCA_1HouseholdCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
CADataCA_1HouseholdCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(CADataCA_1HouseholdCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    CADataCA_1HouseholdCleanTT_hts_aggts[,i], 
    as.character(
      CADataCA_1HouseholdCleanTT_hts_aggts[,i][cumsum(CADataCA_1HouseholdCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  CADataCA_1HouseholdCleanTT_hts_aggts_fit <- auto.arima( 
    CADataCA_1HouseholdCleanTT_hts_aggts[l:k,i],
    xreg = cal_CA_sell_CA_1_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  CADataCA_1HouseholdCleanTT_hts_aggts_forecast[, i] <- forecast(
    CADataCA_1HouseholdCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_CA_sell_CA_1_clean_forcast_reg_agg[,i]
  )$mean
  #CADataCA_1HouseholdCleanTT_hts_aggts_resid[l:k, i] <-  
  # CADataCA_1HouseholdCleanTT_hts_aggts[l:k, i] - fitted(CADataCA_1HouseholdCleanTT_hts_aggts_fit) 
  
}

CADataCA_1HouseholdCleanTT_hts_aggts_forecast[!is.finite(CADataCA_1HouseholdCleanTT_hts_aggts_forecast)] <- 0
CADataCA_1HouseholdCleanTT_hts_aggts_forecast_ts <- ts(
  CADataCA_1HouseholdCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

CADataCA_1HouseholdCleanTT_hts_aggts_forecast_ts_f <- combinef(
  CADataCA_1HouseholdCleanTT_hts_aggts_forecast_ts,
  get_nodes(CADataCA_1HouseholdCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

colnames(CADataCA_1HouseholdCleanTT_hts_aggts_forecast_ts_f) <- 
  c(colnames(CADataCA_1HouseholdCleanTT_hts_aggts)[c(-1,-2,-3)])

write_csv(as.data.frame(CADataCA_1HouseholdCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/CADataCA_1HouseholdCleanTT_hts_aggts_forecast_ts_f.csv") 
Sys.time() 

########################################################end

CADataCA_1Household_Depart1 <- CADataCA_1Household %>% 
  filter(dept_id == "HOUSEHOLD_1") %>% select(-dept_id)
head(CADataCA_1Household_Depart1[1:3])

CADataCA_1Household_Depart2 <- CADataCA_1Household %>% 
  filter(dept_id == "HOUSEHOLD_2") %>% select(-dept_id)
head(CADataCA_1Household_Depart2[1:3])

#CA_1FOODS###################################
CADataCA_1Foods <- CADataCA_1 %>% 
  filter(cat_id == "FOODS") %>% select(-cat_id)
head(CADataCA_1Foods[1:3])

#CA_1FOODShts#########################

#clean FOOD CA_1
cleanCAFOOD <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

CADataCA_1FoodClean <- cleanCAFOOD(CADataCA_1Foods) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(CADataCA_1FoodClean)

CADataCA_1FoodCleanT <- t(CADataCA_1FoodClean[,-1914])
colnames(CADataCA_1FoodCleanT) <- c(CADataCA_1FoodClean$groupS)
CADataCA_1FoodCleanTT <- as_tibble(CADataCA_1FoodCleanT)

head(CADataCA_1FoodCleanTT)

cal_CA <- calendar %>% select(date, wm_yr_wk)
head(cal_CA)

#CA_1
sell_CA_1 <- sell_prices %>% filter(store_id == "CA_1") %>% 
  filter(grepl('FOOD', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_CA_1)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_CA_sell_CA_1 <- merge(cal_CA, sell_CA_1, by = "wm_yr_wk", all = TRUE) 

cal_CA_sell_CA_1_clean <-  cleanSell(cal_CA_sell_CA_1) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_CA_sell_CA_1_clean[is.na(cal_CA_sell_CA_1_clean)] = 0
cal_CA_sell_CA_1_clean %<>%  as.matrix()

cal_CA_sell_CA_1_clean_train_reg <- cal_CA_sell_CA_1_clean[1:1913, ]
cal_CA_sell_CA_1_clean_forcast_reg <- cal_CA_sell_CA_1_clean[1914:1941,]


library(hts)
cal_CA_sell_CA_1_clean_train_reg_agg <- aggts(
  hts(cal_CA_sell_CA_1_clean_train_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)
cal_CA_sell_CA_1_clean_forcast_reg_agg <- aggts(
  hts(cal_CA_sell_CA_1_clean_forcast_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)

CADataCA_1FoodCleanTT_hts <- hts(CADataCA_1FoodCleanTT, 
                                 characters = c(7,3)) #convert to hts object

h <- 28
CADataCA_1FoodCleanTT_hts_aggts <- aggts(CADataCA_1FoodCleanTT_hts, 
                                         levels = c(0,1,2))
n <- nrow(CADataCA_1FoodCleanTT_hts_aggts)
p <- ncol(CADataCA_1FoodCleanTT_hts_aggts)

CADataCA_1FoodCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
CADataCA_1FoodCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(CADataCA_1FoodCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    CADataCA_1FoodCleanTT_hts_aggts[,i], 
    as.character(
      CADataCA_1FoodCleanTT_hts_aggts[,i][cumsum(CADataCA_1FoodCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  CADataCA_1FoodCleanTT_hts_aggts_fit <- auto.arima( 
    CADataCA_1FoodCleanTT_hts_aggts[l:k,i],
    xreg = cal_CA_sell_CA_1_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  CADataCA_1FoodCleanTT_hts_aggts_forecast[, i] <- forecast(
    CADataCA_1FoodCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_CA_sell_CA_1_clean_forcast_reg_agg[,i]
  )$mean
  #CADataCA_1FoodCleanTT_hts_aggts_resid[l:k, i] <-  
  # CADataCA_1FoodCleanTT_hts_aggts[l:k, i] - fitted(CADataCA_1FoodCleanTT_hts_aggts_fit) 
}

CADataCA_1FoodCleanTT_hts_aggts_forecast[!is.finite(CADataCA_1FoodCleanTT_hts_aggts_forecast)] <- 0
CADataCA_1FoodCleanTT_hts_aggts_forecast_ts <- ts(
  CADataCA_1FoodCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

CADataCA_1FoodCleanTT_hts_aggts_forecast_ts_f <- combinef(
  CADataCA_1FoodCleanTT_hts_aggts_forecast_ts,
  get_nodes(CADataCA_1FoodCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

colnames(CADataCA_1FoodCleanTT_hts_aggts_forecast_ts_f) <- 
  c(colnames(CADataCA_1FoodCleanTT_hts_aggts)[c(-1,-2,-3,-4)])
  

write_csv(as.data.frame(CADataCA_1FoodCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/CADataCA_1FoodCleanTT_hts_aggts_forecast_ts_f.csv") 
Sys.time()

########################################################end


CADataCA_1Foods_Depart1 <- CADataCA_1Foods %>% 
  filter(dept_id == "FOODS_1") %>% select(-dept_id)
head(CADataCA_1Foods_Depart1[1:3])

CADataCA_1Foods_Depart2 <- CADataCA_1Foods %>% 
  filter(dept_id == "FOODS_2") %>% select(-dept_id)
head(CADataCA_1Foods_Depart2[1:3])

CADataCA_1Foods_Depart3 <- CADataCA_1Foods %>% 
  filter(dept_id == "FOODS_3") %>% select(-dept_id)
head(CADataCA_1Foods_Depart3[1:3])


#CA_2######################
#Store CA_2
CADataCA_2 <- CAData %>% filter(store_id == "CA_2") %>% 
  select(-store_id)
head(CADataCA_2[1:4])

#CA_2HOBBIES###############
CADataCA_2Hobbies <- CADataCA_2 %>% 
  filter(cat_id == "HOBBIES") %>% select(-cat_id)
head(CADataCA_2Hobbies[1:3])

#CA_2HOBBIEShts###############################################

#clean HOBS CA_2
cleanCAHOBS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

CADataCA_2HobbiesClean <- cleanCAHOBS(CADataCA_2Hobbies) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(CADataCA_2HobbiesClean)

CADataCA_2HobbiesCleanT <- t(CADataCA_2HobbiesClean[,-1914])#[,-1914])
colnames(CADataCA_2HobbiesCleanT) <- c(CADataCA_2HobbiesClean$groupS)#as.list
CADataCA_2HobbiesCleanTT <- as_tibble(CADataCA_2HobbiesCleanT)

head(CADataCA_2HobbiesCleanTT)

cal_CA <- calendar %>% select(date, wm_yr_wk)
head(cal_CA)

#CA_2
sell_CA_2 <- sell_prices %>% filter(store_id == "CA_2") %>% 
  filter(grepl('HOBB', item_id)) %>% 
  #slice() %>% 
  select(-store_id)#####key part
head(sell_CA_2)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_CA_sell_CA_2 <- merge(cal_CA, sell_CA_2, by = "wm_yr_wk", all = TRUE) 

cal_CA_sell_CA_2_clean <-  cleanSell(cal_CA_sell_CA_2) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_CA_sell_CA_2_clean[is.na(cal_CA_sell_CA_2_clean)] = 0
cal_CA_sell_CA_2_clean %<>%  as.matrix()

dim(cal_CA_sell_CA_2_clean)

cal_CA_sell_CA_2_clean_train_reg <- cal_CA_sell_CA_2_clean[1:1913, ]#[1:1913, ]
cal_CA_sell_CA_2_clean_forcast_reg <- cal_CA_sell_CA_2_clean[1914:1941,]#[1914:1941,]


library(hts)
cal_CA_sell_CA_2_clean_train_reg_agg <- aggts(
  hts(cal_CA_sell_CA_2_clean_train_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)
cal_CA_sell_CA_2_clean_forcast_reg_agg <- aggts(
  hts(cal_CA_sell_CA_2_clean_forcast_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)

CADataCA_2HobbiesCleanTT_hts <- hts(CADataCA_2HobbiesCleanTT, 
                                    characters = c(7,3)) #convert to hts object

h <- 28
CADataCA_2HobbiesCleanTT_hts_aggts <- aggts(CADataCA_2HobbiesCleanTT_hts, 
                                            levels = c(0,1,2))
n <- nrow(CADataCA_2HobbiesCleanTT_hts_aggts)
p <- ncol(CADataCA_2HobbiesCleanTT_hts_aggts)

CADataCA_2HobbiesCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
CADataCA_2HobbiesCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(CADataCA_2HobbiesCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    CADataCA_2HobbiesCleanTT_hts_aggts[,i], 
    as.character(
      CADataCA_2HobbiesCleanTT_hts_aggts[,i][cumsum(CADataCA_2HobbiesCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  CADataCA_2HobbiesCleanTT_hts_aggts_fit <- auto.arima( 
    CADataCA_2HobbiesCleanTT_hts_aggts[l:k,i],
    xreg = cal_CA_sell_CA_2_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  CADataCA_2HobbiesCleanTT_hts_aggts_forecast[, i] <- forecast(
    CADataCA_2HobbiesCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_CA_sell_CA_2_clean_forcast_reg_agg[,i]
  )$mean
  #CADataCA_2HobbiesCleanTT_hts_aggts_resid[l:k, i] <-  
  # CADataCA_2HobbiesCleanTT_hts_aggts[l:k, i] - fitted(CADataCA_2HobbiesCleanTT_hts_aggts_fit) 
}

CADataCA_2HobbiesCleanTT_hts_aggts_forecast[!is.finite(CADataCA_2HobbiesCleanTT_hts_aggts_forecast)] <- 0
CADataCA_2HobbiesCleanTT_hts_aggts_forecast_ts <- ts(
  CADataCA_2HobbiesCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

CADataCA_2HobbiesCleanTT_hts_aggts_forecast_ts_f <- combinef(
  CADataCA_2HobbiesCleanTT_hts_aggts_forecast_ts,
  get_nodes(CADataCA_2HobbiesCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(CADataCA_1HobbiesCleanTT_hts_aggts)
colnames(CADataCA_2HobbiesCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(CADataCA_2HobbiesCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/CADataCA_2HobbiesCleanTT_hts_aggts_forecast_ts_f.csv") 
Sys.time()

########################################end


CADataCA_2Hobbies_Depart1 <- CADataCA_2Hobbies %>% 
  filter(dept_id == "HOBBIES_1") %>% select(-dept_id)
head(CADataCA_2Hobbies_Depart1[1:3])

CADataCA_2Hobbies_Depart2 <- CADataCA_2Hobbies %>% 
  filter(dept_id == "HOBBIES_2") %>% select(-dept_id)
head(CADataCA_2Hobbies_Depart2[1:3])

#CA_2HOUSE####################
CADataCA_2Household <- CADataCA_2 %>% 
  filter(cat_id == "HOUSEHOLD") %>% select(-cat_id)
head(CADataCA_2Household[1:3])

#CA_2HOUSEhts#########################

#clean HOUS CA_2
cleanCAHOUS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

CADataCA_2HouseholdClean <- cleanCAHOUS(CADataCA_2Household) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(CADataCA_2HouseholdClean)

CADataCA_2HouseholdCleanT <- t(CADataCA_2HouseholdClean[,-1914])
colnames(CADataCA_2HouseholdCleanT) <- c(CADataCA_2HouseholdClean$groupS)
CADataCA_2HouseholdCleanTT <- as_tibble(CADataCA_2HouseholdCleanT)

head(CADataCA_2HouseholdCleanTT)

cal_CA <- calendar %>% select(date, wm_yr_wk)
head(cal_CA)

#CA_2
sell_CA_2 <- sell_prices %>% filter(store_id == "CA_2") %>%  
  filter(grepl('HOUS', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_CA_2)
tail(sell_CA_2)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_CA_sell_CA_2 <- merge(cal_CA, sell_CA_2, by = "wm_yr_wk", all = TRUE) 

cal_CA_sell_CA_2_clean <-  cleanSell(cal_CA_sell_CA_2) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_CA_sell_CA_2_clean[is.na(cal_CA_sell_CA_2_clean)] = 0
cal_CA_sell_CA_2_clean %<>%  as.matrix()

dim(cal_CA_sell_CA_2_clean)

cal_CA_sell_CA_2_clean_train_reg <- cal_CA_sell_CA_2_clean[1:1913, ]
cal_CA_sell_CA_2_clean_forcast_reg <- cal_CA_sell_CA_2_clean[1914:1941,]


library(hts)
cal_CA_sell_CA_2_clean_train_reg_agg <- aggts(
  hts(cal_CA_sell_CA_2_clean_train_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)
cal_CA_sell_CA_2_clean_forcast_reg_agg <- aggts(
  hts(cal_CA_sell_CA_2_clean_forcast_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)

CADataCA_2HouseholdCleanTT_hts <- hts(CADataCA_2HouseholdCleanTT, 
                                      characters = c(7,3)) #convert to hts object

h <- 28
CADataCA_2HouseholdCleanTT_hts_aggts <- aggts(CADataCA_2HouseholdCleanTT_hts, 
                                              levels = c(0,1,2))
n <- nrow(CADataCA_2HouseholdCleanTT_hts_aggts)
p <- ncol(CADataCA_2HouseholdCleanTT_hts_aggts)

CADataCA_2HouseholdCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
CADataCA_2HouseholdCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
#for (level in c(0.5,2.5, 16.5, 16.5, 25, 50, 75, 83.5, 97.5, 99.5)){
for(i in 1:p)
{ 
  k <- length(CADataCA_2HouseholdCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    CADataCA_2HouseholdCleanTT_hts_aggts[,i], 
    as.character(
      CADataCA_2HouseholdCleanTT_hts_aggts[,i][cumsum(CADataCA_2HouseholdCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  CADataCA_2HouseholdCleanTT_hts_aggts_fit <- auto.arima( 
    CADataCA_2HouseholdCleanTT_hts_aggts[l:k,i],
    xreg = cal_CA_sell_CA_2_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  CADataCA_2HouseholdCleanTT_hts_aggts_forecast[, i] <- forecast(
    CADataCA_2HouseholdCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_CA_sell_CA_2_clean_forcast_reg_agg[,i]
  )$mean
  #CADataCA_2HouseholdCleanTT_hts_aggts_resid[l:k, i] <-  
  # CADataCA_2HouseholdCleanTT_hts_aggts[l:k, i] - fitted(CADataCA_2HouseholdCleanTT_hts_aggts_fit) 
}

CADataCA_2HouseholdCleanTT_hts_aggts_forecast[!is.finite(CADataCA_2HouseholdCleanTT_hts_aggts_forecast)] <- 0
CADataCA_2HouseholdCleanTT_hts_aggts_forecast_ts <- ts(
  CADataCA_2HouseholdCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

CADataCA_2HouseholdCleanTT_hts_aggts_forecast_ts_f <- combinef(
  CADataCA_2HouseholdCleanTT_hts_aggts_forecast_ts,
  get_nodes(CADataCA_2HouseholdCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(CADataCA_2HouseholdCleanTT_hts_aggts)
colnames(CADataCA_2HouseholdCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(CADataCA_2HouseholdCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/CADataCA_2HouseholdCleanTT_hts_aggts_forecast_ts_f.csv" ) 
Sys.time()

########################################################end


CADataCA_2Household_Depart1 <- CADataCA_2Household %>% 
  filter(dept_id == "HOUSEHOLD_1") %>% select(-dept_id)
head(CADataCA_2Household_Depart1[1:3])

CADataCA_2Household_Depart2 <- CADataCA_2Household %>% 
  filter(dept_id == "HOUSEHOLD_2") %>% select(-dept_id)
head(CADataCA_2Household_Depart2[1:3])

#CA_2FOOD#########################
CADataCA_2Foods <- CADataCA_2 %>% 
  filter(cat_id == "FOODS") %>% select(-cat_id)
head(CADataCA_2Foods[1:3])

#CA_2FOODhts#########################

#clean FOOD CA_2
cleanCAFOOD <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

CADataCA_2FoodClean <- cleanCAFOOD(CADataCA_2Foods) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(CADataCA_2FoodClean)

CADataCA_2FoodCleanT <- t(CADataCA_2FoodClean[,-1914])
colnames(CADataCA_2FoodCleanT) <- c(CADataCA_2FoodClean$groupS)
CADataCA_2FoodCleanTT <- as_tibble(CADataCA_2FoodCleanT)

head(CADataCA_2FoodCleanTT)

cal_CA <- calendar %>% select(date, wm_yr_wk)
head(cal_CA)

#CA_2
sell_CA_2 <- sell_prices %>% filter(store_id == "CA_2") %>% 
  filter(grepl('FOOD', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_CA_2)
tail(sell_CA_2)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_CA_sell_CA_2 <- merge(cal_CA, sell_CA_2, by = "wm_yr_wk", all = TRUE) 

cal_CA_sell_CA_2_clean <-  cleanSell(cal_CA_sell_CA_2) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_CA_sell_CA_2_clean[is.na(cal_CA_sell_CA_2_clean)] = 0
cal_CA_sell_CA_2_clean %<>%  as.matrix()

cal_CA_sell_CA_2_clean_train_reg <- cal_CA_sell_CA_2_clean[1:1913, ]
cal_CA_sell_CA_2_clean_forcast_reg <- cal_CA_sell_CA_2_clean[1914:1941,]


library(hts)
cal_CA_sell_CA_2_clean_train_reg_agg <- aggts(
  hts(cal_CA_sell_CA_2_clean_train_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)
cal_CA_sell_CA_2_clean_forcast_reg_agg <- aggts(
  hts(cal_CA_sell_CA_2_clean_forcast_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)

CADataCA_2FoodCleanTT_hts <- hts(CADataCA_2FoodCleanTT, 
                                 characters = c(7,3)) #convert to hts object

h <- 28
CADataCA_2FoodCleanTT_hts_aggts <- aggts(CADataCA_2FoodCleanTT_hts, 
                                         levels = c(0,1,2))
n <- nrow(CADataCA_2FoodCleanTT_hts_aggts)
p <- ncol(CADataCA_2FoodCleanTT_hts_aggts)

CADataCA_2FoodCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
CADataCA_2FoodCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
#for (level in c(0.5,2.5, 16.5, 16.5, 25, 50, 75, 83.5, 97.5, 99.5)){
for(i in 1:p)
{ 
  k <- length(CADataCA_2FoodCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    CADataCA_2FoodCleanTT_hts_aggts[,i], 
    as.character(
      CADataCA_2FoodCleanTT_hts_aggts[,i][cumsum(CADataCA_2FoodCleanTT_hts_aggts[,i]) != 0][1]
    ))[1]
  
  CADataCA_2FoodCleanTT_hts_aggts_fit <- auto.arima( 
    CADataCA_2FoodCleanTT_hts_aggts[l:k,i],
    xreg = cal_CA_sell_CA_2_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  CADataCA_2FoodCleanTT_hts_aggts_forecast[, i] <- forecast(
    CADataCA_2FoodCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_CA_sell_CA_2_clean_forcast_reg_agg[,i]
  )$mean
  #CADataCA_2FoodCleanTT_hts_aggts_resid[l:k, i] <-  
  # CADataCA_2FoodCleanTT_hts_aggts[l:k, i] - fitted(CADataCA_2FoodCleanTT_hts_aggts_fit) 
}

CADataCA_2FoodCleanTT_hts_aggts_forecast[!is.finite(CADataCA_2FoodCleanTT_hts_aggts_forecast)] <- 0
CADataCA_2FoodCleanTT_hts_aggts_forecast_ts <- ts(
  CADataCA_2FoodCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

CADataCA_2FoodCleanTT_hts_aggts_forecast_ts_f <- combinef(
  CADataCA_2FoodCleanTT_hts_aggts_forecast_ts,
  get_nodes(CADataCA_2FoodCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(CADataCA_2FoodCleanTT_hts_aggts)
colnames(CADataCA_2FoodCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(CADataCA_2FoodCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/CADataCA_2FoodCleanTT_hts_aggts_forecast_ts_f.csv")  

Sys.time()

########################################################end


CADataCA_2Foods_Depart1 <- CADataCA_2Foods %>% 
  filter(dept_id == "FOODS_1") %>% select(-dept_id)
head(CADataCA_2Foods_Depart1[1:3])

CADataCA_2Foods_Depart2 <- CADataCA_2Foods %>% 
  filter(dept_id == "FOODS_2") %>% select(-dept_id)
head(CADataCA_2Foods_Depart2[1:3])

CADataCA_2Foods_Depart3 <- CADataCA_2Foods %>% 
  filter(dept_id == "FOODS_3") %>% select(-dept_id)
head(CADataCA_2Foods_Depart3[1:3])





#CA_3#######################
#Store CA_3
CADataCA_3 <- CAData %>% filter(store_id == "CA_3") %>% 
  select(-store_id)
head(CADataCA_3[1:4])

#CA_3HOBBIES###############
CADataCA_3Hobbies <- CADataCA_3 %>% 
  filter(cat_id == "HOBBIES") %>% select(-cat_id)
head(CADataCA_3Hobbies[1:3])

#CA_3HOBBIEShts################################################################

#clean HOBS CA_3
cleanCAHOBS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

CADataCA_3HobbiesClean <- cleanCAHOBS(CADataCA_3Hobbies) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(CADataCA_3HobbiesClean)

CADataCA_3HobbiesCleanT <- t(CADataCA_3HobbiesClean[,-1914])
colnames(CADataCA_3HobbiesCleanT) <- c(CADataCA_3HobbiesClean$groupS)#as.list
CADataCA_3HobbiesCleanTT <- as_tibble(CADataCA_3HobbiesCleanT)

head(CADataCA_3HobbiesCleanTT)

cal_CA <- calendar %>% select(date, wm_yr_wk)
head(cal_CA)

#CA_3
sell_CA_3 <- sell_prices %>% filter(store_id == "CA_3") %>% 
  filter(grepl('HOBB', item_id)) %>% 
  #slice() %>% 
  select(-store_id)#
head(sell_CA_3)
tail(sell_CA_3)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_CA_sell_CA_3 <- merge(cal_CA, sell_CA_3, by = "wm_yr_wk", all = TRUE) 

cal_CA_sell_CA_3_clean <-  cleanSell(cal_CA_sell_CA_3) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_CA_sell_CA_3_clean[is.na(cal_CA_sell_CA_3_clean)] = 0
cal_CA_sell_CA_3_clean %<>%  as.matrix()

cal_CA_sell_CA_3_clean_train_reg <- cal_CA_sell_CA_3_clean[1:1913, ]
cal_CA_sell_CA_3_clean_forcast_reg <- cal_CA_sell_CA_3_clean[1914:1941,]


library(hts)
cal_CA_sell_CA_3_clean_train_reg_agg <- aggts(
  hts(cal_CA_sell_CA_3_clean_train_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)
cal_CA_sell_CA_3_clean_forcast_reg_agg <- aggts(
  hts(cal_CA_sell_CA_3_clean_forcast_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)

CADataCA_3HobbiesCleanTT_hts <- hts(CADataCA_3HobbiesCleanTT, 
                                    characters = c(7,3)) #convert to hts object

h <- 28
CADataCA_3HobbiesCleanTT_hts_aggts <- aggts(CADataCA_3HobbiesCleanTT_hts, 
                                            levels = c(0,1,2))
n <- nrow(CADataCA_3HobbiesCleanTT_hts_aggts)
p <- ncol(CADataCA_3HobbiesCleanTT_hts_aggts)

CADataCA_3HobbiesCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
CADataCA_3HobbiesCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(CADataCA_3HobbiesCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    CADataCA_3HobbiesCleanTT_hts_aggts[,i], 
    as.character(
      CADataCA_3HobbiesCleanTT_hts_aggts[,i][cumsum(CADataCA_3HobbiesCleanTT_hts_aggts[,i]) != 0][1]
    ))[1]
  
  CADataCA_3HobbiesCleanTT_hts_aggts_fit <- auto.arima( 
    CADataCA_3HobbiesCleanTT_hts_aggts[l:k,i],
    xreg = cal_CA_sell_CA_3_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  CADataCA_3HobbiesCleanTT_hts_aggts_forecast[, i] <- forecast(
    CADataCA_3HobbiesCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_CA_sell_CA_3_clean_forcast_reg_agg[,i]
  )$mean
  #CADataCA_3HobbiesCleanTT_hts_aggts_resid[l:k, i] <-  
  # CADataCA_3HobbiesCleanTT_hts_aggts[l:k, i] - fitted(CADataCA_3HobbiesCleanTT_hts_aggts_fit) 
  
}

CADataCA_3HobbiesCleanTT_hts_aggts_forecast[!is.finite(CADataCA_3HobbiesCleanTT_hts_aggts_forecast)] <- 0
CADataCA_3HobbiesCleanTT_hts_aggts_forecast_ts <- ts(
  CADataCA_3HobbiesCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

CADataCA_3HobbiesCleanTT_hts_aggts_forecast_ts_f <- combinef(
  CADataCA_3HobbiesCleanTT_hts_aggts_forecast_ts,
  get_nodes(CADataCA_3HobbiesCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(CADataCA_3HobbiesCleanTT_hts_aggts)
colnames(CADataCA_3HobbiesCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(CADataCA_3HobbiesCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/CADataCA_3HobbiesCleanTT_hts_aggts_forecast_ts_f.csv")  
Sys.time()


###############################################################################fdssssssss


CADataCA_3Hobbies_Depart1 <- CADataCA_3Hobbies %>% 
  filter(dept_id == "HOBBIES_1") %>% select(-dept_id)
head(CADataCA_3Hobbies_Depart1[1:3])

CADataCA_3Hobbies_Depart2 <- CADataCA_3Hobbies %>% 
  filter(dept_id == "HOBBIES_2") %>% select(-dept_id)
head(CADataCA_3Hobbies_Depart2[1:3])

#CA_3HOUSE###############################################
CADataCA_3Household <- CADataCA_3 %>% 
  filter(cat_id == "HOUSEHOLD") %>% select(-cat_id)
head(CADataCA_3Household[1:3])

#CA_3HOUSEhts############################################
#clean HOUS CA_3
cleanCAHOUS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

CADataCA_3HouseholdClean <- cleanCAHOUS(CADataCA_3Household) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(CADataCA_3HouseholdClean)

CADataCA_3HouseholdCleanT <- t(CADataCA_3HouseholdClean[,-1914])
colnames(CADataCA_3HouseholdCleanT) <- c(CADataCA_3HouseholdClean$groupS)
CADataCA_3HouseholdCleanTT <- as_tibble(CADataCA_3HouseholdCleanT)

head(CADataCA_3HouseholdCleanTT)

cal_CA <- calendar %>% select(date, wm_yr_wk)
head(cal_CA)

#CA_3
sell_CA_3 <- sell_prices %>% filter(store_id == "CA_3") %>%  
  filter(grepl('HOUS', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_CA_3)
tail(sell_CA_3)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_CA_sell_CA_3 <- merge(cal_CA, sell_CA_3, by = "wm_yr_wk", all = TRUE) 

cal_CA_sell_CA_3_clean <-  cleanSell(cal_CA_sell_CA_3) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_CA_sell_CA_3_clean[is.na(cal_CA_sell_CA_3_clean)] = 0
cal_CA_sell_CA_3_clean %<>%  as.matrix()

cal_CA_sell_CA_3_clean_train_reg <- cal_CA_sell_CA_3_clean[1:1913, ]
cal_CA_sell_CA_3_clean_forcast_reg <- cal_CA_sell_CA_3_clean[1914:1941,]


library(hts)
cal_CA_sell_CA_3_clean_train_reg_agg <- aggts(
  hts(cal_CA_sell_CA_3_clean_train_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)
cal_CA_sell_CA_3_clean_forcast_reg_agg <- aggts(
  hts(cal_CA_sell_CA_3_clean_forcast_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)

CADataCA_3HouseholdCleanTT_hts <- hts(CADataCA_3HouseholdCleanTT, 
                                      characters = c(7,3)) #convert to hts object

h <- 28
CADataCA_3HouseholdCleanTT_hts_aggts <- aggts(CADataCA_3HouseholdCleanTT_hts, 
                                              levels = c(0,1,2))
n <- nrow(CADataCA_3HouseholdCleanTT_hts_aggts)
p <- ncol(CADataCA_3HouseholdCleanTT_hts_aggts)

CADataCA_3HouseholdCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
CADataCA_3HouseholdCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(CADataCA_3HouseholdCleanTT_hts_aggts[,i])
  
  l <- stringr::str_which(
    CADataCA_3HouseholdCleanTT_hts_aggts[,i], 
    as.character(
      CADataCA_3HouseholdCleanTT_hts_aggts[,i][cumsum(CADataCA_3HouseholdCleanTT_hts_aggts[,i]) != 0][1]
    ))[1]
  
  CADataCA_3HouseholdCleanTT_hts_aggts_fit <- auto.arima( 
    CADataCA_3HouseholdCleanTT_hts_aggts[l:k,i],
    xreg = cal_CA_sell_CA_3_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  CADataCA_3HouseholdCleanTT_hts_aggts_forecast[, i] <- forecast(
    CADataCA_3HouseholdCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_CA_sell_CA_3_clean_forcast_reg_agg[,i]
  )$mean
  #CADataCA_3HouseholdCleanTT_hts_aggts_resid[l:k, i] <-  
  # CADataCA_3HouseholdCleanTT_hts_aggts[l:k, i] - fitted(CADataCA_3HouseholdCleanTT_hts_aggts_fit) 
}

CADataCA_3HouseholdCleanTT_hts_aggts_forecast[!is.finite(CADataCA_3HouseholdCleanTT_hts_aggts_forecast)] <- 0
CADataCA_3HouseholdCleanTT_hts_aggts_forecast_ts <- ts(
  CADataCA_3HouseholdCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

CADataCA_3HouseholdCleanTT_hts_aggts_forecast_ts_f <- combinef(
  CADataCA_3HouseholdCleanTT_hts_aggts_forecast_ts,
  get_nodes(CADataCA_3HouseholdCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(CADataCA_3HouseholdCleanTT_hts_aggts)
colnames(CADataCA_3HouseholdCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(CADataCA_3HouseholdCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/CADataCA_3HouseholdCleanTT_hts_aggts_forecast_ts_f.csv") 
Sys.time()


########################################################end


CADataCA_3Household_Depart1 <- CADataCA_3Household %>% 
  filter(dept_id == "HOUSEHOLD_1") %>% select(-dept_id)
head(CADataCA_3Household_Depart1[1:3])

CADataCA_3Household_Depart2 <- CADataCA_3Household %>% 
  filter(dept_id == "HOUSEHOLD_2") %>% select(-dept_id)
head(CADataCA_3Household_Depart2[1:3])

#CA_3FOODS#######################################
CADataCA_3Foods <- CADataCA_3 %>% 
  filter(cat_id == "FOODS") %>% select(-cat_id)
head(CADataCA_3Foods[1:3])

#CA_3FOODhts#########################

#clean FOOD CA_3
cleanCAFOOD <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

CADataCA_3FoodClean <- cleanCAFOOD(CADataCA_3Foods) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(CADataCA_3FoodClean)

CADataCA_3FoodCleanT <- t(CADataCA_3FoodClean[,-1914])
colnames(CADataCA_3FoodCleanT) <- c(CADataCA_3FoodClean$groupS)
CADataCA_3FoodCleanTT <- as_tibble(CADataCA_3FoodCleanT)

head(CADataCA_3FoodCleanTT)

cal_CA <- calendar %>% select(date, wm_yr_wk)
head(cal_CA)

#CA_3
sell_CA_3 <- sell_prices %>% filter(store_id == "CA_3") %>% 
  filter(grepl('FOOD', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_CA_3)
tail(sell_CA_3)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_CA_sell_CA_3 <- merge(cal_CA, sell_CA_3, by = "wm_yr_wk", all = TRUE) 

cal_CA_sell_CA_3_clean <-  cleanSell(cal_CA_sell_CA_3) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_CA_sell_CA_3_clean[is.na(cal_CA_sell_CA_3_clean)] = 0
cal_CA_sell_CA_3_clean %<>%  as.matrix()

cal_CA_sell_CA_3_clean_train_reg <- cal_CA_sell_CA_3_clean[1:1913, ]
cal_CA_sell_CA_3_clean_forcast_reg <- cal_CA_sell_CA_3_clean[1914:1941,]


library(hts)
cal_CA_sell_CA_3_clean_train_reg_agg <- aggts(
  hts(cal_CA_sell_CA_3_clean_train_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)
cal_CA_sell_CA_3_clean_forcast_reg_agg <- aggts(
  hts(cal_CA_sell_CA_3_clean_forcast_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)

CADataCA_3FoodCleanTT_hts <- hts(CADataCA_3FoodCleanTT, 
                                 characters = c(7,3)) #convert to hts object

h <- 28
CADataCA_3FoodCleanTT_hts_aggts <- aggts(CADataCA_3FoodCleanTT_hts, 
                                         levels = c(0,1,2))
n <- nrow(CADataCA_3FoodCleanTT_hts_aggts)
p <- ncol(CADataCA_3FoodCleanTT_hts_aggts)

CADataCA_3FoodCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
CADataCA_3FoodCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(CADataCA_3FoodCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    CADataCA_3FoodCleanTT_hts_aggts[,i], 
    as.character(
      CADataCA_3FoodCleanTT_hts_aggts[,i][cumsum(CADataCA_3FoodCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  CADataCA_3FoodCleanTT_hts_aggts_fit <- auto.arima( 
    CADataCA_3FoodCleanTT_hts_aggts[l:k,i],
    xreg = cal_CA_sell_CA_3_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  CADataCA_3FoodCleanTT_hts_aggts_forecast[, i] <- forecast(
    CADataCA_3FoodCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_CA_sell_CA_3_clean_forcast_reg_agg[,i]
  )$mean
  #CADataCA_3FoodCleanTT_hts_aggts_resid[l:k, i] <-  
  # CADataCA_3FoodCleanTT_hts_aggts[l:k, i] - fitted(CADataCA_3FoodCleanTT_hts_aggts_fit) 
  
}

CADataCA_3FoodCleanTT_hts_aggts_forecast[!is.finite(CADataCA_3FoodCleanTT_hts_aggts_forecast)] <- 0
CADataCA_3FoodCleanTT_hts_aggts_forecast_ts <- ts(
  CADataCA_3FoodCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

CADataCA_3FoodCleanTT_hts_aggts_forecast_ts_f <- combinef(
  CADataCA_3FoodCleanTT_hts_aggts_forecast_ts,
  get_nodes(CADataCA_3FoodCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(CADataCA_3FoodCleanTT_hts_aggts)
colnames(CADataCA_3FoodCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(CADataCA_3FoodCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/CADataCA_3FoodCleanTT_hts_aggts_forecast_ts_f.csv") 

Sys.time()

########################################################end


CADataCA_3Foods_Depart1 <- CADataCA_3Foods %>% 
  filter(dept_id == "FOODS_1") %>% select(-dept_id)
head(CADataCA_3Foods_Depart1[1:3])

CADataCA_3Foods_Depart2 <- CADataCA_3Foods %>% 
  filter(dept_id == "FOODS_2") %>% select(-dept_id)
head(CADataCA_3Foods_Depart2[1:3])

CADataCA_3Foods_Depart3 <- CADataCA_3Foods %>% 
  filter(dept_id == "FOODS_3") %>% select(-dept_id)
head(CADataCA_3Foods_Depart3[1:3])




#CA_4######################
#Store CA_4
CADataCA_4 <- CAData %>% filter(store_id == "CA_4") %>% 
  select(-store_id)
head(CADataCA_4[1:4])

#CA_4Hobbies########################
CADataCA_4Hobbies <- CADataCA_4 %>% 
  filter(cat_id == "HOBBIES") %>% select(-cat_id)
head(CADataCA_4Hobbies[1:3])

#CA_4HOBBIEShts######################################################

#clean HOBS CA_4
cleanCAHOBS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

CADataCA_4HobbiesClean <- cleanCAHOBS(CADataCA_4Hobbies) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(CADataCA_4HobbiesClean)

CADataCA_4HobbiesCleanT <- t(CADataCA_4HobbiesClean[,-1914])
colnames(CADataCA_4HobbiesCleanT) <- c(CADataCA_4HobbiesClean$groupS)#as.list
CADataCA_4HobbiesCleanTT <- as_tibble(CADataCA_4HobbiesCleanT)

head(CADataCA_4HobbiesCleanTT)

cal_CA <- calendar %>% select(date, wm_yr_wk)
head(cal_CA)

#CA_4
sell_CA_4 <- sell_prices %>% filter(store_id == "CA_4") %>% 
  filter(grepl('HOBB', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_CA_4)
tail(sell_CA_4)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_CA_sell_CA_4 <- merge(cal_CA, sell_CA_4, by = "wm_yr_wk", all = TRUE) 

cal_CA_sell_CA_4_clean <-  cleanSell(cal_CA_sell_CA_4) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_CA_sell_CA_4_clean[is.na(cal_CA_sell_CA_4_clean)] = 0
cal_CA_sell_CA_4_clean %<>%  as.matrix()

cal_CA_sell_CA_4_clean_train_reg <- cal_CA_sell_CA_4_clean[1:1913, ]
cal_CA_sell_CA_4_clean_forcast_reg <- cal_CA_sell_CA_4_clean[1914:1941,]


library(hts)
cal_CA_sell_CA_4_clean_train_reg_agg <- aggts(
  hts(cal_CA_sell_CA_4_clean_train_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)
cal_CA_sell_CA_4_clean_forcast_reg_agg <- aggts(
  hts(cal_CA_sell_CA_4_clean_forcast_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)

CADataCA_4HobbiesCleanTT_hts <- hts(CADataCA_4HobbiesCleanTT, 
                                    characters = c(7,3)) #convert to hts object

h <- 28
CADataCA_4HobbiesCleanTT_hts_aggts <- aggts(CADataCA_4HobbiesCleanTT_hts, 
                                            levels = c(0,1,2))
n <- nrow(CADataCA_4HobbiesCleanTT_hts_aggts)
p <- ncol(CADataCA_4HobbiesCleanTT_hts_aggts)

CADataCA_4HobbiesCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
CADataCA_4HobbiesCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(CADataCA_4HobbiesCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    CADataCA_4HobbiesCleanTT_hts_aggts[,i], 
    as.character(
      CADataCA_4HobbiesCleanTT_hts_aggts[,i][cumsum(CADataCA_4HobbiesCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  CADataCA_4HobbiesCleanTT_hts_aggts_fit <- auto.arima( 
    CADataCA_4HobbiesCleanTT_hts_aggts[l:k,i],
    xreg = cal_CA_sell_CA_4_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  CADataCA_4HobbiesCleanTT_hts_aggts_forecast[, i] <- forecast(
    CADataCA_4HobbiesCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_CA_sell_CA_4_clean_forcast_reg_agg[,i]
  )$mean
  #CADataCA_4HobbiesCleanTT_hts_aggts_resid[l:k, i] <-  
  # CADataCA_4HobbiesCleanTT_hts_aggts[l:k, i] - fitted(CADataCA_4HobbiesCleanTT_hts_aggts_fit) 
}

CADataCA_4HobbiesCleanTT_hts_aggts_forecast[!is.finite(CADataCA_4HobbiesCleanTT_hts_aggts_forecast)] <- 0
CADataCA_4HobbiesCleanTT_hts_aggts_forecast_ts <- ts(
  CADataCA_4HobbiesCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

CADataCA_4HobbiesCleanTT_hts_aggts_forecast_ts_f <- combinef(
  CADataCA_4HobbiesCleanTT_hts_aggts_forecast_ts,
  get_nodes(CADataCA_4HobbiesCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(CADataCA_4HobbiesCleanTT_hts_aggts)
colnames(CADataCA_4HobbiesCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(CADataCA_4HobbiesCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/CADataCA_4HobbiesCleanTT_hts_aggts_forecast_ts_f.csv" ) 
Sys.time()

###############################################################################fdssssssss



CADataCA_4Hobbies_Depart1 <- CADataCA_4Hobbies %>% 
  filter(dept_id == "HOBBIES_1") %>% select(-dept_id)
head(CADataCA_4Hobbies_Depart1[1:3])

CADataCA_4Hobbies_Depart2 <- CADataCA_4Hobbies %>% 
  filter(dept_id == "HOBBIES_2") %>% select(-dept_id)
head(CADataCA_4Hobbies_Depart2[1:3])

#CA_4HOUSE###############################################
CADataCA_4Household <- CADataCA_4 %>% 
  filter(cat_id == "HOUSEHOLD") %>% select(-cat_id)
head(CADataCA_4Household[1:3])

#CA_4HOUSEhts#########################
#clean HOUS CA_4
cleanCAHOUS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

CADataCA_4HouseholdClean <- cleanCAHOUS(CADataCA_4Household) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(CADataCA_4HouseholdClean)

CADataCA_4HouseholdCleanT <- t(CADataCA_4HouseholdClean[,-1914])
colnames(CADataCA_4HouseholdCleanT) <- c(CADataCA_4HouseholdClean$groupS)
CADataCA_4HouseholdCleanTT <- as_tibble(CADataCA_4HouseholdCleanT)

head(CADataCA_4HouseholdCleanTT)

cal_CA <- calendar %>% select(date, wm_yr_wk)
head(cal_CA)

#CA_4
sell_CA_4<- sell_prices %>% filter(store_id == "CA_4") %>%  
  filter(grepl('HOUS', item_id)) %>%
  #slice() %>% 
  select(-store_id)
head(sell_CA_4)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_CA_sell_CA_4 <- merge(cal_CA, sell_CA_4, by = "wm_yr_wk", all = TRUE) 

cal_CA_sell_CA_4_clean <-  cleanSell(cal_CA_sell_CA_4) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_CA_sell_CA_4_clean[is.na(cal_CA_sell_CA_4_clean)] = 0
cal_CA_sell_CA_4_clean %<>%  as.matrix()

cal_CA_sell_CA_4_clean_train_reg <- cal_CA_sell_CA_4_clean[1:1913, ]
cal_CA_sell_CA_4_clean_forcast_reg <- cal_CA_sell_CA_4_clean[1914:1941,]


library(hts)
cal_CA_sell_CA_4_clean_train_reg_agg <- aggts(
  hts(cal_CA_sell_CA_4_clean_train_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)
cal_CA_sell_CA_4_clean_forcast_reg_agg <- aggts(
  hts(cal_CA_sell_CA_4_clean_forcast_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)

CADataCA_4HouseholdCleanTT_hts <- hts(CADataCA_4HouseholdCleanTT, 
                                      characters = c(7,3)) #convert to hts object

h <- 28
CADataCA_4HouseholdCleanTT_hts_aggts <- aggts(CADataCA_4HouseholdCleanTT_hts, 
                                              levels = c(0,1,2))
n <- nrow(CADataCA_4HouseholdCleanTT_hts_aggts)
p <- ncol(CADataCA_4HouseholdCleanTT_hts_aggts)

CADataCA_4HouseholdCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
CADataCA_4HouseholdCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(CADataCA_4HouseholdCleanTT_hts_aggts[,i])
  
  l <- stringr::str_which(
    CADataCA_4HouseholdCleanTT_hts_aggts[,i], 
    as.character(
      CADataCA_4HouseholdCleanTT_hts_aggts[,i][cumsum(CADataCA_4HouseholdCleanTT_hts_aggts[,i]) != 0][1]
    ))[1]
  
  CADataCA_4HouseholdCleanTT_hts_aggts_fit <- auto.arima( 
    CADataCA_4HouseholdCleanTT_hts_aggts[l:k,i],
    xreg = cal_CA_sell_CA_4_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  CADataCA_4HouseholdCleanTT_hts_aggts_forecast[, i] <- forecast(
    CADataCA_4HouseholdCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_CA_sell_CA_4_clean_forcast_reg_agg[,i]
  )$mean
  #CADataCA_4HouseholdCleanTT_hts_aggts_resid[l:k, i] <-  
  # CADataCA_4HouseholdCleanTT_hts_aggts[l:k, i] - fitted(CADataCA_4HouseholdCleanTT_hts_aggts_fit) 
  
}

CADataCA_4HouseholdCleanTT_hts_aggts_forecast[!is.finite(CADataCA_4HouseholdCleanTT_hts_aggts_forecast)] <- 0
CADataCA_4HouseholdCleanTT_hts_aggts_forecast_ts <- ts(
  CADataCA_4HouseholdCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

CADataCA_4HouseholdCleanTT_hts_aggts_forecast_ts_f <- combinef(
  CADataCA_4HouseholdCleanTT_hts_aggts_forecast_ts,
  get_nodes(CADataCA_4HouseholdCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(CADataCA_4HouseholdCleanTT_hts_aggts)
colnames(CADataCA_4HouseholdCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(CADataCA_4HouseholdCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/CADataCA_4HouseholdCleanTT_hts_aggts_forecast_ts_f.csv") 
Sys.time()

########################################################end

CADataCA_4Household_Depart1 <- CADataCA_4Household %>% 
  filter(dept_id == "HOUSEHOLD_1") %>% select(-dept_id)
head(CADataCA_4Household_Depart1[1:3])

CADataCA_4Household_Depart2 <- CADataCA_4Household %>% 
  filter(dept_id == "HOUSEHOLD_2") %>% select(-dept_id)
head(CADataCA_4Household_Depart2[1:3])

#CA_4FOODS################################################
CADataCA_4Foods <- CADataCA_4 %>% 
  filter(cat_id == "FOODS") %>% select(-cat_id)
head(CADataCA_4Foods[1:3])

#CA_4FOODhts#########################

#clean FOOD CA_4
cleanCAFOOD <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

CADataCA_4FoodClean <- cleanCAFOOD(CADataCA_4Foods) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(CADataCA_4FoodClean)

CADataCA_4FoodCleanT <- t(CADataCA_4FoodClean[,-1914])
colnames(CADataCA_4FoodCleanT) <- c(CADataCA_4FoodClean$groupS)
CADataCA_4FoodCleanTT <- as_tibble(CADataCA_4FoodCleanT)

head(CADataCA_4FoodCleanTT)

cal_CA <- calendar %>% select(date, wm_yr_wk)
head(cal_CA)

#CA_4
sell_CA_4 <- sell_prices %>% filter(store_id == "CA_4") %>%  
  filter(grepl('FOO', item_id)) %>%
  #slice() %>% 
  select(-store_id)
head(sell_CA_4)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_CA_sell_CA_4 <- merge(cal_CA, sell_CA_4, by = "wm_yr_wk", all = TRUE) 

cal_CA_sell_CA_4_clean <-  cleanSell(cal_CA_sell_CA_4) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_CA_sell_CA_4_clean[is.na(cal_CA_sell_CA_4_clean)] = 0
cal_CA_sell_CA_4_clean %<>%  as.matrix()

cal_CA_sell_CA_4_clean_train_reg <- cal_CA_sell_CA_4_clean[1:1913, ]
cal_CA_sell_CA_4_clean_forcast_reg <- cal_CA_sell_CA_4_clean[1914:1941,]


library(hts)
cal_CA_sell_CA_4_clean_train_reg_agg <- aggts(
  hts(cal_CA_sell_CA_4_clean_train_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)
cal_CA_sell_CA_4_clean_forcast_reg_agg <- aggts(
  hts(cal_CA_sell_CA_4_clean_forcast_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)

CADataCA_4FoodCleanTT_hts <- hts(CADataCA_4FoodCleanTT, 
                                 characters = c(7,3)) #convert to hts object

h <- 28
CADataCA_4FoodCleanTT_hts_aggts <- aggts(CADataCA_4FoodCleanTT_hts, 
                                         levels = c(0,1,2))
n <- nrow(CADataCA_4FoodCleanTT_hts_aggts)
p <- ncol(CADataCA_4FoodCleanTT_hts_aggts)

CADataCA_4FoodCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
CADataCA_4FoodCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(CADataCA_4FoodCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    CADataCA_4FoodCleanTT_hts_aggts[,i], 
    as.character(
      CADataCA_4FoodCleanTT_hts_aggts[,i][cumsum(CADataCA_4FoodCleanTT_hts_aggts[,i]) != 0][1]
    ))[1]
  
  CADataCA_4FoodCleanTT_hts_aggts_fit <- auto.arima( 
    CADataCA_4FoodCleanTT_hts_aggts[l:k,i],
    xreg = cal_CA_sell_CA_4_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  CADataCA_4FoodCleanTT_hts_aggts_forecast[, i] <- forecast(
    CADataCA_4FoodCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_CA_sell_CA_4_clean_forcast_reg_agg[,i]
  )$mean
  #CADataCA_4FoodCleanTT_hts_aggts_resid[l:k, i] <-  
  # CADataCA_4FoodCleanTT_hts_aggts[l:k, i] - fitted(CADataCA_4FoodCleanTT_hts_aggts_fit) 
}

CADataCA_4FoodCleanTT_hts_aggts_forecast[!is.finite(CADataCA_4FoodCleanTT_hts_aggts_forecast)] <- 0
CADataCA_4FoodCleanTT_hts_aggts_forecast_ts <- ts(
  CADataCA_4FoodCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

CADataCA_4FoodCleanTT_hts_aggts_forecast_ts_f <- combinef(
  CADataCA_4FoodCleanTT_hts_aggts_forecast_ts,
  get_nodes(CADataCA_4FoodCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(CADataCA_4FoodCleanTT_hts_aggts)
colnames(CADataCA_4FoodCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(CADataCA_4FoodCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/CADataCA_4FoodCleanTT_hts_aggts_forecast_ts_f.csv")  
Sys.time()

########################################################end


CADataCA_4Foods_Depart1 <- CADataCA_4Foods %>% 
  filter(dept_id == "FOODS_1") %>% select(-dept_id)
head(CADataCA_4Foods_Depart1[1:3])

CADataCA_4Foods_Depart2 <- CADataCA_4Foods %>% 
  filter(dept_id == "FOODS_2") %>% select(-dept_id)
head(CADataCA_4Foods_Depart2[1:3])

CADataCA_4Foods_Depart3 <- CADataCA_4Foods %>% 
  filter(dept_id == "FOODS_3") %>% select(-dept_id)
head(CADataCA_4Foods_Depart3[1:3])


# TX ----------------------------------------------------------------------

####### take TX state data, group by store_id, group by category for later EDA

TXData <- sales_train_validation %>% filter(state_id == "TX") %>% 
  select(-state_id)
head(TXData[1:5])


#TX_1#####################
#store TX_1
TXDataTX_1 <- TXData %>% filter(store_id == "TX_1") %>% 
  select(-store_id)
head(TXDataTX_1[1:4])

#unique(TXDataTX_1$cat_id)

#TX_1Hobbies#########################
TXDataTX_1Hobbies <- TXDataTX_1 %>% 
  filter(cat_id == "HOBBIES") %>% select(-cat_id)
head(TXDataTX_1Hobbies[1:3])

#TX_1HobbiesHTS###################################################

#clean HOBS TX_1
cleanTXHOBS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

TXDataTX_1HobbiesClean <- cleanTXHOBS(TXDataTX_1Hobbies) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(TXDataTX_1HobbiesClean)

TXDataTX_1HobbiesCleanT <- t(TXDataTX_1HobbiesClean[,-1914])
colnames(TXDataTX_1HobbiesCleanT) <- c(TXDataTX_1HobbiesClean$groupS)#as.list
TXDataTX_1HobbiesCleanTT <- as_tibble(TXDataTX_1HobbiesCleanT)

head(TXDataTX_1HobbiesCleanTT)

cal_TX <- calendar %>% select(date, wm_yr_wk)
head(cal_TX)

#TX_1
sell_TX_1 <- sell_prices %>% filter(store_id == "TX_1") %>% 
  filter(grepl('HOBB', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_TX_1)
tail(sell_TX_1)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_TX_sell_TX_1 <- merge(cal_TX, sell_TX_1, by = "wm_yr_wk", all = TRUE) 

cal_TX_sell_TX_1_clean <-  cleanSell(cal_TX_sell_TX_1) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_TX_sell_TX_1_clean[is.na(cal_TX_sell_TX_1_clean)] = 0
cal_TX_sell_TX_1_clean %<>%  as.matrix()

cal_TX_sell_TX_1_clean_train_reg <- cal_TX_sell_TX_1_clean[1:1913, ]
cal_TX_sell_TX_1_clean_forcast_reg <- cal_TX_sell_TX_1_clean[1914:1941,]


library(hts)
cal_TX_sell_TX_1_clean_train_reg_agg <- aggts(
  hts(cal_TX_sell_TX_1_clean_train_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)
cal_TX_sell_TX_1_clean_forcast_reg_agg <- aggts(
  hts(cal_TX_sell_TX_1_clean_forcast_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)

TXDataTX_1HobbiesCleanTT_hts <- hts(TXDataTX_1HobbiesCleanTT, 
                                    characters = c(7,3)) #convert to hts object

h <- 28
TXDataTX_1HobbiesCleanTT_hts_aggts <- aggts(TXDataTX_1HobbiesCleanTT_hts, 
                                            levels = c(0,1,2))
n <- nrow(TXDataTX_1HobbiesCleanTT_hts_aggts)
p <- ncol(TXDataTX_1HobbiesCleanTT_hts_aggts)

TXDataTX_1HobbiesCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
TXDataTX_1HobbiesCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(TXDataTX_1HobbiesCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    TXDataTX_1HobbiesCleanTT_hts_aggts[,i], 
    as.character(
      TXDataTX_1HobbiesCleanTT_hts_aggts[,i][cumsum(TXDataTX_1HobbiesCleanTT_hts_aggts[,i]) != 0][1]
    ))[1]
  
  TXDataTX_1HobbiesCleanTT_hts_aggts_fit <- auto.arima( 
    TXDataTX_1HobbiesCleanTT_hts_aggts[l:k,i],
    xreg = cal_TX_sell_TX_1_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  TXDataTX_1HobbiesCleanTT_hts_aggts_forecast[, i] <- forecast(
    TXDataTX_1HobbiesCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_TX_sell_TX_1_clean_forcast_reg_agg[,i]
  )$mean
  #TXDataTX_1HobbiesCleanTT_hts_aggts_resid[l:k, i] <-  
  # TXDataTX_1HobbiesCleanTT_hts_aggts[l:k, i] - fitted(TXDataTX_1HobbiesCleanTT_hts_aggts_fit) 
  
}

TXDataTX_1HobbiesCleanTT_hts_aggts_forecast[!is.finite(TXDataTX_1HobbiesCleanTT_hts_aggts_forecast)] <- 0
TXDataTX_1HobbiesCleanTT_hts_aggts_forecast_ts <- ts(
  TXDataTX_1HobbiesCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

TXDataTX_1HobbiesCleanTT_hts_aggts_forecast_ts_f <- combinef(
  TXDataTX_1HobbiesCleanTT_hts_aggts_forecast_ts,
  get_nodes(TXDataTX_1HobbiesCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(TXDataTX_1HobbiesCleanTT_hts_aggts)
colnames(TXDataTX_1HobbiesCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(TXDataTX_1HobbiesCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/TXDataTX_1HobbiesCleanTT_hts_aggts_forecast_ts_f.csv" ) 
Sys.time()


###############################################################################fdssssssss


TXDataTX_1Hobbies_Depart1 <- TXDataTX_1Hobbies %>% 
  filter(dept_id == "HOBBIES_1") %>% select(-dept_id)
head(TXDataTX_1Hobbies_Depart1[1:3])

TXDataTX_1Hobbies_Depart2 <- TXDataTX_1Hobbies %>% 
  filter(dept_id == "HOBBIES_2") %>% select(-dept_id)
head(TXDataTX_1Hobbies_Depart2[1:3])

#TX_1HOUSE##################
TXDataTX_1Household <- TXDataTX_1 %>% 
  filter(cat_id == "HOUSEHOLD") %>% select(-cat_id)
head(TXDataTX_1Household[1:3])

#TX_1HOUSEhts#########################

#clean HOUS TX_1
cleanTXHOUS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

TXDataTX_1HouseholdClean <- cleanTXHOUS(TXDataTX_1Household) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(TXDataTX_1HouseholdClean)

TXDataTX_1HouseholdCleanT <- t(TXDataTX_1HouseholdClean[,-1914])
colnames(TXDataTX_1HouseholdCleanT) <- c(TXDataTX_1HouseholdClean$groupS)
TXDataTX_1HouseholdCleanTT <- as_tibble(TXDataTX_1HouseholdCleanT)

head(TXDataTX_1HouseholdCleanTT)

cal_TX <- calendar %>% select(date, wm_yr_wk)
head(cal_TX)

#TX_1
sell_TX_1 <- sell_prices %>% filter(store_id == "TX_1") %>%  
  filter(grepl('HOUS', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_TX_1)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_TX_sell_TX_1 <- merge(cal_TX, sell_TX_1, by = "wm_yr_wk", all = TRUE) 

cal_TX_sell_TX_1_clean <-  cleanSell(cal_TX_sell_TX_1) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_TX_sell_TX_1_clean[is.na(cal_TX_sell_TX_1_clean)] = 0
cal_TX_sell_TX_1_clean %<>%  as.matrix()

cal_TX_sell_TX_1_clean_train_reg <- cal_TX_sell_TX_1_clean[1:1913, ]
cal_TX_sell_TX_1_clean_forcast_reg <- cal_TX_sell_TX_1_clean[1914:1941,]


library(hts)
cal_TX_sell_TX_1_clean_train_reg_agg <- aggts(
  hts(cal_TX_sell_TX_1_clean_train_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)
cal_TX_sell_TX_1_clean_forcast_reg_agg <- aggts(
  hts(cal_TX_sell_TX_1_clean_forcast_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)

TXDataTX_1HouseholdCleanTT_hts <- hts(TXDataTX_1HouseholdCleanTT, 
                                      characters = c(7,3)) #convert to hts object

h <- 28
TXDataTX_1HouseholdCleanTT_hts_aggts <- aggts(TXDataTX_1HouseholdCleanTT_hts, 
                                              levels = c(0,1,2))
n <- nrow(TXDataTX_1HouseholdCleanTT_hts_aggts)
p <- ncol(TXDataTX_1HouseholdCleanTT_hts_aggts)

TXDataTX_1HouseholdCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
TXDataTX_1HouseholdCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
#for (level in c(0.5,2.5, 16.5, 16.5, 25, 50, 75, 83.5, 97.5, 99.5)){
for(i in 1:p)
{ 
  k <- length(TXDataTX_1HouseholdCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    TXDataTX_1HouseholdCleanTT_hts_aggts[,i], 
    as.character(
      TXDataTX_1HouseholdCleanTT_hts_aggts[,i][cumsum(TXDataTX_1HouseholdCleanTT_hts_aggts[,i]) != 0][1]
    ))[1]
  
  TXDataTX_1HouseholdCleanTT_hts_aggts_fit <- auto.arima( 
    TXDataTX_1HouseholdCleanTT_hts_aggts[l:k,i],
    xreg = cal_TX_sell_TX_1_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  TXDataTX_1HouseholdCleanTT_hts_aggts_forecast[, i] <- forecast(
    TXDataTX_1HouseholdCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_TX_sell_TX_1_clean_forcast_reg_agg[,i]
  )$mean
  #TXDataTX_1HouseholdCleanTT_hts_aggts_resid[l:k, i] <-  
  # TXDataTX_1HouseholdCleanTT_hts_aggts[l:k, i] - fitted(TXDataTX_1HouseholdCleanTT_hts_aggts_fit) 
  
}

TXDataTX_1HouseholdCleanTT_hts_aggts_forecast[!is.finite(TXDataTX_1HouseholdCleanTT_hts_aggts_forecast)] <- 0
TXDataTX_1HouseholdCleanTT_hts_aggts_forecast_ts <- ts(
  TXDataTX_1HouseholdCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

TXDataTX_1HouseholdCleanTT_hts_aggts_forecast_ts_f <- combinef(
  TXDataTX_1HouseholdCleanTT_hts_aggts_forecast_ts,
  get_nodes(TXDataTX_1HouseholdCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(TXDataTX_1HouseholdCleanTT_hts_aggts)
colnames(TXDataTX_1HouseholdCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(TXDataTX_1HouseholdCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/TXDataTX_1HouseholdCleanTT_hts_aggts_forecast_ts_f.csv") 

Sys.time()

########################################################end

TXDataTX_1Household_Depart1 <- TXDataTX_1Household %>% 
  filter(dept_id == "HOUSEHOLD_1") %>% select(-dept_id)
head(TXDataTX_1Household_Depart1[1:3])

TXDataTX_1Household_Depart2 <- TXDataTX_1Household %>% 
  filter(dept_id == "HOUSEHOLD_2") %>% select(-dept_id)
head(TXDataTX_1Household_Depart2[1:3])

#TX_1FOODS#########################################
TXDataTX_1Foods <- TXDataTX_1 %>% 
  filter(cat_id == "FOODS") %>% select(-cat_id)
head(TXDataTX_1Foods[1:3])

#TX_1FOODhts#########################

#clean FOOD TX_1
cleanTXFOOD <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

TXDataTX_1FoodClean <- cleanTXFOOD(TXDataTX_1Foods) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(TXDataTX_1FoodClean)

TXDataTX_1FoodCleanT <- t(TXDataTX_1FoodClean[,-1914])
colnames(TXDataTX_1FoodCleanT) <- c(TXDataTX_1FoodClean$groupS)
TXDataTX_1FoodCleanTT <- as_tibble(TXDataTX_1FoodCleanT)

head(TXDataTX_1FoodCleanTT)

cal_TX <- calendar %>% select(date, wm_yr_wk)
head(cal_TX)

#TX_1
sell_TX_1 <- sell_prices %>% filter(store_id == "TX_1") %>%  
  filter(grepl('FOOD', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_TX_1)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_TX_sell_TX_1 <- merge(cal_TX, sell_TX_1, by = "wm_yr_wk", all = TRUE) 

cal_TX_sell_TX_1_clean <-  cleanSell(cal_TX_sell_TX_1) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_TX_sell_TX_1_clean[is.na(cal_TX_sell_TX_1_clean)] = 0
cal_TX_sell_TX_1_clean %<>%  as.matrix()

cal_TX_sell_TX_1_clean_train_reg <- cal_TX_sell_TX_1_clean[1:1913, ]
cal_TX_sell_TX_1_clean_forcast_reg <- cal_TX_sell_TX_1_clean[1914:1941,]


library(hts)
cal_TX_sell_TX_1_clean_train_reg_agg <- aggts(
  hts(cal_TX_sell_TX_1_clean_train_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)
cal_TX_sell_TX_1_clean_forcast_reg_agg <- aggts(
  hts(cal_TX_sell_TX_1_clean_forcast_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)

TXDataTX_1FoodCleanTT_hts <- hts(TXDataTX_1FoodCleanTT, 
                                 characters = c(7,3)) #convert to hts object

h <- 28
TXDataTX_1FoodCleanTT_hts_aggts <- aggts(TXDataTX_1FoodCleanTT_hts, 
                                         levels = c(0,1,2))
n <- nrow(TXDataTX_1FoodCleanTT_hts_aggts)
p <- ncol(TXDataTX_1FoodCleanTT_hts_aggts)

TXDataTX_1FoodCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
TXDataTX_1FoodCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(TXDataTX_1FoodCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    TXDataTX_1FoodCleanTT_hts_aggts[,i], 
    as.character(
      TXDataTX_1FoodCleanTT_hts_aggts[,i][cumsum(TXDataTX_1FoodCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  TXDataTX_1FoodCleanTT_hts_aggts_fit <- auto.arima( 
    TXDataTX_1FoodCleanTT_hts_aggts[l:k,i],
    xreg = cal_TX_sell_TX_1_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  TXDataTX_1FoodCleanTT_hts_aggts_forecast[, i] <- forecast(
    TXDataTX_1FoodCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_TX_sell_TX_1_clean_forcast_reg_agg[,i]
  )$mean
  #TXDataTX_1FoodCleanTT_hts_aggts_resid[l:k, i] <-  
  # TXDataTX_1FoodCleanTT_hts_aggts[l:k, i] - fitted(TXDataTX_1FoodCleanTT_hts_aggts_fit) 
  
}

TXDataTX_1FoodCleanTT_hts_aggts_forecast[!is.finite(TXDataTX_1FoodCleanTT_hts_aggts_forecast)] <- 0
TXDataTX_1FoodCleanTT_hts_aggts_forecast_ts <- ts(
  TXDataTX_1FoodCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

TXDataTX_1FoodCleanTT_hts_aggts_forecast_ts_f <- combinef(
  TXDataTX_1FoodCleanTT_hts_aggts_forecast_ts,
  get_nodes(TXDataTX_1FoodCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(TXDataTX_1FoodCleanTT_hts_aggts)
colnames(TXDataTX_1FoodCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(TXDataTX_1FoodCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/TXDataTX_1FoodCleanTT_hts_aggts_forecast_ts_f.csv") 

Sys.time()

########################################################end


TXDataTX_1Foods_Depart1 <- TXDataTX_1Foods %>% 
  filter(dept_id == "FOODS_1") %>% select(-dept_id)
head(TXDataTX_1Foods_Depart1[1:3])

TXDataTX_1Foods_Depart2 <- TXDataTX_1Foods %>% 
  filter(dept_id == "FOODS_2") %>% select(-dept_id)
head(TXDataTX_1Foods_Depart2[1:3])

TXDataTX_1Foods_Depart3 <- TXDataTX_1Foods %>% 
  filter(dept_id == "FOODS_3") %>% select(-dept_id)
head(TXDataTX_1Foods_Depart3[1:3])





#TX_2##############################################
#Store TX_2
TXDataTX_2 <- TXData %>% filter(store_id == "TX_2") %>% 
  select(-store_id)
head(TXDataTX_2[1:4])

#unique(TXDataTX_2$cat_id)

#TX_2Hobbies###########################################
TXDataTX_2Hobbies <- TXDataTX_2 %>% 
  filter(cat_id == "HOBBIES") %>% select(-cat_id)
head(TXDataTX_2Hobbies[1:3])

#TX_2HobbiesHTS#######################################################

#clean HOBS TX_2
cleanTXHOBS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

TXDataTX_2HobbiesClean <- cleanTXHOBS(TXDataTX_2Hobbies) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(TXDataTX_2HobbiesClean)

TXDataTX_2HobbiesCleanT <- t(TXDataTX_2HobbiesClean[,-1914])
colnames(TXDataTX_2HobbiesCleanT) <- c(TXDataTX_2HobbiesClean$groupS)#as.list
TXDataTX_2HobbiesCleanTT <- as_tibble(TXDataTX_2HobbiesCleanT)

head(TXDataTX_2HobbiesCleanTT)

cal_TX <- calendar %>% select(date, wm_yr_wk)
head(cal_TX)

#TX_2
sell_TX_2 <- sell_prices %>% filter(store_id == "TX_2") %>% 
  filter(grepl('HOBB', item_id)) %>%
  #slice() %>% 
  select(-store_id)
head(sell_TX_2)
tail(sell_TX_2)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_TX_sell_TX_2 <- merge(cal_TX, sell_TX_2, by = "wm_yr_wk", all = TRUE) 

cal_TX_sell_TX_2_clean <-  cleanSell(cal_TX_sell_TX_2) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_TX_sell_TX_2_clean[is.na(cal_TX_sell_TX_2_clean)] = 0
cal_TX_sell_TX_2_clean %<>%  as.matrix()

cal_TX_sell_TX_2_clean_train_reg <- cal_TX_sell_TX_2_clean[1:1913, ]
cal_TX_sell_TX_2_clean_forcast_reg <- cal_TX_sell_TX_2_clean[1914:1941,]


library(hts)
cal_TX_sell_TX_2_clean_train_reg_agg <- aggts(
  hts(cal_TX_sell_TX_2_clean_train_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)
cal_TX_sell_TX_2_clean_forcast_reg_agg <- aggts(
  hts(cal_TX_sell_TX_2_clean_forcast_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)

TXDataTX_2HobbiesCleanTT_hts <- hts(TXDataTX_2HobbiesCleanTT, 
                                    characters = c(7,3)) #convert to hts object

h <- 28
TXDataTX_2HobbiesCleanTT_hts_aggts <- aggts(TXDataTX_2HobbiesCleanTT_hts, 
                                            levels = c(0,1,2))
n <- nrow(TXDataTX_2HobbiesCleanTT_hts_aggts)
p <- ncol(TXDataTX_2HobbiesCleanTT_hts_aggts)

TXDataTX_2HobbiesCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
TXDataTX_2HobbiesCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(TXDataTX_2HobbiesCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    TXDataTX_2HobbiesCleanTT_hts_aggts[,i], 
    as.character(
      TXDataTX_2HobbiesCleanTT_hts_aggts[,i][cumsum(TXDataTX_2HobbiesCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  TXDataTX_2HobbiesCleanTT_hts_aggts_fit <- auto.arima( 
    TXDataTX_2HobbiesCleanTT_hts_aggts[l:k,i],
    xreg = cal_TX_sell_TX_2_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  TXDataTX_2HobbiesCleanTT_hts_aggts_forecast[, i] <- forecast(
    TXDataTX_2HobbiesCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_TX_sell_TX_2_clean_forcast_reg_agg[,i]
  )$mean
  #TXDataTX_2HobbiesCleanTT_hts_aggts_resid[l:k, i] <-  
  # TXDataTX_2HobbiesCleanTT_hts_aggts[l:k, i] - fitted(TXDataTX_2HobbiesCleanTT_hts_aggts_fit) 
  
}

TXDataTX_2HobbiesCleanTT_hts_aggts_forecast[!is.finite(TXDataTX_2HobbiesCleanTT_hts_aggts_forecast)] <- 0
TXDataTX_2HobbiesCleanTT_hts_aggts_forecast_ts <- ts(
  TXDataTX_2HobbiesCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

TXDataTX_2HobbiesCleanTT_hts_aggts_forecast_ts_f <- combinef(
  TXDataTX_2HobbiesCleanTT_hts_aggts_forecast_ts,
  get_nodes(TXDataTX_2HobbiesCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(TXDataTX_2HobbiesCleanTT_hts_aggts)
colnames(TXDataTX_2HobbiesCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(TXDataTX_2HobbiesCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/TXDataTX_2HobbiesCleanTT_hts_aggts_forecast_ts_f.csv")  
Sys.time() 


###############################################################################fdssssssss



TXDataTX_2Hobbies_Depart1 <- TXDataTX_2Hobbies %>% 
  filter(dept_id == "HOBBIES_1") %>% select(-dept_id)
head(TXDataTX_2Hobbies_Depart1[1:3])

TXDataTX_2Hobbies_Depart2 <- TXDataTX_2Hobbies %>% 
  filter(dept_id == "HOBBIES_2") %>% select(-dept_id)
head(TXDataTX_2Hobbies_Depart2[1:3])

#TX_2HOUSE#########################
TXDataTX_2Household <- TXDataTX_2 %>% 
  filter(cat_id == "HOUSEHOLD") %>% select(-cat_id)
head(TXDataTX_2Household[1:3])

#TX_2HOUSEhts#########################

#clean HOUS TX_2
cleanTXHOUS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

TXDataTX_2HouseholdClean <- cleanTXHOUS(TXDataTX_2Household) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(TXDataTX_2HouseholdClean)

TXDataTX_2HouseholdCleanT <- t(TXDataTX_2HouseholdClean[,-1914])
colnames(TXDataTX_2HouseholdCleanT) <- c(TXDataTX_2HouseholdClean$groupS)
TXDataTX_2HouseholdCleanTT <- as_tibble(TXDataTX_2HouseholdCleanT)

head(TXDataTX_2HouseholdCleanTT)

cal_TX <- calendar %>% select(date, wm_yr_wk)
head(cal_TX)

#TX_2
sell_TX_2 <- sell_prices %>% filter(store_id == "TX_2") %>%  
  filter(grepl('HOUS', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_TX_2)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_TX_sell_TX_2 <- merge(cal_TX, sell_TX_2, by = "wm_yr_wk", all = TRUE) 

cal_TX_sell_TX_2_clean <-  cleanSell(cal_TX_sell_TX_2) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_TX_sell_TX_2_clean[is.na(cal_TX_sell_TX_2_clean)] = 0
cal_TX_sell_TX_2_clean %<>%  as.matrix()

cal_TX_sell_TX_2_clean_train_reg <- cal_TX_sell_TX_2_clean[1:1913, ]
cal_TX_sell_TX_2_clean_forcast_reg <- cal_TX_sell_TX_2_clean[1914:1941,]


library(hts)
cal_TX_sell_TX_2_clean_train_reg_agg <- aggts(
  hts(cal_TX_sell_TX_2_clean_train_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)
cal_TX_sell_TX_2_clean_forcast_reg_agg <- aggts(
  hts(cal_TX_sell_TX_2_clean_forcast_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)

TXDataTX_2HouseholdCleanTT_hts <- hts(TXDataTX_2HouseholdCleanTT, 
                                      characters = c(7,3)) #convert to hts object

h <- 28
TXDataTX_2HouseholdCleanTT_hts_aggts <- aggts(TXDataTX_2HouseholdCleanTT_hts, 
                                              levels = c(0,1,2))
n <- nrow(TXDataTX_2HouseholdCleanTT_hts_aggts)
p <- ncol(TXDataTX_2HouseholdCleanTT_hts_aggts)

TXDataTX_2HouseholdCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
TXDataTX_2HouseholdCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(TXDataTX_2HouseholdCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    TXDataTX_2HouseholdCleanTT_hts_aggts[,i], 
    as.character(
      TXDataTX_2HouseholdCleanTT_hts_aggts[,i][cumsum(TXDataTX_2HouseholdCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  TXDataTX_2HouseholdCleanTT_hts_aggts_fit <- auto.arima( 
    TXDataTX_2HouseholdCleanTT_hts_aggts[l:k,i],
    xreg = cal_TX_sell_TX_2_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  TXDataTX_2HouseholdCleanTT_hts_aggts_forecast[, i] <- forecast(
    TXDataTX_2HouseholdCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_TX_sell_TX_2_clean_forcast_reg_agg[,i]
  )$mean
  #TXDataTX_2HouseholdCleanTT_hts_aggts_resid[l:k, i] <-  
  # TXDataTX_2HouseholdCleanTT_hts_aggts[l:k, i] - fitted(TXDataTX_2HouseholdCleanTT_hts_aggts_fit) 
  
}

TXDataTX_2HouseholdCleanTT_hts_aggts_forecast[!is.finite(TXDataTX_2HouseholdCleanTT_hts_aggts_forecast)] <- 0
TXDataTX_2HouseholdCleanTT_hts_aggts_forecast_ts <- ts(
  TXDataTX_2HouseholdCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

TXDataTX_2HouseholdCleanTT_hts_aggts_forecast_ts_f <- combinef(
  TXDataTX_2HouseholdCleanTT_hts_aggts_forecast_ts,
  get_nodes(TXDataTX_2HouseholdCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(TXDataTX_2HouseholdCleanTT_hts_aggts)
colnames(TXDataTX_2HouseholdCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(TXDataTX_2HouseholdCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/TXDataTX_2HouseholdCleanTT_hts_aggts_forecast_ts_f.csv")  

Sys.time()

########################################################end

TXDataTX_2Household_Depart1 <- TXDataTX_2Household %>% 
  filter(dept_id == "HOUSEHOLD_1") %>% select(-dept_id)
head(TXDataTX_2Household_Depart1[1:3])

TXDataTX_2Household_Depart2 <- TXDataTX_2Household %>% 
  filter(dept_id == "HOUSEHOLD_2") %>% select(-dept_id)
head(TXDataTX_2Household_Depart2[1:3])

#TX_2FOOD#############################
TXDataTX_2Foods <- TXDataTX_2 %>% 
  filter(cat_id == "FOODS") %>% select(-cat_id)
head(TXDataTX_2Foods[1:3])

#TX_2FOODhts#########################

#clean FOOD TX_2
cleanTXFOOD <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

TXDataTX_2FoodClean <- cleanTXFOOD(TXDataTX_2Foods) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(TXDataTX_2FoodClean)

TXDataTX_2FoodCleanT <- t(TXDataTX_2FoodClean[,-1914])
colnames(TXDataTX_2FoodCleanT) <- c(TXDataTX_2FoodClean$groupS)
TXDataTX_2FoodCleanTT <- as_tibble(TXDataTX_2FoodCleanT)

head(TXDataTX_2FoodCleanTT)

cal_TX <- calendar %>% select(date, wm_yr_wk)
head(cal_TX)

#TX_2
sell_TX_2 <- sell_prices %>% filter(store_id == "TX_2") %>%  
  filter(grepl('FOOD', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_TX_2)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_TX_sell_TX_2 <- merge(cal_TX, sell_TX_2, by = "wm_yr_wk", all = TRUE) 

cal_TX_sell_TX_2_clean <-  cleanSell(cal_TX_sell_TX_2) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_TX_sell_TX_2_clean[is.na(cal_TX_sell_TX_2_clean)] = 0
cal_TX_sell_TX_2_clean %<>%  as.matrix()

cal_TX_sell_TX_2_clean_train_reg <- cal_TX_sell_TX_2_clean[1:1913, ]
cal_TX_sell_TX_2_clean_forcast_reg <- cal_TX_sell_TX_2_clean[1914:1941,]


library(hts)
cal_TX_sell_TX_2_clean_train_reg_agg <- aggts(
  hts(cal_TX_sell_TX_2_clean_train_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)
cal_TX_sell_TX_2_clean_forcast_reg_agg <- aggts(
  hts(cal_TX_sell_TX_2_clean_forcast_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)

TXDataTX_2FoodCleanTT_hts <- hts(TXDataTX_2FoodCleanTT, 
                                 characters = c(7,3)) #convert to hts object

h <- 28
TXDataTX_2FoodCleanTT_hts_aggts <- aggts(TXDataTX_2FoodCleanTT_hts, 
                                         levels = c(0,1,2))
n <- nrow(TXDataTX_2FoodCleanTT_hts_aggts)
p <- ncol(TXDataTX_2FoodCleanTT_hts_aggts)

TXDataTX_2FoodCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
TXDataTX_2FoodCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(TXDataTX_2FoodCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    TXDataTX_2FoodCleanTT_hts_aggts[,i], 
    as.character(
      TXDataTX_2FoodCleanTT_hts_aggts[,i][cumsum(TXDataTX_2FoodCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  TXDataTX_2FoodCleanTT_hts_aggts_fit <- auto.arima( 
    TXDataTX_2FoodCleanTT_hts_aggts[l:k,i],
    xreg = cal_TX_sell_TX_2_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  TXDataTX_2FoodCleanTT_hts_aggts_forecast[, i] <- forecast(
    TXDataTX_2FoodCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_TX_sell_TX_2_clean_forcast_reg_agg[,i]
  )$mean
  #TXDataTX_2FoodCleanTT_hts_aggts_resid[l:k, i] <-  
  # TXDataTX_2FoodCleanTT_hts_aggts[l:k, i] - fitted(TXDataTX_2FoodCleanTT_hts_aggts_fit) 
  
}

TXDataTX_2FoodCleanTT_hts_aggts_forecast[!is.finite(TXDataTX_2FoodCleanTT_hts_aggts_forecast)] <- 0
TXDataTX_2FoodCleanTT_hts_aggts_forecast_ts <- ts(
  TXDataTX_2FoodCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

TXDataTX_2FoodCleanTT_hts_aggts_forecast_ts_f <- combinef(
  TXDataTX_2FoodCleanTT_hts_aggts_forecast_ts,
  get_nodes(TXDataTX_2FoodCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(TXDataTX_2FoodCleanTT_hts_aggts)
colnames(TXDataTX_2FoodCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(TXDataTX_2FoodCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/TXDataTX_2FoodCleanTT_hts_aggts_forecast_ts_f.csv") 

Sys.time() 

########################################################end


TXDataTX_2Foods_Depart1 <- TXDataTX_2Foods %>% 
  filter(dept_id == "FOODS_1") %>% select(-dept_id)
head(TXDataTX_2Foods_Depart1[1:3])

TXDataTX_2Foods_Depart2 <- TXDataTX_2Foods %>% 
  filter(dept_id == "FOODS_2") %>% select(-dept_id)
head(TXDataTX_2Foods_Depart2[1:3])

TXDataTX_2Foods_Depart3 <- TXDataTX_2Foods %>% 
  filter(dept_id == "FOODS_3") %>% select(-dept_id)
head(TXDataTX_2Foods_Depart3[1:3])

#TX_3##################################
#Store TX_3
TXDataTX_3 <- TXData %>% filter(store_id == "TX_3") %>% 
  select(-store_id)
head(TXDataTX_3[1:4])

#TX_3HOBBIES###############################
TXDataTX_3Hobbies <- TXDataTX_3 %>% 
  filter(cat_id == "HOBBIES") %>% select(-cat_id)
head(TXDataTX_3Hobbies[1:3])

#TX_3HOBBIEShts####################################

#clean HOBS TX_3
cleanTXHOBS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

TXDataTX_3HobbiesClean <- cleanTXHOBS(TXDataTX_3Hobbies) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(TXDataTX_3HobbiesClean)

TXDataTX_3HobbiesCleanT <- t(TXDataTX_3HobbiesClean[,-1914])
colnames(TXDataTX_3HobbiesCleanT) <- c(TXDataTX_3HobbiesClean$groupS)#as.list
TXDataTX_3HobbiesCleanTT <- as_tibble(TXDataTX_3HobbiesCleanT)

head(TXDataTX_3HobbiesCleanTT)

cal_TX <- calendar %>% select(date, wm_yr_wk)
head(cal_TX)

#TX_3
sell_TX_3 <- sell_prices %>% filter(store_id == "TX_3") %>% 
  filter(grepl('HOBB', item_id)) %>% 
  #slice() %>% 
  select(-store_id)##
head(sell_TX_3)
tail(sell_TX_3)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_TX_sell_TX_3 <- merge(cal_TX, sell_TX_3, by = "wm_yr_wk", all = TRUE) 

cal_TX_sell_TX_3_clean <-  cleanSell(cal_TX_sell_TX_3) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_TX_sell_TX_3_clean[is.na(cal_TX_sell_TX_3_clean)] = 0
cal_TX_sell_TX_3_clean %<>%  as.matrix()

cal_TX_sell_TX_3_clean_train_reg <- cal_TX_sell_TX_3_clean[1:1913, ]
cal_TX_sell_TX_3_clean_forcast_reg <- cal_TX_sell_TX_3_clean[1914:1941,]


library(hts)
cal_TX_sell_TX_3_clean_train_reg_agg <- aggts(
  hts(cal_TX_sell_TX_3_clean_train_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)
cal_TX_sell_TX_3_clean_forcast_reg_agg <- aggts(
  hts(cal_TX_sell_TX_3_clean_forcast_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)

TXDataTX_3HobbiesCleanTT_hts <- hts(TXDataTX_3HobbiesCleanTT, 
                                    characters = c(7,3)) #convert to hts object

h <- 28
TXDataTX_3HobbiesCleanTT_hts_aggts <- aggts(TXDataTX_3HobbiesCleanTT_hts, 
                                            levels = c(0,1,2))
n <- nrow(TXDataTX_3HobbiesCleanTT_hts_aggts)
p <- ncol(TXDataTX_3HobbiesCleanTT_hts_aggts)

TXDataTX_3HobbiesCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
TXDataTX_3HobbiesCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(TXDataTX_3HobbiesCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    TXDataTX_3HobbiesCleanTT_hts_aggts[,i], 
    as.character(
      TXDataTX_3HobbiesCleanTT_hts_aggts[,i][cumsum(TXDataTX_3HobbiesCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  TXDataTX_3HobbiesCleanTT_hts_aggts_fit <- auto.arima( 
    TXDataTX_3HobbiesCleanTT_hts_aggts[l:k,i],
    xreg = cal_TX_sell_TX_3_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  TXDataTX_3HobbiesCleanTT_hts_aggts_forecast[, i] <- forecast(
    TXDataTX_3HobbiesCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_TX_sell_TX_3_clean_forcast_reg_agg[,i]
  )$mean
  #TXDataTX_3HobbiesCleanTT_hts_aggts_resid[l:k, i] <-  
  # TXDataTX_3HobbiesCleanTT_hts_aggts[l:k, i] - fitted(TXDataTX_3HobbiesCleanTT_hts_aggts_fit) 
  
}

TXDataTX_3HobbiesCleanTT_hts_aggts_forecast[!is.finite(TXDataTX_3HobbiesCleanTT_hts_aggts_forecast)] <- 0
TXDataTX_3HobbiesCleanTT_hts_aggts_forecast_ts <- ts(
  TXDataTX_3HobbiesCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

TXDataTX_3HobbiesCleanTT_hts_aggts_forecast_ts_f <- combinef(
  TXDataTX_3HobbiesCleanTT_hts_aggts_forecast_ts,
  get_nodes(TXDataTX_3HobbiesCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(TXDataTX_3HobbiesCleanTT_hts_aggts)
colnames(TXDataTX_3HobbiesCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(TXDataTX_3HobbiesCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/TXDataTX_3HobbiesCleanTT_hts_aggts_forecast_ts_f.csv") 
Sys.time()


###############################################################################fdssssssss


TXDataTX_3Hobbies_Depart1 <- TXDataTX_3Hobbies %>% 
  filter(dept_id == "HOBBIES_1") %>% select(-dept_id)
head(TXDataTX_3Hobbies_Depart1[1:3])

TXDataTX_3Hobbies_Depart2 <- TXDataTX_3Hobbies %>% 
  filter(dept_id == "HOBBIES_2") %>% select(-dept_id)
head(TXDataTX_3Hobbies_Depart2[1:3])

#TX_3HOUSEHOLD####################
TXDataTX_3Household <- TXDataTX_3 %>% 
  filter(cat_id == "HOUSEHOLD") %>% select(-cat_id)
head(TXDataTX_3Household[1:3])

#TX_3HOUSEhts#########################

#clean HOUS TX_3
cleanTXHOUS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

TXDataTX_3HouseholdClean <- cleanTXHOUS(TXDataTX_3Household) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(TXDataTX_3HouseholdClean)

TXDataTX_3HouseholdCleanT <- t(TXDataTX_3HouseholdClean[,-1914])
colnames(TXDataTX_3HouseholdCleanT) <- c(TXDataTX_3HouseholdClean$groupS)
TXDataTX_3HouseholdCleanTT <- as_tibble(TXDataTX_3HouseholdCleanT)

head(TXDataTX_3HouseholdCleanTT)

cal_TX <- calendar %>% select(date, wm_yr_wk)
head(cal_TX)

#TX_3
sell_TX_3 <- sell_prices %>% filter(store_id == "TX_3") %>%  
  filter(grepl('HOUS', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_TX_3)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_TX_sell_TX_3 <- merge(cal_TX, sell_TX_3, by = "wm_yr_wk", all = TRUE) 

cal_TX_sell_TX_3_clean <-  cleanSell(cal_TX_sell_TX_3) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_TX_sell_TX_3_clean[is.na(cal_TX_sell_TX_3_clean)] = 0
cal_TX_sell_TX_3_clean %<>%  as.matrix()

cal_TX_sell_TX_3_clean_train_reg <- cal_TX_sell_TX_3_clean[1:1913, ]
cal_TX_sell_TX_3_clean_forcast_reg <- cal_TX_sell_TX_3_clean[1914:1941,]


library(hts)
cal_TX_sell_TX_3_clean_train_reg_agg <- aggts(
  hts(cal_TX_sell_TX_3_clean_train_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)
cal_TX_sell_TX_3_clean_forcast_reg_agg <- aggts(
  hts(cal_TX_sell_TX_3_clean_forcast_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)

TXDataTX_3HouseholdCleanTT_hts <- hts(TXDataTX_3HouseholdCleanTT, 
                                      characters = c(7,3)) #convert to hts object

h <- 28
TXDataTX_3HouseholdCleanTT_hts_aggts <- aggts(TXDataTX_3HouseholdCleanTT_hts, 
                                              levels = c(0,1,2))
n <- nrow(TXDataTX_3HouseholdCleanTT_hts_aggts)
p <- ncol(TXDataTX_3HouseholdCleanTT_hts_aggts)

TXDataTX_3HouseholdCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
TXDataTX_3HouseholdCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(TXDataTX_3HouseholdCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    TXDataTX_3HouseholdCleanTT_hts_aggts[,i], 
    as.character(
      TXDataTX_3HouseholdCleanTT_hts_aggts[,i][cumsum(TXDataTX_3HouseholdCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  TXDataTX_3HouseholdCleanTT_hts_aggts_fit <- auto.arima( 
    TXDataTX_3HouseholdCleanTT_hts_aggts[l:k,i],
    xreg = cal_TX_sell_TX_3_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  TXDataTX_3HouseholdCleanTT_hts_aggts_forecast[, i] <- forecast(
    TXDataTX_3HouseholdCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_TX_sell_TX_3_clean_forcast_reg_agg[,i]
  )$mean
  #TXDataTX_3HouseholdCleanTT_hts_aggts_resid[l:k, i] <-  
  # TXDataTX_3HouseholdCleanTT_hts_aggts[l:k, i] - fitted(TXDataTX_3HouseholdCleanTT_hts_aggts_fit) 
  
}

TXDataTX_3HouseholdCleanTT_hts_aggts_forecast[!is.finite(TXDataTX_3HouseholdCleanTT_hts_aggts_forecast)] <- 0
TXDataTX_3HouseholdCleanTT_hts_aggts_forecast_ts <- ts(
  TXDataTX_3HouseholdCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

TXDataTX_3HouseholdCleanTT_hts_aggts_forecast_ts_f <- combinef(
  TXDataTX_3HouseholdCleanTT_hts_aggts_forecast_ts,
  get_nodes(TXDataTX_3HouseholdCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(TXDataTX_3HouseholdCleanTT_hts_aggts)
colnames(TXDataTX_3HouseholdCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(TXDataTX_3HouseholdCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/TXDataTX_3HouseholdCleanTT_hts_aggts_forecast_ts_f.csv") 
Sys.time()

########################################################end


TXDataTX_3Household_Depart1 <- TXDataTX_3Household %>% 
  filter(dept_id == "HOUSEHOLD_1") %>% select(-dept_id)
head(TXDataTX_3Household_Depart1[1:3])

TXDataTX_3Household_Depart2 <- TXDataTX_3Household %>% 
  filter(dept_id == "HOUSEHOLD_2") %>% select(-dept_id)
head(TXDataTX_3Household_Depart2[1:3])

#TX_3FOODS#####################################
TXDataTX_3Foods <- TXDataTX_3 %>% 
  filter(cat_id == "FOODS") %>% select(-cat_id)
head(TXDataTX_3Foods[1:3])

#TX_3FOODhts#########################

#clean FOOD TX_3
cleanTXFOOD <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

TXDataTX_3FoodClean <- cleanTXFOOD(TXDataTX_3Foods) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(TXDataTX_3FoodClean)

TXDataTX_3FoodCleanT <- t(TXDataTX_3FoodClean[,-1914])
colnames(TXDataTX_3FoodCleanT) <- c(TXDataTX_3FoodClean$groupS)
TXDataTX_3FoodCleanTT <- as_tibble(TXDataTX_3FoodCleanT)

head(TXDataTX_3FoodCleanTT)

cal_TX <- calendar %>% select(date, wm_yr_wk)
head(cal_TX)

#TX_3
sell_TX_3 <- sell_prices %>% filter(store_id == "TX_3") %>%  
  filter(grepl('FOOD', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_TX_3)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_TX_sell_TX_3 <- merge(cal_TX, sell_TX_3, by = "wm_yr_wk", all = TRUE) 

cal_TX_sell_TX_3_clean <-  cleanSell(cal_TX_sell_TX_3) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_TX_sell_TX_3_clean[is.na(cal_TX_sell_TX_3_clean)] = 0
cal_TX_sell_TX_3_clean %<>%  as.matrix()

cal_TX_sell_TX_3_clean_train_reg <- cal_TX_sell_TX_3_clean[1:1913, ]
cal_TX_sell_TX_3_clean_forcast_reg <- cal_TX_sell_TX_3_clean[1914:1941,]


library(hts)
cal_TX_sell_TX_3_clean_train_reg_agg <- aggts(
  hts(cal_TX_sell_TX_3_clean_train_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)
cal_TX_sell_TX_3_clean_forcast_reg_agg <- aggts(
  hts(cal_TX_sell_TX_3_clean_forcast_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)

TXDataTX_3FoodCleanTT_hts <- hts(TXDataTX_3FoodCleanTT, 
                                 characters = c(7,3)) #convert to hts object

h <- 28
TXDataTX_3FoodCleanTT_hts_aggts <- aggts(TXDataTX_3FoodCleanTT_hts, 
                                         levels = c(0,1,2))
n <- nrow(TXDataTX_3FoodCleanTT_hts_aggts)
p <- ncol(TXDataTX_3FoodCleanTT_hts_aggts)

TXDataTX_3FoodCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
TXDataTX_3FoodCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(TXDataTX_3FoodCleanTT_hts_aggts[,i])
  l <- match(
    grep(
      TXDataTX_3FoodCleanTT_hts_aggts[,i][cumsum(TXDataTX_3FoodCleanTT_hts_aggts[,i]) != 0], 
      TXDataTX_3FoodCleanTT_hts_aggts[,i], value = T),
    TXDataTX_3FoodCleanTT_hts_aggts[,i])[1]
  
  TXDataTX_3FoodCleanTT_hts_aggts_fit <- auto.arima( 
    TXDataTX_3FoodCleanTT_hts_aggts[l:k,i],
    xreg = cal_TX_sell_TX_3_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  TXDataTX_3FoodCleanTT_hts_aggts_forecast[, i] <- forecast(
    TXDataTX_3FoodCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_TX_sell_TX_3_clean_forcast_reg_agg[,i]
  )$mean
  #TXDataTX_3FoodCleanTT_hts_aggts_resid[l:k, i] <-  
  # TXDataTX_3FoodCleanTT_hts_aggts[l:k, i] - fitted(TXDataTX_3FoodCleanTT_hts_aggts_fit) 
  
}

TXDataTX_3FoodCleanTT_hts_aggts_forecast[!is.finite(TXDataTX_3FoodCleanTT_hts_aggts_forecast)] <- 0
TXDataTX_3FoodCleanTT_hts_aggts_forecast_ts <- ts(
  TXDataTX_3FoodCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

TXDataTX_3FoodCleanTT_hts_aggts_forecast_ts_f <- combinef(
  TXDataTX_3FoodCleanTT_hts_aggts_forecast_ts,
  get_nodes(TXDataTX_3FoodCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(TXDataTX_3FoodCleanTT_hts_aggts)
colnames(TXDataTX_3FoodCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(TXDataTX_3FoodCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/TXDataTX_3FoodCleanTT_hts_aggts_forecast_ts_f.csv") 
Sys.time()

########################################################end


TXDataTX_3Foods_Depart1 <- TXDataTX_3Foods %>% 
  filter(dept_id == "FOODS_1") %>% select(-dept_id)
head(TXDataTX_3Foods_Depart1[1:3])

TXDataTX_3Foods_Depart2 <- TXDataTX_3Foods %>% 
  filter(dept_id == "FOODS_2") %>% select(-dept_id)
head(TXDataTX_3Foods_Depart2[1:3])

TXDataTX_3Foods_Depart3 <- TXDataTX_3Foods %>% 
  filter(dept_id == "FOODS_3") %>% select(-dept_id)
head(TXDataTX_3Foods_Depart3[1:3])

#rm(list = ls())
#gc()

#WI----------------------------------------------------------------------

####### take WI state data, group by store_id, group by category for later EDA

WIData <- sales_train_validation %>% filter(state_id == "WI") %>% 
  select(-state_id)
head(WIData[1:5])


#WI_1##################################################
#Store WI_1
WIDataWI_1 <- WIData %>% filter(store_id == "WI_1") %>% 
  select(-store_id)
head(WIDataWI_1[1:4])

#unique(WIDataWI_1$cat_id)

#WI_1HOBBIES####################################
WIDataWI_1Hobbies <- WIDataWI_1 %>% 
  filter(cat_id == "HOBBIES") %>% select(-cat_id)
head(WIDataWI_1Hobbies[1:3])

#WI_1HOBBIEShts###############################################

#clean HOBS WI_1
cleanWIHOBS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

WIDataWI_1HobbiesClean <- cleanWIHOBS(WIDataWI_1Hobbies) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(WIDataWI_1HobbiesClean)

WIDataWI_1HobbiesCleanT <- t(WIDataWI_1HobbiesClean[,-1914])
colnames(WIDataWI_1HobbiesCleanT) <- c(WIDataWI_1HobbiesClean$groupS)#as.list
WIDataWI_1HobbiesCleanTT <- as_tibble(WIDataWI_1HobbiesCleanT)

head(WIDataWI_1HobbiesCleanTT)

cal_WI <- calendar %>% select(date, wm_yr_wk)
head(cal_WI)

#WI_1
sell_WI_1 <- sell_prices %>% filter(store_id == "WI_1") %>%  
  filter(grepl('HOBB', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_WI_1)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_WI_sell_WI_1 <- merge(cal_WI, sell_WI_1, by = "wm_yr_wk", all = TRUE) 

cal_WI_sell_WI_1_clean <-  cleanSell(cal_WI_sell_WI_1) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_WI_sell_WI_1_clean[is.na(cal_WI_sell_WI_1_clean)] = 0
cal_WI_sell_WI_1_clean %<>%  as.matrix()

cal_WI_sell_WI_1_clean_train_reg <- cal_WI_sell_WI_1_clean[1:1913, ]
cal_WI_sell_WI_1_clean_forcast_reg <- cal_WI_sell_WI_1_clean[1914:1941,]


library(hts)
cal_WI_sell_WI_1_clean_train_reg_agg <- aggts(
  hts(cal_WI_sell_WI_1_clean_train_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)
cal_WI_sell_WI_1_clean_forcast_reg_agg <- aggts(
  hts(cal_WI_sell_WI_1_clean_forcast_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)

WIDataWI_1HobbiesCleanTT_hts <- hts(WIDataWI_1HobbiesCleanTT, 
                                    characters = c(7,3)) #convert to hts object

h <- 28
WIDataWI_1HobbiesCleanTT_hts_aggts <- aggts(WIDataWI_1HobbiesCleanTT_hts, 
                                            levels = c(0,1,2))
n <- nrow(WIDataWI_1HobbiesCleanTT_hts_aggts)
p <- ncol(WIDataWI_1HobbiesCleanTT_hts_aggts)

WIDataWI_1HobbiesCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
WIDataWI_1HobbiesCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(WIDataWI_1HobbiesCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    WIDataWI_1HobbiesCleanTT_hts_aggts[,i], 
    as.character(
      WIDataWI_1HobbiesCleanTT_hts_aggts[,i][cumsum(WIDataWI_1HobbiesCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  WIDataWI_1HobbiesCleanTT_hts_aggts_fit <- auto.arima( 
    WIDataWI_1HobbiesCleanTT_hts_aggts[l:k,i],
    xreg = cal_WI_sell_WI_1_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  WIDataWI_1HobbiesCleanTT_hts_aggts_forecast[, i] <- forecast(
    WIDataWI_1HobbiesCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_WI_sell_WI_1_clean_forcast_reg_agg[,i]
  )$mean
  #WIDataWI_1HobbiesCleanTT_hts_aggts_resid[l:k, i] <-  
  # WIDataWI_1HobbiesCleanTT_hts_aggts[l:k, i] - fitted(WIDataWI_1HobbiesCleanTT_hts_aggts_fit) 
  
}

WIDataWI_1HobbiesCleanTT_hts_aggts_forecast[!is.finite(WIDataWI_1HobbiesCleanTT_hts_aggts_forecast)] <- 0
WIDataWI_1HobbiesCleanTT_hts_aggts_forecast_ts <- ts(
  WIDataWI_1HobbiesCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

WIDataWI_1HobbiesCleanTT_hts_aggts_forecast_ts_f <- combinef(
  WIDataWI_1HobbiesCleanTT_hts_aggts_forecast_ts,
  get_nodes(WIDataWI_1HobbiesCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(WIDataWI_1HobbiesCleanTT_hts_aggts)
colnames(WIDataWI_1HobbiesCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(WIDataWI_1HobbiesCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/WIDataWI_1HobbiesCleanTT_hts_aggts_forecast_ts_f.csv") 
Sys.time() 


###############################################################################fdssssssss


WIDataWI_1Hobbies_Depart1 <- WIDataWI_1Hobbies %>% 
  filter(dept_id == "HOBBIES_1") %>% select(-dept_id)
head(WIDataWI_1Hobbies_Depart1[1:3])

WIDataWI_1Hobbies_Depart2 <- WIDataWI_1Hobbies %>% 
  filter(dept_id == "HOBBIES_2") %>% select(-dept_id)
head(WIDataWI_1Hobbies_Depart2[1:3])

#WI_1HOUSE############################
WIDataWI_1Household <- WIDataWI_1 %>% 
  filter(cat_id == "HOUSEHOLD") %>% select(-cat_id)
head(WIDataWI_1Household[1:3])

#WI_1HOUSEhts#####################

#clean HOUS WI_1
cleanWIHOUS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

WIDataWI_1HouseholdClean <- cleanWIHOUS(WIDataWI_1Household) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(WIDataWI_1HouseholdClean)

WIDataWI_1HouseholdCleanT <- t(WIDataWI_1HouseholdClean[,-1914])
colnames(WIDataWI_1HouseholdCleanT) <- c(WIDataWI_1HouseholdClean$groupS)
WIDataWI_1HouseholdCleanTT <- as_tibble(WIDataWI_1HouseholdCleanT)

head(WIDataWI_1HouseholdCleanTT)

cal_WI <- calendar %>% select(date, wm_yr_wk)
head(cal_WI)

#WI_1
sell_WI_1 <- sell_prices %>% filter(store_id == "WI_1") %>%  
  filter(grepl('HOUS', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_WI_1)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_WI_sell_WI_1 <- merge(cal_WI, sell_WI_1, by = "wm_yr_wk", all = TRUE) 

cal_WI_sell_WI_1_clean <-  cleanSell(cal_WI_sell_WI_1) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_WI_sell_WI_1_clean[is.na(cal_WI_sell_WI_1_clean)] = 0
cal_WI_sell_WI_1_clean %<>%  as.matrix()

cal_WI_sell_WI_1_clean_train_reg <- cal_WI_sell_WI_1_clean[1:1913, ]
cal_WI_sell_WI_1_clean_forcast_reg <- cal_WI_sell_WI_1_clean[1914:1941,]


library(hts)
cal_WI_sell_WI_1_clean_train_reg_agg <- aggts(
  hts(cal_WI_sell_WI_1_clean_train_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)
cal_WI_sell_WI_1_clean_forcast_reg_agg <- aggts(
  hts(cal_WI_sell_WI_1_clean_forcast_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)

WIDataWI_1HouseholdCleanTT_hts <- hts(WIDataWI_1HouseholdCleanTT, 
                                      characters = c(7,3)) #convert to hts object

h <- 28
WIDataWI_1HouseholdCleanTT_hts_aggts <- aggts(WIDataWI_1HouseholdCleanTT_hts, 
                                              levels = c(0,1,2))
n <- nrow(WIDataWI_1HouseholdCleanTT_hts_aggts)
p <- ncol(WIDataWI_1HouseholdCleanTT_hts_aggts)

WIDataWI_1HouseholdCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
WIDataWI_1HouseholdCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(WIDataWI_1HouseholdCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    WIDataWI_1HouseholdCleanTT_hts_aggts[,i], 
    as.character(
      WIDataWI_1HouseholdCleanTT_hts_aggts[,i][cumsum(WIDataWI_1HouseholdCleanTT_hts_aggts[,i]) != 0][1]
    ))[1]
  
  WIDataWI_1HouseholdCleanTT_hts_aggts_fit <- auto.arima( 
    WIDataWI_1HouseholdCleanTT_hts_aggts[l:k,i],
    xreg = cal_WI_sell_WI_1_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  WIDataWI_1HouseholdCleanTT_hts_aggts_forecast[, i] <- forecast(
    WIDataWI_1HouseholdCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_WI_sell_WI_1_clean_forcast_reg_agg[,i]
  )$mean
  #WIDataWI_1HouseholdCleanTT_hts_aggts_resid[l:k, i] <-  
  # WIDataWI_1HouseholdCleanTT_hts_aggts[l:k, i] - fitted(WIDataWI_1HouseholdCleanTT_hts_aggts_fit) 
  
}

WIDataWI_1HouseholdCleanTT_hts_aggts_forecast[!is.finite(WIDataWI_1HouseholdCleanTT_hts_aggts_forecast)] <- 0
WIDataWI_1HouseholdCleanTT_hts_aggts_forecast_ts <- ts(
  WIDataWI_1HouseholdCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

WIDataWI_1HouseholdCleanTT_hts_aggts_forecast_ts_f <- combinef(
  WIDataWI_1HouseholdCleanTT_hts_aggts_forecast_ts,
  get_nodes(WIDataWI_1HouseholdCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(WIDataWI_1HouseholdCleanTT_hts_aggts)
colnames(WIDataWI_1HouseholdCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(WIDataWI_1HouseholdCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/WIDataWI_1HouseholdCleanTT_hts_aggts_forecast_ts_f.csv") 
Sys.time()

########################################################end


WIDataWI_1Household_Depart1 <- WIDataWI_1Household %>% 
  filter(dept_id == "HOUSEHOLD_1") %>% select(-dept_id)
head(WIDataWI_1Household_Depart1[1:3])

WIDataWI_1Household_Depart2 <- WIDataWI_1Household %>% 
  filter(dept_id == "HOUSEHOLD_2") %>% select(-dept_id)
head(WIDataWI_1Household_Depart2[1:3])

#WI_1FOOD###########################
WIDataWI_1Foods <- WIDataWI_1 %>% 
  filter(cat_id == "FOODS") %>% select(-cat_id)
head(WIDataWI_1Foods[1:3])

#WI_1FOODhts###########################

#clean FOOD WI_1
cleanWIFOOD <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

WIDataWI_1FoodClean <- cleanWIFOOD(WIDataWI_1Foods) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(WIDataWI_1FoodClean)

WIDataWI_1FoodCleanT <- t(WIDataWI_1FoodClean[,-1914])
colnames(WIDataWI_1FoodCleanT) <- c(WIDataWI_1FoodClean$groupS)
WIDataWI_1FoodCleanTT <- as_tibble(WIDataWI_1FoodCleanT)

head(WIDataWI_1FoodCleanTT)

cal_WI <- calendar %>% select(date, wm_yr_wk)
head(cal_WI)

#WI_1
sell_WI_1 <- sell_prices %>% filter(store_id == "WI_1") %>%  
  filter(grepl('FOO', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_WI_1)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_WI_sell_WI_1 <- merge(cal_WI, sell_WI_1, by = "wm_yr_wk", all = TRUE) 

cal_WI_sell_WI_1_clean <-  cleanSell(cal_WI_sell_WI_1) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_WI_sell_WI_1_clean[is.na(cal_WI_sell_WI_1_clean)] = 0
cal_WI_sell_WI_1_clean %<>%  as.matrix()

cal_WI_sell_WI_1_clean_train_reg <- cal_WI_sell_WI_1_clean[1:1913, ]
cal_WI_sell_WI_1_clean_forcast_reg <- cal_WI_sell_WI_1_clean[1914:1941,]


library(hts)
cal_WI_sell_WI_1_clean_train_reg_agg <- aggts(
  hts(cal_WI_sell_WI_1_clean_train_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)
cal_WI_sell_WI_1_clean_forcast_reg_agg <- aggts(
  hts(cal_WI_sell_WI_1_clean_forcast_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)

WIDataWI_1FoodCleanTT_hts <- hts(WIDataWI_1FoodCleanTT, 
                                 characters = c(7,3)) #convert to hts object

h <- 28
WIDataWI_1FoodCleanTT_hts_aggts <- aggts(WIDataWI_1FoodCleanTT_hts, 
                                         levels = c(0,1,2))
n <- nrow(WIDataWI_1FoodCleanTT_hts_aggts)
p <- ncol(WIDataWI_1FoodCleanTT_hts_aggts)

WIDataWI_1FoodCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
WIDataWI_1FoodCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(WIDataWI_1FoodCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    WIDataWI_1FoodCleanTT_hts_aggts[,i], 
    as.character(
      WIDataWI_1FoodCleanTT_hts_aggts[,i][cumsum(WIDataWI_1FoodCleanTT_hts_aggts[,i]) != 0][1]
    ))[1]
  
  WIDataWI_1FoodCleanTT_hts_aggts_fit <- auto.arima( 
    WIDataWI_1FoodCleanTT_hts_aggts[l:k,i],
    xreg = cal_WI_sell_WI_1_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  WIDataWI_1FoodCleanTT_hts_aggts_forecast[, i] <- forecast(
    WIDataWI_1FoodCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_WI_sell_WI_1_clean_forcast_reg_agg[,i]
  )$mean
  #WIDataWI_1FoodCleanTT_hts_aggts_resid[l:k, i] <-  
  # WIDataWI_1FoodCleanTT_hts_aggts[l:k, i] - fitted(WIDataWI_1FoodCleanTT_hts_aggts_fit) 
  
}

WIDataWI_1FoodCleanTT_hts_aggts_forecast[!is.finite(WIDataWI_1FoodCleanTT_hts_aggts_forecast)] <- 0
WIDataWI_1FoodCleanTT_hts_aggts_forecast_ts <- ts(
  WIDataWI_1FoodCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

WIDataWI_1FoodCleanTT_hts_aggts_forecast_ts_f <- combinef(
  WIDataWI_1FoodCleanTT_hts_aggts_forecast_ts,
  get_nodes(WIDataWI_1FoodCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(WIDataWI_1FoodCleanTT_hts_aggts)
colnames(WIDataWI_1FoodCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(WIDataWI_1FoodCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/WIDataWI_1FoodCleanTT_hts_aggts_forecast_ts_f.csv") 

Sys.time() 

########################################################end


WIDataWI_1Foods_Depart1 <- WIDataWI_1Foods %>% 
  filter(dept_id == "FOODS_1") %>% select(-dept_id)
head(WIDataWI_1Foods_Depart1[1:3])

WIDataWI_1Foods_Depart2 <- WIDataWI_1Foods %>% 
  filter(dept_id == "FOODS_2") %>% select(-dept_id)
head(WIDataWI_1Foods_Depart2[1:3])

WIDataWI_1Foods_Depart3 <- WIDataWI_1Foods %>% 
  filter(dept_id == "FOODS_3") %>% select(-dept_id)
head(WIDataWI_1Foods_Depart3[1:3])


#WI_2################################
#Store WI_2
WIDataWI_2 <- WIData %>% filter(store_id == "WI_2") %>% 
  select(-store_id)
head(WIDataWI_2[1:4])

#WI_2HOBBIES#####################################
WIDataWI_2Hobbies <- WIDataWI_2 %>% 
  filter(cat_id == "HOBBIES") %>% select(-cat_id)
head(WIDataWI_2Hobbies[1:3])

#WI_2HOBBIEShts#########################################################

#clean HOBS WI_2
cleanWIHOBS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

WIDataWI_2HobbiesClean <- cleanWIHOBS(WIDataWI_2Hobbies) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(WIDataWI_2HobbiesClean)

WIDataWI_2HobbiesCleanT <- t(WIDataWI_2HobbiesClean[,-1914])
colnames(WIDataWI_2HobbiesCleanT) <- c(WIDataWI_2HobbiesClean$groupS)#as.list
WIDataWI_2HobbiesCleanTT <- as_tibble(WIDataWI_2HobbiesCleanT)

head(WIDataWI_2HobbiesCleanTT)

cal_WI <- calendar %>% select(date, wm_yr_wk)
head(cal_WI)

#WI_2
sell_WI_2 <- sell_prices %>% filter(store_id == "WI_2") %>%  
  filter(grepl('HOBB', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_WI_2)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_WI_sell_WI_2 <- merge(cal_WI, sell_WI_2, by = "wm_yr_wk", all = TRUE) 

cal_WI_sell_WI_2_clean <-  cleanSell(cal_WI_sell_WI_2) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_WI_sell_WI_2_clean[is.na(cal_WI_sell_WI_2_clean)] = 0
cal_WI_sell_WI_2_clean %<>%  as.matrix()

cal_WI_sell_WI_2_clean_train_reg <- cal_WI_sell_WI_2_clean[1:1913, ]
cal_WI_sell_WI_2_clean_forcast_reg <- cal_WI_sell_WI_2_clean[1914:1941,]


library(hts)
cal_WI_sell_WI_2_clean_train_reg_agg <- aggts(
  hts(cal_WI_sell_WI_2_clean_train_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)
cal_WI_sell_WI_2_clean_forcast_reg_agg <- aggts(
  hts(cal_WI_sell_WI_2_clean_forcast_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)

WIDataWI_2HobbiesCleanTT_hts <- hts(WIDataWI_2HobbiesCleanTT, 
                                    characters = c(7,3)) #convert to hts object

h <- 28
WIDataWI_2HobbiesCleanTT_hts_aggts <- aggts(WIDataWI_2HobbiesCleanTT_hts, 
                                            levels = c(0,1,2))
n <- nrow(WIDataWI_2HobbiesCleanTT_hts_aggts)
p <- ncol(WIDataWI_2HobbiesCleanTT_hts_aggts)

WIDataWI_2HobbiesCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
WIDataWI_2HobbiesCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(WIDataWI_2HobbiesCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    WIDataWI_2HobbiesCleanTT_hts_aggts[,i], 
    as.character(
      WIDataWI_2HobbiesCleanTT_hts_aggts[,i][cumsum(WIDataWI_2HobbiesCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  WIDataWI_2HobbiesCleanTT_hts_aggts_fit <- auto.arima( 
    WIDataWI_2HobbiesCleanTT_hts_aggts[l:k,i],
    xreg = cal_WI_sell_WI_2_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  WIDataWI_2HobbiesCleanTT_hts_aggts_forecast[, i] <- forecast(
    WIDataWI_2HobbiesCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_WI_sell_WI_2_clean_forcast_reg_agg[,i]
  )$mean
  #WIDataWI_2HobbiesCleanTT_hts_aggts_resid[l:k, i] <-  
  # WIDataWI_2HobbiesCleanTT_hts_aggts[l:k, i] - fitted(WIDataWI_2HobbiesCleanTT_hts_aggts_fit) 
  
}

WIDataWI_2HobbiesCleanTT_hts_aggts_forecast[!is.finite(WIDataWI_2HobbiesCleanTT_hts_aggts_forecast)] <- 0
WIDataWI_2HobbiesCleanTT_hts_aggts_forecast_ts <- ts(
  WIDataWI_2HobbiesCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

WIDataWI_2HobbiesCleanTT_hts_aggts_forecast_ts_f <- combinef(
  WIDataWI_2HobbiesCleanTT_hts_aggts_forecast_ts,
  get_nodes(WIDataWI_2HobbiesCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(WIDataWI_2HobbiesCleanTT_hts_aggts)
colnames(WIDataWI_2HobbiesCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(WIDataWI_2HobbiesCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/WIDataWI_2HobbiesCleanTT_hts_aggts_forecast_ts_f.csv")  
Sys.time() 


###############################################################################fdssssssss


WIDataWI_2Hobbies_Depart1 <- WIDataWI_2Hobbies %>% 
  filter(dept_id == "HOBBIES_1") %>% select(-dept_id)
head(WIDataWI_2Hobbies_Depart1[1:3])

WIDataWI_2Hobbies_Depart2 <- WIDataWI_2Hobbies %>% 
  filter(dept_id == "HOBBIES_2") %>% select(-dept_id)
head(WIDataWI_2Hobbies_Depart2[1:3])

#WI_2HOUSE###########################

WIDataWI_2Household <- WIDataWI_2 %>% 
  filter(cat_id == "HOUSEHOLD") %>% select(-cat_id)
head(WIDataWI_2Household[1:3])

#WI_2HOUSEhts###########################

#clean HOUS WI_2
cleanWIHOUS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

WIDataWI_2HouseholdClean <- cleanWIHOUS(WIDataWI_2Household) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(WIDataWI_2HouseholdClean)

WIDataWI_2HouseholdCleanT <- t(WIDataWI_2HouseholdClean[,-1914])
colnames(WIDataWI_2HouseholdCleanT) <- c(WIDataWI_2HouseholdClean$groupS)
WIDataWI_2HouseholdCleanTT <- as_tibble(WIDataWI_2HouseholdCleanT)

head(WIDataWI_2HouseholdCleanTT)

cal_WI <- calendar %>% select(date, wm_yr_wk)
head(cal_WI)

#WI_2
sell_WI_2 <- sell_prices %>% filter(store_id == "WI_2") %>%  
  filter(grepl('HOUS', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_WI_2)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_WI_sell_WI_2 <- merge(cal_WI, sell_WI_2, by = "wm_yr_wk", all = TRUE) 

cal_WI_sell_WI_2_clean <-  cleanSell(cal_WI_sell_WI_2) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_WI_sell_WI_2_clean[is.na(cal_WI_sell_WI_2_clean)] = 0
cal_WI_sell_WI_2_clean %<>%  as.matrix()

cal_WI_sell_WI_2_clean_train_reg <- cal_WI_sell_WI_2_clean[1:1913, ]
cal_WI_sell_WI_2_clean_forcast_reg <- cal_WI_sell_WI_2_clean[1914:1941,]


library(hts)
cal_WI_sell_WI_2_clean_train_reg_agg <- aggts(
  hts(cal_WI_sell_WI_2_clean_train_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)
cal_WI_sell_WI_2_clean_forcast_reg_agg <- aggts(
  hts(cal_WI_sell_WI_2_clean_forcast_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)

WIDataWI_2HouseholdCleanTT_hts <- hts(WIDataWI_2HouseholdCleanTT, 
                                      characters = c(7,3)) #convert to hts object

h <- 28
WIDataWI_2HouseholdCleanTT_hts_aggts <- aggts(WIDataWI_2HouseholdCleanTT_hts, 
                                              levels = c(0,1,2))
n <- nrow(WIDataWI_2HouseholdCleanTT_hts_aggts)
p <- ncol(WIDataWI_2HouseholdCleanTT_hts_aggts)

WIDataWI_2HouseholdCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
WIDataWI_2HouseholdCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(WIDataWI_2HouseholdCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    WIDataWI_2HouseholdCleanTT_hts_aggts[,i], 
    as.character(
      WIDataWI_2HouseholdCleanTT_hts_aggts[,i][cumsum(WIDataWI_2HouseholdCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  WIDataWI_2HouseholdCleanTT_hts_aggts_fit <- auto.arima( 
    WIDataWI_2HouseholdCleanTT_hts_aggts[l:k,i],
    xreg = cal_WI_sell_WI_2_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  WIDataWI_2HouseholdCleanTT_hts_aggts_forecast[, i] <- forecast(
    WIDataWI_2HouseholdCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_WI_sell_WI_2_clean_forcast_reg_agg[,i]
  )$mean
  #WIDataWI_2HouseholdCleanTT_hts_aggts_resid[l:k, i] <-  
  # WIDataWI_2HouseholdCleanTT_hts_aggts[l:k, i] - fitted(WIDataWI_2HouseholdCleanTT_hts_aggts_fit) 
  
}

WIDataWI_2HouseholdCleanTT_hts_aggts_forecast[!is.finite(WIDataWI_2HouseholdCleanTT_hts_aggts_forecast)] <- 0
WIDataWI_2HouseholdCleanTT_hts_aggts_forecast_ts <- ts(
  WIDataWI_2HouseholdCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

WIDataWI_2HouseholdCleanTT_hts_aggts_forecast_ts_f <- combinef(
  WIDataWI_2HouseholdCleanTT_hts_aggts_forecast_ts,
  get_nodes(WIDataWI_2HouseholdCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(WIDataWI_2HouseholdCleanTT_hts_aggts)
colnames(WIDataWI_2HouseholdCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(WIDataWI_2HouseholdCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/WIDataWI_2HouseholdCleanTT_hts_aggts_forecast_ts_f.csv")  
Sys.time() 

########################################################end


WIDataWI_2Household_Depart1 <- WIDataWI_2Household %>% 
  filter(dept_id == "HOUSEHOLD_1") %>% select(-dept_id)
head(WIDataWI_2Household_Depart1[1:3])

WIDataWI_2Household_Depart2 <- WIDataWI_2Household %>% 
  filter(dept_id == "HOUSEHOLD_2") %>% select(-dept_id)
head(WIDataWI_2Household_Depart2[1:3])

#WI_2FOOD##################################

WIDataWI_2Foods <- WIDataWI_2 %>% 
  filter(cat_id == "FOODS") %>% select(-cat_id)
head(WIDataWI_2Foods[1:3])

#WI_2FOODhts###########################

#clean FOOD WI_2
cleanWIFOOD <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

WIDataWI_2FoodClean <- cleanWIFOOD(WIDataWI_2Foods) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(WIDataWI_2FoodClean)

WIDataWI_2FoodCleanT <- t(WIDataWI_2FoodClean[,-1914])
colnames(WIDataWI_2FoodCleanT) <- c(WIDataWI_2FoodClean$groupS)
WIDataWI_2FoodCleanTT <- as_tibble(WIDataWI_2FoodCleanT)

head(WIDataWI_2FoodCleanTT)

cal_WI <- calendar %>% select(date, wm_yr_wk)
head(cal_WI)

#WI_2
sell_WI_2 <- sell_prices %>% filter(store_id == "WI_2") %>%  
  filter(grepl('FOOD', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_WI_2)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_WI_sell_WI_2 <- merge(cal_WI, sell_WI_2, by = "wm_yr_wk", all = TRUE) 

cal_WI_sell_WI_2_clean <-  cleanSell(cal_WI_sell_WI_2) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_WI_sell_WI_2_clean[is.na(cal_WI_sell_WI_2_clean)] = 0
cal_WI_sell_WI_2_clean %<>%  as.matrix()

cal_WI_sell_WI_2_clean_train_reg <- cal_WI_sell_WI_2_clean[1:1913, ]
cal_WI_sell_WI_2_clean_forcast_reg <- cal_WI_sell_WI_2_clean[1914:1941,]


library(hts)
cal_WI_sell_WI_2_clean_train_reg_agg <- aggts(
  hts(cal_WI_sell_WI_2_clean_train_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)
cal_WI_sell_WI_2_clean_forcast_reg_agg <- aggts(
  hts(cal_WI_sell_WI_2_clean_forcast_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)

WIDataWI_2FoodCleanTT_hts <- hts(WIDataWI_2FoodCleanTT, 
                                 characters = c(7,3)) #convert to hts object

h <- 28
WIDataWI_2FoodCleanTT_hts_aggts <- aggts(WIDataWI_2FoodCleanTT_hts, 
                                         levels = c(0,1,2))
n <- nrow(WIDataWI_2FoodCleanTT_hts_aggts)
p <- ncol(WIDataWI_2FoodCleanTT_hts_aggts)

WIDataWI_2FoodCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
WIDataWI_2FoodCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(WIDataWI_2FoodCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    WIDataWI_2FoodCleanTT_hts_aggts[,i], 
    as.character(
      WIDataWI_2FoodCleanTT_hts_aggts[,i][cumsum(WIDataWI_2FoodCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  WIDataWI_2FoodCleanTT_hts_aggts_fit <- auto.arima( 
    WIDataWI_2FoodCleanTT_hts_aggts[l:k,i],
    xreg = cal_WI_sell_WI_2_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  WIDataWI_2FoodCleanTT_hts_aggts_forecast[, i] <- forecast(
    WIDataWI_2FoodCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_WI_sell_WI_2_clean_forcast_reg_agg[,i]
  )$mean
  #WIDataWI_2FoodCleanTT_hts_aggts_resid[l:k, i] <-  
  # WIDataWI_2FoodCleanTT_hts_aggts[l:k, i] - fitted(WIDataWI_2FoodCleanTT_hts_aggts_fit) 
  
}

WIDataWI_2FoodCleanTT_hts_aggts_forecast[!is.finite(WIDataWI_2FoodCleanTT_hts_aggts_forecast)] <- 0
WIDataWI_2FoodCleanTT_hts_aggts_forecast_ts <- ts(
  WIDataWI_2FoodCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

WIDataWI_2FoodCleanTT_hts_aggts_forecast_ts_f <- combinef(
  WIDataWI_2FoodCleanTT_hts_aggts_forecast_ts,
  get_nodes(WIDataWI_2FoodCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(WIDataWI_2FoodCleanTT_hts_aggts)
colnames(WIDataWI_2FoodCleanTT_hts_aggts_forecast_ts_f) <- stringr::str_c(dtnames, dtlevel, sep = "_")

write_csv(as.data.frame(WIDataWI_2FoodCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/WIDataWI_2FoodCleanTT_hts_aggts_forecast_ts_f.csv")  

Sys.time() 

########################################################end

WIDataWI_2Foods_Depart1 <- WIDataWI_2Foods %>% 
  filter(dept_id == "FOODS_1") %>% select(-dept_id)
head(WIDataWI_2Foods_Depart1[1:3])

WIDataWI_2Foods_Depart2 <- WIDataWI_2Foods %>% 
  filter(dept_id == "FOODS_2") %>% select(-dept_id)
head(WIDataWI_2Foods_Depart2[1:3])

WIDataWI_2Foods_Depart3 <- WIDataWI_2Foods %>% 
  filter(dept_id == "FOODS_3") %>% select(-dept_id)
head(WIDataWI_2Foods_Depart3[1:3])


#WI_3#########################################
#Store WI_3
WIDataWI_3 <- WIData %>% filter(store_id == "WI_3") %>% 
  select(-store_id)
head(WIDataWI_3[1:4])

#unique(WIDataWI_2$cat_id)

#WI_3HOBBIES##################################
WIDataWI_3Hobbies <- WIDataWI_3 %>% 
  filter(cat_id == "HOBBIES") %>% select(-cat_id)
head(WIDataWI_3Hobbies[1:3])

#WI_3HOBBIEShts############################################

#clean HOBS WI_3
cleanWIHOBS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

WIDataWI_3HobbiesClean <- cleanWIHOBS(WIDataWI_3Hobbies) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(WIDataWI_3HobbiesClean)

WIDataWI_3HobbiesCleanT <- t(WIDataWI_3HobbiesClean[,-1914])
colnames(WIDataWI_3HobbiesCleanT) <- c(WIDataWI_3HobbiesClean$groupS)#as.list
WIDataWI_3HobbiesCleanTT <- as_tibble(WIDataWI_3HobbiesCleanT)

head(WIDataWI_3HobbiesCleanTT)

cal_WI <- calendar %>% select(date, wm_yr_wk)
head(cal_WI)

#WI_3
sell_WI_3 <- sell_prices %>% filter(store_id == "WI_3") %>% 
  filter(grepl('HOBB', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_WI_3)
tail(sell_WI_3)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_WI_sell_WI_3 <- merge(cal_WI, sell_WI_3, by = "wm_yr_wk", all = TRUE) 

cal_WI_sell_WI_3_clean <-  cleanSell(cal_WI_sell_WI_3) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_WI_sell_WI_3_clean[is.na(cal_WI_sell_WI_3_clean)] = 0
cal_WI_sell_WI_3_clean %<>%  as.matrix()

cal_WI_sell_WI_3_clean_train_reg <- cal_WI_sell_WI_3_clean[1:1913, ]
cal_WI_sell_WI_3_clean_forcast_reg <- cal_WI_sell_WI_3_clean[1914:1941,]


library(hts)
cal_WI_sell_WI_3_clean_train_reg_agg <- aggts(
  hts(cal_WI_sell_WI_3_clean_train_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)
cal_WI_sell_WI_3_clean_forcast_reg_agg <- aggts(
  hts(cal_WI_sell_WI_3_clean_forcast_reg, 
      characters = c(10,3)),levels = c(0,1,2)
)

WIDataWI_3HobbiesCleanTT_hts <- hts(WIDataWI_3HobbiesCleanTT, 
                                    characters = c(7,3)) #convert to hts object

h <- 28
WIDataWI_3HobbiesCleanTT_hts_aggts <- aggts(WIDataWI_3HobbiesCleanTT_hts, 
                                            levels = c(0,1,2))
n <- nrow(WIDataWI_3HobbiesCleanTT_hts_aggts)
p <- ncol(WIDataWI_3HobbiesCleanTT_hts_aggts)

WIDataWI_3HobbiesCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
WIDataWI_3HobbiesCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(WIDataWI_3HobbiesCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    WIDataWI_3HobbiesCleanTT_hts_aggts[,i], 
    as.character(
      WIDataWI_3HobbiesCleanTT_hts_aggts[,i][cumsum(WIDataWI_3HobbiesCleanTT_hts_aggts[,i]) != 0][1]
    ))[1]
  
  WIDataWI_3HobbiesCleanTT_hts_aggts_fit <- auto.arima( 
    WIDataWI_3HobbiesCleanTT_hts_aggts[l:k,i],
    xreg = cal_WI_sell_WI_3_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  WIDataWI_3HobbiesCleanTT_hts_aggts_forecast[, i] <- forecast(
    WIDataWI_3HobbiesCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_WI_sell_WI_3_clean_forcast_reg_agg[,i]
  )$mean
  #WIDataWI_3HobbiesCleanTT_hts_aggts_resid[l:k, i] <-  
  # WIDataWI_3HobbiesCleanTT_hts_aggts[l:k, i] - fitted(WIDataWI_3HobbiesCleanTT_hts_aggts_fit) 
  
}

WIDataWI_3HobbiesCleanTT_hts_aggts_forecast[!is.finite(WIDataWI_3HobbiesCleanTT_hts_aggts_forecast)] <- 0
WIDataWI_3HobbiesCleanTT_hts_aggts_forecast_ts <- ts(
  WIDataWI_3HobbiesCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

WIDataWI_3HobbiesCleanTT_hts_aggts_forecast_ts_f <- combinef(
  WIDataWI_3HobbiesCleanTT_hts_aggts_forecast_ts,
  get_nodes(WIDataWI_3HobbiesCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(WIDataWI_3HobbiesCleanTT_hts_aggts)
colnames(WIDataWI_3HobbiesCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(WIDataWI_3HobbiesCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/WIDataWI_3HobbiesCleanTT_hts_aggts_forecast_ts_f.csv")  

Sys.time()

####################################################hj

WIDataWI_3Hobbies_Depart1 <- WIDataWI_3Hobbies %>% 
  filter(dept_id == "HOBBIES_1") %>% select(-dept_id)
head(WIDataWI_3Hobbies_Depart1[1:3])

WIDataWI_3Hobbies_Depart2 <- WIDataWI_3Hobbies %>% 
  filter(dept_id == "HOBBIES_2") %>% select(-dept_id)
head(WIDataWI_3Hobbies_Depart2[1:3])

#WI_3HOUSE###########################

WIDataWI_3Household <- WIDataWI_3 %>% 
  filter(cat_id == "HOUSEHOLD") %>% select(-cat_id)
head(WIDataWI_3Household[1:3])

#WI_3HOUSEhts###########################

#clean HOUS WI_3
cleanWIHOUS <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

WIDataWI_3HouseholdClean <- cleanWIHOUS(WIDataWI_3Household) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(WIDataWI_3HouseholdClean)

WIDataWI_3HouseholdCleanT <- t(WIDataWI_3HouseholdClean[,-1914])
colnames(WIDataWI_3HouseholdCleanT) <- c(WIDataWI_3HouseholdClean$groupS)
WIDataWI_3HouseholdCleanTT <- as_tibble(WIDataWI_3HouseholdCleanT)

head(WIDataWI_3HouseholdCleanTT)

cal_WI <- calendar %>% select(date, wm_yr_wk)
head(cal_WI)

#WI_3
sell_WI_3 <- sell_prices %>% filter(store_id == "WI_3") %>%  
  filter(grepl('HOUS', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_WI_3)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_WI_sell_WI_3 <- merge(cal_WI, sell_WI_3, by = "wm_yr_wk", all = TRUE) 

cal_WI_sell_WI_3_clean <-  cleanSell(cal_WI_sell_WI_3) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_WI_sell_WI_3_clean[is.na(cal_WI_sell_WI_3_clean)] = 0
cal_WI_sell_WI_3_clean %<>%  as.matrix()

cal_WI_sell_WI_3_clean_train_reg <- cal_WI_sell_WI_3_clean[1:1913, ]
cal_WI_sell_WI_3_clean_forcast_reg <- cal_WI_sell_WI_3_clean[1914:1941,]


library(hts)
cal_WI_sell_WI_3_clean_train_reg_agg <- aggts(
  hts(cal_WI_sell_WI_3_clean_train_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)
cal_WI_sell_WI_3_clean_forcast_reg_agg <- aggts(
  hts(cal_WI_sell_WI_3_clean_forcast_reg, 
      characters = c(12,3)),levels = c(0,1,2)
)

WIDataWI_3HouseholdCleanTT_hts <- hts(WIDataWI_3HouseholdCleanTT, 
                                      characters = c(7,3)) #convert to hts object

h <- 28
WIDataWI_3HouseholdCleanTT_hts_aggts <- aggts(WIDataWI_3HouseholdCleanTT_hts, 
                                              levels = c(0,1,2))
n <- nrow(WIDataWI_3HouseholdCleanTT_hts_aggts)
p <- ncol(WIDataWI_3HouseholdCleanTT_hts_aggts)

WIDataWI_3HouseholdCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
WIDataWI_3HouseholdCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(WIDataWI_3HouseholdCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    WIDataWI_3HouseholdCleanTT_hts_aggts[,i], 
    as.character(
      WIDataWI_3HouseholdCleanTT_hts_aggts[,i][cumsum(WIDataWI_3HouseholdCleanTT_hts_aggts[,i]) != 0][1]
    ))[1] 
  
  WIDataWI_3HouseholdCleanTT_hts_aggts_fit <- auto.arima( 
    WIDataWI_3HouseholdCleanTT_hts_aggts[l:k,i],
    xreg = cal_WI_sell_WI_3_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  WIDataWI_3HouseholdCleanTT_hts_aggts_forecast[, i] <- forecast(
    WIDataWI_3HouseholdCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_WI_sell_WI_3_clean_forcast_reg_agg[,i]
  )$mean
  #WIDataWI_3HouseholdCleanTT_hts_aggts_resid[l:k, i] <-  
  # WIDataWI_3HouseholdCleanTT_hts_aggts[l:k, i] - fitted(WIDataWI_3HouseholdCleanTT_hts_aggts_fit) 
  
}

WIDataWI_3HouseholdCleanTT_hts_aggts_forecast[!is.finite(WIDataWI_3HouseholdCleanTT_hts_aggts_forecast)] <- 0
WIDataWI_3HouseholdCleanTT_hts_aggts_forecast_ts <- ts(
  WIDataWI_3HouseholdCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

WIDataWI_3HouseholdCleanTT_hts_aggts_forecast_ts_f <- combinef(
  WIDataWI_3HouseholdCleanTT_hts_aggts_forecast_ts,
  get_nodes(WIDataWI_3HouseholdCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(WIDataWI_3HouseholdCleanTT_hts_aggts)
colnames(WIDataWI_3HouseholdCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(WIDataWI_3HouseholdCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/WIDataWI_3HouseholdCleanTT_hts_aggts_forecast_ts_f.csv")  

Sys.time() 

########################################################end


WIDataWI_3Household_Depart1 <- WIDataWI_3Household %>% 
  filter(dept_id == "HOUSEHOLD_1") %>% select(-dept_id)
head(WIDataWI_3Household_Depart1[1:3])

WIDataWI_3Household_Depart2 <- WIDataWI_3Household %>% 
  filter(dept_id == "HOUSEHOLD_2") %>% select(-dept_id)
head(WIDataWI_3Household_Depart2[1:3])

#WI_3FOODS#########################

WIDataWI_3Foods <- WIDataWI_3 %>% 
  filter(cat_id == "FOODS") %>% select(-cat_id)
head(WIDataWI_3Foods[1:3])

#WI_3FOODShts#########################

#clean FOOD WI_3
cleanWIFOOD <- function(df){ #create groups
  subDept <- function( df ){
    for (i in 1:length(df$dept_id)){
      df$dept_id[i] <- str_c(str_sub(df$dept_id[i], 1,4), str_sub(df$dept_id[i], -2,-1), sep = "")
    }
    df
  }
  
  df %<>% subDept()
  
  subItem <- function( df ){
    for (i in 1:length(df$item_id)){
      df$item_id[i] <- str_c(df$dept_id[i], str_sub(df$item_id[i], -3,-1), sep = "_")
    }
    df
  } 
  
  df %<>% subItem()
  df
}

WIDataWI_3FoodClean <- cleanWIFOOD(WIDataWI_3Foods) %>% 
  mutate( groupS = item_id ) %>% select(-dept_id, -item_id, -id)

dim(WIDataWI_3FoodClean)

WIDataWI_3FoodCleanT <- t(WIDataWI_3FoodClean[,-1914])
colnames(WIDataWI_3FoodCleanT) <- c(WIDataWI_3FoodClean$groupS)
WIDataWI_3FoodCleanTT <- as_tibble(WIDataWI_3FoodCleanT)

head(WIDataWI_3FoodCleanTT)

cal_WI <- calendar %>% select(date, wm_yr_wk)
head(cal_WI)

#WI_3
sell_WI_3 <- sell_prices %>% filter(store_id == "WI_3") %>% 
  filter(grepl('FOOD', item_id)) %>% 
  #slice() %>% 
  select(-store_id)
head(sell_WI_3)

cleanSell <- function(k){
  k$itN <- as.numeric(str_sub(k$item_id,-3,-1))
  k$stID <- as.numeric(str_sub(k$item_id, -5,-5))
  k$Ca <- str_sub(k$item_id, 1,3)
  k$Cal <- str_sub(k$item_id, 1,1)
  k <- k[order(-xtfrm(k$Cal) ,xtfrm(k$Ca), k$stID, k$itN), ]
  k <- k[,!(names(k) %in% c("itN","stID","Ca","Cal")) ]
}

cal_WI_sell_WI_3 <- merge(cal_WI, sell_WI_3, by = "wm_yr_wk", all = TRUE) 

cal_WI_sell_WI_3_clean <-  cleanSell(cal_WI_sell_WI_3) %>% select(-wm_yr_wk) %>% 
  pivot_wider(names_from =  item_id, values_from = sell_price) %>% 
  arrange(date) %>% select(-date)

cal_WI_sell_WI_3_clean[is.na(cal_WI_sell_WI_3_clean)] = 0
cal_WI_sell_WI_3_clean %<>%  as.matrix()

cal_WI_sell_WI_3_clean_train_reg <- cal_WI_sell_WI_3_clean[1:1913, ]
cal_WI_sell_WI_3_clean_forcast_reg <- cal_WI_sell_WI_3_clean[1914:1941,]


library(hts)
cal_WI_sell_WI_3_clean_train_reg_agg <- aggts(
  hts(cal_WI_sell_WI_3_clean_train_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)
cal_WI_sell_WI_3_clean_forcast_reg_agg <- aggts(
  hts(cal_WI_sell_WI_3_clean_forcast_reg, 
      characters = c(8,3)),levels = c(0,1,2)
)

WIDataWI_3FoodCleanTT_hts <- hts(WIDataWI_3FoodCleanTT, 
                                 characters = c(7,3)) #convert to hts object

h <- 28
WIDataWI_3FoodCleanTT_hts_aggts <- aggts(WIDataWI_3FoodCleanTT_hts, 
                                         levels = c(0,1,2))
n <- nrow(WIDataWI_3FoodCleanTT_hts_aggts)
p <- ncol(WIDataWI_3FoodCleanTT_hts_aggts)

WIDataWI_3FoodCleanTT_hts_aggts_forecast <- matrix(NA, nrow = h, ncol = p)
WIDataWI_3FoodCleanTT_hts_aggts_resid <- matrix(NA, nrow = n, ncol = p)

Sys.time()
for(i in 1:p)
{ 
  k <- length(WIDataWI_3FoodCleanTT_hts_aggts[,i])
  l <- stringr::str_which(
    WIDataWI_3FoodCleanTT_hts_aggts[,i], 
    as.character(
      WIDataWI_3FoodCleanTT_hts_aggts[,i][cumsum(WIDataWI_3FoodCleanTT_hts_aggts[,i]) != 0][1]
    ))[1]
  
  WIDataWI_3FoodCleanTT_hts_aggts_fit <- auto.arima( 
    WIDataWI_3FoodCleanTT_hts_aggts[l:k,i],
    xreg = cal_WI_sell_WI_3_clean_train_reg_agg[l:k,i],
    nmodels = 200, stationary = T,
    max.p = 15, max.d = 5,
    max.q = 15, max.D = 5,
    max.P = 12, allowmean = T,
    max.Q = 12, allowdrift = T,
    max.order = 35,
    lambda = "auto", 
    biasadj = T)
  WIDataWI_3FoodCleanTT_hts_aggts_forecast[, i] <- forecast(
    WIDataWI_3FoodCleanTT_hts_aggts_fit, 
    h = h, 
    xreg = cal_WI_sell_WI_3_clean_forcast_reg_agg[,i]
  )$mean
  #WIDataWI_3FoodCleanTT_hts_aggts_resid[l:k, i] <-  
  # WIDataWI_3FoodCleanTT_hts_aggts[l:k, i] - fitted(WIDataWI_3FoodCleanTT_hts_aggts_fit) 
  
}

WIDataWI_3FoodCleanTT_hts_aggts_forecast[!is.finite(WIDataWI_3FoodCleanTT_hts_aggts_forecast)] <- 0
WIDataWI_3FoodCleanTT_hts_aggts_forecast_ts <- ts(
  WIDataWI_3FoodCleanTT_hts_aggts_forecast, start = 1, frequency = 1)

WIDataWI_3FoodCleanTT_hts_aggts_forecast_ts_f <- combinef(
  WIDataWI_3FoodCleanTT_hts_aggts_forecast_ts,
  get_nodes(WIDataWI_3FoodCleanTT_hts), 
  keep ="bottom", algorithms = "lu",
  parallel = T,control.nn = list(ptype = "random"),
  nonnegative = T)

dtnames <- colnames(WIDataWI_3FoodCleanTT_hts_aggts)
colnames(WIDataWI_3FoodCleanTT_hts_aggts_forecast_ts_f) <- dtnames

write_csv(as.data.frame(WIDataWI_3FoodCleanTT_hts_aggts_forecast_ts_f), 
          "m5A/WIDataWI_3FoodCleanTT_hts_aggts_forecast_ts_f.csv")  
Sys.time() 

########################################################end


WIDataWI_3Foods_Depart1 <- WIDataWI_3Foods %>% 
  filter(dept_id == "FOODS_1") %>% select(-dept_id)
head(WIDataWI_3Foods_Depart1[1:3])

WIDataWI_3Foods_Depart2 <- WIDataWI_3Foods %>% 
  filter(dept_id == "FOODS_2") %>% select(-dept_id)
head(WIDataWI_3Foods_Depart2[1:3])

WIDataWI_3Foods_Depart3 <- WIDataWI_3Foods %>% 
  filter(dept_id == "FOODS_3") %>% select(-dept_id)
head(WIDataWI_3Foods_Depart3[1:3])



# close -------------------------------------------------------------------

stopCluster(cl)

