library(data.table)
#library(quantmod)
library(jsonlite)
library(RODBC)
library(grid)
library(reshape)
library(ggplot2)
library(sde)
library(zoo)
library(plyr)

### Globals ###
locationID <- 30000142 #JITA=30000142
date_range <- 100
typeid <- 29668

patch_list <- as.POSIXlt(c("2015-04-28 13:00","2015-05-05 13:00","2015-06-02 13:00"))

plot.width <- 1600
plot.height <- 900

plot.path <- "C:/Users/Lockefox/Plots/"
title_date <- Sys.Date()
title_head <- "Buy/Sell Prices - predictions"
groupName ="PLEX"
fileTitle <- "prediction"

location_name <- ""

### FETCH DB STUFF ###
SDE <- odbcConnect('sde_lookup')
EC  <- odbcConnect('remote_ec')

sde_q <- paste0("SELECT typeID,typeName FROM invtypes WHERE typeid=",typeid," ")
sde_lookup <- sqlQuery(SDE,sde_q)

ec_q <- paste0("SELECT price_date as `date`, price_time as `hour`, ",
               "SUM(IF(locationid = ",locationID,", IF(buy_sell=1, price_best,0),0)) AS 'SellOrder', ",
               "SUM(IF(locationid = ",locationID,", IF(buy_sell=0, price_best,0),0)) AS 'BuyOrder', typeid ",
               "FROM snapshot_evecentral ",
               "WHERE typeid=",typeid," ",
               "AND locationid=",locationID," ",
               "AND price_date > (SELECT MAX(price_date) FROM snapshot_evecentral) - INTERVAL ",date_range," DAY ",
               "GROUP BY price_date, price_time, typeid")

ec_data <- sqlQuery(EC,ec_q)
ec_data <- merge(ec_data, sde_lookup, by.x="typeid", by.y="typeID") #MERGE typeid/name locally

loc_q <- paste0("SELECT solarSystemName, regionID FROM mapsolarsystems WHERE solarSystemID=",locationID)
loc <- sqlQuery(SDE,loc_q)
location_name <- as.character(loc$solarSystemName[1])
regionid <- as.numeric(loc$regionID)

### FETCH CREST INFO FOR STATS ###
query_addr <- paste0("http://public-crest.eveonline.com/market/",regionid,"/types/",typeid,"/history/")
market.json <- fromJSON(readLines(query_addr))
CREST_data <- market.json$items
CREST_data$date <- as.Date(CREST_data$date)

### CALCULATE VARIANCE ###

filename <- paste0(fileTitle,"_",groupName,"_",location_name,"_",Sys.Date(),".png")
charthead <- paste0("Buy/Sell Margins - ",groupName," - ",location_name," - ",date_range,"days - ",Sys.Date())
