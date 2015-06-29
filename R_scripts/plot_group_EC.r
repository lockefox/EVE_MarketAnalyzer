library(data.table)
#library(quantmod)
#library(jsonlite)
library(RODBC)
library(grid)
library(reshape)
library(ggplot2)

### Globals ###
locationID <- 30000142 #JITA=30000142
date_range <- 60

patch_list <- as.POSIXlt(c("2015-04-28 13:00","2015-05-05 13:00","2015-06-02 13:00"))

plot.width <- 1600
plot.height <- 900

plot.path <- "C:/Users/Lockefox/Plots/"
title_date <- Sys.Date()
title_head <- "Buy/Sell Prices - Small Pulse Lasers"
groupName ="PulseLaser"
location_name <- ""
### FETCH DB STUFF ###
SDE <- odbcConnect('sde_lookup')
EC  <- odbcConnect('remote_ec')

#typeid_list <- c(450,451,452,453,454,2993,3001,3017,3033,3041,5175,5177,5179,5181,5215,5217,5219,5221,6631,6633,6635,6637,6671,6673,6675,6677,6715,6717,6719,6721)
#typeid_list <- c(450,451,453,3001,3017,3041,5175,5177,5179,5181,5215,5217,5219,5221,6671,6673,6675,6677)
typeid_list <- c(450
                 ,451
                 ,453
                 ,3001
                 ,3017
                 ,3041)
sde_q <- paste0("SELECT typeID,typeName FROM invtypes WHERE typeid IN (",paste(typeid_list, collapse=','),")")
sde_lookup <- sqlQuery(SDE,sde_q)

ec_q <- paste0("SELECT price_date as `date`, price_time as `hour`, ",
               "SUM(IF(locationid = ",locationID,", IF(buy_sell=1, price_best,0),0)) AS 'SellOrder', ",
               "SUM(IF(locationid = ",locationID,", IF(buy_sell=0, price_best,0),0)) AS 'BuyOrder', typeid ",
               "FROM snapshot_evecentral ",
               "WHERE typeid IN (",paste(typeid_list, collapse=','),") ",
               "AND locationid=",locationID," ",
               "AND price_date > (SELECT MAX(price_date) FROM snapshot_evecentral) - INTERVAL ",date_range," DAY ",
               "GROUP BY price_date, price_time, typeid")

ec_data <- sqlQuery(EC,ec_q)
ec_data <- merge(ec_data, sde_lookup, by.x="typeid", by.y="typeID") #MERGE typeid/name locally

loc_q <- paste0("SELECT solarSystemName FROM mapsolarsystems WHERE solarSystemID=",locationID)
loc <- sqlQuery(SDE,loc_q)
location_name <- as.character(loc$solarSystemName[1])

### Clean up ec_data for charting ###
ec_data$datetime <- NA
ec_data$datetime <- paste(ec_data$date,ec_data$hour, sep=" ")
ec_data$datetime <- as.POSIXlt(ec_data$datetime)
ec_data$spread <- ec_data$SellOrder - ec_data$BuyOrder
ec_data <- subset(ec_data, spread>0)

### Stack Data ###
ec_stack <- melt.data.frame(ec_data, id.vars=c("datetime","typeName","spread"), measure.vars=c("SellOrder","BuyOrder"))

### Chart Profile ###
theme_dark <- function( ... ) {
  theme(
    text = element_text(color="gray90"),
    title = element_text(size=rel(2),hjust=0.05,vjust=3.5),
    axis.title.x = element_text(size=rel(1),hjust=0.5, vjust=0),
    axis.title.y = element_text(size=rel(1),hjust=0.5, vjust=1.5),
    plot.margin = unit(c(2,1,1,1), "cm"),
    plot.background=element_rect(fill="gray8",color="gray8"),
    panel.background=element_rect(fill="gray10",color="gray10"),
    panel.grid.major = element_line(colour="gray17"),
    panel.grid.minor = element_line(colour="gray12"),        
    axis.line = element_line(color = "gray50"),
    plot.title = element_text(color="gray80"),
    axis.title = element_text(color="gray70"),
    axis.text = element_text(color="gray50",size=rel(1.1)),        
    legend.key = element_rect(fill="gray8",color="gray8"),
    legend.background = element_rect(fill="gray8"),
    legend.title = element_text(size=rel(0.6)),
    legend.text = element_text(size=rel(1.1)),
    strip.background = element_rect(fill="gray1"),
    strip.text = element_text(size=rel(1.2))
  ) + theme(...)
}

filename <- paste0("EC-group_",groupName,"_",location_name,"_",Sys.Date(),".png")
charthead <- paste0("Buy/Sell Margins - ",location_name," - ",date_range,"days - ",Sys.Date())

png(paste0(plot.path,filename), width=plot.width, height=plot.height)
GG <- ggplot(ec_stack, aes(x=datetime,y=value, color=variable))
GG <- GG + geom_line() + facet_wrap(~typeName, scales="free_y") + theme_dark()
GG <- GG + labs(title=charthead, color="PriceKey", x="date", y="price")
GG <- GG + geom_vline(xintercept=as.numeric(patch_list), color="white")

print(GG)
dev.off()