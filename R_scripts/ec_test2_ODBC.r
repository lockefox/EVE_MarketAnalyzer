library(RODBC)
library(ggplot2)
library(grid)
library(reshape)

locationID <- 30000142 #JITA=30000142
date_range <- 60

patch_list <- as.POSIXlt(c("2015-04-28 13:00","2015-05-05 13:00","2015-06-02 13:00"))

plot.width <- 1600
plot.height <- 900

x_intercepts <- patch_list
chart_repo <- "C:/Users/Lockefox/Plots/"
title_date <- Sys.Date()
title_head <- "Buy/Sell Prices"

### FETCH DB STUFF ###
SDE <- odbcConnect('sde_lookup')
EC  <- odbcConnect('remote_ec')

sde_q <- paste0("SELECT typeID,typeName FROM invtypes")
sde_lookup <- sqlQuery(SDE,sde_q)

ec_q <- paste0("SELECT price_date as `date`, price_time as `hour`, ",
               "SUM(IF(locationid = ",locationID,", IF(buy_sell=1, price_best,0),0)) AS 'SellOrder', ",
               "SUM(IF(locationid = ",locationID,", IF(buy_sell=0, price_best,0),0)) AS 'BuyOrder', typeid ",
               "FROM snapshot_evecentral ",
               "WHERE locationid=",locationID," ",
               "AND price_date > (SELECT MAX(price_date) FROM snapshot_evecentral) - INTERVAL ",date_range," DAY ",
               "GROUP BY price_date, price_time, typeid")

ec_data <- sqlQuery(EC,ec_q)
market_data2 <- merge(ec_data, sde_lookup, by.x="typeid", by.y="typeID") #MERGE typeid/name locally

market_data2$date <- as.Date(market_data2$date)
market_data2$typeid <- as.factor(market_data2$typeid)
market_data2$locationid <- as.factor(market_data2$locationid)



bs <- c(638,639,640,641,642,643,644,645,24688,24690,24692,24694,17736,17738,17740,17918)
t3 <- c(29984,29986,29988,29990)
mins <- c(34,35,36,37,38,39,40,11399)
PI4 <- c(2867,2868,2869,2870,2871,2872,2875,2876)
adv_materials <- c(16670,16671,16672,16673,16678,16679,16680,16681,16682,16683,17317,33359,33360,33361,33362)
interceptors <- c(11176,11178,11184,11186,11196,11198,11200,11202,33673)
paint_ships.frigs <- c(33655,33657,33659,33661,33663,33665,33667,33669,33677)
paint_ships.cruiser <- c(33639,33641,33643,33645,33647,33649,33651,33653)
datacore <- c(20171,20172,20410,20411,20412,20413,20414,20415,20416,20417,20418,20419,20420,20421,20423,20424,25887)

plex <- c(29668,34133,34132)
tokens <- c(2833,32792,32793,29668,34133,34132)
tokens2 <- c(2833,32793,29668,34133,34132)
ca_implants <-c(33393,33394)
ca_implants2 <-c(2082,2589,33393,33394)

jfs <- c(28844,28846,28848,28850)
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


chart_name <- "PLEX"
#market_data.tmp <- subset(market_data2, typeid %in% plex & date > filter_date)
market_data.tmp <- subset(market_data2, typeid == 29668)

market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)

market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))

png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.stack1, aes(x=datetime,y=value, color=variable))
GG <- GG + geom_line() + theme_dark() + scale_y_continuous(limits=c(min(market_data.stack1$value),NA),labels=function(x)sprintf("%.1fB",x/1e9))
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()

chart_name <- "RMT_Tokens"
market_data.tmp <- subset(market_data2, typeid %in% plex)

market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)

market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))
#NORMALIZE real-curency tokens
market_data.tmp$SellOrder[market_data.tmp$typeid == 32793] <- 3500/500 * market_data.tmp$SellOrder[market_data.tmp$typeid ==32793]
market_data.tmp$BuyOrder[market_data.tmp$typeid == 32793] <- 3500/500 * market_data.tmp$BuyOrder[market_data.tmp$typeid ==32793]
market_data.tmp$SellOrder[market_data.tmp$typeid == 32792] <- 3500/100 * market_data.tmp$SellOrder[market_data.tmp$typeid ==32792]
market_data.tmp$BuyOrder[market_data.tmp$typeid == 32792] <- 3500/100 * market_data.tmp$BuyOrder[market_data.tmp$typeid ==32792]
market_data.tmp$SellOrder[market_data.tmp$typeid == 2833] <- 3500/1000 * market_data.tmp$SellOrder[market_data.tmp$typeid ==2833]
market_data.tmp$BuyOrder[market_data.tmp$typeid == 2833] <- 3500/1000 * market_data.tmp$BuyOrder[market_data.tmp$typeid ==2833]
market_data.tmp$SellOrder[market_data.tmp$typeid == 34132] <- 3500/1000 * market_data.tmp$SellOrder[market_data.tmp$typeid ==34132]
market_data.tmp$BuyOrder[market_data.tmp$typeid == 34132] <- 3500/1000 * market_data.tmp$BuyOrder[market_data.tmp$typeid ==34132]

#alpha_group <- c(0.3,0.3,1.0,0.3,0.3,0.3)
alpha_group <- c(1.0,0.3,0.3)
png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.tmp, aes(x=datetime, ymin=BuyOrder, ymax=SellOrder, fill=typeName, alpha=typeName))
GG <- GG + geom_ribbon() + theme_dark() + scale_alpha_manual(values = alpha_group) + scale_y_continuous(limits=c(min(market_data.tmp$BuyOrder),NA),labels=function(x)sprintf("%.1fB",x/1e9))
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()

chart_name <- "RMT Tokens (w AUR Tokens)"
market_data.tmp <- subset(market_data2, typeid %in% tokens)

market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)

market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))
#NORMALIZE real-curency tokens
market_data.tmp$SellOrder[market_data.tmp$typeid == 32793] <- 3500/500 * market_data.tmp$SellOrder[market_data.tmp$typeid ==32793]
market_data.tmp$BuyOrder[market_data.tmp$typeid == 32793] <- 3500/500 * market_data.tmp$BuyOrder[market_data.tmp$typeid ==32793]
market_data.tmp$SellOrder[market_data.tmp$typeid == 32792] <- 3500/100 * market_data.tmp$SellOrder[market_data.tmp$typeid ==32792]
market_data.tmp$BuyOrder[market_data.tmp$typeid == 32792] <- 3500/100 * market_data.tmp$BuyOrder[market_data.tmp$typeid ==32792]
market_data.tmp$SellOrder[market_data.tmp$typeid == 2833] <- 3500/1000 * market_data.tmp$SellOrder[market_data.tmp$typeid ==2833]
market_data.tmp$BuyOrder[market_data.tmp$typeid == 2833] <- 3500/1000 * market_data.tmp$BuyOrder[market_data.tmp$typeid ==2833]
market_data.tmp$SellOrder[market_data.tmp$typeid == 34132] <- 3500/1000 * market_data.tmp$SellOrder[market_data.tmp$typeid ==34132]
market_data.tmp$BuyOrder[market_data.tmp$typeid == 34132] <- 3500/1000 * market_data.tmp$BuyOrder[market_data.tmp$typeid ==34132]

alpha_group <- c(0.3,0.3,1.0,0.3,0.3,0.3)
#alpha_group <- c(1.0,0.3,0.3)
png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.tmp, aes(x=datetime, ymin=BuyOrder, ymax=SellOrder, fill=typeName, alpha=typeName))
GG <- GG + geom_ribbon() + theme_dark() + scale_alpha_manual(values = alpha_group) + scale_y_continuous(limits=c(min(market_data.stack1$value),NA),labels=function(x)sprintf("%.1fB",x/1e9))
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()


chart_name <- "RMT Tokens (w AUR Tokens, -100AUR)"
market_data.tmp <- subset(market_data2, typeid %in% tokens2)

market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)

market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))
#NORMALIZE real-curency tokens
market_data.tmp$SellOrder[market_data.tmp$typeid == 32793] <- 3500/500 * market_data.tmp$SellOrder[market_data.tmp$typeid ==32793]
market_data.tmp$BuyOrder[market_data.tmp$typeid == 32793] <- 3500/500 * market_data.tmp$BuyOrder[market_data.tmp$typeid ==32793]
market_data.tmp$SellOrder[market_data.tmp$typeid == 32792] <- 3500/100 * market_data.tmp$SellOrder[market_data.tmp$typeid ==32792]
market_data.tmp$BuyOrder[market_data.tmp$typeid == 32792] <- 3500/100 * market_data.tmp$BuyOrder[market_data.tmp$typeid ==32792]
market_data.tmp$SellOrder[market_data.tmp$typeid == 2833] <- 3500/1000 * market_data.tmp$SellOrder[market_data.tmp$typeid ==2833]
market_data.tmp$BuyOrder[market_data.tmp$typeid == 2833] <- 3500/1000 * market_data.tmp$BuyOrder[market_data.tmp$typeid ==2833]
market_data.tmp$SellOrder[market_data.tmp$typeid == 34132] <- 3500/1000 * market_data.tmp$SellOrder[market_data.tmp$typeid ==34132]
market_data.tmp$BuyOrder[market_data.tmp$typeid == 34132] <- 3500/1000 * market_data.tmp$BuyOrder[market_data.tmp$typeid ==34132]

alpha_group <- c(0.3,1.0,0.3,0.3,0.3)
#alpha_group <- c(1.0,0.3,0.3)
png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.tmp, aes(x=datetime, ymin=BuyOrder, ymax=SellOrder, fill=typeName, alpha=typeName))
GG <- GG + geom_ribbon() + theme_dark() + scale_alpha_manual(values = alpha_group) + scale_y_continuous(limits=c(min(market_data.stack1$value),NA),labels=function(x)sprintf("%.1fB",x/1e9))
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()


chart_name <- "Minerals"
market_data.tmp <- subset(market_data2, typeid %in% mins)
market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)
market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))

png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.stack1, aes(x=datetime,y=value, color=variable))
GG <- GG + geom_line() + facet_wrap(~typeName, scales="free_y") + theme_dark()
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()


chart_name <- "PI4"
market_data.tmp <- subset(market_data2, typeid %in% PI4)
market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)
market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))

png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.stack1, aes(x=datetime,y=value, color=variable))
GG <- GG + geom_line() + facet_wrap(~typeName, scales="free_y") + theme_dark()
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()


chart_name <- "Advanced Materials"
market_data.tmp <- subset(market_data2, typeid %in% adv_materials)
market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)
market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))

png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.stack1, aes(x=datetime,y=value, color=variable))
GG <- GG + geom_line() + facet_wrap(~typeName, scales="free_y") + theme_dark()
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()



chart_name <- "Ship Skins - Frigates"
market_data.tmp <- subset(market_data2, typeid %in% paint_ships.frigs)
market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)
market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))

png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.stack1, aes(x=datetime,y=value, color=variable))
GG <- GG + geom_line() + facet_wrap(~typeName, scales="free_y") + theme_dark()
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()


chart_name <- "Ship Skins - Cruisers"
market_data.tmp <- subset(market_data2, typeid %in% paint_ships.cruiser)
market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)
market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))

png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.stack1, aes(x=datetime,y=value, color=variable))
GG <- GG + geom_line() + facet_wrap(~typeName, scales="free_y") + theme_dark()
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()


chart_name <- "Battleships"
market_data.tmp <- subset(market_data2, typeid %in% bs)
market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)
market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))

png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.stack1, aes(x=datetime,y=value, color=variable))
GG <- GG + geom_line() + facet_wrap(~typeName, scales="free_y") + theme_dark()
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()


chart_name <- "Datacores"
market_data.tmp <- subset(market_data2, typeid %in% datacore)
market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)
market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))

png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.stack1, aes(x=datetime,y=value, color=variable))
GG <- GG + geom_line() + facet_wrap(~typeName, scales="free_y") + theme_dark()
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()

x_intercepts <- as.POSIXlt(c("2015-05-04 18:30"))
filter_date <- "2015-04-15"
chart_name <- "Genolution Implants"
market_data.tmp <- subset(market_data2, typeid %in% ca_implants)
market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)
market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))

png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.stack1, aes(x=datetime,y=value, color=variable))
GG <- GG + geom_line() + facet_wrap(~typeName, scales="free_y") + theme_dark() + scale_y_continuous(limits=c(min(market_data.stack1$value),NA),labels=function(x)sprintf("%.1fM",x/1e6))
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()

#x_intercepts <- as.POSIXlt(c("2015-05-04 18:30"))
#filter_date <- "2015-04-01"
chart_name <- "Genolution Implants (ALL)"
market_data.tmp <- subset(market_data2, typeid %in% ca_implants2)
market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)
market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))

png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.stack1, aes(x=datetime,y=value, color=variable))
GG <- GG + geom_line() + facet_wrap(~typeName, scales="free_y") + theme_dark() + scale_y_continuous(limits=c(min(market_data.stack1$value),NA),labels=function(x)sprintf("%.1fM",x/1e6))
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()

chart_name <- "Jump Freighters"
market_data.tmp <- subset(market_data2, typeid %in% jfs)
market_data.tmp$spread <- market_data.tmp$SellOrder - market_data.tmp$BuyOrder
market_data.tmp <- subset(market_data.tmp, spread>0)
market_data.tmp$margin <- market_data.tmp$spread / market_data.tmp$BuyOrder * 100
market_data.tmp$datetime <- paste(market_data.tmp$date,market_data.tmp$hour, sep=" ")
market_data.tmp$datetime <- as.POSIXlt(market_data.tmp$datetime)
market_data.stack1 <- melt.data.frame(market_data.tmp, id.vars=c("datetime","typeName","typeid","spread","margin"),measure.vars=c("SellOrder","BuyOrder"))

png(paste0(chart_repo,chart_name,"_",title_date,".png"),width=1600,height=900)
GG <- ggplot(market_data.stack1, aes(x=datetime,y=value, color=variable))
GG <- GG + geom_line() + facet_wrap(~typeName, scales="free_y") + theme_dark() + scale_y_continuous(limits=c(min(market_data.stack1$value),NA),labels=function(x)sprintf("%.1fM",x/1e6))
GG <- GG + labs(title=paste(title_head,chart_name,title_date, sep=" - "),color="PriceKey",x="date", y="price") + geom_vline(xintercept=as.numeric(x_intercepts), color="white")
print(GG)
dev.off()