library(ggplot2)
library(grid)
library(reshape)

dir_path = "--RAW_CSV_PATH_GOES_HERE--"
market_data <- read.csv(dir_path, header=TRUE)

#recast variables for easier manipulation
market_data$date <- as.factor(as.Date(market_data$date))
market_data$typeid <- as.factor(market_data$typeid)
market_data$locationid <- as.factor(market_data$locationid)
tmp_hr <- as.factor(as.numeric(strftime(strptime(market_data$hour, format="%H:%M:%S"),"%H")))
market_data$hour <- tmp_hr

#[spread|margin]_[source]_[destination]
#Calculate local margin/spread
market_data$Spread_JitaBuy_JitaSell = market_data$Jita_Sell - market_data$Jita_Buy
market_data$Margin_JitaBuy_JitaSell = market_data$Spread_JitaBuy_JitaSell / market_data$Jita_Buy * 100
#market_data$Spread_DodixieBuy_DodixieSell = market_data$Dodixie_Sell - market_data$Dodixie_Buy
#market_data$Margin_DodixieBuy_DodixieSell = market_data$Spread_DodixieBuy_DodixieSell / market_data$Dodixie_Buy * 100
#market_data$Spread_AmarrBuy_AmarrSell = market_data$Amarr_Sell - market_data$Amarr_Buy
#market_data$Margin_AmarrBuy_AmarrSell = market_data$Spread_AmarrBuy_AmarrSell / market_data$Amarr_Buy * 100

#Calculate remote v Jita spreads
# market_data$Spread_JitaBuy_DodixieSell  = market_data$Dodixie_Sell - market_data$Jita_Buy  - 1500000
# market_data$Spread_JitaBuy_DodixieBuy   = market_data$Dodixie_Buy  - market_data$Jita_Buy  - 1500000
# market_data$Spread_JitaSell_DodixieBuy  = market_data$Dodixie_Buy  - market_data$Jita_Sell - 1500000 
# market_data$Spread_JitaSell_DodixieSell = market_data$Dodixie_Sell - market_data$Jita_Sell - 1500000 
# market_data$Spread_JitaBuy_AmarrSell    = market_data$Amarr_Sell   - market_data$Jita_Buy  - 1000000
# market_data$Spread_JitaBuy_AmarrBuy     = market_data$Amarr_Buy    - market_data$Jita_Buy  - 1000000
# market_data$Spread_JitaSell_AmarrBuy    = market_data$Amarr_Buy    - market_data$Jita_Sell - 1000000
# market_data$Spread_JitaSell_AmarrSell   = market_data$Amarr_Sell   - market_data$Jita_Sell - 1000000
market_data$Spread_JitaBuy_DodixieSell  = market_data$Dodixie_Sell - market_data$Jita_Buy  
market_data$Spread_JitaBuy_DodixieBuy   = market_data$Dodixie_Buy  - market_data$Jita_Buy  
market_data$Spread_JitaSell_DodixieBuy  = market_data$Dodixie_Buy  - market_data$Jita_Sell 
market_data$Spread_JitaSell_DodixieSell = market_data$Dodixie_Sell - market_data$Jita_Sell 
market_data$Spread_JitaBuy_AmarrSell    = market_data$Amarr_Sell   - market_data$Jita_Buy  
market_data$Spread_JitaBuy_AmarrBuy     = market_data$Amarr_Buy    - market_data$Jita_Buy  
market_data$Spread_JitaSell_AmarrBuy    = market_data$Amarr_Buy    - market_data$Jita_Sell 
market_data$Spread_JitaSell_AmarrSell   = market_data$Amarr_Sell   - market_data$Jita_Sell 

market_data$Margin_JitaBuy_DodixieSell  = market_data$Spread_JitaBuy_DodixieSell  / market_data$Jita_Buy  * 100
market_data$Margin_JitaBuy_DodixieBuy   = market_data$Spread_JitaBuy_DodixieBuy   / market_data$Jita_Buy  * 100
market_data$Margin_JitaSell_DodixieBuy  = market_data$Spread_JitaSell_DodixieBuy  / market_data$Jita_Sell * 100
market_data$Margin_JitaSell_DodixieSell = market_data$Spread_JitaSell_DodixieSell / market_data$Jita_Sell * 100
market_data$Margin_JitaBuy_AmarrSell    = market_data$Spread_JitaBuy_AmarrSell    / market_data$Jita_Buy  * 100
market_data$Margin_JitaBuy_AmarrBuy     = market_data$Spread_JitaBuy_AmarrBuy     / market_data$Jita_Buy  * 100
market_data$Margin_JitaSell_AmarrBuy    = market_data$Spread_JitaSell_AmarrBuy    / market_data$Jita_Sell * 100
market_data$Margin_JitaSell_AmarrSell   = market_data$Spread_JitaSell_AmarrSell   / market_data$Jita_Sell * 100

bs <- c(638,639,640,641,642,643,644,645,24688,24690,24692,24694,17736,17738,17740,17918)
adv_material <- c(16670,16671,16672,16673,16678,16679,16680,16681,16682,16683,17317,33359,33360,33361,33362)
mins <- c(34,35,36,37,38,39,40,11399)

market_data.bs <- subset(market_data,typeid %in% mins)
#write.csv(market_data.bs, file="margin_matrix_rawdata_mins.csv", row.names=FALSE, na="")

#Build list of what should be stacked
measure_list <- grep("Margin",names(market_data.bs),value=TRUE)
measure_list <- append(grep("Spread",names(market_data.bs),value=TRUE),measure_list)
tmp_stack <- melt.data.frame(market_data.bs, id.vars=c("date","hour","typeid","typeName"),measure.vars=measure_list)#melt acts like JMP stack

#Split trend name into describer columns
tmp_stack$margin_spread = sapply(strsplit(as.character(tmp_stack$variable),"_"),"[[",1)
tmp_stack$Source = sapply(strsplit(as.character(tmp_stack$variable),"_"),"[[",2)
tmp_stack$Destination = sapply(strsplit(as.character(tmp_stack$variable),"_"),"[[",3)

#build graph profile
color_blind <- c("#644B00","#39B143","#406FDF","#CF7926","#21BD91","#A12CDC","#C8C127","#1FB6B6","#C925CD","#91B720","#239DC3","#D2269E")
theme_dark <- function( ... ) {
  theme(
    text = element_text(color="gray90"),
    title = element_text(size=rel(2),hjust=0.05,vjust=3.5),
    axis.title.x = element_text(size=rel(1),hjust=0.5, vjust=0),
    axis.title.y = element_text(size=rel(1),hjust=0.5, vjust=1.5),
    plot.margin = unit(c(1.5,1,1,1), "cm"),
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

#Subset graph data
t_market_data.margin <- subset(tmp_stack, margin_spread=="Margin")
t_market_data.spread <- subset(tmp_stack, margin_spread=="Spread")

#Build ggplot2 graph
GG <- ggplot(t_market_data.margin, aes(x=value, color=typeName))
gg <- GG + stat_ecdf(size=1) + facet_grid(Source ~ Destination,labeller = label_both, drop=TRUE) + ggtitle(expression(atop("Buy/Sell %margin Minerals (CDF)",atop(italic("eve-central data - (2014/02/01 - 2014/03/07)")))))
gG <- gg + theme_dark() + xlim(-10,20) + ylim(0.05,0.95) + geom_vline(x=0,color="white") + geom_hline(y=0.5, color="white") + ylab("Percentile") + xlab("%margin")
gG <- gG + scale_color_manual(values=color_blind)
print(gG)
