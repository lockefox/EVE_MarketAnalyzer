library(RODBC)
library(ggplot2)
library(grid)
market_data <- read.csv("C:/Users/Lockefox/EC_test_full.csv", header=TRUE)

market_data$date <- as.Date(market_data$date)
market_data$typeid <- as.factor(market_data$typeid)
market_data$hour <- as.factor(market_data$HOUR.hour)
market_data$locationid <- as.factor(market_data$locationid)


bs <- c(638,639,640,641,642,643,644,645,24688,24690,24692,24694)
t3 <- c(29984,29986,29988,29990)
market_data.sub <- subset(market_data, typeid %in% t3 & spread>0)

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
GG <- ggplot(market_data.sub, aes(x=spread,fill=systemName))
gg <- GG + geom_histogram() + facet_wrap(~typeName,scales = "free") + labs(title="Buy/Sell Margins (2015-02-01 - 2015-03-07)",fill="Hub",x="Absolute Buy/Sell Spread")
gG <- gg + theme_dark() + scale_color_manual(values=c("Dodixie"="#097686","Jita"="#2169E0","Amarr"="#EA8B25"))
print(gG)

