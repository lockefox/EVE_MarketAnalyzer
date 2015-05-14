library(ggplot2)
library(grid)
library(RODBC)
chart_name = paste0("FuelIsotopes_",Sys.Date(),".png")

emd <- odbcConnect('evemarketdata')
fuel.query <- 'SELECT price_date,regionid,
  if(itemid = 16274, "Helium Isotopes",
	if(itemid = 17887, "Oxygen Isotopes",
	if(itemid = 17888, "Nitrogen Isotopes",
	if(itemid = 17889, "Hydrogen Isotopes",NULL)))) AS `product`,
	avgPrice as `price`
	
FROM crest_markethistory 
WHERE regionid = 10000002
AND itemid in (16274,17887,17888,17889)
AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL 100 DAY
GROUP BY price_date,regionid,`product`'
fuel_data <- sqlQuery(emd, fuel.query)

theme_dark <- function( ... ) {
  theme(
    text = element_text(color="gray90"),
    title = element_text(size=rel(2.8),hjust=0.05,vjust=3.5),
    axis.title = element_text(size=rel(0.5),hjust=0.5),
    plot.margin = unit(c(2,1,0,1), "cm"),
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
    legend.text = element_text(size=rel(1.1))
  ) + theme(...)
}
fuel_data$price_date = as.Date(fuel_data$price_date)
x_intercepts <- as.Date(c("2015-02-17","2015-03-18","2015-05-05"))
#y_min = min(fuel_data$price)*0.8
#y_max = max(fuel_data$price)*1.2

png(chart_name,width=1600,height=900)
P <- ggplot(subset(fuel_data, price_date > "2014-12-01"), aes(x=price_date,y=price,color=product))
p <- P + geom_line(size=1) + labs(title="Fuel Isotopes",color="Isotope",x="")+theme_dark()
p <- p + scale_color_manual(values=c("Oxygen Isotopes"="#097686","Hydrogen Isotopes"="#B7090D","Nitrogen Isotopes"="#2169E0","Helium Isotopes"="#EA8B25"))
p <- p + geom_vline(xintercept=as.numeric(x_intercepts), color="white", label="patch")#+ylim(y_min,y_max)
print(p)
dev.off()
