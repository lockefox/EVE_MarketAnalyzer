library(data.table)
library(quantmod)
library(jsonlite)
library(RODBC)
library(ggplot2)

emd <- odbcConnect('evemarketdata')
theme_dark <- function( ... ) {
  theme(
    text = element_text(color="gray90"),
    plot.background=element_rect(fill="gray8",color="gray8"),
    panel.background=element_rect(fill="gray10",color="gray10"),
    panel.grid.major = element_line(colour="gray17"),
    panel.grid.minor = element_line(colour="gray12"),        
    axis.line = element_line(color = "gray50"),
    plot.title = element_text(color="gray80"),
    axis.title = element_text(color="gray70"),
    axis.text = element_text(color="gray30"),        
    legend.key = element_rect(fill="gray10")
  ) + theme(...)
}

fuel.query = 'SELECT price_date,regionid,
    if(itemid = 16274, "Helium Isotopes",
    if(itemid = 17887, avgPrice, NULL)) as `Oxygen Isotopes`,
    sum(if(itemid = 17888, avgPrice, NULL)) as `Nitrogen Isotopes`,
    sum(if(itemid = 17889, avgPrice, NULL)) as `Hydrogen Isotopes`,
    sum(if(itemid = 4051, avgPrice, NULL)) as `Caldari Fuel Block`,
    sum(if(itemid = 4246, avgPrice, NULL)) as `Minmatar Fuel Block`,
    sum(if(itemid = 4247, avgPrice, NULL)) as `Amarr Fuel Block`,
    sum(if(itemid = 4312, avgPrice, NULL)) as `Gallente Fuel Block`,
    sum(if(itemid = 44, avgPrice*4,
      if(itemid = 3683, avgPrice*20,
      if(itemid = 3689, avgPrice * 4,
      if(itemid = 9832, avgPrice*9,
      if(itemid = 9848, avgPrice,
      if(itemid = 16272, avgPrice * 150,
      if(itemid = 16273, avgPrice * 150,NULL))))))))/40 as `PI Component`
  FROM crest_markethistory 
  WHERE regionid = 10000002
  GROUP BY price_date,regionid'
fuel.sqldata <- sqlQuery(emd, fuel.query)

p <- ggplot(fuel.sqldata,aes(x=date,y=price))
p + theme_dark() + geom_line()
