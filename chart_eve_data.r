required_packages <- c('rjson','quantmod','data.table')
new_packages <- required_packages[!(required_packages %in% installed.packages()[,'Package'])]
if(length(new_packages)) install.packages(new_packages)

#library(rjson)
library(jsonlite)
library(quantmod)
library(data.table)
#library(RMySQL)
quote(expr =)
#local_path = 'K:\\scripts\\EVE_MarketAnalyzer\\'
local_path = 'C:/Users/Lockefox/EVE_MarketAnalyzer/'
setwd(local_path)
config <- fromJSON(readLines('chart_list.json'))#make this dynamic

print (Sys.Date())
dir.create(paste('plots/',Sys.Date(),sep=''))
for (group in config$forced_plots)
{
  print (group)
  for (itemid in group)
  {
    #print(paste('\t',itemid))
    
    query_str <- paste('http://public-crest.eveonline.com/market/10000002/types/',itemid,'/history/',sep='')
    print(query_str)
    market.json <- fromJSON(readLines(query_str))
    market.data <- data.table(market.json$items)
    
    market.data <- market.data[,list(Date=as.Date(date),
                                     Volume=volume,
                                     High=highPrice,
                                     Low=lowPrice,
                                     Close=avgPrice[-1], #allow for faked candlesticks
                                     Open=avgPrice)]
    n<-nrow(market.data)
    market.data<-market.data[1:n-1,]
    market.data.ts <- xts(market.data[,-1,with=F],order.by=market.data[,Date],period=7)
    png(paste('plots/',Sys.Date(),'/',itemid,'_',Sys.Date(),'.png',sep=''),
        width = config$plot_defaults$width,
        height = config$plot_defaults$height)
    chartSeries(market.data.ts, 
                name=paste(itemid,Sys.Date(),sep='_'),
                TA=config$default_args$quantmod,
                subset=config$default_args$subset)
    
    dev.off()
  }
}
print (config$forced_plots[1])