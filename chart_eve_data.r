required_packages <- c('rjson','quantmod')
new_packages <- required_packages[!(required_packages %in% installed.packages()[,'Package'])]
if(length(new_packages)) install.packages(new_packages)

library(rjson)
library(quantmod)
#library(RMySQL)

local_path = 'K:\\scripts\\EVE_MarketAnalyzer\\'
setwd(local_path)
config <- fromJSON(file='chart_list.json')#make this dynamic

dir.create(paste('/plots/',Sys.Date(),sep=''))
for (group in config$forced_plots)
{
  print (group)
  for (itemid in group)
  {
    #print(paste('\t',itemid))
    
    query_str <- paste('http://public-crest.eveonline.com/market/10000002/types/',itemid,'/history/',sep='')
    market.json <- fromJSON(query_str)
    market.data <- data.table(market.json$items)
    
    market.data <- market.data[,list(Date = as.Date(date),
                                     Volume = volume,
                                     High = highPrice,
                                     Low = lowPrice,
                                     Close = avgPrice,
                                     Open = avgPrice[-1])]
    market.data.ts <- xts(market.data[,-1,with=F],order.by=market.data[,Date],period=7)
    png(paste('/plots/',itemid,'_',Sys.Date(),sep=''),
        width = config$plot_defaults$width,
        height = config$plot_defaults$height)
    chartSeries(market.data.ts, 
                name=paste(itemid,Sys.Date(),sep='_'),
                TA=config$default_args$quantmod,
                subset=config$default_args$subset))
    
    dev.off()
  }
}
print (config$forced_plots[1])