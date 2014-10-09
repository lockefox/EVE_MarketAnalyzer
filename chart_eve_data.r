required_packages <- c('rjson','quantmod')
new_packages <- required_packages[!(required_packages %in% installed.packages()[,'Package'])]
if(length(new_packages)) install.packages(new_packages)

library(rjson)
library(quantmod)

local_path = 'K:\\scripts\\EVE_MarketAnalyzer\\'
config <- fromJSON(file=paste(local_path,'chart_list.json', sep=''))#make this dynamic

for (group in config$forced_plots)
{
  print (group)
}