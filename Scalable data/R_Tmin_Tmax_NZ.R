library(tidyverse)
library(ggplot2)
library(plyr)
library(readr)
library(readxl)

#################
col <- c('id','date','element','value','name')

df_NZ_temp <- data.frame(matrix(ncol = 5, nrow = 0))
x <- col
colnames(df_NZ_temp) <- x
g <- c(1,2,4,6,8,9)
#to load part files
for (i in  g) {
  print(i)
  str=sprintf("part-0000%d-94f00cfd-fbe2-4232-b644-d000124f155d-c000.csv",i)
  
  #str = "part-000%d-0800da88-c2b6-48f9-b293-c5d7fb5aba4e-c000.csv"
  data <- read.csv(str,header = FALSE)
  
  names(data) <- col
  df_NZ_temp <- rbind(df_NZ_temp,data.frame(data))
  
  
}

g <- c(10,11,13,14,15)
for (i in g) {
  print(i)
  str=sprintf("part-000%d-94f00cfd-fbe2-4232-b644-d000124f155d-c000.csv",i)
  
  #str = "part-000%d-0800da88-c2b6-48f9-b293-c5d7fb5aba4e-c000.csv"
  data <- read.csv(str,header = FALSE)
  
  names(data) <- col
  df_NZ_temp <- rbind(df_NZ_temp,data.frame(data))
  
  
  
}




df_NZ_temp$date = as.Date(as.character(df_NZ_temp$date), "%Y%m%d")

# Adding a new column to the dataframe. The new column is tenths of degrees celsius.

df_NZ_temp$value1 = df_NZ_temp$value / 10


# Pplotting time series for each station in NZ 

plot1 = ggplot(df_NZ_temp, aes(date, value1, colour = element)) + geom_line()

plot2 = plot1 +labs(title = "Daily Temperatures - New Zealand"
              , x = "Year", y = "Temperature (degrees C)", col = "Legend") + facet_wrap(~name) 

plot2 = plot2 + scale_color_hue(labels = c("Max Temperature", "Min Temperature"))+ theme(plot.title = element_text(hjust = 0.5)) + scale_color_manual(values = c("red", "blue"))


print(plot2)






# aggregating by Element and Date
average_temps = aggregate(value1 ~ element + date, df_NZ_temp, FUN= mean)

plotb = ggplot(average_temps, aes(date, value1, colour = element)) + geom_line()
plotb = plotb + labs(title = "Average temperature - New Zealand"
                     , x = "Year", y = "Temperature (degrees C)", col = "Legend")
plotb = plotb + scale_color_hue(labels = c("Max Temperature", "Min Temperature")) + theme(plot.title = element_text(hjust = 0.5)) + scale_color_manual(values = c("yellow", "green"))

tiff("Avg_Temperatures_NZ.tiff", height = 18, width = 35, units = 'cm', 
     compression = "lzw", res = 300)

plotb
