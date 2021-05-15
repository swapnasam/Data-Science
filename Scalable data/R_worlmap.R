library(tidyverse)
library(maps)
library(ggthemes)
library(haven)
library(ggpubr)
library(scales)
library(zoo)
library(gganimate)
library(gifski) #good rendering engine for gganimate!
library(viridis)
library(rworldmap)
library(RColorBrewer)

col <- c('year','region','average_rainfall')

df_world<- data.frame(matrix(ncol = 3, nrow = 0))
x <- col
colnames(df_world) <- x
for (i in 0:9 ) {
  str=sprintf("part-0000%d-98877ae0-bb08-433e-8e7f-85ae33dd1f0f-c000.csv",i)
  
  #str = "part-000%d-0800da88-c2b6-48f9-b293-c5d7fb5aba4e-c000.csv"
  data <- read.csv(str,header = FALSE)
  
  names(data) <- col
  df_world <- rbind(df_world,data.frame(data))
  
  
}

for (i in 10:15) {
  str=sprintf("part-000%d-98877ae0-bb08-433e-8e7f-85ae33dd1f0f-c000.csv",i)
  
  #str = "part-000%d-0800da88-c2b6-48f9-b293-c5d7fb5aba4e-c000.csv"
  data <- read.csv(str,header = FALSE)
  
  names(data) <- col
  df_world <- rbind(df_world,data.frame(data))
  
  
  
}


#subset to 2020 forecast data
df_world1 <- df_world[df_world$year==2017, ]
df_world1$region <- as.character(df_world1$region)
df_world1$region[df_world1$region == 'United States'] <- 'USA'
df_world1$region <- as.factor(df_world1$region)

world_map <- map_data("world")

#now merge the world map data with our example dataframe

df.map <- right_join(df_world1, world_map, by = "region")
df.map <- df.map[is.na(df.map$logpredict==FALSE, ]





map2 <- ggplot(data = df.map) +
  geom_polygon(aes(x = long, y = lat, fill = average_rainfall, group = group),
               color = "black") +
  expand_limits(x = df.map$long, y = df.map$lat) +
  scale_fill_viridis_b(option = "magma", direction = -1, name = "Precipitation in  tenths of mm",
                     guide = guide_colorbar(
                       direction = "horizontal",
                       barheight = unit(2, units = "mm"),
                       barwidth = unit(100, units = "mm"),
                       draw.ulim = FALSE,
                       title.position = 'top',
                       title.hjust = 0.5,
                       title.vjust = 0.5
                     )) + 
  theme_hc() + theme(axis.text.x = element_blank(),axis.text.y = element_blank(),axis.ticks = element_blank(),
                     legend.position = "bottom") +
  xlab(" ") + ylab(" ") +
  labs(title  = "Average rainfall for the year 2017")

map2
