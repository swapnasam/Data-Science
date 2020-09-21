library(shiny)
library(shinythemes)#im unable to install
library(aplpack)
library(GGally)
library(RColorBrewer)
library(shinyWidgets)


library(tidyverse)
library(stringr)

library(reshape2)

library(scales)

library(dbscan)
library(plotly)

library(pls)
library(ggplot2)
library(DT)
library(recipes)

auto_train <- read.csv("AutoTrain.csv", stringsAsFactors = FALSE)
auto_test <- read.csv("AutoTest.csv",  stringsAsFactors = FALSE)
auto_train <- auto_train[,c(4:6,2)]
auto_test <- auto_test[,c(4:6,2)]

vchoices <- 1:ncol(auto_test)
names(vchoices) <- names(auto_test)

yj_estimates <- recipe(mpg ~ displacement + horsepower + weight , data = auto_train) %>%
  step_YeoJohnson(all_numeric()) %>%
  prep(data = auto_train)  # "prep" trains the recipe (that holds the YJ transform step)

yj_te <- bake(yj_estimates, auto_test)  # bake runs the trained-transform on some data


tchoices <- c("BoxPlot","BagPlot","Mahalanobis","Cooks","DBScan", "SVM")


