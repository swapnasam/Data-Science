#
# This is the user-interface definition of a Shiny web application. You can
# run the application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#


# Define UI for application that draws a histogram
shinyUI(fluidPage(theme = shinytheme("flatly"),
                  

    # Application title
    titlePanel("Assignment 2: Outlier Detection Methods - SWAPNA JOSMI SAM (74281128) "),
    #setting background color
    setBackgroundColor(
        color = c("#e0ffff"),
        gradient = "linear",
        direction = "bottom"
    ),

   
    
    
    tabsetPanel(
        tabPanel("Raw Dataset",
                 h3("Auto Mobile Raw Data, Summary and Plot Display "),
                 tabsetPanel(
                     tabPanel("Raw Data",
                              h4(" Auto Test Data Before Yeo Johnson Power Tranform"),
                              DT::dataTableOutput(outputId = "AutoTest"),
                              h4(" Auto Test Data After Yeo Johnson Power Tranform"),
                              DT::dataTableOutput(outputId = "YJTest"),
                              ),
                     tabPanel("Summary",
                              h4(" Auto Test Data Before Yeo Johnson Power Tranform"),
                              verbatimTextOutput(outputId = "SummaryAT"),
                              h4(" Auto Test Data After Yeo Johnson Power Tranform"),
                              verbatimTextOutput(outputId = "SummaryYT"),
                                 ),
                     tabPanel("Plot",
                              h4("Before and After Yeo Johnson Power Tranform"),
                              selectizeInput('columnnor', 'Select One', 
                                             choices = vchoices, 
                                             selected = 'all'),                               
                              plotOutput(outputId = "PlotAuto"),
                              plotOutput(outputId = "PlotYJ"),
                                 )
                 )
                 ),
        
        tabPanel("Univariate Outlier Detection",
                 tabsetPanel(
                    
                     
                     tabPanel("Visualisation: Boxplot",
                              sidebarLayout(position = "left",
                                            sidebarPanel(
                                                         selectizeInput(inputId = "columns", label = "Select variables:", choices = vchoices, multiple = TRUE, selected = vchoices[c(1,2,3,4)]),
                                                         selectizeInput('WhichOne', 'Select One', 
                                                                        choices = c('YJ Transformed Data', 'Not YJ Transformed Data'), 
                                                                        selected = 'all'),                                                        
                                                         checkboxInput(inputId = "standardise", label = "Show standardized", value = TRUE),
                                                         sliderInput(inputId = "range", label = "IQR Multiplier", min = 0, max = 5, step = 0.1, value = 1.5),
                                            ),
                                            mainPanel(
                                                      column(6,plotlyOutput(outputId = "Boxplot", width="800px",height="400px"))
                                            )),
                              
                              #checkboxGroupInput("columns","Select Columns",choices=vchoices,inline = T,selected = vchoices[c(1,2,3,4)]),
                              
                              
                              h4("Outliers are shown only for the variable named Horsepower when Yeo Johnson Transform is not applied which are given below:"),
                              DT::dataTableOutput(outputId = "Box_Out")
                     ))
                     ),
        tabPanel("Bivariate Outlier Detection",
                 tabsetPanel(
                     tabPanel("Visualisation: Bagplot",
                              selectizeInput(inputId = "columns1", label = "Select variables:", choices = vchoices, multiple = TRUE, selected = vchoices,options = list(maxItems = 2L)),
                              plotOutput(outputId = "bagplot", hover = "distplot_hover"),
                              textOutput("distplottext"),
                              h4("Outliers detected  through Bagplot are:"),
                              DT::dataTableOutput(outputId = "Bag_Out"),
                              h5("Please Note: Using YJ transformed data , we get same point when using hover option as the row values are scaled.")
                             
                     )
                     
                     )),
        tabPanel("MultiVariate Outlier Detection",
                 tabsetPanel(
        
                     tabPanel("Mahalanobis Method",
                              plotlyOutput(outputId = "maha"),

                              selectInput('Which', 'Select One', 
                                            choices = c('YJ Transformed Data','Not YJ Transformed Data'), 
                                            ),
                              h4("The Outliers detected by Mahalanobis methods are given below"),
                              DT::dataTableOutput(outputId = "YJMaha"),
                              
                              )
                     )),
        tabPanel("Model Based Outlier Detection",
                 tabsetPanel(
                     tabPanel("Cooks Method",
                              plotlyOutput(outputId = "cook"),
                              #checkboxInput(inputId = "Whichto", label = "Show standardized", value = FALSE),
                              
                              selectInput('Whichto', 'Select One', 
                                          choices = c('YJ Transformed Data','Not YJ Transformed Data'), 
                              ),
                              h4("The Outliers detected by Cook's distance are given below"),
                              DT::dataTableOutput(outputId = "YJCook"),
                              
                     ),
                     tabPanel("DBScan Method",
                              h4("The outliers detected through DBScan method are given below:"),
                              DT::dataTableOutput(outputId = "dbscan")
     
                     ),
                     tabPanel("SVM",
                              selectizeInput(inputId = "columnsvar", label = "Select variables:", choices = vchoices, multiple = TRUE, selected = vchoices,options = list(maxItems = 2L)),
                              plotOutput(outputId = "svmplot", hover = "distplot_hover1"),
                              textOutput("distplottext1"),
                              h4("The outliers are given below:"),
                              DT::dataTableOutput(outputId = "svm")
                              
                     )
                     
                 )),
        
        tabPanel("Inference and Results",
                 h4("There are several outlier detection methods. As part of this assignment we have tried to detect the novelty in one dataset using 6 techiques. Each techinque has detected outliers and few of outliers were detected in all the methods."),
                 
                 selectizeInput(inputId = "methods", label = "Select Method:", choices = tchoices, selected = vchoices,options = list(maxItems = 1L)),
                 DT::dataTableOutput(outputId = "table"),
                 h4("Result"),
                 h5("There is no perfect methodology for outlier detection that works for all problems. However, observation number 2,4, 8 appear in most of the methods. Therefore, we can declare these as Outliers")
                 )
        
                     
                 )
                 
                 
        
        
))
