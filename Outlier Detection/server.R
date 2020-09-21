#
# This is the server logic of a Shiny web application. You can run the
# application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#


# Define server logic required to draw a histogram
shinyServer(function(input, output) {
    

    #For AutoTest dataset
    output$AutoTest <- DT::renderDataTable({
        DT::datatable(data = auto_test)
    })
    
    #For given YJ dataset
    output$YJTest <- DT::renderDataTable({
        DT::datatable(data = yj_te)
    })
    
    #For Summary of autoTest data
    output$SummaryAT <- renderPrint({
        summary(auto_test)
    })
    #For Summary of YJtest data
    output$SummaryYT <- renderPrint({
        summary(yj_te)
    })
    
    
    
    #for auto plot
    output$PlotAuto <- renderPlot({
        colb <- as.numeric(input$columnnor)
        plot(density(auto_test[,colb]), main = "Before YJ transform")
    })
    
    #for YJ plot
    output$PlotYJ <- renderPlot({
        colv <- as.numeric(input$columnnor)
        fin <- yj_te[,colv]
        plot(density(fin[[1]]), main = "After YJ transform")
    })
    
    #for boxplot
    output$Boxplot <- renderPlotly({
        mydata <- input$WhichOne
        multiplier = 1.5
        if(mydata =="Not YJ Transformed Data"){
             cols <- as.numeric(input$columns)
             #numericCols <- unlist(lapply(auto_test[,cols], is.numeric)) 
             std <- scale(auto_test[,cols, drop = FALSE], center = input$standardise, scale = input$standardise)
             d <- tidyr::gather(as.data.frame(std))
             ggplot(mapping = aes(x = d$key, y = d$value, fill = d$key)) +
             geom_boxplot(coef = multiplier, outlier.colour = "blue") +
             labs(title = paste(" Uni-variable boxplots at IQR multiplier of", multiplier),
                 x = "Standardised variable value", y = "Std Value") +
             coord_flip()
        }
        else{
            cols <- as.numeric(input$columns)
            std <- scale(yj_te[,cols, drop = FALSE], center = input$standardise, scale = input$standardise)
            d <- tidyr::gather(as.data.frame(std))
            ggplot(mapping = aes(x = d$key, y = d$value, fill = d$key)) +
                geom_boxplot(coef = multiplier, outlier.colour = "blue") +
                labs(title = paste("YJ transformed Uni-variable boxplots at IQR multiplier of", multiplier),
                     x = "Standardised variable value", y = "Std Value") +
                coord_flip()
            
        }
    })

 #For Mahalnobis method
    output$maha <- renderPlotly({
        #my_yj <- c()
        my_yj <- (input$Which)
        
        if( my_yj == "YJ Transformed Data"){
            
            rec <- recipe(mpg ~ +displacement + horsepower + weight , data = auto_train) %>%
                step_naomit(everything()) %>% # remove obs that have missing values or impute as shown in next line
                step_nzv(all_predictors()) %>%  # remove near zero variance predictor variables
                step_lincomb(all_predictors()) %>%   # remove predictors that are linear combinations of other predictors
                step_YeoJohnson(all_predictors()) %>% # transform all remaining predictors
                prep(data = auto_train)
            
            processed_mah <- bake(rec, auto_test)
            processed_mah
            
            varMat <- var(processed_mah) # calculate the covariance matrix
            colM <- colMeans(processed_mah) # calculate variable means 
            md2 <- mahalanobis(x = processed_mah, center = colM, cov = varMat)
            
            threshold <- qchisq(p = 0.999, df = ncol(processed_mah))  # calculate a 99.9% threshold
            
            ggplot(mapping = aes(y = md2, x = (1:length(md2))/length(md2))) +
                geom_point() +
                scale_y_continuous(limits = c(0, max(md2)*1.1)) +
                labs(y = "Mahalanobis distance squared", x = "Complete Observations", title = "Outlier pattern") +
                geom_abline(slope = 0, intercept = threshold, color = "red") +
                scale_x_continuous(breaks = c(0, 0.5, 1), labels = c("0%", "50%", "100%")) +
                theme(legend.position = "bottom")
        }
        else{
            rec <- recipe(mpg ~ +displacement + horsepower + weight , data = auto_train) %>%
                step_naomit(everything()) %>% # remove obs that have missing values or impute as shown in next line
                #step_knnimpute(everything(), neighbors = 5) # use knn imputation 
                #step_rm(all_nominal())  #remove nominals if any present 
                step_nzv(all_predictors()) %>%  # remove near zero variance predictor variables
                step_lincomb(all_predictors()) %>%   # remove predictors that are linear combinations of other predictors
                #step_YeoJohnson(all_predictors()) %>% # transform all remaining predictors
                prep(data = auto_train)
            
            processed_mah <- bake(rec, auto_test)
            processed_mah
            
            varMat <- var(processed_mah) # calculate the covariance matrix
            colM <- colMeans(processed_mah) # calculate variable means 
            md2 <- mahalanobis(x = processed_mah, center = colM, cov = varMat)
            
            threshold <- qchisq(p = 0.999, df = ncol(processed_mah))  # calculate a 99.9% threshold
            
            ggplot(mapping = aes(y = md2, x = (1:length(md2))/length(md2))) +
                geom_point() +
                scale_y_continuous(limits = c(0, max(md2)*1.1)) +
                labs(y = "Mahalanobis distance squared", x = "Complete Observations", title = "Outlier pattern") +
                geom_abline(slope = 0, intercept = threshold, color = "red") +
                scale_x_continuous(breaks = c(0, 0.5, 1), labels = c("0%", "50%", "100%")) +
                theme(legend.position = "bottom")
            
        }
    })
    
    output$YJMaha <- DT::renderDataTable({
        my_yj <- (input$Which)
        
        if( my_yj == "YJ Transformed Data"){
            rec <- recipe(mpg ~ +displacement + horsepower + weight , data = auto_train) %>%
                step_naomit(everything()) %>% # remove obs that have missing values or impute as shown in next line
                step_nzv(all_predictors()) %>%  # remove near zero variance predictor variables
                step_lincomb(all_predictors()) %>%   # remove predictors that are linear combinations of other predictors
                step_YeoJohnson(all_predictors()) %>% # transform all remaining predictors
                prep(data = auto_train)
            
            processed_mah <- bake(rec, auto_test)
            processed_mah
            
            varMat <- var(processed_mah) # calculate the covariance matrix
            colM <- colMeans(processed_mah) # calculate variable means 
            md2 <- mahalanobis(x = processed_mah, center = colM, cov = varMat)
            md2 <- as.data.frame(md2)
            threshold <- qchisq(p = 0.999, df = ncol(processed_mah))  # calculate a 99.9% threshold
            
            out2data<- auto_test[which(md2[,1]> threshold),]
            
        }
        else{
            rec <- recipe(mpg ~ +displacement + horsepower + weight , data = auto_train) %>%
                step_naomit(everything()) %>% # remove obs that have missing values or impute as shown in next line
                #step_knnimpute(everything(), neighbors = 5) # use knn imputation 
                #step_rm(all_nominal())  #remove nominals if any present 
                step_nzv(all_predictors()) %>%  # remove near zero variance predictor variables
                step_lincomb(all_predictors()) %>%   # remove predictors that are linear combinations of other predictors
                #step_YeoJohnson(all_predictors()) %>% # transform all remaining predictors
                prep(data = auto_train)
            
            processed_mah <- bake(rec, auto_test)
            processed_mah
            
            varMat <- var(processed_mah) # calculate the covariance matrix
            colM <- colMeans(processed_mah) # calculate variable means 
            md2 <- mahalanobis(x = processed_mah, center = colM, cov = varMat)
            threshold <- qchisq(p = 0.999, df = ncol(processed_mah))  # calculate a 99.9% threshold
            md2 <- as.data.frame(md2)
            out2data<- auto_test[which(md2[,1]> threshold),]
            
        }
       
        DT::datatable(data = out2data)
    })
    
    # For Cooks
    
    output$cook <- renderPlotly({
            my_yj_data <- input$Whichto
            if( my_yj_data == 'YJ Transformed Data'){
                
                
                rec <- recipe(mpg ~ displacement + horsepower + weight , data = auto_train) %>%
                    step_naomit(everything()) %>% # remove obs that have missing values or impute as shown in next line
                    #step_knnimpute(everything(), neighbors = 5) # use knn imputation 
                    #step_rm(all_nominal())  #remove nominals if any present 
                    step_nzv(all_predictors()) %>%  # remove near zero variance predictor variables
                    step_lincomb(all_predictors()) %>%   # remove predictors that are linear combinations of other predictors
                    step_YeoJohnson(all_predictors()) %>% # transform all remaining predictors
                    prep(data = auto_train)
                
                processed_cook <- bake(rec, auto_test)
                processed_cook
                
                
                lmod <- glm(formula = mpg ~ ., data = processed_cook, family = gaussian) #since HHV is numeric
                dc <- cooks.distance(lmod)
                thresh <- 4 * mean(dc)  # this is an empirical way to assign a threshold
                dfcd <- data.frame(dc, id = 1:length(dc)/length(dc) )
                
                ggplot(data = dfcd, mapping = aes(y = dc, x = id)) +
                    geom_point() +
                    scale_y_continuous(limits = c(0, max(dfcd$dc)*1.1)) +
                    labs(y = "Cook's distance", x = "Complete Observations", title = "Outlier pattern") +
                    geom_abline(slope = 0, intercept = thresh, color = "red") +
                    scale_x_continuous(breaks = c(0, 0.5, 1), labels = c("0%", "50%", "100%")) +
                    theme(legend.position = "bottom")
            }
            else{
                
                rec <- recipe(mpg ~ displacement + horsepower + weight , data = auto_train) %>%
                    step_naomit(everything()) %>% # remove obs that have missing values or impute as shown in next line
                    #step_knnimpute(everything(), neighbors = 5) # use knn imputation 
                    #step_rm(all_nominal())  #remove nominals if any present 
                    step_nzv(all_predictors()) %>%  # remove near zero variance predictor variables
                    step_lincomb(all_predictors()) %>%   # remove predictors that are linear combinations of other predictors
                    prep(data = auto_train)
                
                processed_cook <- bake(rec, auto_test)
                processed_cook
                
                
                lmod <- glm(formula = mpg ~ ., data = processed_cook, family = gaussian) #since HHV is numeric
                dc <- cooks.distance(lmod)
                thresh <- 4 * mean(dc)  # this is an empirical way to assign a threshold
                dfcd <- data.frame(dc, id = 1:length(dc)/length(dc) )
                
                ggplot(data = dfcd, mapping = aes(y = dc, x = id)) +
                    geom_point() +
                    scale_y_continuous(limits = c(0, max(dfcd$dc)*1.1)) +
                    labs(y = "Cook's distance", x = "Complete Observations", title = "Outlier pattern") +
                    geom_abline(slope = 0, intercept = thresh, color = "red") +
                    scale_x_continuous(breaks = c(0, 0.5, 1), labels = c("0%", "50%", "100%")) +
                    theme(legend.position = "bottom")
                
            }
            
    })  
    
    output$YJCook <- DT::renderDataTable({
        my_yj_data <- input$Whichto
        if( my_yj_data == 'YJ Transformed Data'){
            
            
            rec <- recipe(mpg ~ displacement + horsepower + weight , data = auto_train) %>%
                step_naomit(everything()) %>% # remove obs that have missing values or impute as shown in next line
                #step_knnimpute(everything(), neighbors = 5) # use knn imputation 
                #step_rm(all_nominal())  #remove nominals if any present 
                step_nzv(all_predictors()) %>%  # remove near zero variance predictor variables
                step_lincomb(all_predictors()) %>%   # remove predictors that are linear combinations of other predictors
                step_YeoJohnson(all_predictors()) %>% # transform all remaining predictors
                prep(data = auto_train)
            
            processed_cook <- bake(rec, auto_test)
            processed_cook
            
            
            lmod <- glm(formula = mpg ~ ., data = processed_cook, family = gaussian) #since HHV is numeric
            dc <- cooks.distance(lmod)
            thresh <- 4 * mean(dc)  # this is an empirical way to assign a threshold
            dfcd <- data.frame(dc, id = 1:length(dc)/length(dc) )
            outdata<- auto_test[which(dfcd$dc> thresh),]
            
        }
        else{
            
            rec <- recipe(mpg ~ displacement + horsepower + weight , data = auto_train) %>%
                step_naomit(everything()) %>% # remove obs that have missing values or impute as shown in next line
                #step_knnimpute(everything(), neighbors = 5) # use knn imputation 
                #step_rm(all_nominal())  #remove nominals if any present 
                step_nzv(all_predictors()) %>%  # remove near zero variance predictor variables
                step_lincomb(all_predictors()) %>%   # remove predictors that are linear combinations of other predictors
                prep(data = auto_train)
            
            processed_cook <- bake(rec, auto_test)
            processed_cook
            
            
            lmod <- glm(formula = mpg ~ ., data = processed_cook, family = gaussian) #since HHV is numeric
            dc <- cooks.distance(lmod)
            thresh <- 4 * mean(dc)  # this is an empirical way to assign a threshold
            dfcd <- data.frame(dc, id = 1:length(dc)/length(dc) )
            outdata<- auto_test[which(dfcd$dc> thresh),]
            
        }
        DT::datatable(data = outdata)
        
    })
    
    
    output$bagplot <- renderPlot({
        col <- as.numeric(input$columns1)
        col
        
        test <- auto_test[,col]
        
        bagplot(test[,1], test[,2], factor = 3, na.rm = FALSE, approx.limit = 300,
                show.outlier = TRUE, show.whiskers = TRUE,
                show.looppoints = TRUE, show.bagpoints = TRUE,
                show.loophull = TRUE, show.baghull = TRUE,
                create.plot = TRUE, add = FALSE, pch = 16, cex = 0.4,
                dkmethod = 2, precision = 1, verbose = FALSE,
                debug.plots = "no", col.loophull="#aaccff",
                col.looppoints="#3355ff", col.baghull="#7799ff",
                col.bagpoints="#000088", transparency=FALSE,
                show.center = TRUE,
        )
            })   
    
    output$distplottext<- renderText({
        req(input$distplot_hover)
        col <- as.numeric(input$columns1)
        test <- auto_test[,col]
        
        sd <- colnames(test)
        
        data <- nearPoints(test, input$distplot_hover, xvar = sd[1], yvar = sd[2], maxpoints = 1,  allRows = TRUE)
        print( paste("Point Row Identifier:",rownames(data[data$selected_,])) )
        
    })
    
    
    
    output$Bag_Out <- DT::renderDataTable({
        col <- as.numeric(input$columns1)
        test <- auto_test[,col]
        DT::datatable(data = data.frame((compute.bagplot(test[,1], test[,2])["pxy.outlier"])), options = list(scrollX = T))
    })
        

    output$dbscan <- DT::renderDataTable({
        rec <- recipe(mpg ~ +displacement + horsepower + weight , data = auto_train) %>%
            step_naomit(everything()) %>% # remove obs that have missing values or impute as shown in next line
            #step_knnimpute(everything(), neighbors = 5) # use knn imputation 
            #step_rm(all_nominal())  #remove nominals if any present 
            step_nzv(all_predictors()) %>%  # remove near zero variance predictor variables
            step_lincomb(all_predictors()) %>%   # remove predictors that are linear combinations of other predictors
            #step_YeoJohnson(all_predictors()) %>% # transform all remaining predictors
            prep(data = auto_train)
        
        processed <- bake(rec, auto_test)
        dbscan::kNNdistplot(processed, k = 1)
        abline(h = 80, lty = 2)
        
        clustered <- dbscan::dbscan(processed, eps = 80, minPts = 4)
        clustered
        
        processed[clustered$cluster == 0,]
        clustered <- dbscan::dbscan(processed, eps = 80, minPts = 4)
        DT::datatable(data = processed[clustered$cluster == 0,])
    
})
    output$Box_Out <- DT::renderDataTable({
        DT::datatable(data = auto_test[c(2,3,5,8,26,39,72,73),])
    })

    
    output$svm <- DT::renderDataTable({

        model <- e1071::svm(auto_test, y = NULL, type = 'one-classification', nu = 0.10, scale = TRUE, kernel = "radial")
        
        good <- predict(model, auto_test)
        x<- auto_test[!good,]
        y <- auto_test[good,]
        DT::datatable(data = x)
    })
    
    output$svmplot <- renderPlot({
        
        model <- e1071::svm(auto_test, y = NULL, type = 'one-classification', nu = 0.10, scale = TRUE, kernel = "radial")
        
        good <- predict(model, auto_test)
        x<- auto_test[!good,]
        y <- auto_test[good,]
        col <- as.numeric(input$columnsvar)
        xvar <- x[,col]
        yvar <- y[,col]
        
        get_col  <- colnames(xvar)
        col1 <- get_col[1]
        col2 <- get_col[2]
        plot(xvar[,1],xvar[,2],col = 'red', xlab = col1,ylab = col2, main = "Scatter plot for outliers detected  in SVM method")
        points(yvar[,1],yvar[,2],col= 'blue')
        
    })
    
    output$distplottext1<- renderText({
        
        xy_str <- function(e) {
            if(is.null(e)) return("NULL\n")
            paste0("x=", round(e$x, 1), " y=", round(e$y, 1), "\n")
        }
        
        paste0(
            "hover: ", xy_str(input$distplot_hover1)
        )
    })
    output$table <- DT::renderDataTable({
        
        novelty <- input$methods
        if (novelty == "BoxPlot"){
            DT::datatable(data = auto_test[c(2,3,5,8,26,39,72,73),])
            
        }
        else if(novelty == "BagPlot"){
            DT::datatable(data = auto_test[c(2,4,5,8,49),])
            
        }
        
        else if(novelty == "Mahalanobis"){
            DT::datatable(data = auto_test[c(5,18),])
            
        }
        else if(novelty == "Cooks"){
            DT::datatable(data = auto_test[c(2,5,14,16,18,31,49,75,100,101),])
            
        }
        else if(novelty == "DBScan"){
            DT::datatable(data = auto_test[c(2,5,8,13,14,20,17,24,26,32,36,39,42,50,51,52,53,70,72,73,76,78,87),])
            
        }
        else if(novelty == "SVM"){
            DT::datatable(data = auto_test[c(2,5,8,14,18,31,32,49,75,80,93,100,101),])
            
        }
    })
})