##FOr running via command line by moving into that directory just type
##Rscript RCode.R ETLoptionsEOD filename.csv.zip ETLoptionsStatsEOD filename.csv.zip ETLStockQuotesEOD filename.csv.zip
##or by
##Rscript ETLoptionsStatsEOD filename.csv.zip ETLStockQuotesEOD filename.csv.zip 
##or by
##Rscript ETLoptionsEOD filename.csv.zip

rm(list=ls(all=TRUE))
options(warn=-1)
if(!require(RCurl)){
        install.packages("RCurl")
        library(RCurl)
}
if(!require(plyr)){
        install.packages("plyr")
        library(plyr)
}
if(!require(RMySQL)){
        install.packages("RMySQL")
        library(RMySQL)
}

##function for mapping cmd input 
CmdMapOutput<-function(input)
{
        output<-0
        if(input == "ETLoptionsEOD")
                {output<-1}
        else if(input == "ETLoptionsStatsEOD")
               { output<-2}
        else if(input == "ETLStockQuotesEOD")
                {output<-3}
        return (output)           
}

CmdFetchInput<-function(args,tempCounter,checkIndex){
        if(!is.na(args[tempCounter+1])){
                if(CmdMapOutput(args[tempCounter+1])==checkIndex){
                        return (T)
                }
        }
        return (F)
}

##Reading configuration Files
config<-read.csv("conf.txt",header = F)

user<-config[2,2]
password <-config[3,2]
host <- config[1,2]
userpwd <- paste(user, password, sep=":")
serverDir<-config[4,2]
downloadDir<-config[5,2]

##Connecting to database
conn = dbConnect(MySQL(), user=as.character(config[7,2]), password=as.character(config[8,2]), dbname=as.character(config[9,2]), host=as.character(config[6,2]))

FtpDownload<-function(fileName){
         file<-try(getBinaryURL(paste(host,paste("/",serverDir,"/",fileName,sep = ""),sep=""), userpwd = userpwd))
         return (file)
}

SaveToFile<-function(downloadDir,fileName,dat_options){
        con <- file(paste(downloadDir,fileName,sep = "/"), open = "wb")
        writeBin(dat_options, con)
        close(con)
}

UnzipAndRead<-function(downloadDir,fileName){
        data<-read.csv(unz(paste(downloadDir,fileName,sep = "/"),filename = substr(fileName,1,nchar(fileName)-4)), row.names=NULL,sep=",",header = F)
        return (data)
}

QueryPreProcess<-function(sql,sqlPreProcessStatement){
        tempsql<-substr(sql,1,nchar(sql)-1)
        tempsql<-paste(sqlPreProcessStatement,tempsql,sep="")
        return (tempsql)
}
TransformEtlOptionsEod<-function(input){
        dat_options<-input
        dat_options[,5]<-revalue(dat_options[,5], c("call"="C", "put"="P"))
        dat_options[6] <- lapply(dat_options[6], as.character)
        dat_options[7] <- lapply(dat_options[7], as.character)
        
        for(i in 1:nrow(dat_options)){
                if(!is.na(dat_options[,6][i])){
                        temp<-unlist(strsplit(dat_options[,6][i],"/"))
                        dat_options[,6][i]<-paste(temp[3],sprintf("%02d", as.numeric(temp[1])+1),sprintf("%02d", as.numeric(temp[2])),sep = "-")
                }
                if(!is.na(dat_options[,7][i])){
                        temp<-unlist(strsplit(substr(dat_options[,7][i],1,nchar(dat_options[,7][i])-12),"/"))
                        dat_options[,7][i]<-paste(temp[3],sprintf("%02d", as.numeric(temp[1])+1),sprintf("%02d", as.numeric(temp[2])),sep = "-")
                }
        }
        dat_options[19]<-Sys.Date()
        return (dat_options)
}
TransformStatsEod<-function(input){
        dat_options_stats<-input
        dat_options_stats[2]<- lapply(dat_options_stats[2], as.character)
        
        for(i in 1:nrow(dat_options_stats)){
                if(!is.na(dat_options_stats[,2][i])){
                        dat_options_stats[,2][i]<-paste(substr(dat_options_stats[,2][i],1,4),"-",substr(dat_options_stats[,2][i],5,6),"-",substr(dat_options_stats[,2][i],7,8),sep = "")
                }   
        }
        dat_options_stats[10]<-Sys.Date()
        return (dat_options_stats)
}
TransformQuotesEod<-function(input){
        dat_eod_stock_quotes<-input
        dat_eod_stock_quotes[2]<- lapply(dat_eod_stock_quotes[2], as.character)
        
        for(i in 1:nrow(dat_eod_stock_quotes)){
                if(!is.na(dat_eod_stock_quotes[,2][i])){
                        temp<-unlist(strsplit(dat_eod_stock_quotes[,2][i],"/"))
                        dat_eod_stock_quotes[,2][i]<-paste(temp[3],sprintf("%02d", as.numeric(temp[1])+1),sprintf("%02d", as.numeric(temp[2])),sep = "-")
                }
        }
        dat_eod_stock_quotes[8]<-Sys.Date()
        return (dat_eod_stock_quotes)
}

EtlOptionsEod <- function(fileName) {
        ##First File Download
        
        message("Attempting to connect to ftp server and downloading first file")
        dat_options <- FtpDownload(fileName)
        message(paste("Downloaded",fileName,"!"))
        if(substr(dat_options,1,5)=="Error"){
                stop("Unable to fetch file eod_options")
        }else{
                
                message(paste("Saving",fileName,"!"))
                SaveToFile(downloadDir,fileName,dat_options)
                message(paste("Save",fileName,"Successful"))
                
                dat_options<-UnzipAndRead(downloadDir,fileName)
                
                
                dat_options=dat_options[-c(20,5)]
                
                
                
                ##Transformation of first file
                message(paste("Starting transformation",fileName,"."))
                dat_options<-TransformEtlOptionsEod(dat_options)
                message(paste("Transformed",nrow(dat_options),"Rows of",fileName))
                
                
                message(paste("Now starting upload",nrow(dat_options),"rows of",fileName))
                
                options_eod_table<-config[10,2]
                sqlPreProcessStatement<-paste("INSERT INTO `",options_eod_table,"`(`underlying`, `underlying_last_price`, `exchange`, `option_symbol`,
                                      `option_type`, `expiration_date`, `quote_date`, `strike_price`, `last_price`, 
                                      `bid_price`, `ask_price`, `volume`, 
                                      `open_interest`, `implied_volatility`, `delta`, `gamma`, `theta`, `vega`, 
                                      `data_load_date`) VALUES ",sep="")
                sql<-""
                for(i in 1:nrow(dat_options)){
                        sql <- paste0(sql,"(\"",
                                      dat_options[,1][i],"\", ",
                                      dat_options[,2][i], ", ",
                                      "\"", dat_options[,3][i],"\", ",
                                      "\"", dat_options[,4][i],"\", ",
                                      "\"", dat_options[,5][i],"\", ",
                                      "\"", dat_options[,6][i],"\", ",
                                      "\"", dat_options[,7][i],"\", ",
                                      "", dat_options[,8][i],", ",
                                      "", dat_options[,9][i],", ",
                                      "", dat_options[,10][i],", ",
                                      "", dat_options[,11][i],", ",
                                      "", dat_options[,12][i],", ",
                                      "", dat_options[,13][i],", ",
                                      "", dat_options[,14][i],", ",
                                      "", dat_options[,15][i],", ",
                                      "", dat_options[,16][i],", ",
                                      "", dat_options[,17][i],", ",
                                      "", dat_options[,18][i],", ",
                                      "\"", dat_options[,19][i],"\" ",
                                      "),") 
                }
                sql<-QueryPreProcess(sql,sqlPreProcessStatement)
                dbSendQuery(conn, sql)
                message(paste("Uploaded",nrow(dat_options),"rows of",fileName))
                rm(dat_options)
        }
}


EtlOptionsStatsEOD<-function(fileName) {
        ##Second File Download
        
        message("Attempting to download second file")
        dat_options_stats <- FtpDownload(fileName)
        message(paste("Downloaded",fileName,"!"))
        if(substr(dat_options_stats,1,5)=="Error"){
                stop("Unable to fetch file eod_options_stat")
        }else{
                
                message(paste("Saving",fileName,"!"))    
                
                SaveToFile(downloadDir,fileName,dat_options_stats)
                message(paste("Save",fileName,"Successful"))
                dat_options_stats<-UnzipAndRead(downloadDir,fileName)
                
                
                ##Transformation of second file
                message(paste("Starting transformation",fileName,"."))
                dat_options_stats<-TransformStatsEod(dat_options_stats)
                message(paste("Transformed",nrow(dat_options_stats),"Rows of",fileName))
                message(paste("Now starting upload",nrow(dat_options_stats),"rows of",fileName))
                
                optionstats_eod_table<-config[11,2]
                sqlPreProcessStatement<-paste("INSERT INTO `",optionstats_eod_table,"` ( `underlying`, `quote_date`, 
                                      `call_iv`, `put_iv`, `mean_iv`, `call_vol`, `call_open_interest`, 
                                      `put_open_interest`, `data_load_date`)
                                      VALUES ",sep="")
                sql<-""
                for(i in 1:nrow(dat_options_stats)){
                        sql <- paste0(sql," (\"",
                                      dat_options_stats[,1][i],"\", ",
                                      "\"",dat_options_stats[,2][i],"\", ",
                                      "", dat_options_stats[,3][i],", ",
                                      "", dat_options_stats[,4][i],", ",
                                      "", dat_options_stats[,5][i],", ",
                                      "", dat_options_stats[,6][i],", ",
                                      "", dat_options_stats[,7][i],", ",
                                      "", dat_options_stats[,8][i],", ",
                                      "\"", dat_options_stats[,10][i],"\" ",
                                      "),")       
                }
                sql<-QueryPreProcess(sql,sqlPreProcessStatement)
                dbSendQuery(conn, sql)
                message(paste("Uploaded",nrow(dat_options_stats),"rows of",fileName))
                rm(dat_options_stats)
        }
}

EtlStockQuotesEOD<-function(fileName){
        ##Third File Download
        
        
        message("Attempting to download the third file")
        dat_eod_stock_quotes <- FtpDownload(fileName)
        message(paste("Downloaded",fileName,"!"))
        if(substr(dat_eod_stock_quotes,1,5)=="Error"){
                stop("Unable to fetch file eod_stock_quotes")
        }else{
                message(paste("Saving",fileName,"!")) 
                
                SaveToFile(downloadDir,fileName,dat_eod_stock_quotes)
                message(paste("Save",fileName,"Successful"))
                dat_eod_stock_quotes<-UnzipAndRead(downloadDir,fileName)
                
                
                
                ##Transformation of third file
                message(paste("Starting transformation",fileName,"."))
                dat_eod_stock_quotes<-TransformQuotesEod(dat_eod_stock_quotes)
                message(paste("Transformed",nrow(dat_eod_stock_quotes),"Rows of",fileName))
                ##Database uploading
                
                message(paste("Now starting upload",nrow(dat_eod_stock_quotes),"rows of",fileName))
                stockquotes_eod_table<-config[12,2]
                sqlPreProcessStatement<-paste("INSERT INTO `",stockquotes_eod_table,"` ( `symbol`, 
                                      `quote_date`, `open`, `high`, `low`, `close`, `volume`, `data_load_date`) 
                                      VALUES ",sep="")
                sql<-""
                for(i in 1:nrow(dat_eod_stock_quotes)){
                        sql <- paste0(sql," (\"",
                                      dat_eod_stock_quotes[,1][i],"\", ",
                                      "\"",dat_eod_stock_quotes[,2][i],"\", ",
                                      "", dat_eod_stock_quotes[,3][i],", ",
                                      "", dat_eod_stock_quotes[,4][i],", ",
                                      "", dat_eod_stock_quotes[,5][i],", ",
                                      "", dat_eod_stock_quotes[,6][i],", ",
                                      "", dat_eod_stock_quotes[,7][i],", ",
                                      "\"", dat_eod_stock_quotes[,8][i],"\" ",
                                      "),")
                }
                sql<-QueryPreProcess(sql,sqlPreProcessStatement)
                dbSendQuery(conn, sql)
                message(paste("Uploaded",nrow(dat_eod_stock_quotes),"rows of",fileName))
                
        } 
}

##Reading CMD Arguments
args <- commandArgs(TRUE)

tempCounter<-0

##Function for ETL Options EOD
if(CmdFetchInput(args,tempCounter,1)){
        tempCounter<-tempCounter+2
        EtlOptionsEod(args[tempCounter])        
}else{
        EtlOptionsEod("eod_options.csv.zip")
}

##Function for ETL Options Stats EOD
if(CmdFetchInput(args,tempCounter,2)){
        tempCounter<-tempCounter+2
        EtlOptionsStatsEOD(args[tempCounter])
}else{
        EtlOptionsStatsEOD("eod_options_stat.csv.zip")
}

##Function for ETL Stock Quotes EOD
if(CmdFetchInput(args,tempCounter,3)){
        tempCounter<-tempCounter+2
        EtlStockQuotesEOD(args[tempCounter])
}else{
        EtlStockQuotesEOD("eod_stock_quotes.csv.zip")
}

##Disconnecting from Database
dbDisconnect(conn)

message("All done!");
options(warn=0)