rm(list=ls(all=TRUE))

library(RCurl)
library(plyr)
library(RMySQL)

user<-commandArgs(TRUE)[2]
password <-commandArgs(TRUE)[3]
host <- commandArgs(TRUE)[1]
userpwd <- paste(user, password, sep=":")
serverDir<-commandArgs(TRUE)[4]
downloadDir<-commandArgs(TRUE)[5]


##First File Download
fileName<-"eod_options.csv.zip"
message("Attemting to connect to ftp server")
dat_options <- try(getBinaryURL(paste(host,paste("/",serverDir,"/",fileName,sep = ""),sep=""), userpwd = userpwd))
message(paste("Downloaded",fileName,"!"))
if(substr(dat_options,1,5)=="Error"){
        stop("Unable to fetch file eod_options")
}else{

message(paste("Saving",fileName,"!"))
con <- file(paste(downloadDir,fileName,sep = "/"), open = "wb")
writeBin(dat_options, con)
close(con)
message(paste("Save",fileName,"Successful"))

dat_options<-read.csv(unz(paste(downloadDir,fileName,sep = "/"),filename = substr(fileName,1,nchar(fileName)-4)), row.names=NULL,sep=",",header = F)


dat_options=dat_options[-c(20,5)]



##Transformation of first file
message(paste("Now start transforming",fileName,"."))
dat_options[,5] <-revalue(dat_options[,5], c("call"="C", "put"="P"))
dat_options[6]<- lapply(dat_options[6], as.character)
dat_options[7]<- lapply(dat_options[7], as.character)

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

message(paste("Transformed",nrow(dat_options),"Rows of",fileName))
dat_options[19]<-Sys.Date()

message(paste("Now starting uploading",nrow(dat_options),"rows of",fileName))
DbHost<-commandArgs(TRUE)[6]
DbUser<-commandArgs(TRUE)[7]
DbPassword<-commandArgs(TRUE)[8]
DbSelDatabase<-commandArgs(TRUE)[9]
conn = dbConnect(MySQL(), user='root', password='', dbname='test', host='localhost')

options_eod_table<-commandArgs(TRUE)[10]

for(i in 1:nrow(dat_options)){
        sql <- paste0("INSERT INTO `",options_eod_table,"`(`underlying`, `underlying_last_price`, `exchange`, `option_symbol`,
                      `option_type`, `expiration_date`, `quote_date`, `strike_price`, `last_price`, 
                      `bid_price`, `ask_price`, `volume`, 
                      `open_interest`, `implied_volatility`, `delta`, `gamma`, `theta`, `vega`, `data_load_date`) 
                      VALUES
                      (\"",
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
                      ");")
        
        dbSendQuery(conn, sql)
}
message(paste("Uploaded",nrow(dat_options),"rows of",fileName))
rm(dat_options)



##Second File Download
fileName<-"eod_options_stat.csv.zip"
message("Attemting for second file")
dat_options_stats <- try(getBinaryURL(paste(host,paste("/",serverDir,"/",fileName,sep = ""),sep=""), userpwd = userpwd))
message(paste("Downloaded",fileName,"!"))
if(substr(dat_options_stats,1,5)=="Error"){
        stop("Unable to fetch file eod_options_stat")
}else{

        
       
        
message(paste("Saving",fileName,"!"))    
con <- file(paste(downloadDir,fileName,sep = "/"), open = "wb")
writeBin(dat_options_stats, con)
close(con)
message(paste("Save",fileName,"Successful"))
dat_options_stats<-read.csv(unz(paste(downloadDir,fileName,sep = "/"),filename = substr(fileName,1,nchar(fileName)-4)), row.names=NULL,sep=",",header = F)


##Transformation of second file
message(paste("Now start transforming",fileName,"."))
dat_options_stats[2]<- lapply(dat_options_stats[2], as.character)

for(i in 1:nrow(dat_options_stats)){
        if(!is.na(dat_options_stats[,2][i])){
        dat_options_stats[,2][i]<-paste(substr(dat_options_stats[,2][i],1,4),"-",substr(dat_options_stats[,2][i],5,6),"-",substr(dat_options_stats[,2][i],7,8),sep = "")
        }   
}
dat_options_stats[10]<-Sys.Date()
message(paste("Transformed",nrow(dat_options_stats),"Rows of",fileName))
message(paste("Now starting uploading",nrow(dat_options_stats),"rows of",fileName))

optionstats_eod_table<-commandArgs(TRUE)[11]
for(i in 1:nrow(dat_options_stats)){
        sql <- paste0("INSERT INTO `",optionstats_eod_table,"`( `underlying`, `quote_date`, 
                        `call_iv`, `put_iv`, `mean_iv`, `call_vol`, `call_open_interest`, 
                        `put_open_interest`, `data_load_date`)
                      VALUES
                      (\"",
                      dat_options_stats[,1][i],"\", ",
                      "\"",dat_options_stats[,2][i],"\", ",
                      "", dat_options_stats[,3][i],", ",
                      "", dat_options_stats[,4][i],", ",
                      "", dat_options_stats[,5][i],", ",
                      "", dat_options_stats[,6][i],", ",
                      "", dat_options_stats[,7][i],", ",
                      "", dat_options_stats[,8][i],", ",
                      "\"", dat_options_stats[,10][i],"\" ",
                      ");")
        
        dbSendQuery(conn, sql)
}
message(paste("Uploaded",nrow(dat_options_stats),"rows of",fileName))
rm(dat_options_stats)


##Third FIle Downlaod
fileName<-"eod_stock_quotes.csv.zip"
message("Attemting for third file")
dat_eod_stock_quotes <- try(getBinaryURL(paste(host,paste("/",serverDir,"/",fileName,sep = ""),sep=""), userpwd = userpwd))
message(paste("Downloaded",fileName,"!"))
if(substr(dat_eod_stock_quotes,1,5)=="Error"){
        stop("Unable to fetch file eod_stock_quotes")
}else{
message(paste("Saving",fileName,"!")) 
con <- file(paste(downloadDir,fileName,sep = "/"), open = "wb")
writeBin(dat_eod_stock_quotes, con)
close(con)
message(paste("Save",fileName,"Successful"))
dat_eod_stock_quotes<-read.csv(unz(paste(downloadDir,fileName,sep = "/"),filename = substr(fileName,1,nchar(fileName)-4)), row.names=NULL,sep=",",header = F)



##Transformation of third file
message(paste("Now start transforming",fileName,"."))
dat_eod_stock_quotes[2]<- lapply(dat_eod_stock_quotes[2], as.character)

for(i in 1:nrow(dat_eod_stock_quotes)){
        if(!is.na(dat_eod_stock_quotes[,2][i])){
        temp<-unlist(strsplit(dat_eod_stock_quotes[,2][i],"/"))
        dat_eod_stock_quotes[,2][i]<-paste(temp[3],sprintf("%02d", as.numeric(temp[1])+1),sprintf("%02d", as.numeric(temp[2])),sep = "-")
        }
}
dat_eod_stock_quotes[8]<-Sys.Date()
message(paste("Transformed",nrow(dat_eod_stock_quotes),"Rows of",fileName))
##Database uploading

message(paste("Now starting uploading",nrow(dat_eod_stock_quotes),"rows of",fileName))
stockquotes_eod_table<-commandArgs(TRUE)[12]
for(i in 1:nrow(dat_eod_stock_quotes)){
        sql <- paste0("INSERT INTO `",stockquotes_eod_table,"`( `symbol`, 
                        `quote_date`, `open`, `high`, `low`, `close`, `volume`, `data_load_date`) 
                      VALUES
                      (\"",
                      dat_eod_stock_quotes[,1][i],"\", ",
                      "\"",dat_eod_stock_quotes[,2][i],"\", ",
                      "", dat_eod_stock_quotes[,3][i],", ",
                      "", dat_eod_stock_quotes[,4][i],", ",
                      "", dat_eod_stock_quotes[,5][i],", ",
                      "", dat_eod_stock_quotes[,6][i],", ",
                      "", dat_eod_stock_quotes[,7][i],", ",
                      "\"", dat_eod_stock_quotes[,8][i],"\" ",
                      ");")
        
        dbSendQuery(conn, sql)
}
message(paste("Uploaded",nrow(dat_eod_stock_quotes),"rows of",fileName))
dbDisconnect(conn)
}
}
}

message("All done!");