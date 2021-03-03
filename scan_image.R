#!/usr/bin/env Rscript
suppressMessages(library(parallel))
suppressMessages(library(doParallel))
suppressMessages(library(dplyr))
suppressMessages(library(optparse))
suppressMessages(library(RSQLite))
suppressMessages(library(tools))
library(exifr)


option_list = list (
  make_option(opt_str = c("-c", "--csvout"), 
              help = paste0("the output path for the exif data ",
                            "(pipe delmitted, .csv file)")),
  make_option(opt_str = c("-r", "--rout"), 
              help = paste0("the output path for the exif data ",
                            "(R native, .RDS file)")),
  make_option(opt_str = c("-d", "--db"), 
              help = paste0("the output path for the exif data ",
                            "(sqlite database)")),
  make_option(opt_str = c("-t", "--tags"), 
              help = paste0("a set of exif tags to restrict the output to", 
                            "(comma separated list)")),
  make_option(opt_str = c("-x", "--cores"), default = 2, 
              help = paste0("cores to leave free during execution")),
  make_option(opt_str = c("-a", "--hash"), action = "store_true", 
              help = "calculate the md5 hash of the photos."),
  make_option(opt_str = c("-k", "--recursive"), action = "store_true", 
              help = "scan input directory recursively")
  
)
opt_parser = OptionParser(usage = paste0("usage: %prog [options] ",
                                         "scan_path"), 
                          option_list=option_list, prog = NULL, 
  description = paste0("\nThis script will scan a directory and create a table",
                       " with exif metadata for each photo found to an output ",
                       "format of choice.\n"))

opt = parse_args(opt_parser, positional_arguments = 1)


imagefiles<-list.files(path = opt$args[1], full.names = TRUE, 
                       pattern=c(".JPG|.jpg"), include.dirs = TRUE, 
                       recursive = opt$options$recursive)

use.mc <- switch(Sys.info()[['sysname']],
                 Windows= "PSOCK",
                 Linux  = "FORK",
                 Darwin = "FORK")
num.cores <- max(1, detectCores() - opt$options$cores)
cl <- makeCluster(num.cores, type = use.mc)
registerDoParallel(cl)
if(!is.null(opt$options$tags)){
  tags = strsplit(opt$options$tags, ",")
} else {
  tags <- NULL
}
imagefilesinfo  <- foreach(image = iter(imagefiles), 
                           .combine = bind_rows,
                           .packages = c("exifr", "dplyr", "tools")) %dopar% {
  if(!is.null(tags)){
    df <- read_exif(image, tags = tags)
  } else {
    df <- read_exif(image)
  }
  if(opt$options$hash == TRUE){
    hash <- md5sum(image)
    df <- mutate(df, md5hash = hash)
  }
  df
} 

if (!is.null(opt$options$csvout)) {
  cat(paste("Writing delimitted output to", opt$options$csvout, "\n"))
  write.table(imagefilesinfo, file = opt$options$csvout, row.names=FALSE, 
              na="", col.names=TRUE, sep="|")
}
if (!is.null(opt$options$rout)) {
  cat(paste("Writing RDS output to", opt$options$rout, "\n"))
  saveRDS(imagefilesinfo, file = opt$options$rout)
}
if (!is.null(opt$options$db)) {
  cat(paste("Writing sqlite output to", opt$options$db, "\n"))
  mydb <- dbConnect(RSQLite::SQLite(), opt$options$db)
  dbWriteTable(mydb, "photos", imagefilesinfo)
  dbDisconnect(mydb)
  unlink("my-db.sqlite")
}
cat("\nScript finished.\n")