args <- commandArgs(trailingOnly=TRUE)
data <- read.csv(args[1], header=TRUE)

sprintf("Total MD5sum mins: %.2f", sum(data$md5_minutes))
sprintf("Total upload mins: %.2f", sum(data$upload_minutes))
