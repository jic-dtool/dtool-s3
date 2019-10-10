args <- commandArgs(trailingOnly=TRUE)
data <- read.csv(args[1], header=TRUE)

sprintf("Minutes spent on MD5sum calculations: %.2f", sum(data$md5_minutes))
