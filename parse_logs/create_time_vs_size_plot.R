library(ggplot2)

args <- commandArgs(trailingOnly=TRUE)
data <- read.csv(args[1], header=TRUE)

g <- ggplot(data=data, aes(x=size_in_bytes, y=upload_minutes, color=num_retries))
g <- g + geom_jitter()

ggsave("time_vs_size.png")
