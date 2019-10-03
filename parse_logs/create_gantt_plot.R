library(ggplot2)
library(scales)

args <- commandArgs(trailingOnly=TRUE)
data <- read.csv(args[1], header=TRUE)
data$ID <- seq.int(nrow(data))
data$num_retries <- as.factor(data$num_retries)
data$upload_start <- as.POSIXct(data$upload_start, origin="1970-01-01", tz="GMT")
data$upload_end <- as.POSIXct(data$upload_end, origin="1970-01-01", tz="GMT")
data$size_in_gib <- data$size_in_bytes / (1024 * 1024 * 1024)

g <- ggplot(data=data, aes(x=upload_start, xend=upload_end, y=ID, yend=ID, size=size_in_gib, colour=num_retries))
g <- g + geom_segment()
g <- g + scale_x_datetime(labels=date_format("%H:%M:%S"))
g <- g + labs(
    title="ECS upload performance",
    x="Time",
    y="Item ID",
    size="Size (GiB)",
    colour="Retries"
)

ggsave("gantt.png")
