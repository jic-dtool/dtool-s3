args <- commandArgs(trailingOnly=TRUE)
data <- read.csv(args[1], header=TRUE)

png("upload_time_histogram.png")
hist(
    data$upload_minutes,
    breaks=20,
    main="Histogram of upload times",
    xlab="Minutes"
)
dev.off()

png("md5sum_time_histogram.png")
hist(
    data$md5_minutes,
    breaks=20,
    main="Histogram of md5sum calculation times",
    xlab="Minutes"
)
dev.off()

png("upload_time_vs_size_in_bytes.png")
plot(
    data$size_in_bytes,
    data$upload_minutes,
    main="Relation between size and upload time",
    xlab="Size in bytes",
    ylab="Minutes"
)
dev.off()

png("md5sum_time_vs_size_in_bytes.png")
plot(
    data$size_in_bytes,
    data$md5_minutes,
    main="Relation between size and md5sum calculation time",
    xlab="Size in bytes",
    ylab="Minutes"
)
dev.off()
