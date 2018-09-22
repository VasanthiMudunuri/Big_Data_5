#!/usr/bin/env Rscript
f <- file("stdin")
open(f)
while(length(line <- readLines(f,n=1)) > 0) {
# process line
	contents <- Map(as.numeric,strsplit(line, ",")) #splitting line
	Average <- contents[[1]][1]/contents[[1]][2] #performing average
	write(Average, stdout())
}
