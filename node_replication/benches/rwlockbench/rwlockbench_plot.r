# Copyright © 2017-2019 Jon Gjengset <jon@thesquareplanet.com>.
# Copyright © 2019 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0 OR MIT

v = read.table(file("rwlockbench_results.log"))
t <- data.frame(readers=v[,1], writers=v[,2], variant=v[,3], opss=v[,4], op=v[,6], id=v[,7])

library(plyr)
t$writers = as.factor(t$writers)
t$readers = as.numeric(t$readers)

r = t[t$op == "read",]
r <- ddply(r, c("readers", "writers", "variant", "op"), summarise, opss = sum(opss))
w = t[t$op == "write",]
w <- ddply(w, c("readers", "writers", "variant", "op"), summarise, opss = sum(opss))

library(ggplot2)

r$opss = r$opss / 1000000.0
p <- ggplot(data=r, aes(x=readers, y=opss, color=variant))
p <- p + xlim(c(0, NA))
p <- p + facet_grid(. ~ writers, labeller = labeller(writers = label_both))
p <- p + geom_point(size = .4, alpha = .1)
p <- p + geom_line(size = .5)
p <- p + xlab("readers") + ylab("M reads/s") + ggtitle("Total reads/s with increasing # of readers")
ggsave('rwlock-read-throughput.png',plot=p,width=10,height=6)

w$opss = w$opss / 1000000.0
p <- ggplot(data=w, aes(x=readers, y=opss, color=variant))
p <- p + facet_grid(. ~ writers, labeller = labeller(writers = label_both))
p <- p + geom_point(size = 1, alpha = .2)
p <- p + geom_line(size = .5)
p <- p + xlim(c(0, NA))
p <- p + xlab("readers") + ylab("M writes/s") + ggtitle("Total writes/s with increasing # of readers")
ggsave('rwlock-write-throughput.png',plot=p,width=10,height=6)
