#Start sparkR with bin/SparkR

# Code for Reading and writing Spark DataFrames section
csvPath <- "file:///Users/aurobindosarkar/Downloads/bank-additional/bank-additional-full.csv"
df <- read.df(csvPath, "csv", header = "true", inferSchema = "true", na.strings = "NA", delimiter= ";")
write.df(df, path = "hdfs://localhost:9000/Users/aurobindosarkar/Downloads/df.parquet", source = "parquet", mode = "overwrite")

#Code for Exploring structure and contents of Spark DataFrames section
persist(df, "MEMORY_ONLY")
df
printSchema(df)
names(df)
str(df)
head(df, 2)
dim(df)
count(df)
nrow(df)
count(distinct(select(df, df$age)))

#Code for Running basic operations on Spark DataFrames section
head(select(df, df$education))
head(select(df, "education"))
subsetMarried <- subset(df, df$marital == "married")
head(subsetMarried, 2)
head(filter(df, df$education == "basic.4y"), 2)
maritaldf <- agg(groupBy(df, df$marital), count = n(df$marital))
head(maritaldf)
maritalCounts <- summarize(groupBy(df, df$marital), count = n(df$marital))
nMarriedCategories <- count(maritalCounts)
head(arrange(maritalCounts, desc(maritalCounts$count)), num = nMarriedCategories)
library(magrittr)
educationdf <- filter(df, df$education == "basic.4y") %>%  groupBy(df$marital) %>%  summarize(count = n(df$marital))
head(educationdf)
collect(summarize(df,avg_age = mean(df$age)))

ls1df <- collect(sample(df, FALSE, 0.1, 11L))
nrow(df)
nrow(ls1df)
df$durationMins <- round(df$duration / 60)
head(df, 2)

#Code for Executing SQL statements on Spark DataFrames section
createOrReplaceTempView(df, "customer")
sqldf <- sql("SELECT education, age, marital, housing, loan FROM customer WHERE age >= 13 AND age <= 19")
head(sqldf)

#Code for Merging SparkR DataFrames section
library(magrittr)
csvPath <- "file:///Users/aurobindosarkar/Downloads/CommViolPredUnnormalizedData.csv"
df <- read.df(csvPath, "csv", header = "false", inferSchema = "false", na.strings = "NA", delimiter= ",")
crimesStatesSubset = subset(df, select = c(1,2, 130, 132, 134, 136, 138, 140, 142, 144))
head(crimesStatesdf, 2)
state_names <- read.df("file:///Users/aurobindosarkar/downloads/csv_hus/states.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA", delimiter= ",")
names(crimesStatesdf)
names(state_names)
m1df <- merge(crimesStatesdf, state_names)
head(m1df, 2)
m2df <- merge(crimesStatesdf, state_names, by = "code")
head(m2df, 2)

library(magrittr)
usPath <- "file:///Users/aurobindosarkar/Downloads/Tennis-Major-Tournaments-Match-Statistics/USOpen-women-2013.csv"
usdf <- read.df(usPath, "csv", header = "true", inferSchema = "true", na.strings = "NA", delimiter= ",")
ussubdf <- select(usdf, "Player 1", "Player 2", "ROUND", "Result")%>% withColumnRenamed("Player 1", "p1") %>% withColumnRenamed("Player 2", "p2")
showDF(ussubdf, 2)
wimPath <- "file:///Users/aurobindosarkar/Downloads/Tennis-Major-Tournaments-Match-Statistics/Wimbledon-women-2013.csv"
wimdf <- read.df(wimPath, "csv", header = "true", inferSchema = "true", na.strings = "NA", delimiter= ",")
wimsubdf <- select(usdf, "Player 1", "Player 2", "ROUND", "Result")%>% withColumnRenamed("Player 1", "p1") %>% withColumnRenamed("Player 2", "p2") 
showDF(wimsubdf, 2)
(nA <- nrow(ussubdf))
(nB <- nrow(wimsubdf))
nA + nB
AintB <- intersect(ussubdf, wimsubdf)
nrow(AintB)
df1 <- rbind(ussubdf, wimsubdf)
showDF(df1, 203)

#Code for Using User Defined Functions (UDFs) section
csvPath <- "file:///Users/aurobindosarkar/Downloads/bank-additional/bank-additional-full.csv"
df <- read.df(csvPath, "csv", header = "true", inferSchema = "true", na.strings = "NA", delimiter= ";")

df1 <- select(df, df$duration)
schema <- structType(structField("duration", "integer"),
                     structField("durMins", "double"))
df1 <- select(df, df$duration, df$age)
schema <- structType(structField("age", "integer"), structField("maxDuration", "integer"))
result <- gapply(
    df1,
    "age",
    function(key, x) {
        y <- data.frame(key, max(x$duration))
    },
    schema)
head(collect(arrange(result, "maxDuration", decreasing = TRUE)))

#Code for Using SparkR for computing summary statistics section
sumstatsdf <- describe(df, "duration", "campaign", "previous", "age")
showDF(sumstatsdf)
avgagedf <- agg(df, mean = mean(df$age))

showDF(avgagedf) # Print this DF

agerangedf <- agg(df, minimum = min(df$age), maximum = max(df$age), range_width = abs(max(df$age) - min(df$age)))
showDF(agerangedf)
agevardf <- agg(df, variance = var(df$age))

showDF(agevardf)

agesddf <- agg(df, std_dev = sd(df$age))

showDF(agesddf)

df1 <- dropna(df, cols = "age")

quartilesdf <- approxQuantile(x = df1, col = "age", probabilities = c(0.25, 0.5, 0.75), relativeError = 0.001)

quartilesdf

ageskdf <- agg(df, skewness = skewness(df$age))

showDF(ageskdf)

agekrdf <- agg(df, kurtosis = kurtosis(df$age))

showDF(agekrdf)
covagedurdf <- cov(df, "age", "duration")

corragedurdf <- corr(df, "age", "duration", method = "pearson")

covagedurdf
corragedurdf
covagedurdf <- cov(df, "age", "duration")

corragedurdf <- corr(df, "age", "duration", method = "pearson")

covagedurdf
corragedurdf
contabdf <- crosstab(df, "job", "marital")

contabdf

#Code for Using SparkR for data visualization section
csvPath <- "file:///Users/aurobindosarkar/Downloads/bank-additional/bank-additional-full.csv"
df <- read.df(csvPath, "csv", header = "true", inferSchema = "true", na.strings = "NA", delimiter= ";")
persist(df, "MEMORY_ONLY")
agedf<-select(df, df$age)
histStats <- histogram(agedf, agedf$age, nbins = 5)
require(ggplot2)
plot <- ggplot(histStats, aes(x = centroids, y = counts)) +
        geom_bar(stat = "identity") +
        xlab("Age") + ylab("Frequency")
plot
ldf <- collect(select(df, df$age, df$duration, df$education, df$marital, df$job))
g1 <- ggplot(ldf, aes(x = marital))
g1 + geom_bar()
library(MASS)
par(mfrow=c(2,2))
truehist(ldf$"age", h = 5, col="slategray3", xlab="Age Groups(5 years)")
barplot((table(ldf$education)), names.arg=c("1", "2", "3", "4", "5", "6", "7", "8"), col=c("slateblue", "slateblue2", "slateblue3", "slateblue4", "slategray", "slategray2", "slategray3", "slategray4"), main="Education")
barplot((table(ldf$marital)), names.arg=c("Divorce", "Married", "Single", "Unknown"), col=c("slategray", "slategray1", "slategray2", "slategray3"), main="Marital Status")
barplot((table(ldf$job)), , names.arg=c("1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c"), main="Job")
g2 <- ggplot(ldf, aes(x = marital, fill = education))
g2 + geom_bar(position = "fill")
g3 <- ggplot(ldf, aes(age))
g3 + geom_histogram(binwidth=5)
g3 + geom_freqpoly(binwidth=5)
g3 + geom_histogram() + facet_wrap(~education)
g4 <- ggplot(ldf, aes(x = marital, y = duration))
g4 + geom_boxplot()
par(mfrow=c(1,2))
boxplot(ldf$age, col="slategray2", pch=19, main="Age")
boxplot(ldf$duration, col="slategray2", pch=19, main="Duration")

ggplot(ldf, aes(age, duration)) + geom_point(alpha = 0.3) + stat_smooth()

freqpoly.SparkR <- function(df, a, binw){
  
  library(ggplot2)
  
  dat <- collect(df)
  
  p <- ggplot(dat, aes(x = dat$a)) + geom_freqpoly(binwidth=binw) + xlab(a)
  
  return(p)
}

agedf = select(df, df$age)
g5 <- freqpoly.SparkR(df = agedf, a = "age", 5)
g5

ageAndDurationValuesByMarital <- ggplot(ldf, aes(age, duration)) + geom_point(alpha = "0.2")  + facet_wrap(~marital) 
ageAndDurationValuesByMarital

createOrReplaceTempView(df, "customer")
localAvgDurationEducationAgeDF <- collect(sql("select education, avg(age) as avgAge, avg(duration) as avgDuration from customer group by education"))
avgAgeAndDurationValuesByEducation <- ggplot(localAvgDurationEducationAgeDF, aes(group=education, x=avgAge, y=avgDuration)) + geom_point() + geom_text(data=localAvgDurationEducationAgeDF, mapping=aes(x=avgAge, y=avgDuration, label=education), size=2, vjust=2, hjust=0.75)
avgAgeAndDurationValuesByEducation
plot(density(ldf$duration), main = "Density Plot", xlab = "Duration", yaxt = 'n')
abline(v = mean(ldf$duration), col = 'green', lwd = 2)
legend('topright', legend = c("Actual Data", "Mean"), fill = c('black', 'green'))

#Code for Visualizing data on a map section
csvPath <- "file:///Users/aurobindosarkar/Downloads/CommViolPredUnnormalizedData.csv"
df <- read.df(csvPath, "csv", header = "false", inferSchema = "false", na.strings = "NA", delimiter= ",")
persist(df, "MEMORY_ONLY")
xdf = select(df, "_c1","_c143")
newDF <- withColumnRenamed(xdf, "_c1", "state")
arsonsstatesdf <- withColumnRenamed(newDF, "_c143", "narsons")
avgArsons <- collect(agg(groupBy(arsonsstatesdf, "state"), AVG_ARSONS=avg(arsonsstatesdf$narsons)))
state_names <- read.csv("file:///Users/aurobindosarkar/downloads/csv_hus/states.csv")
avgArsons$region <- factor(avgArsons$state, levels=state_names$code, labels=tolower(state_names$name))
states_map <- map_data("state")
merged_data <- merge(states_map, avgArsons, by="region")
ggplot(merged_data, aes(x = long, y = lat, group = group, fill = AVG_ARSONS)) + geom_polygon(color = "white") + theme_bw()

#Code for Visualizing graph nodes and edges section
library(igraph)

library(magrittr)

inDF <- read.df("file:///Users/aurobindosarkar/Downloads/sx-askubuntu.txt", "csv", header="false", delimiter=" ")

linksDF <- subset(inDF, select = c(1, 2)) %>% withColumnRenamed("_c0", "src") %>% withColumnRenamed("_c1", "dst")

llinksDF <- collect(sample(linksDF, FALSE, 0.01, 1L))

g1 <- graph_from_data_frame(llinksDF, directed = TRUE, vertices = NULL)

plot(g1, edge.arrow.size=.001, vertex.label=NA, vertex.size=0.1)

inDF <- read.df("file:///Users/aurobindosarkar/Downloads/sx-askubuntu.txt", "csv", header="false", delimiter=" ")

linksDF <- subset(inDF, select = c(1, 2)) %>% withColumnRenamed("_c0", "src") %>% withColumnRenamed("_c1", "dst")

llinksDF <- collect(sample(linksDF, FALSE, 0.0005, 1L))

g1 <- graph_from_data_frame(llinksDF, directed = FALSE)

g1 <- simplify(g1, remove.multiple = F, remove.loops = T)

plot(g1, edge.color="black", vertex.color="red", vertex.label=NA, vertex.size=2)

#Code for Using SparkR for machine learning section
library(magrittr)
csvPath <- "file:///Users/aurobindosarkar/Downloads/winequality/winequality-white.csv"
indf <- read.df(csvPath, "csv", header = "true", inferSchema = "true", na.strings = "NA", delimiter= ";") %>% withColumnRenamed("fixed acidity", "fixed_acidity") %>% withColumnRenamed("volatile acidity", "volatile_acidity") %>% withColumnRenamed("citric acid", "citric_acid") %>% withColumnRenamed("residual sugar", "residual_sugar") %>% withColumnRenamed("free sulfur dioxide", "free_sulfur_dioxide") %>% withColumnRenamed("total sulfur dioxide", "total_sulfur_dioxide")
winedf <- mutate(indf, label = ifelse(indf$quality >= 6, 1, 0))
winedf <- drop(winedf, "quality")
seed <- 12345
trainingdf <- sample(winedf, withReplacement=FALSE, fraction=0.9, seed=seed)
testdf <- except(winedf, trainingdf)
model <- spark.logit(trainingdf, label ~ ., maxIter = 10, regParam = 0.1, elasticNetParam = 0.8)
summary(model)
predictions <- predict(model, testdf)
showDF(select(predictions, "label", "rawPrediction", "probability", "prediction"), 5)
nrow(filter(predictions, predictions$label != predictions$prediction))

model <- spark.randomForest(trainingdf, label ~ ., type="classification", maxDepth = 5, numTrees = 10)
summary(model)
predictions <- predict(model, testdf)
showDF(select(predictions, "label", "rawPrediction", "probability", "prediction"), 5)
nrow(filter(predictions, predictions$label != predictions$prediction))

csvPath <- "file:///Users/aurobindosarkar/Downloads/winequality/winequality-white.csv"
indf <- read.df(csvPath, "csv", header = "true", inferSchema = "true", na.strings = "NA", delimiter= ";") %>% withColumnRenamed("fixed acidity", "fixed_acidity") %>% withColumnRenamed("volatile acidity", "volatile_acidity") %>% withColumnRenamed("citric acid", "citric_acid") %>% withColumnRenamed("residual sugar", "residual_sugar") %>% withColumnRenamed("free sulfur dioxide", "free_sulfur_dioxide") %>% withColumnRenamed("total sulfur dioxide", "total_sulfur_dioxide")
trainingdf <- sample(indf, withReplacement=FALSE, fraction=0.9, seed=seed)
testdf <- except(indf, trainingdf)
model <- spark.glm(indf, quality ~ ., family = gaussian, tol = 1e-06, maxIter = 25, weightCol = NULL, regParam = 0.1)
summary(model)
predictions <- predict(model, testdf)
showDF(select(predictions, "quality", "prediction"), 5)
winedf <- mutate(indf, label = ifelse(indf$quality >= 6, 1, 0))
winedf <- drop(winedf, "quality")
trainingdf <- sample(winedf, withReplacement=FALSE, fraction=0.9, seed=seed)
testdf <- except(winedf, trainingdf)
testdf <- except(winedf, trainingdf)
model <- spark.gaussianMixture(trainingdf, ~ sulphates + citric_acid + fixed_acidity + total_sulfur_dioxide + chlorides + free_sulfur_dioxide + density + volatile_acidity + alcohol + pH + residual_sugar, k = 2)
summary(model)
predictions <- predict(model, testdf)
showDF(select(predictions, "label", "prediction"), 5)
test <- spark.kstest(indf, "fixed_acidity", "norm", c(0, 1))
testSummary <- summary(test)
testSummary

library(magrittr)
csvPath <- "file:///Users/aurobindosarkar/Downloads/winequality/winequality-white.csv"
indf <- read.df(csvPath, "csv", header = "true", inferSchema = "true", na.strings = "NA", delimiter= ";") %>% withColumnRenamed("fixed acidity", "fixed_acidity") %>% withColumnRenamed("volatile acidity", "volatile_acidity") %>% withColumnRenamed("citric acid", "citric_acid") %>% withColumnRenamed("residual sugar", "residual_sugar") %>% withColumnRenamed("free sulfur dioxide", "free_sulfur_dioxide") %>% withColumnRenamed("total sulfur dioxide", "total_sulfur_dioxide")
lindf <- collect(indf)

families <- c("gaussian", "poisson")

train <- function(family) {
 model <- glm(quality ~ ., lindf, family = family)
 summary(model)
}

model.summaries <- spark.lapply(families, train)

print(model.summaries)

