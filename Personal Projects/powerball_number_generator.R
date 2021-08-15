# Load packages
library(data.table)
library(skimr)
library(naniar)
library(dplyr)
library(tidyr)
library(magrittr)
library(ggplot2)
library(plotly)
library(MASS)
library(yardstick)
library(janitor)

## 01. Simple EDA
# Load dataset
df_raw <- fread('powerball.csv')

# View dataset
View(df_raw)
str(df_raw)

# Check missing values
miss_var_summary(df_raw)

# Replace missing values with 0
df_raw[is.na(df_raw)] <- 0
miss_var_summary(df_raw)

# Drop columns
df <- subset(df_raw, select = -c(1, 8, 9))

# Change column names
names(df)[names(df) == "Number 1"] <- "n1"
names(df)[names(df) == "Number 2"] <- "n2"
names(df)[names(df) == "Number 3"] <- "n3"
names(df)[names(df) == "Number 4"] <- "n4"
names(df)[names(df) == "Number 5"] <- "n5"
names(df)[names(df) == "Powerball"] <- "pb"

# Add new rows
nov_4_7 <- c(23, 32, 33, 45, 49, 14, 14, 16, 37, 48, 58, 18)
nov_new_df <- as.data.frame(matrix(nov_4_7, byrow = TRUE, nrow = 2))
colnames(nov_new_df) <- c("n1", "n2", "n3", "n4", "n5", "pb")

# Combine new df to main df
df <- rbind(nov_new_df, df)
head(df)

# Create a data frame with n1 ~ n5
df_wb <- as.data.frame(df$n1, df$n2, df$n3, df$n4, df$n5)

# Add a column name
names(df_wb) <- c("numbers")

# Convert integer to numeric
df_wb$numbers <- as.numeric(df_wb$numbers)

head(df_wb)
class(df_wb$numbers)

# Get the top 10 numbers (1 to 69)
top_10 <- df_wb %>%
  group_by(numbers) %>%
  tally() %>%
  ungroup() %>%
  top_n(10) %>%
  arrange(desc(n))

# Change column names
names(top_10) <- c("Numbers", "Count")

# Convert integer to numeric
top_10$Count <- as.numeric(top_10$Count)
top_10

# Visualize the frequencies of top 10 numbers (1 to 69)
p <- ggplot(top_10, aes(Numbers, Count)) +
  geom_text(aes(label=Numbers, hjust=0, vjust=0)) +
  labs(x="Powerball Numbers", 
       y="Total Number Occurences", 
       title="Top 10 Numbers (1 to 69)") +
  theme(plot.title = element_text(hjust=0.5))

fig <- ggplotly(p)
fig

# Plot the top 10 numbers (1 to 69) with barplot
p1 <- ggplot(data=top_10, aes(x=Numbers, y=Count, fill=Numbers))+
  geom_bar(stat="identity", width=0.5) +
  scale_fill_gradient(low="blue", high='yellow') +
  labs(x="Powerball Numbers", 
       y="Total Number Occurences", 
       title="Top 10 Numbers (1 to 69)") +
  theme(plot.title = element_text(hjust=0.5))

fig1 <- ggplotly(p1)
fig1

# Create a dataset containing all numbers(1 to 69) and its occurence
all_wb <- df_wb %>%
  group_by(numbers) %>%
  tally() %>%
  ungroup() %>%
  top_n(69) %>%
  arrange(desc(numbers))

# Change column names
names(all_wb) <- c("Numbers", "Count")

# Convert integer to numeric
all_wb$Count <- as.numeric(all_wb$Count)

head(all_wb)

# Plot all numbers (1 to 69) with barplot
p2 <- ggplot(data=all_wb, aes(x=Numbers, y=Count, fill=Numbers))+
  geom_bar(stat="identity", width=0.5) +
  scale_fill_gradient(low="pink", high='navy') +
  labs(x="Powerball Numbers", 
       y="Total Number Occurences", 
       title="All Numbers (1 to 69)") +
  theme(plot.title = element_text(hjust=0.5))

fig2 <- ggplotly(p2)
fig2

# Create a data frame with pb: Powerball
df_rb <- as.data.frame(df$pb)

# Add a column name
names(df_rb) <- c("numbers")

# Convert integer to numeric
df_rb$numbers <- as.numeric(df_rb$numbers)

head(df_rb)
class(df_wb$numbers)

# Create a dataset containing top 5 powerball numbers(1 to 26) and its occurence
all_rb_top5 <- df_rb %>%
  group_by(numbers) %>%
  tally() %>%
  ungroup() %>%
  top_n(5) %>%
  arrange(desc(numbers))

# Create a dataset containing all powerball numbers(1 to 26) and its occurence
all_rb <- df_rb %>%
  group_by(numbers) %>%
  tally() %>%
  ungroup() %>%
  top_n(26) %>%
  arrange(desc(numbers))

# Change column names
names(all_rb_top5) <-c("Numbers", "Count")
names(all_rb) <- c("Numbers", "Count")

# Convert integer to numeric
all_rb_top5$Count <- as.numeric(all_rb_top5$Count)
all_rb$Count <- as.numeric(all_rb$Count)

all_rb_top5
head(all_rb)

# Visualize the frequencies of top 10 numbers (1 to 69)
p <- ggplot(all_rb_top5, aes(Numbers, Count)) +
  geom_text(aes(label=Numbers, hjust=0, vjust=0)) +
  labs(x="Powerball Numbers", 
       y="Total Number Occurences", 
       title="Top 5 Numbers (1 to 26)") +
  theme(plot.title = element_text(hjust=0.5))

fig <- ggplotly(p)
fig

# Plot all powrball numbers (1 to 26) with barplot
p3 <- ggplot(data=all_rb, aes(x=Numbers, y=Count, fill=Numbers))+
  geom_bar(stat="identity", width=0.5) +
  scale_fill_gradient(low="orange", high='brown') +
  labs(x="Powerball Numbers", 
       y="Total Number Occurences", 
       title="All Powerball Numbers (1 to 26)") +
  theme(plot.title = element_text(hjust=0.5))

fig3 <- ggplotly(p3)
fig3


## 02. Probabilistic Predictions by Logistic Regression Analysis
# Create a dataframe filled with binary value of 1 (1: win, 0: lose)
df_result <- as.data.frame(rep(c(1), time=1508))
colnames(df_result) <- c("result")

# Merge df_result to main dataset: df
df$result <- paste(df_result$result)

# Convert character to integer
df$result <- as.integer(df$result)

head(df)

# Create a dataset that generates dummy values that will be used for predictions
df_dummy_raw <- data.frame(matrix(nrow=10000, ncol=7))
colnames(df_dummy_raw) <- c("idx", "n1", "n2", "n3", "n4", "n5", "pb")

for (i in 1:10000) {
  num_wb <- sample(1:69, 5, replace=FALSE)
  num_rb <- sample(1:26, 1, replace=FALSE)
  
  for (j in 1:5) {
    df_dummy_raw[i, 1] = i
    df_dummy_raw[i, 2:6] = num_wb
    df_dummy_raw[i, 7] = num_rb
  }
}

head(df_dummy_raw)

# Drop a index column in df_dummy dataset
df_dummy <- subset(df_dummy_raw, select=-c(1))

# Add a result column filled with 0
df_dummy_result <- as.data.frame(rep(c(0), time=10000))

# Change a column name
colnames(df_dummy_result) <- c("result")

# Merge df_result to a dataset: df_dummy_1
df_dummy$result <- paste(df_dummy_result$result)

# Convert character to integer
df_dummy$result <- as.integer(df_dummy$result)

# Merge df_dummy_1 into df
df_all <- rbind(df, df_dummy)

head(df_all)

# Quick EDA for a combined dataset: df_all
skimr::skim(df_all)

# Check duplicates in each row
get_dupes(df_all, n1, n2, n3, n4, n5)

# Fit a model
mdl <- glm(result ~ n1 + n2 + n3 + n4 + n5 + pb,
           data = df_all,
           family = binomial(link="logit"))

# Summary results of the model 
round(summary(mdl)$coef, digits=5)

# Obtain the actual powerball results and predicted results by the model
actual_result <- df_all$result
predicted_result <- round(fitted(mdl))

# Tabulate both responses
outcomes <- table(predicted_result, actual_result)
outcomes

# Convert the table into a yardstick confusion matrix object using conf_mat()
confusion <- conf_mat(outcomes)

# Gain the major model performance metrics
summary(confusion, event_level = "second")[1:4, ]

# Create a dataset for predictions
pick1 <- c(14, 26, 38, 45, 46, 13)
pick2 <- c(1, 20, 22, 60, 66, 3)
pick3 <- c(3, 4, 11, 41, 67, 5)
pick4 <- c(3, 43, 45, 61, 65, 14)
pick5 <- c(10, 24, 27, 35, 53, 18)
pick6 <- c(6, 13, 38, 39, 53, 6)
pick7 <- c(27, 32, 34, 43, 52, 13)
pick8 <- c(4, 23, 37, 61, 67, 7)
pick9 <- c(17, 54, 56, 63, 69, 20)
pick10 <- c(11, 14, 31, 47, 48, 4)

pick_all <- c(pick1, pick2, pick3, pick4, pick5, pick6, pick7, pick8, pick9, pick10)
pick_all_mtx <- matrix(pick_all, byrow=TRUE, nrow=10)
df_pick <- data.frame(pick_all_mtx)
colnames(df_pick) <- c("n1", "n2", "n3", "n4", "n5", "pb")

df_pick

# Generate probabilistic predictions for given numbers
prob_win <- round(predict(mdl, df_pick, type = "response"), digits = 3)
prob_win

mean_prob_win <- mean(prob_win)
mean_prob_win

range_prob_win <- range(prob_win)
range_prob_win

IQR_prob_win <- IQR(prob_win)
IQR_prob_win

df_draw_raw <- data.frame(matrix(nrow=1000, ncol=7))
colnames(df_draw_raw) <- c("idx", "n1", "n2", "n3", "n4", "n5", "pb")

for (i in 1:1000) {
  num_wb <- sample(1:69, 5, replace=FALSE)
  num_rb <- sample(1:26, 1, replace=FALSE)
  
  for (j in 1:5) {
    df_draw_raw[i, 1] = i
    df_draw_raw[i, 2:6] = num_wb
    df_draw_raw[i, 7] = num_rb
  }
}

df_draw <- subset(df_draw_raw, select = -c(1))

df_draw %>%
  get_dupes(-c(pb))

# Create a data frame that provide numbers that have a probability of more than 0.3 to win the lottery 
pred_result <- data.frame(matrix(nrow=1000, ncol=8))
colnames(pred_result) <- c("n1", "n2", "n3", "n4", "n5", "pb", "prob", "result")

prob <- rep(NA, nrow(df_draw))
result <- rep(NA, nrow(df_draw))

for (i in 1:nrow(df_draw)) {
  prob = predict(mdl, df_draw[i, ], type='response')
  
  if (prob >= .30 ) {
    result[i] = 1
  }
  else {
    result[i] = 0
  }
  
  pred_result[i, 1:6] = df_draw[i, 1:6]
  pred_result[i, 7] = prob
  pred_result[i, 8] = result[i]
  pred_result_win <- subset(pred_result, result == 1)    
}

# Get the top 5 set of numbers
pred_result_win %>%
  group_by(result) %>%
  top_n(3) %>%
  arrange(desc(prob))