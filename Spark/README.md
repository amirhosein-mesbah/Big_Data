# Using Apache Spark for NLP and Machine Learning tasks

In this project, I've used Apache Spark for NLP and Machine Learning tasks.

## Spark for Machine Learning

### model
In this Task, I've used `spark ML` to build an mlp model and apply a multi-label classification on a `spark data frame. After Preprocessing Data and creating a column for labels, Standardization and PCA are applied to data respectfully.

### Results
The results of training a mlp model on the proposed dataset are shown in the table below:

| Model      | Test Accuracy | Test Recall | Test Precision |
| ----------- | ----------- | ----------- | ----------- |
| MLP      | 96,01%       | 96,01%        | 92,19%       |


## Spark for NLP
For this task, I downloaded the `Les Misérables` book and created a `spark dataframe` with sentences from the book. After that, I created the `bigram` and `trigram` of the prepared data frame and compute the count of each bigram and trigram.
for the last part, I implemented a logistic regression to see if there is any relationship between the length of words in bigram or trigram.

