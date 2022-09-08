# Using Apache Spark for NLP and Machine Learning tasks

In this project I've use Apache Spark for NLP and Machine Learning tasks.

## Spark for Machine Learning

### model
In this Task, I've used `spark ML` to build an mlp model and apply a multi-label classification on a `spark dataframe`. After Preprocessing Data and creating a column for labels, Standardization and PCA are applied to data respectfully.

### Results
results of training a mlp model on the proposed dataset are shown in the table below:

| Model      | Test Accuracy | Test Recall | Test Precision |
| ----------- | ----------- | ----------- | ----------- |
| MLP      | 96,01%       | 96,01%        | 92,19%       |


## Spark for NLP
In this task I've downloaded `Les Misérables` book and created a `spark dataframe` with sentences of book. After that I've created `bigram` and `trigram` of prepared dataframe and compute count of each bigram and trigram.
for the last part, I implemented a logistic regression to see if there is any relationship between length of words in bigram or trigram.

