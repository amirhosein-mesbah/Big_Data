import pandas as pd
import numpy as np
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf, StringType
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import MultilayerPerceptronClassifier

df = pd.read_csv('/home/mesbah/recipes.csv')
other_df =(df.RecipeCategory!='< 15 Mins') & (df.RecipeCategory!= '< 60 Mins') & ( df.RecipeCategory!= '< 30 Mins') & (df.RecipeCategory!= '< 4 Hours')
df.loc[other_df,'RecipeCategory'] = 'Other'
df.to_csv("/home/mesbah/recipes_edited.csv", index=False)

sparkSession = SparkSession.builder.appName("recipes").getOrCreate()
path = '/home/mesbah/recipes_edited.csv'
df = sparkSession.read.load(path,
                     format='com.databricks.spark.csv',
                     header='true',
                     inferSchema='true')
                     
indexer = StringIndexer(inputCol="RecipeCategory", outputCol="label")
df = indexer.fit(df).transform(df)

columns_to_drop = ['RecipeId', 'Name', 'RecipeCategory']
df = df.drop(*columns_to_drop)

assembler = VectorAssembler(
    inputCols = ['Calories', 'CholesterolContent', 'CarbohydrateContent', 'SugarContent',
                'ProteinContent', 'RecipeServings'], outputCol = 'features')

# use it to transform the dataset and select just
# the output column
df = assembler.transform(df).select('features', 'label')


layers = [4, 32, 64, 128, 64,32, 5]
classifier = MultilayerPerceptronClassifier(labelCol='label',
                                            featuresCol='pcaFeatures',
                                            maxIter=100,
                                            layers=layers,
                                            blockSize=64,
                                            seed=1)

model = classifier.fit(train_pca)
train_output_df = model.transform(train_pca)
test_output_df = model.transform(test_pca)
train_predictionAndLabels = train_output_df.select('prediction', 'label')
test_predictionAndLabels = test_output_df.select('prediction', 'label')
metrics = ['weightedPrecision', 'weightedRecall', 'accuracy']
for metric in metrics:
    evaluator = MulticlassClassificationEvaluator(metricName=metric)
    print('Train ' + metric + ' = ' + str(evaluator.evaluate(
            train_predictionAndLabels)))
    print('Test ' + metric + ' = ' + str(evaluator.evaluate(
            test_predictionAndLabels)))


train, test = df.randomSplit([0.8, 0.2], 1234)

scaler = StandardScaler(
    inputCol = 'features', 
    outputCol = 'scaledFeatures',
    withMean = True,
    withStd = True
).fit(train)

train_scaled = scaler.transform(train)
test_scaled = scaler.transform(test)

n_components = 4
pca = PCA(
    k = n_components, 
    inputCol = 'scaledFeatures', 
    outputCol = 'pcaFeatures'
).fit(train_scaled)


train_pca = pca.transform(train_scaled)
test_pca = pca.transform(test_scaled)

print('Explained Variance Ratio', pca.explainedVariance.toArray())
