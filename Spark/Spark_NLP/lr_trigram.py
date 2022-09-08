import pyspark
from pyspark.sql import SparkSession
import re
import pandas as pd
import numpy as np
from collections import OrderedDict
from pyspark.sql.functions import regexp_replace, trim, col, lower, size
from pyspark.sql.functions import struct
from pyspark.sql.functions import greatest
from pyspark.sql import functions as F
from pyspark.ml.feature import NGram
from  pyspark.sql.functions import abs, max
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline


sparkSession = SparkSession.builder.appName("SimpleSession").getOrCreate()
path = '/home/mesbah/book_df.csv'
df = sparkSession.read.load(path,
                     format='com.databricks.spark.csv',
                     header='true',
                     inferSchema='true')
clean_df = df.dropna(subset=('sentences'))


with open("/home/mesbah/135-0.txt") as book:
    text = book.read().replace('\n',' ').replace('“',' ').replace('”',' ').replace('[',' ').replace(')',' ').replace('(',' ').replace(';',' ').replace(',',' ').replace("''",' ').replace(':',' ').strip()
    text = ' '.join(text.split())
print("book's word count", len(text.split(" ")))   
sentences = re.split(r' *[\.\?!][\'"\)\]]* *', text)
sentences = sentences[501:-259]
sentences_length = [len(sen.split(" ")) for sen in sentences]
df_dict = {'sentences':sentences, 'label':sentences_length}
df = pd.DataFrame(df_dict)
df.head()
df.to_csv('/home/mesbah/book_df.csv',index=False, sep = ',')

def removePunctuation(column):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        column (Column): A Column containing a sentence.

    Returns:
        Column: A Column named 'sentence' with clean-up operations applied.
    """
    return trim(lower(regexp_replace(column, '[^\sa-zA-Z0-9]', ''))).alias('sentence')

punc_rem = (clean_df
 .select(removePunctuation(col('sentences'))))

# create tokens from tweets
tk = Tokenizer(inputCol= "sentence", outputCol = "tokens")
wordsData_punc_rem = tk.transform(punc_rem)


# Define NGram transformer
ngram = NGram(n=3, inputCol="tokens", outputCol="trigram")

# Create bigram_df as a transform of unigram_df using NGram tranformer
trigram_df = ngram.transform(wordsData_punc_rem)
trigram_df = trigram_df.where(size(col("trigram")) >= 3)

tri_df = (trigram_df
     .withColumn('word3', F.explode('trigram'))
    .groupBy('word3')
    .agg(
        F.count('*').alias('word_cnt')
    )
     .sort(col("word_cnt").desc())
)

tri_df = (tri_df
 .withColumn('words', F.split('word3', ' '))
)
threshold = 2
tri_df = tri_df.select(tri_df.words[0], tri_df.words[1], tri_df.words[2]).withColumn('first_length', F.length('words[0]')).withColumn('second_length', F.length('words[1]')).withColumn('third_length', F.length('words[2]'))
distance_df = tri_df.withColumn('distance', greatest(abs(tri_df['first_length'] - tri_df['second_length']),abs(tri_df['second_length'] - tri_df['third_length']))).withColumn('label',
    (F.col('distance') > F.lit(threshold)).cast('int'))



ignore = ['words[0]', 'words[1]', 'words[2]', 'distance', 'label']
assembler = VectorAssembler(
    inputCols=[x for x in distance_df.columns if x not in ignore],
    outputCol='features')

lr = LogisticRegression(maxIter=10)
pipeline = Pipeline(stages=[assembler, lr])
model = pipeline.fit(distance_df)


trainingSummary = model.stages[-1].summary

# Obtain the objective per iteration
objectiveHistory = trainingSummary.objectiveHistory
print("objectiveHistory:")
for objective in objectiveHistory:
    print(objective)

# for multiclass, we can inspect metrics on a per-label basis
print("False positive rate by label:")
for i, rate in enumerate(trainingSummary.falsePositiveRateByLabel):
    print("label %d: %s" % (i, rate))

print("True positive rate by label:")
for i, rate in enumerate(trainingSummary.truePositiveRateByLabel):
    print("label %d: %s" % (i, rate))

print("Precision by label:")
for i, prec in enumerate(trainingSummary.precisionByLabel):
    print("label %d: %s" % (i, prec))

print("Recall by label:")
for i, rec in enumerate(trainingSummary.recallByLabel):
    print("label %d: %s" % (i, rec))

print("F-measure by label:")
for i, f in enumerate(trainingSummary.fMeasureByLabel()):
    print("label %d: %s" % (i, f))

accuracy = trainingSummary.accuracy
print(accuracy)
