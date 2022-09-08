]import re
import pandas as pd
import numpy as np
from collections import OrderedDict
import pyspark
from pyspark.sql import SparkSessiona
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS

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

sparkSession = SparkSession.builder.appName("SimpleSession").getOrCreate()
path = '/home/mesbah/book_df.csv'
df = sparkSession.read.load(path,
                     format='com.databricks.spark.csv',
                     header='true',
                     inferSchema='true')
clean_df = df.dropna(subset=('sentences'))

# create tokens from tweets
tk = Tokenizer(inputCol= "sentences", outputCol = "tokens")
wordsData = tk.transform(clean_df)

# create term frequencies for each of the tokens
tf1 = HashingTF(inputCol="tokens", outputCol="rawFeatures", numFeatures=1000)
tf_data = tf1.transform(wordsData)

# create tf-idf for each of the tokens
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(tf_data)
idf_data = idfModel.transform(tf_data)

frequencies = idf_data.select("tokens","features").toPandas()

frequencies.features = frequencies['features'].apply(lambda x : np.array(x.toArray()).reshape(1,-1))
frequencies.features = frequencies['features'].apply(lambda x: list(x[x!=0]))
frequencies.tokens = frequencies['tokens'].apply(lambda x: list(OrderedDict.fromkeys(x)))


tokens = np.concatenate(frequencies.tokens.values, axis=0)
weights = np.concatenate(frequencies.features.values, axis=0)

dict_tokens = dict(zip(tokens, weights))

wordcloud = WordCloud(stopwords = STOPWORDS, max_words=50, width = 3000, height = 2000,background_color='white')
wordcloud.generate_from_frequencies(dict_tokens)
plt.figure(figsize=(40, 30))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()
