import requests
from lxml import etree
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

def news_crawler():
    news_df = {"id":[], "title":[], "text":[], "category":[], "summary":[], "writer":[],"date":[], "tags":[],"link":[]}
    url = "https://www.tgju.org/news"
    html = requests.get(url).text
    soup = BeautifulSoup(html,'html.parser')
    dom = etree.HTML(str(soup))
    links = dom.xpath('//*[@id="news-main"]/div[3]/div/div[1]/div/div/h3/a/@href')
    counter = 1
    for link in links:
        info = []
        correct_link = "https://www.tgju.org" + link
        html = requests.get(correct_link).text
        soup = BeautifulSoup(html,'html.parser')
        dom = etree.HTML(str(soup))
        info.append(counter)
        title = dom.xpath('//*[@id="news-main"]/div/div[2]/div/div[1]/div/div[2]/h1/a')[0].text
        info.append(title) 
        text_elements = dom.xpath('//*[@id="news-main"]/div/div[2]/div/div[1]/div/div[2]/div[4]/p')
        text = ''
        for el in text_elements:
            try:
                text = text + el.text
            except:
                pass
        info.append(text)
        cat = dom.xpath('//*[@id="news-main"]/div/div[2]/div/div[1]/div/div[2]/div[1]/a')[0].text
        info.append(cat)
        summary = dom.xpath('//*[@id="news-main"]/div/div[2]/div/div[1]/div/div[2]/div[3]/p')
        sum_text = ''
        for el in summary:
            try:
                sum_text = sum_text + el.text
            except:
                pass
        info.append(sum_text)
        writer = dom.xpath('//*[@id="news-main"]/div/div[2]/div/div[1]/div/div[2]/div[6]/a')[0].text
        info.append(writer)
        date = dom.xpath('//*[@id="news-main"]/div/div[2]/div/div[1]/div/div[2]/div[6]/div')[0].text
        info.append(date)
        tags = dom.xpath('//*[@id="news-main"]/div/div[2]/div/div[1]/div/div[2]/ul/li/a')
        tag_list = [tag.text for tag in tags]
        info.append(tag_list)
        info.append(correct_link)
        counter +=1
        if len(info) == 9:
            news_df['link'].append(info[8])
            news_df['tags'].append(info[7])
            news_df['date'].append(info[6])
            news_df['writer'].append(info[5])
            news_df['summary'].append(info[4])
            news_df['category'].append(info[3])
            news_df['text'].append(info[2])
            news_df['title'].append(info[1])
            news_df['id'].append(info[0])


    hour = datetime.now().hour
    news_dataframe = pd.DataFrame(news_df)
    news_dataframe.to_csv("./dags/data/news_df_" + str(hour)+ ".csv", index=False)
