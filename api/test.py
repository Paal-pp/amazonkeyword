import os
import time

from requests.api import options
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from cleaner import *
keyword ="cable+railing"
page=1
# selenium配置参数
options = Options()
# 配置无头参数,即不打开浏览器
options.add_argument('--headless')
# 配置Chrome浏览器的selenium驱动
chromedriver = "C:\Program Files\Google\Chrome\Application\chromedriver.exe"
os.environ["webdriver.chrome.driver"] = chromedriver
# 将参数设置+浏览器驱动组合
browser = webdriver.Chrome(chromedriver, chrome_options=options)
browser.delete_all_cookies()
url = "https://www.amazon.com/s?k={keyword}&s=featured_rank&page={page}&language=en_US&ref=nb_sb_noss_1".format(keyword,str(page))
print(url)
#通过selenium来访问亚马逊
browser.get(url)

#将爬取到的网页信息，写入到本地文件
fw=open(f'E:/{keyword}-{page}.html','w',encoding='utf-8')
fw.write(str(browser.page_source))
fw.close()
time.sleep(3)
dataclearn =AmazonCleaner()
result =dataclearn.goodsList(str(browser.page_source))
print(result)
browser.close()
