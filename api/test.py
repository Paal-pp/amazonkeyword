import os
import time

from requests.api import options
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from cleaner import *
global chromedriver
chromedriver = "C:\Program Files\Google\Chrome\Application\chromedriver.exe"
# selenium配置参数


def get_selenium_amazonkeyword(keyword,pages):
    print(keyword,pages)
    options = Options()
    # 配置无头参数,即不打开浏览器
    options.add_argument('--headless')
    # 配置Chrome浏览器的selenium驱动

    os.environ["webdriver.chrome.driver"] = chromedriver
    # 将参数设置+浏览器驱动组合
    browser = webdriver.Chrome(chromedriver, chrome_options=options)
    browser.delete_all_cookies()
    url = f"https://www.amazon.com/s?k={keyword}&s=featured_rank&page={pages}&language=en_US&ref=nb_sb_noss_1"
    print(url)
    # 通过selenium来访问亚马逊
    browser.get(url)
    # 将爬取到的网页信息，写入到本地文件
    fw = open(f'E:/{keyword}-{pages}.html', 'w', encoding='utf-8')
    fw.write(str(browser.page_source))
    fw.close()
    dataclearn = AmazonCleaner()
    result = dataclearn.goodsList(str(browser.page_source))
    browser.close()
    return result





