import pymysql as mysql
import requests as requests

from tools.agentUtil import *
from time import strftime
import datetime
import random
import os
from test import *
from cleaner import *
from api.cleaner import *
from func_timeout import func_timeout, exceptions
import json
import re
from tools.data import *
from tools.decorators import *
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
bgsched = BackgroundScheduler()
requests.packages.urllib3.disable_warnings()
global base_url, db, cur
db = mysql.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    passwd="Mld901",
    db="advertising_distribution")
cur = db.cursor()



class AmazonCrawler:
    def __init__(self):
        self.dataCleaner = AmazonCleaner()

        self.request_time_out = 90
        self.session = None

    def request(self, taskDetail):
        """
        对任务进行详细分发
        :param taskDetail:
        :return:
        """
        task_type = taskDetail['type']
        if task_type == 'amazonKeyword':
            return self.getGoodsByKeyword(
                self.getParam(taskDetail, 'keyword'),
                self.getParam(taskDetail, 'page',),
                self.getParam(taskDetail, 'sort', 'featured_rank'),
                self.getParam(taskDetail, 'countryCode', ),
                self.getParam(taskDetail, 'zipCode', ''),
                self.getParam(taskDetail, 'language'))

    def getGoodsByKeyword(
            self,
            keyword,
            page,
            sort,
            country_code,
            zip_code,
            language):
        '''
        搜索商品 (requests)
        :param keyword: 关键词
        :param page: 页码
        :param sort: 排序
        :return:
        '''
        res = {}
        keyword = self.cleanArgs(keyword, 'keyword')
        page = self.cleanArgs(page, 'page')
        sort = self.cleanArgs(sort, 'sort')
        country_code = self.cleanArgs(country_code, 'country_code')
        language = self.cleanArgs(language, 'language')
        # url = 'https://www.amazon.com/s?k={keyword}&s={sort}&page={page}&language={language}&qid={qid}'.format(keyword=keyword, sort=sort, page=page,language=language,qid=int(time.time()))
        # url = 'https://www.amazon.com/s/ref=nb_sb_noss_1?url=search-alias%3Daps&field-keywords={keyword}&page={page}&s={sort}&language={language}'.format(
        #     keyword=keyword, page=page, sort=sort, language=language)
        url = 'https://api.crawlbase.com/?token=g5eankdfZ8v8utNjJyzJvw&url=https://www.amazon.com/s?k={keyword}&s={sort}&page={page}&language={language}&ref=nb_sb_noss_1'.format(
            keyword=keyword, page=page, sort=sort, language=language)
        print(f"url:{url}")
        payload = {}
        headers = {
        }
        response = requests.request("GET", url, headers=headers, data=payload,timeout=(90,90))
        # self.session = self.get_session() if self.session is None else self.session
        # if country_code != 'US':
        #     self.session = self.get_country_session(self.session, country_code)
        # elif zip_code != '':
        #     self.session = self.get_zipcode_session(self.session, zip_code)
        # result = func_timeout(self.request_time_out, lambda: self.session.get(url, headers=headers))
        print(f"code:{response.status_code}")
        # print(response.text)
        if response.status_code==200:
            print("成功")
            html_doc = response.text
            now = datetime.datetime.now()
            nowdatetime = now.strftime("%Y-%m-%d")
            fw = open(
                f'E:/{nowdatetime}/{keyword}-{page}.html',
                'w',
                encoding='utf-8')
            fw.write(str(html_doc))
            fw.close()
            result = self.dataCleaner.goodsList(html_doc)
            return result
        else:
            print("失败")
            self.getGoodsByKeyword(keyword,
            page,
            sort,
            country_code,
            zip_code,
            language)
        # res['msg'] = str(traceback.format_exc())

    def getCode(self, url):
        ct = requests.get(url, verify=False).content

        result = requests.post(
            url='http://192.168.0.123:5658',
            data=ct,
            verify=False).text
        if result and len(result):
            return result
        else:
            result = requests.post(
                url='http://192.168.0.200:5658',
                data=ct,
                verify=False).text
            return result

    def getParam(self, taskDetail, param, default=None):
        """
        获取参数
        :param taskDetail:
        :param param:
        :return:
        """
        if param in taskDetail:
            return taskDetail[param]
        else:
            return default

    def cleanArgs(self, data, type):
        '''
        参数清洗 格式化为最终请求携带的参数
        :param data: 参数数据
        :param type: 参数类型
        :return:
        '''
        result = None
        if type == 'keyword':
            result = re.sub(' +', ' ', data.strip()).replace(' ', '+')

        elif type == 'page':
            if not data:
                result = 1
            else:
                try:
                    result = int(data)
                except BaseException:
                    result = 1

        elif type == 'sort':
            default_sort = 'featured_rank'
            if not data or data not in [
                'featured_rank',
                'price-asc-rank',
                'price-desc-rank',
                'review-rank',
                    'date-desc-rank']:
                result = default_sort
            else:
                result = data

        elif type == 'country_code':
            default_code = 'US'
            if not data or data not in COUNTRY_CODE_DICT.keys():
                result = default_code
            else:
                result = data

        elif type == 'language':
            default_lang = 'en_US'
            data = data.strip()
            if not data or data not in COUNTRY_LANG_DICT.keys():
                result = default_lang
            else:
                result = data

        elif type == 'comment_sort':
            default_sort = 'recent'
            if not data or data not in ['recent', 'helpful']:
                result = default_sort
            else:
                result = data

        elif type == 'reviewerType':
            default = 'all_reviews'
            if not data or data not in REVIEWER_TYPE.keys():
                result = default
            else:
                result = data

        elif type == 'filterByStar':
            default = 'all_stars'
            if not data or data not in FILTER_BY_STAR.keys():
                result = default
            else:
                result = data

        elif type == 'formatType':
            default = 'all_formats'
            if not data or data not in FORMAT_TYPE.keys():
                result = default
            else:
                result = data

        elif type == 'mediaType':
            default = 'all_contents'
            if not data or data not in MEDIA_TYPE.keys():
                result = default
            else:
                result = data

        return result

    def get_country_session(self, session, country_code):
        url = 'https://www.amazon.com/gp/delivery/ajax/address-change.html'
        post_data = f'locationType=COUNTRY&district={country_code}&countryCode={country_code}&storeContext=gateway&deviceType=web'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
        }
        try:
            resp = session.post(url, headers=headers, data=post_data)
        except BaseException:
            pass

        return session

    def check_is_limited(self, result, type=0):
        '''
        检测接口是否被限制
        :param result: 返回None代表未被限制
        :param type:
            0:被检测为机器人
        :return:
        '''
        res = None
        limited_msg = '请求太过频繁，请稍后再试'

        if type == 0:
            if 'Robot Check' in result:
                res = {}
                res['code'] = 401
                res['msg'] = limited_msg

        return res

    def get_session(self):
        '''
        获取session
        :return:
        '''
        # session = HTMLSession()
        # session.get('https://www.ixigua.com/').html.render()
        # time.sleep(2)
        session = requests.session()
        return session

    def test(self):
        select_sql = """
            select keyword from keywordlist
        """
        cur.execute(select_sql)
        rest = cur.fetchall()
        db.commit()
        print(rest)
        now = datetime.datetime.now()
        nowdatetime = now.strftime("%Y-%m-%d")
        if os.path.exists(f"E:/{nowdatetime}/") == False:
            os.mkdir(f"E:/{nowdatetime}/")
        for solokeyword in rest:
            itemlist = []
            adlist = []
            keyword = solokeyword[0]
            print(keyword)
            item_No = 1
            aditem_No = 1
            for page in range(1, 4):
                taskDetail1 = {
                    'type': 'amazonKeyword',
                    'keyword': f'{keyword}',
                    'sort': 'featured_rank',
                    'page': f'{page}',
                    'countryCode': 'US',
                    'zipCode': '',
                    'language': 'en_US'}
                result = self.request(taskDetail1)
                json.dumps(result)
                print(result)
                new_itemlist = result['data'][0]['itemlist']
                new_aditemlist = result['data'][0]['adlist']
                itemlist = itemlist + new_itemlist
                adlist = adlist + new_aditemlist
            allrangking=len(itemlist)
            adallranking =len(adlist)
            for soloitem in itemlist:
                itemid = soloitem['item_id']
                IS = soloitem['IS']
                insert_sql = f"""
                INSERT INTO keywordranking(datetime,keyword,ASIN,ranking,YOR,allranking) values ('{nowdatetime}','{keyword}','{itemid}',{item_No},{IS},{allrangking})
                """
                item_No = item_No + 1
                print(insert_sql)
                cur.execute(insert_sql)
                db.commit()
                print("插入成功")
            for adsoloitem in adlist:
                select_sql = f"""
                    select * from asin where ASIN ="{adsoloitem}"
                """
                cur.execute(select_sql)
                db.commit()
                rest = cur.fetchall()
                db.commit()

                if len(rest) == 0:
                    IS = 0
                else:
                    IS = 1
                insert_sql = f"""
                INSERT INTO adkeywordranking(datetime,keyword,ASIN,ranking,YOR,allranking) values ('{nowdatetime}','{keyword}','{adsoloitem}',{aditem_No},{IS},{adallranking})
                """
                aditem_No = aditem_No + 1
                print(insert_sql)
                cur.execute(insert_sql)
                db.commit()
                print("插入成功")
        self.rate()

    def rate(self):
        keyword_select_sql = """
        select * from keywordlist
        """
        cur.execute(keyword_select_sql)
        keyword_select_sql_res = cur.fetchall()
        db.commit()
        now = datetime.datetime.now()
        nowdatetime = now.strftime("%Y-%m-%d")
        for solokeyword_inf in keyword_select_sql_res:
            solokeyword = solokeyword_inf[2]
            get_keyword_list = f"""
            select * from keywordranking where keyword= '{solokeyword}' And datetime='{nowdatetime}'
            """
            get_keyword_list_myinf = f"""
            select * from keywordranking where keyword= '{solokeyword}' AND YOR=1 AND datetime='{nowdatetime}'
            """
            cur.execute(get_keyword_list)
            get_keyword_list_res = cur.fetchall()
            db.commit()
            all_num = len(get_keyword_list_res)
            cur.execute(get_keyword_list_myinf)
            get_keyword_list_myinf_res = cur.fetchall()
            db.commit()
            yes_num = len(get_keyword_list_myinf_res)
            try:
                nature_rate = round(yes_num / all_num * 100, 2)
            except ZeroDivisionError:
                nature_rate =0
            get_keyword_list = f"""
                        select * from adkeywordranking where keyword= '{solokeyword}' And datetime='{nowdatetime}'
                        """
            get_keyword_list_myinf = f"""
                        select * from adkeywordranking where keyword= '{solokeyword}' AND YOR=1 AND datetime='{nowdatetime}'
                        """
            cur.execute(get_keyword_list)
            get_keyword_list_res = cur.fetchall()
            db.commit()
            all_num = len(get_keyword_list_res)
            cur.execute(get_keyword_list_myinf)
            get_keyword_list_myinf_res = cur.fetchall()
            db.commit()
            yes_num = len(get_keyword_list_myinf_res)
            try:
                ad_rate = round(yes_num / all_num * 100, 2)
            except ZeroDivisionError:
                ad_rate =0
            print(f"""{solokeyword}:自然排名：{nature_rate}%，广告占比：{ad_rate}%""")
            insert_sql = f"""
            Insert into keyword_rate(datetime, keyword, nature_rate, ad_rate) VALUES ('{nowdatetime}','{solokeyword}','{nature_rate}','{ad_rate}')
            """
            cur.execute(insert_sql)
            db.commit()

    def test2(self):
        select_sql = """
            select keyword from keywordlist
        """
        cur.execute(select_sql)
        rest = cur.fetchall()
        db.commit()
        print(rest)
        now = datetime.datetime.now()
        nowdatetime = now.strftime("%Y-%m-%d")
        if os.path.exists(f"E:/{nowdatetime}/") == False:
            os.mkdir(f"E:/{nowdatetime}/")
        for solokeyword in rest:
            itemlist = []
            adlist = []
            keyword = solokeyword[0]
            keyword=keyword.replace(" ","+")
            print(keyword)
            item_No = 1
            aditem_No = 1
            for page in range(1, 4):
                # time.sleep(random.uniform(1,4))
                result =get_selenium_amazonkeyword(keyword,page)
                print(type(result))
                json.dumps(result)
                print(type(result))
                new_itemlist = result['data'][0]['itemlist']
                new_aditemlist = result['data'][0]['adlist']
                itemlist = itemlist + new_itemlist
                adlist = adlist + new_aditemlist
            allrangking=len(itemlist)
            adallranking =len(adlist)
            for soloitem in itemlist:
                itemid = soloitem['item_id']
                IS = soloitem['IS']
                insert_sql = f"""
                INSERT INTO keywordranking(datetime,keyword,ASIN,ranking,YOR,allranking) values ('{nowdatetime}','{keyword}','{itemid}',{item_No},{IS},{allrangking})
                """
                item_No = item_No + 1
                print(insert_sql)
                cur.execute(insert_sql)
                db.commit()
                print("插入成功")
            for adsoloitem in adlist:
                select_sql = f"""
                    select * from asin where ASIN ="{adsoloitem}"
                """
                cur.execute(select_sql)
                db.commit()
                rest = cur.fetchall()
                db.commit()

                if len(rest) == 0:
                    IS = 0
                else:
                    IS = 1
                insert_sql = f"""
                INSERT INTO adkeywordranking(datetime,keyword,ASIN,ranking,YOR,allranking) values ('{nowdatetime}','{keyword}','{adsoloitem}',{aditem_No},{IS},{adallranking})
                """
                aditem_No = aditem_No + 1
                print(insert_sql)
                cur.execute(insert_sql)
                db.commit()
                print("插入成功")
        self.rate()

    def get_ip(self):
        response = requests.get(
            url="http://httpbin.org/ip",
            proxies={"http": "http://g5eankdfZ8v8utNjJyzJvw:@smartproxy.crawlbase.com:8012",
                     "https": "http://g5eankdfZ8v8utNjJyzJvw:@smartproxy.crawlbase.com:8012"},
            verify=False
        )
        print('Response Code: ', response.status_code)
        print('Response Body: ', response.text)
        result= json.loads(response.text)
        ip = result['origin']
        proxies = {'http': f'http://{ip}',
                   'https': f'http://{ip}'}
        return proxies

if __name__ == '__main__':
    crawler = AmazonCrawler()
    crawler.test()
    crawler.rate()
    # scheduler =BlockingScheduler()
    # scheduler.add_job(crawler.test,'cron', month="*",day_of_week= "3", hour="9")
    # scheduler.start()
