from api.cleaner import *
from func_timeout import func_timeout, exceptions
import json, re
from tools.data import *
from tools.decorators import *
import requests
requests.packages.urllib3.disable_warnings()
import random
from tools.agentUtil import *


class AmazonCrawler:
    def __init__(self):
        self.dataCleaner = AmazonCleaner()


        self.request_time_out = 60
        self.session = None

    def request(self, taskDetail):
        """
        对任务进行详细分发
        :param taskDetail:
        :return:
        """
        task_type = taskDetail['type']
        if task_type == 'amazonKeyword':
            return self.getGoodsByKeyword(self.getParam(taskDetail, 'keyword'),
                                          self.getParam(taskDetail, 'page',),
                                          self.getParam(taskDetail, 'sort', 'featured_rank'),
                                          self.getParam(taskDetail, 'countryCode', ),
                                          self.getParam(taskDetail, 'zipCode', ''),
                                          self.getParam(taskDetail, 'language',))

    @retry_by_msg(retry_time=5)
    def getGoodsByKeyword(self, keyword, page, sort, country_code, zip_code, language):
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
        url ='https://www.amazon.com/s?k={keyword}&s={sort}&page={page}&language={language}&ref=nb_sb_noss_1'.format(
            keyword=keyword, page=page, sort=sort, language=language)
        print(url)
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-us,us;q=0.9',
            'referer': 'https://www.amazon.com/ref=nav_logo',
            'user-agent': random.choice(agents)
        }
        try:
            self.session = self.get_session() if self.session is None else self.session
            if country_code != 'US':
                self.session = self.get_country_session(self.session, country_code)
            elif zip_code != '':
                self.session = self.get_zipcode_session(self.session, zip_code)
            result = func_timeout(self.request_time_out, lambda: self.session.get(url, headers=headers))
            html_doc = result.text
            print(result.status_code)
            res = self.check_is_limited(html_doc)
            if res is not None:
                self.session = None
                return res

            result = self.dataCleaner.goodsList(html_doc)
            return result

        except exceptions.FunctionTimedOut:
            res['code'] = 401
            res['msg'] = "请求超时"
            return res

        except:
            res['code'] = 401
            res['msg'] = "请求出错"
            # res['msg'] = str(traceback.format_exc())
            return res

    def getCode(self, url):
        ct = requests.get(url, verify=False).content

        result = requests.post(url='http://192.168.0.123:5658', data=ct, verify=False).text
        if result and len(result):
            return result
        else:
            result = requests.post(url='http://192.168.0.200:5658', data=ct, verify=False).text
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
                except:
                    result = 1

        elif type == 'sort':
            default_sort = 'featured_rank'
            if not data or data not in ['featured_rank', 'price-asc-rank', 'price-desc-rank', 'review-rank',
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
        except:
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
        taskDetail1 = {'type': 'amazonKeyword', 'keyword': 'cable railing', 'sort': 'featured_rank', 'page': 2,
                       'countryCode': 'US', 'zipCode': '', 'language': 'en_US'}

        result = self.request(taskDetail1)
        print(json.dumps(result, ensure_ascii=False, indent=4))





if __name__ == '__main__':
    crawler = AmazonCrawler()
    count = 1
    crawler.test()
