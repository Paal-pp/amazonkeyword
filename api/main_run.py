

from proxycrawl import CrawlingAPI, ScraperAPI, LeadsAPI, ScreenshotsAPI, StorageAPI
from cleaner import *
import pymysql as mysql
import  sys


class crawl():
    def retry(func):
        def inner(*args, **kwargs):
            ret = func(*args, **kwargs)
            max_retry = 3
            number = 0
            if not ret:
                while number < max_retry:
                    number += 1
                    print("尝试第:{}次".format(number))
                    result = func(*args, **kwargs)
                    if result:
                        break

        return inner

    # @retry
    def crawl_main(self,link):
        result ={}
        api =CrawlingAPI({'token':'g5eankdfZ8v8utNjJyzJvw','timeout':90})
        response =api.get(url=link,options={})
        return response


class program():
    def __int__(self):
        db = mysql.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            passwd="Mld901",
            db="advertising_distribution")
        cur = db.cursor()

    def url_link(self,keyword,page):
        url_link=f"""https://www.amazon.com/s?k={keyword}&s=featured_rank&page={page}&language=en_US&ref=nb_sb_noss_1&zipCode=37377"""
        return  url_link

    def savefile(self,keyword,page,res):
        res = str(res).replace("""\\n""", '')  # 替换换行符
        res = str(res).rstrip('\\n')
        nowdatetime =datetime.date.today()
        if os.path.exists(f"E:/keyword/{nowdatetime}/") == False:
            os.mkdir(f"E:/keyword/{nowdatetime}/")
        fw = open(
            f'E:/keyword/{nowdatetime}/{keyword}-{page}.html',
            'w',
            encoding='utf-8')
        fw.write(str(res))
        fw.close()
        return res

    def request_main(self,keyword,page,link):
        print(link)
        result = crawl().crawl_main(link)
        print(result)
        print(result['status_code'])
        if result['status_code'] ==200:
            print("请求成功，开始处理数据")
            self.savefile(keyword,page,result['body'])
            deal_result = AmazonCleaner().goodsList(result['body'])
            print("数据处理完成，返回数据")
            print(deal_result)
        else:
            while result['status_code'] == 200:
                if result['status_code'] == 400 or result['status_code'] == 401 or result['status_code'] == 403:
                    print("程序出错")
                    print(result)
                    sys.exit()
                elif result['status_code'] == 429:
                    time.sleep(10)
                    result = self.spider(link)
                elif result['status_code'] == 499 or result['status_code'] == 520 or result['status_code'] == 500:
                    result = self.spider(link)
            print("请求成功，开始处理数据")
            self.savefile(keyword, page, result['body'])
            deal_result = AmazonCleaner().goodsList(result['body'])
            print("数据处理完成，返回数据")
            print(deal_result)
        return deal_result

    def program_main(self):
        cur=db.cursor()
        select_sql = """
                    select keyword from keywordlist
                """
        cur.execute(select_sql)
        rest = cur.fetchall()
        db.commit()
        print(rest)
        now = datetime.datetime.now()
        nowdatetime = now.strftime("%Y-%m-%d")
        if os.path.exists(f"E:/keyword/{nowdatetime}/") == False:
            os.mkdir(f"E:/keyword/{nowdatetime}/")
        for solokeyword in rest:
            adlist = []
            keyword = solokeyword[0]
            keyword=keyword.replace(" ","+")
            print(keyword)
            item_No = 1
            aditem_No = 1
            item_num=0
            page=1
            urllink =self.url_link(keyword,page)
            print(f"""请求{keyword}，第{page}页""")
            deal_result =self.request_main(keyword,page,urllink)
            while item_num <=100:
                new_itemlist = deal_result['data'][0]['itemlist']
                new_aditemlist = deal_result['data'][0]['adlist']
                adlist = adlist + new_aditemlist
                adallranking = len(adlist)
                for soloitem in new_itemlist:
                    itemid = soloitem['item_id']
                    IS = soloitem['IS']
                    insert_sql = f"""
                            INSERT INTO keywordranking(datetime,keyword,ASIN,ranking,YOR,allranking) values ('{nowdatetime}','{keyword}','{itemid}',{item_No},{IS},100)
                            """
                    item_No = item_No + 1
                    print(insert_sql)

                    cur.execute(insert_sql)
                    db.commit()
                    print("插入成功")
                    item_num =item_num+1
                    print(f"""现在有{item_num}个""")
                    if item_num ==100:
                        break
                if item_num == 100:
                    break
                else:
                    if page ==10:
                        break
                    page = page + 1
                    urllink = self.url_link(keyword, page)
                    deal_result = self.request_main(keyword,page,urllink)
                    print(f"""请求{keyword}，第{page}页""")
            for adsoloitem in adlist:
                select_sql = f"""
                            select * from asin where ASIN ='{adsoloitem}'
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
            dealsolokeyword =solokeyword.replace(" ","+")
            get_keyword_list = f"""
            select * from keywordranking where keyword= '{dealsolokeyword}' And datetime='{nowdatetime}'
            """
            get_keyword_list_myinf = f"""
            select * from keywordranking where keyword= '{dealsolokeyword}' AND YOR=1 AND datetime='{nowdatetime}'
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
                        select * from adkeywordranking where keyword= '{dealsolokeyword}' And datetime='{nowdatetime}'
                        """
            get_keyword_list_myinf = f"""
                        select * from adkeywordranking where keyword= '{dealsolokeyword}' AND YOR=1 AND datetime='{nowdatetime}'
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

if __name__== "__main__":
    # program().program_main()
    program().rate()