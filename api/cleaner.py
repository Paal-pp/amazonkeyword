import json
import traceback
from lxml import etree
import re
import copy
from urllib.parse import unquote
from lxml.html import tostring
from  main import *
import time



class AmazonCleaner:
    def goodsList(self, data):
        """
        商品列表清洗
        :param data:
        :return:
        """
        res = {}
        tree = etree.HTML(data)

        res_data = []
        items_data=[]
        ad_data=[]
        items = tree.xpath('//div[contains(@class,"s-result-list")]/div[@data-asin]')
        if len(items) == 0:
            res['code'] = 201
            res['msg'] = '列表为空'
            return res

        try:
            for item in items:
                judge_res= self.judge_item(item)
                if judge_res =='yes':
                    item_id= self.goodsListFieldCleaner(item, 'itemId')
                    item_title = self.goodsListFieldCleaner(item, 'title')
                    if "Muzata" in item_title:
                        IS = 1
                    else:
                        IS =0
                    solo_item = {
                        "item_id":item_id,
                        "IS":IS
                    }
                    items_data.append(solo_item)
                elif judge_res =="adyes":
                    aditem_id= self.goodsListFieldCleaner(item, 'itemId')
                    ad_data.append(aditem_id)
                else:
                    continue
            res_datadic={
                "itemlist":items_data,
                "adlist":ad_data
            }
            # res_datadic =self.mark_share(res_datadic)
            res_data.append(res_datadic)
            res['code'] = 200
            res['msg'] = 'true'
            res['data'] = res_data

        except:
            res['code'] = 401
            res['msg'] = str(traceback.format_exc())

        return res


    def judge_item(self,item):
        judge_item_yes='yes'
        judge_item_no = 'no'
        judge_itemad_yes = 'adyes'
        asin=item.xpath('./@data-asin')
        ad=item.xpath('.//span[@ class="a-color-secondary"]/text()')
        res_asin=asin[0]
        if res_asin =='':
            if len(ad) == 1:
                return judge_itemad_yes
            else:
                return judge_item_no
        else:
            if len(ad)==1:
                return judge_itemad_yes
            else:
                return judge_item_yes

    def goodsListFieldCleaner(self, item, type, clean_for_special=False):
        '''
        商品列表字段清洗
        :param item: 数据项
        :param type: 数据类型
        :return:
        '''
        # 拷贝节点对象，不然如果中途对节点做了修改，可能会出问题，比如价格取不到取更多选择中的价格时
        item = copy.copy(item)
        result = ''
        if type == 'itemId':
            result = item.xpath('./@data-asin')
            result = result[0] if result else ''

        elif type == 'img':
            result = item.xpath('.//img[@class="s-image"]/@src')
            result = result[0] if result else ''

        elif type == 'title':
            result = item.xpath('.//span[contains(@class, "a-color-base a-text-normal")]/text()')
            result = result[0] if result else ''

        return result

    def htmlCaptchaCleaner(self,data):
        tree = etree.HTML(data)
        imgUrl_list = tree.xpath('//div[@class="a-row a-text-center"]/img/@src')
        imgUrl = imgUrl_list[0] if imgUrl_list else ''
        amzn_list = tree.xpath('//input[@name="amzn"]/@value')
        amzn = amzn_list[0] if amzn_list else ''
        amznR_list = tree.xpath('//input[@name="amzn-r"]/@value')
        amznR = amznR_list[0] if amznR_list else ''
        parmCaptcha = {
            'imgUrl':imgUrl,
            'amzn': amzn,
            'amzn-r':amznR
        }
        return parmCaptcha

    def cllb (self,list):
        '''处理列表空格和括号'''
        sj =''
        for num in range(0,len(list)):
            sj = sj+list[num]
        sj =sj.replace(' ','').replace('(','').replace(')','').replace('\n','')
        return sj

    def remove_invisible_chars(self,s):
        """移除所有不可见字符，除\n外"""
        str = ''
        for x in s:
            if x is not '\n' and not x.isprintable():
                str += ''
            else:
                str += x
        return str

    def img_list_deal(self,img_list):
        img_list2 = list(set(img_list))
        img_list2.sort(key=img_list.index)
        return img_list2

    def html_deal(self,detail_html):
        detail_html = detail_html.replace("<noscript>", "")
        detail_html=detail_html.replace("</noscript>","")
        detail_html = detail_html.replace("\n", "")
        detail_html = detail_html.replace('"', "'")

        return detail_html
    def list_qc_null(self,inf_list):
        j = 0
        for i in range(len(inf_list)):
            if "\n " in inf_list[j] or "   "in inf_list[j]or " "==inf_list[j]:
                inf_list.pop(j)
            else:
                j += 1
        return inf_list





if __name__ == '__main__':
    crawler = AmazonCrawler()
    count = 1
    crawler.test()
