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
        ad_NO=0
        items_NO=0
        if len(items) == 0:
            res['code'] = 201
            res['msg'] = '列表为空'
            return res

        try:
            for item in items:
                judge_res= self.judge_item(item)
                if judge_res =='yes':
                    item_id= self.goodsListFieldCleaner(item, 'itemId')
                    items_NO=items_NO+1
                    solo_item={
                        "itemId":item_id,
                        "itemNo": items_NO
                    }
                    items_data.append(solo_item)
                elif judge_res =="adyes":
                    aditem_id= self.goodsListFieldCleaner(item, 'itemId')
                    ad_NO=ad_NO+1
                    solo_aditem={
                        "aditem_id":aditem_id,
                        "adNO":ad_NO,
                    }
                    ad_data.append(solo_aditem)
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

        elif type == 'badge':
            data_asin = item.xpath('./@data-asin')[0]
            result = item.xpath(f'.//div[@class="sg-row"][1]//span[@id="{data_asin}-label"]//span/text()')
            result = ''.join([i.strip('\n').strip() for i in result])

        elif type == 'sponsored':
            # result = item.xpath(
            #     './/div[@class="a-section a-spacing-none a-spacing-top-small"]/div[@class="a-row a-spacing-micro"]/span[@class="a-size-base a-color-secondary"]/text()')
            result = item.xpath(
                './/div[contains(@class, "a-section a-spacing-none")]/div[@class="a-row a-spacing-micro"]/span[@class="a-size-base a-color-secondary"]/text()')
            result = result[0] if result else ''

        elif type == 'title':
            result = item.xpath('.//span[contains(@class, "a-color-base a-text-normal")]/text()')
            result = result[0] if result else ''

        elif type == 'by':
            result = item.xpath('string(.//h2/following-sibling::div[@class="a-row a-size-base a-color-secondary"])')
            result = re.sub(' +', ' ', result.replace('\n', '').strip())

        elif type == 'url':
            result = item.xpath('.//h2/a[@class="a-link-normal a-text-normal"]/@href')
            if result:
                result = 'https://www.amazon.com' + result[0]
            else:
                result = ''

        elif type == 'rateInfo':
            rating = item.xpath('.//span[@class="a-icon-alt"]/text()')
            rate_num = item.xpath('.//a[@class="a-link-normal"]/span[@class="a-size-base"]/text()')
            rate_link = item.xpath('.//div[@class="a-row a-size-small"]//a[@class="a-link-normal"]/@href')
            itemId = item.xpath('./@data-asin')[0] if item.xpath('./@data-asin') else ''

            result = {
                'rating': rating[0] if rating else '',
                'rateNum': rate_num[0] if rate_num else '',
                'rateLink': f'https://www.amazon.com/dp/{itemId}/{rate_link[0]}' if rate_link else ''
            }

        elif type == 'priceInfo':
            if not clean_for_special:
                new_item = item.xpath('.//div[@class="a-section a-spacing-none a-spacing-top-small"]')
                if new_item:
                    item = new_item[0]
                else:
                    return result

            result = item.xpath(
                './/a[@class="a-size-base a-link-normal s-no-hover a-text-normal"]//span[@class="a-offscreen"]/text()')
            price_range_tag = item.xpath('.//span[@class="a-price-dash"]/text()')
            if price_range_tag:
                result = {'current': '-'.join(result)}
            else:
                keys = ['current', 'original']
                result = dict(zip(keys, result))
                # 单价处理
                unit_price = item.xpath('.//span[@class="a-price"]/following-sibling::span/text()')
                if unit_price:
                    result['byUnit'] = unit_price[0]

            # 若价格为空，则取'更多购买选择'中的价格
            if not result:
                more_buying_choices = self.goodsListFieldCleaner(item, 'moreBuyingChoices')
                if more_buying_choices:
                    choice_info = more_buying_choices['title']
                    current_price = re.search('(US\$[\d.,]*) ', choice_info)
                    result['current'] = current_price.group(1) if current_price else ''

        elif type == 'shipping':
            result = item.xpath('.//span[@class="a-size-small a-color-secondary"]/text()')
            result = result[0] if result else ''

        elif type == 'stockInfo':
            if not clean_for_special:
                new_item = item.xpath(
                    './/div[@class="a-section a-spacing-none a-spacing-top-micro"]/div[@class="a-row a-size-base a-color-secondary"]/span/@aria-label')
                if not new_item:
                    return result

            result = item.xpath('.//div[@class="a-row a-size-base a-color-secondary"]/span/@aria-label')
            result = result[0].strip() if result else ''

        elif type == 'moreBuyingChoices':
            if not clean_for_special:
                new_item = item.xpath('.//div[@class="a-section a-spacing-none a-spacing-top-mini"]')
                if new_item:
                    item = new_item[0]
                else:
                    return result

            more_buying_choices = item.xpath(
                './/div[@class="a-row a-size-base a-color-secondary"]/span[@class="a-size-base a-color-secondary"]/..')
            title_root = ''
            if more_buying_choices:
                node_title_root = more_buying_choices[0].xpath('.//span[@class="a-size-base a-color-secondary"]')
                if node_title_root:
                    title_root = node_title_root[0].xpath('string(.)')
                    more_buying_choices[0].remove(node_title_root[0])

                title = more_buying_choices[0].xpath('string(.)')
                link = more_buying_choices[0].xpath('.//a[@class="a-link-normal"]/@href')
                result = {
                    'title_root': title_root.replace('\n', '').strip(),
                    'title': re.sub(' +', ' ', title.replace('\n', '').strip()),
                    'link': f'https://www.amazon.com{link[0]}' if link else ''
                }

        elif type == 'subscribeSave':
            result = item.xpath(
                './/div[@class="a-section a-spacing-none a-spacing-top-small"]/div[@class="a-row a-size-small a-color-secondary"]/span/text()')
            result = [i for i in result if i.strip('\n').strip() != '']
            result = result[0] if result else ''

        elif type == 'couponSave':
            result = item.xpath(
                './/div[@class="a-section a-spacing-none a-spacing-top-small"]//span[@class="s-coupon-unclipped "]/span/text()')
            result = ' '.join([i.strip('\n').strip() for i in result])

        elif type == 'speciesType':
            result = item.xpath('string(.//a[@class="a-size-base a-link-normal a-text-bold"])')
            result = result.replace('\n', '').strip()

        elif type == 'otherSpecies':
            otherSpecies = []
            results = item.xpath('//div[@class="a-row"]/div[@class="a-row a-spacing-mini"]')
            for result in results:
                species_type = self.goodsListFieldCleaner(result, 'speciesType')
                price_info = self.goodsListFieldCleaner(result, 'priceInfo', clean_for_special=True),
                stock_info = self.goodsListFieldCleaner(result, 'stockInfo', clean_for_special=True),
                more_buying_choices = self.goodsListFieldCleaner(result, 'moreBuyingChoices', clean_for_special=True),

                species = {
                    'speciesType': species_type,
                    'priceInfo': price_info,
                    'stockInfo': stock_info[0] if stock_info else '',
                    'moreBuyingChoices': more_buying_choices[0] if more_buying_choices else '',
                }

                otherSpecies.append(species)

            result = otherSpecies

        elif type == 'otherVersions':
            other_versions = []
            versions = item.xpath(
                './/div[@class="a-row a-spacing-top-micro a-size-small a-color-base"]/a[@class="a-size-small a-link-normal"]')
            for version in versions:
                title = version.xpath('./text()')
                link = version.xpath('./@href')
                other_versions.append({
                    'title': title[0].replace('\n', '').strip() if title else '',
                    'link': f'https://www.amazon.com{link[0]}' if link else '',
                })

            result = other_versions
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

    def mark_share(self,data):
        myself_item=0
        myself_aditem =0
        item_list= data['itemlist']
        aditem_list =data['adlist']
        for solo_item in item_list:
            select_sql="""
                        select 
            """
            if select_sql == 1:
                myself_item=myself_item+1
        item_markshare =myself_item/len(item_list)
        aditem_list = data['adlist']
        for solo_aditem in aditem_list:
            select_sql = """
                                select 
                    """
            if select_sql == 1:
                aditem_list = aditem_list + 1
        aditem_markshare = aditem_list / len(aditem_list)
        data['item_markshare']=item_markshare
        data['aditem_markshare']=aditem_markshare
        return data




if __name__ == '__main__':
    crawler = AmazonCrawler()
    count = 1
    crawler.test()
