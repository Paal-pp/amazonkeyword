# import pymysql as mysql
# global base_url, db, cur
# db = mysql.connect(
#     host="127.0.0.1",
#     port=3306,
#     user="root",
#     passwd="Mld901",
#     db="Advertising_distribution")
# cur = db.cursor()
#
# select_sql = """
#     select keyword from keywordlist
# """
# cur.execute(select_sql)
# rest = cur.fetchall()
# db.commit()
# print(rest)
# for solokeyword in rest:
#     keyword =solokeyword[0]
#     print(keyword)
import json
import datetime
import pymysql as mysql
db = mysql.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    passwd="Mld901",
    db="Advertising_distribution")
cur = db.cursor()
def rate():
    keyword_select_sql= """
    select * from keywordlist
    """
    cur.execute(keyword_select_sql)
    keyword_select_sql_res = cur.fetchall()
    db.commit()
    now = datetime.datetime.now()
    nowdatetime =now.strftime("%Y-%m-%d")
    for solokeyword_inf in keyword_select_sql_res:
        solokeyword =solokeyword_inf[2]
        get_keyword_list =f"""
        select * from keywordranking where keyword= '{solokeyword}' And datetime='{nowdatetime}'
        """
        get_keyword_list_myinf =f"""
        select * from keywordranking where keyword= '{solokeyword}' AND YOR=1 AND datetime='{nowdatetime}'
        """
        cur.execute(get_keyword_list)
        get_keyword_list_res =cur.fetchall()
        db.commit()
        all_num =len(get_keyword_list_res)
        cur.execute(get_keyword_list_myinf)
        get_keyword_list_myinf_res = cur.fetchall()
        db.commit()
        yes_num =len(get_keyword_list_myinf_res)
        nature_rate =round(yes_num/all_num*100,2)
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
        ad_rate = round(yes_num / all_num * 100, 2)
        print(f"""{solokeyword}:自然排名：{nature_rate}%，广告占比：{ad_rate}%""")
        insert_sql =f"""
        Insert into keyword_rate(datetime, keyword, nature_rate, ad_rate) VALUES ('{nowdatetime}','{solokeyword}','{nature_rate}','{ad_rate}')
        """
        cur.execute(insert_sql)
        db.commit()




rate()