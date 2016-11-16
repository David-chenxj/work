# -*-coding:utf-8 -*-

import pyhs2
import logging
import traceback
import json
import datetime
import os
from time import time

HOST = '54.183.118.206'  # 54.183.65.239， 54.183.118.206， 172.31.14.226
PORT = 10000


def get_date_str(day=1):
    today = datetime.datetime.now()
    date = today - datetime.timedelta(days=day)
    return date.strftime("%Y%m%d")


def run(date):
    """
    @begin_date,end_date: %Y%m%d i.e. 20161027
    将从begin_date 到end_date期间用户点击过的文章的profile整合到user profile中去，主要是将点击过的文章profile中的相关字段的第一个关键词添加到user profile中去，
    其中包括topic,topic464,topic256,topic2048,category,keywords,title_keywords,domain
    """
    ret = {}
    user_profile_file_path = 'user_profile_%s' % date
    user_profile_file = open(user_profile_file_path, 'w')
    user_profile_file.close()
    print("start %s" % date)
    t0 = time()
    try:
        with pyhs2.connect(host=HOST, port=PORT, authMechanism="PLAIN", user="root", password="test", database="opera") as conn:
            with conn.cursor() as cur:
                print("init cursor")
                cur.execute('use opera')
                print("after 0 command")
                cur.execute('SET hive.exec.dynamic.partition=true')
                print("after first command")
                cur.execute('SET hive.exec.dynamic.partition.mode=nonstrict')
                print("after second command")
                cur.execute("""insert overwrite table news_running_feature_by_country_merged partition(dt, country)
                                select
                                  `user_id`,
                                  `request_id`,
                                  `ts`,
                                  `user_profile`,
                                  `context`,
                                  `news_entry_id`,
                                  `news_profile`,
                                  dt,
                                  country
                                from news_running_feature_by_country
                                where dt = %s
                """ % date)
                print("after third command")
                cur.execute("""select
                                  a.news_device_id,
                                  b.news_profile
                                from (
                                  select *
                                  from ml_train_prep_impr_click_joined
                                  where dt = '%s' and clicked = 1
                                ) a
                                join (
                                  select *
                                  from news_running_feature_by_country_merged
                                  where dt ='%s' and country = 'us'
                                ) b
                                on a.dt = b.dt and a.news_device_id = b.user_id and
                                  a.request_id = b.request_id and
                                  a.news_entry_id = b.news_entry_id""" % (date, date))  # ml_training_impr_click_joined
                print ("time in %fs", time() - t0)
                for row in cur.fetch():
                    uid, news_profile = row
                    news_profile = json.loads(news_profile)
                    if uid not in ret:
                        ret[uid] = {
                            'topic': {},
                            'topic64': {},
                            'topic256': {},
                            'topic2048': {},
                            'category': {},
                            'keyword': {},
                            'title_keyword': {},
                            'domain': {}
                        }
                    if 'topic' in news_profile and news_profile['topic']:
                        topic = news_profile['topic'].split(',')[0]
                        if topic not in ret[uid]['topic']:
                            ret[uid]['topic'][topic] = 0
                        ret[uid]['topic'][topic] += 1
                    if 'topic64' in news_profile and news_profile['topic64']:
                        topic = news_profile['topic64'].split(',')[0]
                        if topic not in ret[uid]['topic64']:
                            ret[uid]['topic64'][topic] = 0
                        ret[uid]['topic64'][topic] += 1
                    if 'topic256' in news_profile and news_profile['topic256']:
                        topic = news_profile['topic256'].split(',')[0]
                        if topic not in ret[uid]['topic256']:
                            ret[uid]['topic256'][topic] = 0
                        ret[uid]['topic256'][topic] += 1
                    if 'topic2048' in news_profile and news_profile['topic2048']:
                        topic = news_profile['topic2048'].split(',')[0]
                        if topic not in ret[uid]['topic2048']:
                            ret[uid]['topic2048'][topic] = 0
                        ret[uid]['topic2048'][topic] += 1
                    if 'category' in news_profile and news_profile['category']:
                        category = news_profile['category']
                        if category not in ret[uid]['category']:
                            ret[uid]['category'][category] = 0
                        ret[uid]['category'][category] += 1
                    if 'keyword' in news_profile:
                        keywords = filter(bool, news_profile['keyword'].split(','))
                        score = 1
                        for keyword in keywords[:5]:
                            if keyword not in ret[uid]['keyword']:
                                ret[uid]['keyword'][keyword] = 0
                            ret[uid]['keyword'][keyword] += score
                            score -= 0.1
                    if 'title_keyword' in news_profile:
                        keywords = filter(bool, news_profile['title_keyword'].split(','))
                        score = 1
                        for keyword in keywords[:5]:
                            if keyword not in ret[uid]['title_keyword']:
                                ret[uid]['title_keyword'][keyword] = 0
                            ret[uid]['title_keyword'][keyword] += score
                            score -= 0.1
                    if 'domain' in news_profile and news_profile['domain']:
                        domain = news_profile['domain']
                        if domain not in ret[uid]['domain']:
                            ret[uid]['domain'][domain] = 0
                        ret[uid]['domain'][domain] += 1
                    if len(ret) >= 100000:
                        merge_to_user_profile(ret, user_profile_file_path)
                        ret.clear()
        merge_to_user_profile(ret, user_profile_file_path)
    except Exception as e:
        logging.error('run: error: %s, %s', e.message, traceback.format_exc())
        raise e

    return ret


def merge_dict(target, source):
    for key in source:
        if key not in target:
            target[key] = 0
        target[key] += source[key]


def merge_to_user_profile(ret, user_profile_file_path):
    print("write ret to file")
    t0 = time()
    user_profile_file = open(user_profile_file_path, 'r+')
    try:
        if os.stat(user_profile_file_path).st_size == 0:
            print("first dumps")
            json.dump(ret, user_profile_file)
        else:
            print("load from file")
            prev_user_profile = json.load(user_profile_file)

            for uid in ret:
                if uid not in prev_user_profile:
                    prev_user_profile[uid] = ret[uid]
                else:
                    merge_dict(prev_user_profile[uid]['topic'], ret[uid]['topic'])
                    merge_dict(prev_user_profile[uid]['topic64'], ret[uid]['topic64'])
                    merge_dict(prev_user_profile[uid]['topic256'], ret[uid]['topic256'])
                    merge_dict(prev_user_profile[uid]['topic2048'], ret[uid]['topic2048'])
                    merge_dict(prev_user_profile[uid]['category'], ret[uid]['category'])
                    merge_dict(prev_user_profile[uid]['keyword'], ret[uid]['keyword'])
                    merge_dict(prev_user_profile[uid]['title_keyword'], ret[uid]['title_keyword'])
                    merge_dict(prev_user_profile[uid]['domain'], ret[uid]['domain'])

            user_profile_file.seek(0)
            json.dump(prev_user_profile, user_profile_file)
    except Exception as e:
        logging.error('merge_to_user_profile: error: %s, %s', e.message, traceback.format_exc())
        raise e

    finally:
        if user_profile_file:
            user_profile_file.close()
        print ("time in %fs", time() - t0)


def merge_daily_file(folder, file_path):
    if not folder or not os.path.exists(folder):
        raise Exception("input folder path is empty or not exist!")

    t0 = time()
    aggregated_user_profile = {}
    aggregated_user_profile_file = open(file_path, 'w')
    try:
        for file_name in [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]:
            with open(os.path.join(folder, file_name), 'r') as f:
                user_profile = json.load(f)
                dt = file_name[file_name.rindex('_') + 1:]
            for uid in user_profile:
                if uid not in aggregated_user_profile:
                    aggregated_user_profile[uid] = user_profile[uid]
                    aggregated_user_profile[uid]['last_modify_time'] = dt
                else:
                    merge_dict(aggregated_user_profile[uid]['topic'], user_profile[uid]['topic'])
                    merge_dict(aggregated_user_profile[uid]['topic64'], user_profile[uid]['topic64'])
                    merge_dict(aggregated_user_profile[uid]['topic256'], user_profile[uid]['topic256'])
                    merge_dict(aggregated_user_profile[uid]['topic2048'], user_profile[uid]['topic2048'])
                    merge_dict(aggregated_user_profile[uid]['category'], user_profile[uid]['category'])
                    merge_dict(aggregated_user_profile[uid]['keyword'], user_profile[uid]['keyword'])
                    merge_dict(aggregated_user_profile[uid]['title_keyword'], user_profile[uid]['title_keyword'])
                    merge_dict(aggregated_user_profile[uid]['domain'], user_profile[uid]['domain'])
                    if 'last_modify_time' not in aggregated_user_profile[uid]:
                        aggregated_user_profile[uid]['last_modify_time'] = dt
                    else:
                        if datetime.datetime.strptime(dt, '%Y%m%d') > datetime.datetime.strptime(aggregated_user_profile[uid]['last_modify_time'], '%Y%m%d'):
                            aggregated_user_profile[uid]['last_modify_time'] = dt

        json.dump(aggregated_user_profile, aggregated_user_profile_file)
    except Exception as e:
        logging.error('merge_daily_file: error: %s, %s', e.message, traceback.format_exc())
        raise e

    finally:
        if aggregated_user_profile_file:
            aggregated_user_profile_file.close()
        print ("time in %fs", time() - t0)


def merge_two_user_profile_file(old_file, new_file, out_json_file, out_hive_file):
    if not os.path.isfile(old_file) or not os.path.isfile(new_file):
        raise Exception("input file not exist!")

    t0 = time()
    user_profile_file_old = open(old_file, 'r')
    user_profile_file_new = open(new_file, 'r')
    user_profile_file_json = open(out_json_file, 'w')
    user_profile_file_hive = open(out_hive_file, 'w')

    try:
            dt = new_file[new_file.rindex('_') + 1:]
            user_profile_aggregated = json.load(user_profile_file_old)
            user_profile_daily = json.load(user_profile_file_new)

            for uid in user_profile_daily:
                if uid not in user_profile_aggregated:
                    user_profile_aggregated[uid] = user_profile_daily[uid]
                    user_profile_aggregated[uid]['last_modify_time'] = dt
                else:
                    merge_dict(user_profile_aggregated[uid]['topic'], user_profile_daily[uid]['topic'])
                    merge_dict(user_profile_aggregated[uid]['topic64'], user_profile_daily[uid]['topic64'])
                    merge_dict(user_profile_aggregated[uid]['topic256'], user_profile_daily[uid]['topic256'])
                    merge_dict(user_profile_aggregated[uid]['topic2048'], user_profile_daily[uid]['topic2048'])
                    merge_dict(user_profile_aggregated[uid]['category'], user_profile_daily[uid]['category'])
                    merge_dict(user_profile_aggregated[uid]['keyword'], user_profile_daily[uid]['keyword'])
                    merge_dict(user_profile_aggregated[uid]['title_keyword'], user_profile_daily[uid]['title_keyword'])
                    merge_dict(user_profile_aggregated[uid]['domain'], user_profile_daily[uid]['domain'])
                    user_profile_aggregated[uid]['last_modify_time'] = dt

            json.dump(user_profile_aggregated, user_profile_file_json)

            for uid in user_profile_aggregated:
                user_profile_file_hive.write('%s\t%s\t%s\t%s\n' % (uid, user_profile_aggregated[uid], user_profile_aggregated[uid]['last_modify_time'], dt))

    except Exception as e:
        logging.error('merge_to_user_profile: error: %s, %s', e.message, traceback.format_exc())
        raise e

    finally:
        if user_profile_file_json:
            user_profile_file_json.close()
        if user_profile_file_hive:
            user_profile_file_hive.close()
        print ("time in %fs", time() - t0)


def create_user_profile_table(table_name):
    with pyhs2.connect(host=HOST, port=PORT, authMechanism="PLAIN", user="root", password="test",
                       database="opera") as conn:
        with conn.cursor() as cur:
            cur.execute('use opera')
            cur.execute("""
            CREATE TABLE IF NOT EXISTS %s(user_id string,user_profile string, last_modify_time string)
            COMMENT 'us user profile aggregate long time window'
            PARTITIONED BY (dt string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '\t'
            LINES TERMINATED BY '\n'
            """ % table_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    create_user_profile_table('user_profile_long')

