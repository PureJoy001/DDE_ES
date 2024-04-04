from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Search, Q
from multiprocessing import Pool
from utils.continents import find_continents
from databases.Index import Data
from tqdm import tqdm
from functools import partial
import json
import os
import re
import datetime
from databases.sqlServer import MysqlServer
from utils.logger import MyLogger


def set_logger(log_dir='logs'):
    if not os.path.exists(os.path.join(log_dir, 'update_logs')):
        os.makedirs(os.path.join(log_dir, 'update_logs'))
    current_date = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    logger = MyLogger(os.path.join(log_dir, 'update_logs', f'update_{current_date}.log'))
    return logger


def process_item(item, tag_dict, author_dict, logging):
    try:
        if item['is_del'] == -1:
            return None
        doc = Data(meta={'id': item['id']}, sql_id=item['id'], outid=item['outid'], title=item['title'],
                   desc=item['desc'], title_cn=item['title_cn'], desc_cn=item['desc_cn'], privilege=item['privilege'],
                   user_id=item['user_id'], type=item['type'], data_form=item['data_form'], official=item['official'],
                   publish_time=item['publish_time'], views=item['views'], collect_num=item['collect_num'],
                   fork_num=item['fork_num'], comment_num=item['comment_num'], share_num=item['share_num'],
                   sort=item['sort'], download=item['download'], sync=item['sync'], is_del=item['is_del'],
                   dataset_flag=item['dataset_flag'], data_type=item['data_type'], update_date=item['update_date'],
                   create_date=item['create_date'], change_date=item['change_date'], star_level=item['star_level'],
                   ee_flag=item['ee_flag'])
        doc.rank_value = doc.views + doc.collect_num + doc.comment_num + doc.download
        sql_subjects = json.loads(item['subjects'])
        doc.subjects = [item for sublist in sql_subjects for item in sublist]
        doc.author = author_dict[int(item['author'])]
        # doc.subjects = [subject_dict[int(item)] for sublist in sql_subjects for item in sublist]
        doc_tags = [tag_dict[int(i)] for i in item['tags'].split(',')] if item['tags'] else []
        doc.tags = []
        for tag in doc_tags:
            doc.tags.extend(re.split('，；,;', tag))
        if doc.type / 10000 in [2, 6]:
            doc.continents = [7]
        if doc.type / 10000 in [4]:
            doc.tool_type = item['data40001_type']
        if doc.type in [20002, 20006, 60002]:
            doc.dde_flag = 1
        try:
            payload = json.loads(item['payload'])
            extent = json.loads(payload.get('space')) if doc.type == 60001 else payload.get('baseInfo', {})
            if extent.get('minX') and extent.get('maxY') and extent.get('maxX') and extent.get('minY'):
                # doc.location = {
                #     "type": "envelope",
                #     "coordinates": [[float(extent.get('minX')), float(extent.get('maxY'))],
                #                     [float(extent.get('maxX')), float(extent.get('minY'))]]
                # }
                doc.continents = find_continents(float(extent.get('minX')), float(extent.get('maxY')),
                                                 float(extent.get('maxX')), float(extent.get('minY')))
                if not doc.continents:  # 有的空间位置虽然存在，但不属于任何一个洲
                    doc.continents = [7]
            try:
                doc.geologicAge = float(json.loads(payload['temporal'])['geologicAge']) if doc.type == 60001 else float(
                    payload['baseInfo']['geologicAge'])
            except Exception as e:
                logging.warning(str(item['id']) + ': no valid geologicAge ' + str(e))
            # if doc.type % 10000 in [2]:
            #     try:
            #         doc.source = payload['baseInfo']['source']
            #     except Exception as e:
            #         logging.warning(str(item['id']) + ': no valid source ' + str(e))
        except Exception as e:
            logging.warning(str(item['id']) + ': no valid payload ' + str(e))
        return doc.to_dict(include_meta=True)
    except Exception as e:
        logging.warning(str(item['id']) + ' fail: ' + str(e))
        return None


def sql_data2es_docs(sql_data: list, tag_dict: dict, author_dict: dict, logging) -> list:
    logging.info('sql data len: ' + str(len(sql_data)))
    with Pool() as p:
        # es_docs = p.map(partial(process_item, tag_dict=tag_dict), sql_data)
        es_docs = list(tqdm(p.imap(partial(process_item, tag_dict=tag_dict, author_dict=author_dict, logging=logging), sql_data), total=len(sql_data)))
    es_docs = [doc for doc in es_docs if doc is not None]
    logging.info('es docs len: ' + str(len(es_docs)))
    return es_docs


def count_many(s: Search):
    # for item
    count_subjects_dict = {}
    count_keywords_dict = {}
    max_age = float('-inf')
    min_age = float('inf')
    for hit in s.scan():
        for subject in getattr(hit, 'subjects', []):
            count_subjects_dict[subject] = count_subjects_dict.get(subject, 0) + 1
        for tag in getattr(hit, 'tags', []):
            count_keywords_dict[tag] = count_keywords_dict.get(tag, 0) + 1
        if 'geologicAge' in hit:
            max_age = max(max_age, hit['geologicAge'])
            min_age = min(min_age, hit['geologicAge'])
    # service type
    count_service_dict = {
        'HTTP': s.filter(Q("terms", type=[20001])).count(),
        'Database': s.filter(Q("terms", type=[20002])).count(),
        'Map': s.filter(Q("terms", type=[20003])).count(),
        'Link': s.filter(Q("terms", type=[20004])).count(),
        'ObjectStorageData': s.filter(Q("terms", type=[20005])).count(),
        'ImageBase': s.filter(Q("terms", type=[20006])).count(),
    }
    # Spatial
    count_continent_dict = {
        'Africa': s.filter(Q("terms", continents=[3])).count(),
        'Asia': s.filter(Q("terms", continents=[0])).count(),
        'Europe': s.filter(Q("terms", continents=[2])).count(),
        'North America': s.filter(Q("terms", continents=[1])).count(),
        'South America': s.filter(Q("terms", continents=[4])).count(),
        'Oceania': s.filter(Q("terms", continents=[5])).count(),
        'Antarctica': s.filter(Q("terms", continents=[6])).count(),
        'Other': s.filter(Q("terms", continents=[7])).count()
    }
    return {"count_service_type": count_service_dict,
            "count_continent": count_continent_dict,
            "count_subjects": count_subjects_dict,
            "count_keywords": count_keywords_dict,
            "age_range": {"max_age": max_age if max_age != float('-inf') else None,
                          "min_age": min_age if min_age != float('inf') else None}
            }


def sync_data(db_config, start_time, count_cached):
    logging = set_logger()
    logging.info(start_time + " start sync mysql to ES")

    with MysqlServer(db_config) as sqlServer:
        cursor = sqlServer.cursor
        # fetch tags
        sql = "select id, name from tag"
        cursor.execute(sql)
        tag_data = cursor.fetchall()
        tag_dict = {t[0]: t[1] for t in tag_data}
        # fetch author
        sql = "select id, name from author"
        cursor.execute(sql)
        author_data = cursor.fetchall()
        author_dict = {t[0]: t[1] for t in author_data}
        # # fetch subjects
        # sql = "select id, name from subjects"
        # cursor.execute(sql)
        # subjects_data = cursor.fetchall()
        # subjects_dict = {t[0]: t[1] for t in subjects_data}
        # fetch sql data
        sql = "select * from resource where update_date >= '%s'" % (start_time)
        cursor.execute(sql)
        sql_data = cursor.fetchall()
        # 获取字段名称
        fields = [i[0] for i in cursor.description]
        sql_data_dicts = [dict(zip(fields, row)) for row in sql_data]
        es_docs = sql_data2es_docs(sql_data_dicts, tag_dict, author_dict, logging)
        # append deleted docs update
        sql = "select id from resource where is_del=-1"
        cursor.execute(sql)
        sql_data = cursor.fetchall()
        del_docs = [Data(meta={'id': item[0]}, is_del=-1).to_dict(include_meta=True) for item in sql_data]
        es_docs.extend(del_docs)
        # bulk insert/update
        logging.info(es_docs)
        connections.create_connection(hosts=[db_config['es']['host']])
        success, _ = bulk(connections.get_connection(), es_docs, index=db_config['es']['index'], raise_on_error=False)
        logging.info('Performed %d actions' % success)
        s = Search(index=db_config['es']['index'])
        count_cached.update(count_many(s))
        print(count_many(s))


if __name__ == '__main__':
    # logging.basicConfig(filename='logs/' + 'test_sync_data.log',
    #                     format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
    with open('config/config.json') as f:
        config = json.load(f)[os.getenv('ENV')]
        # config = json.load(f)['test']
    sync_data(config['db'], '2000-01-01 00:00:00', {})
