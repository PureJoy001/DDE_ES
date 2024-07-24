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
from datetime import datetime, timedelta
import time
from databases.sqlServer import MysqlServer
from utils.logger import MyLogger
from canal.protocol import EntryProtocol_pb2
from canal.client import Client
from threading import Event
stop_sync_event = Event()  # stop canal


def set_logger(log_dir='logs'):
    if not os.path.exists(os.path.join(log_dir, 'update_logs')):
        os.makedirs(os.path.join(log_dir, 'update_logs'))
    current_date = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
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
        # doc.subjects = [subject_dict[int(item)] for sublist in sql_subjects for item in scdublist]

        doc.author = ', '.join([author_dict.get(int(author_id), "") for author_id in item['author'].split(',')])
        # doc.author = author_dict.get(int(item['author']), "") # 一对一
        doc_tags = [tag_dict[int(i)] for i in item['tags'].split(',')] if item['tags'] else []
        doc.tags = []
        for tag in doc_tags:
            doc.tags.extend(re.split('，；,;', tag))
        if int(doc.type / 10000) in [2, 6]:
            doc.continents = [7]
        if int(doc.type / 10000) in [4]:
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
            if int(doc.type / 10000) in [2]:
                try:
                    doc.source_type = payload['baseInfo']['sourceType']
                    # source_type_dict = {"linkdata": 0, "": 1, "": 2}
                    # doc.source_type_num = source_type_dict[doc.source_type]
                except Exception as e:
                    logging.warning(str(item['id']) + ': no valid sourceType ' + str(e))
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
    # source type
    count_source_type_dict = {
        'Link': s.filter(Q("term", source_type="linkdata")).count(),
        'Host': s.filter(Q("term", source_type="host")).count(),
        'Discovery': s.filter(Q("term", source_type="discovery")).count(),
    }
    return {"count_service_type": count_service_dict,
            "count_continent": count_continent_dict,
            "count_subjects": count_subjects_dict,
            "count_keywords": count_keywords_dict,
            "count_source_type": count_source_type_dict,
            "age_range": {"max_age": max_age if max_age != float('-inf') else None,
                          "min_age": min_age if min_age != float('inf') else None}
            }


# def sync_data(db_config, start_time, count_cached):
#     logging = set_logger()
#     logging.info(start_time + " start sync mysql to ES")
#
#     with MysqlServer(db_config) as sqlServer:
#         cursor = sqlServer.cursor
#         # fetch tags
#         sql = "select id, name from tag"
#         cursor.execute(sql)
#         tag_data = cursor.fetchall()
#         tag_dict = {t[0]: t[1] for t in tag_data}
#         # fetch author
#         sql = "select id, name from author"
#         cursor.execute(sql)
#         author_data = cursor.fetchall()
#         author_dict = {t[0]: t[1] for t in author_data}
#         # # fetch subjects
#         # sql = "select id, name from subjects"
#         # cursor.execute(sql)
#         # subjects_data = cursor.fetchall()
#         # subjects_dict = {t[0]: t[1] for t in subjects_data}
#         # fetch sql data
#         sql = "select * from resource where update_date >= '%s'" % (start_time)
#         cursor.execute(sql)
#         sql_data = cursor.fetchall()
#         # 获取字段名称
#         fields = [i[0] for i in cursor.description]
#         sql_data_dicts = [dict(zip(fields, row)) for row in sql_data]
#         es_docs = sql_data2es_docs(sql_data_dicts, tag_dict, author_dict, logging)
#         # append deleted docs update
#         sql = "select id from resource where is_del=-1"
#         cursor.execute(sql)
#         sql_data = cursor.fetchall()
#         del_docs = [Data(meta={'id': item[0]}, is_del=-1).to_dict(include_meta=True) for item in sql_data]
#         es_docs.extend(del_docs)
#         # bulk insert/update
#         # 通常es_docs中每个doc都要有'_op_type': 'index'这个键值对来指明操作类型（'index', 'create', 'update', 'delete'）
#         # 但是我们只做软删除，所以只做插入和更新，不填该字段就默认index
#         logging.info(es_docs)
#         connections.create_connection(hosts=[db_config['es']['host']])
#         success, _ = bulk(connections.get_connection(), es_docs, index=db_config['es']['index'], raise_on_error=False)
#         logging.info('Performed %d actions' % success)
#         s = Search(index=db_config['es']['index'])
#         count_cached.update(count_many(s))
#         print(count_many(s))


def dump_data(db_config, start_time, count_cached):
    logging = set_logger()
    logging.info(start_time + " start dump mysql to ES")

    sql = "SELECT * FROM resource WHERE update_date >= '%s'" % start_time
    tag_dict, author_dict, sql_data_dicts = get_sql_tables(db_config, sql)
    es_docs = sql_data2es_docs(sql_data_dicts, tag_dict, author_dict, logging)
    insert_update_bulk_and_count(logging, db_config, es_docs, count_cached)


def insert_update_bulk_and_count(logging, db_config, es_docs, count_cached):
    logging.info(es_docs)
    connections.create_connection(hosts=[db_config['es']['host']])
    success, _ = bulk(connections.get_connection(), es_docs, index=db_config['es']['index'], raise_on_error=False)
    logging.info('Performed %d actions' % success)
    s = Search(index=db_config['es']['index'])
    count_cached.update(count_many(s))


def get_sql_tables(db_config, resource_sql):
    with MysqlServer(db_config) as sqlServer:
        cursor = sqlServer.cursor
        # fetch tags
        sql = "SELECT id, name FROM tag"
        cursor.execute(sql)
        tag_data = cursor.fetchall()
        tag_dict = {t[0]: t[1] for t in tag_data}
        # fetch author
        sql = "SELECT id, name FROM author"
        cursor.execute(sql)
        author_data = cursor.fetchall()
        author_dict = {t[0]: t[1] for t in author_data}
        # # fetch subjects
        # sql = "select id, name from subjects"
        # cursor.execute(sql)
        # subjects_data = cursor.fetchall()
        # subjects_dict = {t[0]: t[1] for t in subjects_data}
        # fetch sql data
        if resource_sql:
            cursor.execute(resource_sql)
            sql_data = cursor.fetchall()
            # 获取字段名称
            fields = [i[0] for i in cursor.description]
            sql_data_dicts = [dict(zip(fields, row)) for row in sql_data]
            return tag_dict, author_dict, sql_data_dicts
        else:
            return tag_dict, author_dict, []


def has_changed(before, after):
    item_id = None
    before_name = None
    after_name = None
    for column in before:
        if column.name == "name":
            before_name = column.value
        elif column.name == "id":
            item_id = column.value
    for column in after:
        if column.name == "name":
            after_name = column.value
            break
    if before_name == after_name:
        return None
    else:
        return item_id


def convert_value(mysql_type, value):
    mysql_type = mysql_type.lower()
    try:
        if 'int' in mysql_type:  # TINYINT / SMALLINT / INT / BIGINT
            return int(value)
        elif 'float' in mysql_type or 'double' in mysql_type or 'decimal' in mysql_type or 'numeric' in mysql_type:
            return float(value)
        elif 'date' in mysql_type or 'timestamp' in mysql_type:
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        elif 'time' in mysql_type:
            return datetime.strptime(value, '%H:%M:%S').time()
        elif 'bool' in mysql_type or 'tinyint(1)' in mysql_type:
            return value.lower() in ('true', '1')
        else:
            return value  # 默认返回字符串
    except ValueError as e:
        print(f"Error converting value '{value}' of type {mysql_type}: {e}")
        return value  # 发生错误时返回原始字符串值


def concat_sql_str(update_data):
    author_str = ', '.join(map(str, update_data["author"]))
    tag_str = ''
    for tag in update_data["tag"]:
        tag_str = tag_str + f"FIND_IN_SET('{tag}', tags) > 0 OR "
    if tag_str != '':
        tag_str = tag_str[:-4]
        tag_sql_str = f"SELECT * FROM resource WHERE {tag_str}"
    if author_str != '':
        author_sql_str = f"SELECT * FROM resource WHERE author IN ({author_str})"
    if not tag_str and not author_str:
        return None
    elif tag_str and not author_str:
        return tag_sql_str
    elif not tag_str and author_str:
        return author_sql_str
    else:
        return tag_sql_str + " UNION " + author_sql_str


# 后期业务无需 update_time 来同步ES数据，各种time只用在开启服务时刷新ES
# 删除也能做硬性删除
def sync_data(db_config, count_cached):
    client = Client()
    client.connect(host='127.0.0.1')
    client.check_valid()
    client.subscribe()

    poll_interval = 1  # 初始轮询间隔为1秒
    max_poll_interval = 60  # 最大轮询间隔为60秒
    min_poll_interval = 1  # 最小轮询间隔为1秒

    while not stop_sync_event.is_set():
        # 目前每次修改数量级在千左右，日志设置为每次有回复的轮询一个文件
        message = client.get(1000)
        entries = message['entries']
        if entries:
            logging = set_logger()
            time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logging.info(time_now + " start sync mysql to ES")
            sql_data = []
            update_data = {"tag": [], "author": []}
            # entry是语句级，遍历所有entry，要先把对不同表的操作分拣出来
            for entry in entries:
                entry_type = entry.entryType
                if (entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN, EntryProtocol_pb2.EntryType.TRANSACTIONEND]
                        or entry.header.schemaName != "dde_portal"):
                    continue
                row_change = EntryProtocol_pb2.RowChange()
                row_change.MergeFromString(entry.storeValue)
                # event_type = row_change.eventType
                table = entry.header.tableName
                event_type = entry.header.eventType
                if (table == "tag" or table == "author") and event_type == EntryProtocol_pb2.EventType.UPDATE:
                    for row in row_change.rowDatas:
                        changed_name = has_changed(row.beforeColumns, row.afterColumns)
                        if changed_name:
                            update_data[table].append(changed_name)
                elif table == "resource":
                    for row in row_change.rowDatas:
                        format_data = dict()
                        if event_type == EntryProtocol_pb2.EventType.DELETE:
                            pass
                        # elif event_type == EntryProtocol_pb2.EventType.INSERT:
                        else:
                            for column in row.afterColumns:
                                format_data[column.name] = convert_value(column.mysqlType, column.value)
                            sql_data.append(format_data)
            sql_str = concat_sql_str(update_data)
            # print(sql_str)
            tag_dict, author_dict, sql_data_related = get_sql_tables(db_config, sql_str)
            sql_data.extend(sql_data_related)
            es_docs = sql_data2es_docs(sql_data, tag_dict, author_dict, logging)
            insert_update_bulk_and_count(logging, db_config, es_docs, count_cached)
            poll_interval = min_poll_interval
        else:
            poll_interval = min(poll_interval * 2, max_poll_interval)
        time.sleep(poll_interval)  # 鉴于canal配置的复杂性、协议选取、长连接的稳定性等问题，且现在的瓶颈不在于此，所以依然选用轮询的方式和canal交互
        # 之前修改 is_del 不会修改update_date，所以要把软删除的数据全查出来更新一遍，但是现在所有修改都会同步
        # author 和 tag 表要在之前resource表之前更新，所以不考虑 resource 中的键在其他表中查不到值的情况
        # 效率优化考虑不整个修改doc，只改更新的字段
    client.disconnect()


if __name__ == '__main__':
    # logging.basicConfig(filename='logs/' + 'test_sync_data.log',
    #                     format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
    with open('config/config.json') as f:
        config = json.load(f)[os.getenv('ENV')]
        # config = json.load(f)['test']
    # 仅供测试使用，无需开启，会在main里开启增量同步
    dump_data(config['db'], '2024-07-16 17:48:53', {})
