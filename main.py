from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Search, Q
from fastapi import FastAPI, Query
# from apscheduler.schedulers.asyncio import AsyncIOScheduler
from concurrent.futures import ThreadPoolExecutor
import asyncio
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import List
import logging
from sync import sync_data, count_many, dump_data, stop_sync_event
from functools import reduce
import operator
import json
import sys
import os
import requests
from threading import Thread
from databases.Index import Data

logging.basicConfig(filename='logs/fastapi_log', format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
executor = ThreadPoolExecutor(max_workers=5)
# ENV = 'test'
ENV = os.getenv('ENV')
app = FastAPI()
# scheduler = AsyncIOScheduler()
count_cached = {}

with open('config/config.json') as f:
    config = json.load(f)[ENV]

connections.create_connection(hosts=[config['db']['es']['host']])


def rank_es(hits: list) -> list:
    """
        综合排序es返回的结果
    Args:
        hits: 经过ES文本相关性与条件过滤后命中的结果列表，hits[i][_score]表示文本相关性分数，hits[i][_id]表示数据id(str),
              hits[i][_source]存储了views,collect_num,share_num,fork_num等字段信息

    Returns:
        int类型的id列表
    """
    if not hits:
        return []
    scores = [0] * len(hits)
    for i, item in enumerate(hits):
        scores[i] += item["_source"]["sort"]
        scores[i] += item["_source"]["views"] % 10
        time_delta = datetime.now() - datetime.strptime(item["_source"]["publish_time"], "%Y-%m-%dT%H:%M:%S")
        if time_delta.days == 0:
            scores[i] += 50
        elif time_delta.days <= 14:
            scores[i] += 3
    # 按照score对id进行排序
    idx = [item["_id"] for item in hits]
    id_list = sorted(idx, key=lambda x: scores[idx.index(x)], reverse=True)
    return id_list


class Item(BaseModel):
    text: str = ""  # 待匹配文本
    sort: int = 0  # 0综合排序、1浏览量、2下载量、3收藏量、4更新时间
    tags: List[str] = []
    subjects: List[int] = []
    types: List[int] = []
    source_types: List[str] = []
    tool_types: List[int] = []
    geoAge: List[float] = []
    continents: List[int] = []
    ids: List[int] = []
    search_type: int = 0  # 0all、1title、2author、3keywords


@app.post("/search")
async def search(item: Item, pageNum: int = Query(1), pageSize: int = Query(10), test_mode=False):
    s = Search(index=config['db']['es']['index'])
    fields = [
        ["title^6", "desc", "tags^3", "author^6"],
        ["title"],
        ["author"],
        ["tags"]]
    multi_match_query = Q("multi_match", query=item.text, fields=fields[item.search_type],
                          type="best_fields", fuzziness="AUTO", zero_terms_query="all")
    filters = [Q("term", is_del=0), Q("term", privilege=2)]
    if item.text:
        filters.append(~Q("term", dataset_flag=1))
    if item.ids:
        ids_filter = Q("ids", values=[str(_id) for _id in item.ids])
        filters.append(ids_filter)
    if item.types:
        types_filter = Q("terms", type=item.types)
        filters.append(types_filter)
    if item.subjects:
        subjects_filter = Q("terms", subjects=item.subjects)
        filters.append(subjects_filter)
    if item.tags:
        tags_filter = Q("terms", tags=item.tags)
        filters.append(tags_filter)
    if item.continents:
        continents_filter = Q("terms", continents=item.continents)
        filters.append(continents_filter)
    if item.geoAge:
        geologicAge_filter = Q("range", geologicAge={"gte": item.geoAge[0], "lte": item.geoAge[1]})
        filters.append(geologicAge_filter)
    if item.source_types:
        source_types_filter = Q("terms", source_type=item.source_types)
        filters.append(source_types_filter)
    if item.tool_types:
        tool_types_filter = Q("terms", tool_type=item.tool_types)
        filters.append(tool_types_filter)
    s = s.query(multi_match_query)
    s = s.filter(reduce(operator.and_, filters))
    total_num = s.count()
    # ES sort
    sort_rules = []
    # baseInfo.source来源暂时未确定，实现思路是存ES时就把str按照来源优先级转成相对大小的int，或者在Index.py中设置为keyword
    # if set(item.types).issubset({20001, 20002, 20003, 20004, 20005, 20006}):  # data
    #     sort_rules.extend([
    #         {"star_level": {"order": "desc"}},
    #         {"ee_flag": {"order": "desc"}},
    #         {"dde_flag": {"order": "desc"}}])
    if 0 != item.sort:
        sort_field = [None, "views", "download", "collect_num", "change_date"]  # 0综合排序、1浏览量、2下载量、3收藏量、4更新时间
        sort_rules.append({sort_field[item.sort]: {"order": "desc"}})
    if not item.text and 0 == item.sort:
        sort_rules.extend([
            {"data_type": {"order": "asc"}},
            {"rank_value": {"order": "desc"}}])
    if set(item.types).issubset({40001}):
        sort_rules.append({"tool_type": {"order": "desc"}})
    s = s.sort(*sort_rules)
    # display
    max_pageNum = 1 + (total_num - 1) // pageSize
    if 0 == max_pageNum:
        max_pageNum = 1
    if pageNum > max_pageNum:
        pageNum = max_pageNum
    s_page = s[(pageNum - 1) * pageSize: pageNum * pageSize]
    # s = s.source(["1", "2", "3"])

    # search & rank
    response = s_page.execute()
    es_hits = response['hits']['hits']
    id_list = rank_es(es_hits)
    if not test_mode:
        result = {"total": total_num, "ids": id_list}
        result.update(count_many(s))
        return result
    else:
        return item, es_hits


@app.get("/count_all")
async def count_all():
    return count_cached


@app.get("/count")
async def count(item: Item):
    count_dict = {
        'all': 0,
        'cases': 1,
        'data': 2,
        'models': 3,
        'tools': 4,
        'disciplinary_platform': 5
    }
    cases = [10001]
    data = [20001, 20002, 20003, 20004, 20005, 20006]
    models = [30001, 30003, 30004]
    tools = [40001]
    disciplinary_platform = [50001, 50002, 50003]

    s = Search(index=config['db']['es']['index'])
    multi_match_query = Q("multi_match", query=item.text, fields=["title", "desc", "tags", "author"],
                          type="best_fields", fuzziness="AUTO", zero_terms_query="all")
    s = s.query(multi_match_query)

    filters = [Q("term", is_del=0), Q("term", privilege=2)]
    s = s.filter(reduce(operator.and_, filters))
    count_dict['all'] = s.count()
    count_dict['cases'] = s.filter(Q("terms", type=cases)).count()
    count_dict['data'] = s.filter(Q("terms", type=data)).count()
    count_dict['models'] = s.filter(Q("terms", type=models)).count()
    count_dict['tools'] = s.filter(Q("terms", type=tools)).count()
    count_dict['disciplinary_platform'] = s.filter(Q("terms", type=disciplinary_platform)).count()
    return count_dict


@app.get("/count/continents")
async def count_continents():
    count_dict = {
        'Africa': 3,
        'Asia': 0,
        'Europe': 2,
        'North America': 1,
        'South America': 4,
        'Oceania': 5,
        'Antarctica': 6,
        'Other': 7
    }

    s = Search(index=config['db']['es']['index'])
    multi_match_query = Q("multi_match", query="", fields=["title", "desc", "tags", "author"],
                          type="best_fields", fuzziness="AUTO", zero_terms_query="all")
    s = s.query(multi_match_query)

    filters = [Q("term", is_del=0), Q("term", privilege=2)]
    s = s.filter(reduce(operator.and_, filters))
    count_dict['Africa'] = s.filter(Q("terms", continents=[3])).count()
    count_dict['Asia'] = s.filter(Q("terms", continents=[0])).count()
    count_dict['Europe'] = s.filter(Q("terms", continents=[2])).count()
    count_dict['North America'] = s.filter(Q("terms", continents=[1])).count()
    count_dict['South America'] = s.filter(Q("terms", continents=[4])).count()
    count_dict['Oceania'] = s.filter(Q("terms", continents=[5])).count()
    count_dict['Antarctica'] = s.filter(Q("terms", continents=[6])).count()
    count_dict['Other'] = s.filter(Q("terms", continents=[7])).count()
    return count_dict


@app.post("/recommend")
async def recommend(item: Item, pageNum: int = Query(1), pageSize: int = Query(10)):
    # 根据某条数据推荐相关数据
    id_list = []
    print(item.title)
    return id_list


# @scheduler.scheduled_job('interval', minutes=config['update_interval'])
# async def cron_job():
#     loop = asyncio.get_event_loop()
#     await loop.run_in_executor(executor, sync_data, config['db'], (datetime.now() - timedelta(minutes=(config['update_interval']+1))).strftime("%Y-%m-%d %H:%M:%S"), count_cached)


# start scheduler
@app.on_event("startup")
async def startup_event():
    # 启动命令：INDEX=1 START_TIME="2000-01-01 00:00:00" uvicorn main:app --reload
    # 如果不想在启动的同时重刷ES，就把上述开始时间设置为最近，如果需要修改ES索引，就把INDEX设置为1
    need_index = os.getenv("INDEX", 0)
    if need_index != 0:
        requests.delete(config['db']['es']['host'] + '/' + config['db']['es']['index'])
        Data.init()
    start_time_str = os.getenv("START_TIME", "2025-01-01 00:00:00")
    dump_data(config['db'], start_time_str, count_cached)
    global sync_thread
    sync_thread = Thread(target=sync_data, args=(config['db'], count_cached))
    sync_thread.start()
    # scheduler.start()


@app.on_event("shutdown")
async def shutdown_event():
    print("Shutting down...")
    stop_sync_event.set()  # Signal the sync_service to stop
    sync_thread.join()  # Wait for the thread to finish
