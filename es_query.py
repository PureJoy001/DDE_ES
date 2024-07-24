# from elasticsearch_dsl.connections import connections
# from elasticsearch_dsl import Search
# import json
# import os
#
# ENV = os.getenv('ENV')
# with open('config/config.json') as f:
#     config = json.load(f)[ENV]
# connections.create_connection(hosts=[config['db']['es']['host']])
#
# # 定义查询语句
# query = {
#     "query": {
#         "match": {
#             "out_id": 560
#         }
#     }
# }
#
# # 发送查询请求
# s = Search(index=config['db']['es']['index'])
# results = s(index="mydde", body=query)
#
# # 处理结果
# for hit in results['hits']['hits']:
#     print(hit['_source'])

author_dict = {1: 'Alice', 2: 'Bob', 3: 'Charlie'}  # 示例作者字典
item = {'author': [1, 2, 3]}  # 示例item字典

# 从author_dict中根据item['author']列表提取对应的值
# authors = [author_dict[author_id] for author_id in item['author']]

# 使用逗号拼接作者名字
result = ', '.join([author_dict[author_id] for author_id in item['author']])

print(result)  # 打印结果
