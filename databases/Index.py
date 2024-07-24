from elasticsearch_dsl import Date, analyzer, Keyword, Text, Document, Integer, GeoShape, Float, Long
from elasticsearch_dsl.connections import connections


class Data(Document):
    outid = Keyword()
    author = Text()
    title = Text()
    desc = Text()
    title_cn = Text()
    desc_cn = Text()
    subjects = Integer(multi=True)
    tags = Keyword(multi=True)
    privilege = Integer()
    user_id = Long()
    type = Integer()
    data_form = Integer()
    official = Integer()
    publish_time = Date()
    views = Integer()
    collect_num = Integer()
    fork_num = Integer()
    comment_num = Integer()
    share_num = Integer()
    sort = Integer()
    download = Integer()
    sync = Integer()
    is_del = Integer()
    update_date = Date()
    create_date = Date()
    change_date = Date()
    rank_value = Integer()
    dataset_flag = Integer()
    data_type = Integer()
    # location = GeoShape()
    geologicAge = Float()
    continents = Integer(multi=True)
    star_level = Integer()
    ee_flag = Integer()
    dde_flag = Integer()
    tool_type = Integer()
    source_type = Text()
    source_type_num = Integer()

    class Index:
        name = 'mydde'
        number_of_replicas = 0


if __name__ == '__main__':
    connections.create_connection(hosts=['http://localhost:9200'])
    Data.init()
