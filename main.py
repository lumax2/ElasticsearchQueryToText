import json
import elasticsearch
# 车系 carLine
# 失效模式 failedMode
# 诊断仪检测故障码 troubleCode
# 故障症状 symptomComplaint
# 解决方案 corReactiveAction
# 维修经过 probableCause 结案说明 conclusion

def construct_json(new_doc):
    json_data = {}
    for key in new_doc:
        json_data[key] = new_doc[key]
    return json_data

# Prod env
es = elasticsearch.Elasticsearch(["http://10.179.152.67:9200"])
# print(es.ping())
# Test env
# es = elasticsearch.Elasticsearch(["http://10.130.151.162:9200"])
print(es.ping())

query_json = {
    "query": {
        "match": {"channel": "0"}
    }
}

size = 1000
query = es.search(index="sop_feedback_v1", body=query_json, size=size, scroll="5m")

res = []
documents = query['hits']['hits']
for i in documents:
    new_doc = {}
    carLine = i['_source']['carLine']
    failedMode = i['_source']['failedMode']
    troubleCode = i['_source']['troubleCode']
    symptomComplaint = i['_source']['symptomComplaint']
    corReactiveAction = i['_source']['corReactiveAction']
    probableCause = i['_source']['probableCause']
    conclusion = i['_source']['conclusion']
    if carLine:
        new_doc['车系'] = carLine
    if failedMode:
        new_doc['失效模式'] = failedMode
    if troubleCode:
        new_doc['诊断仪检测故障码'] = troubleCode
    if symptomComplaint:
        new_doc['故障症状'] = symptomComplaint
    if corReactiveAction:
        new_doc['解决方案'] = corReactiveAction
    if probableCause:
        new_doc['维修经过'] = probableCause
    if conclusion:
        new_doc['结案说明'] = conclusion
    res.append(construct_json(new_doc))
total = query["hits"]["total"]
scroll_id = query['_scroll_id']

with open("song.json", "w", encoding="utf-8") as f:
    for index in range(int(total / size)):
        print(index)
        doc_scroll = es.scroll(scroll_id=scroll_id, scroll="5m")["hits"]["hits"]
        for i in doc_scroll:
            new_doc = {}
            carLine = i['_source']['carLine']
            failedMode = i['_source']['failedMode']
            troubleCode = i['_source']['troubleCode']
            symptomComplaint = i['_source']['symptomComplaint']
            corReactiveAction = i['_source']['corReactiveAction']
            probableCause = i['_source']['probableCause']
            conclusion = i['_source']['conclusion']
            if carLine:
                new_doc['车系'] = carLine
            if failedMode:
                new_doc['失效模式'] = failedMode
            if troubleCode:
                new_doc['诊断仪检测故障码'] = troubleCode
            if symptomComplaint:
                new_doc['故障症状'] = symptomComplaint
            if corReactiveAction:
                new_doc['解决方案'] = corReactiveAction
            if probableCause:
                new_doc['维修经过'] = probableCause
            if conclusion:
                new_doc['结案说明'] = conclusion
            res.append(construct_json(new_doc))
    for doc in res:
        f.write(json.dumps(doc, ensure_ascii=False) + "\n")
        res = []
