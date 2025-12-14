curl -k -X POST "https://localhost:9200/users/_search" -u "elastic:HUarUJmrvXjpwU7d+ji7" -H "Content-Type: application/json" -d '{"from":0,"size":10,"query":{"match_all":{}},"sort":[{"id.keyword":{"order":"asc"}}]}'

running the elastic
docker run -d --name elastic -p 9200:9200 -e "discovery.type=single-node" -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" docker.elastic.co/elasticsearch/elasticsearch:8.19.8

changing elastic password
bin/elasticsearch-reset-password -u elastic

making schema for products
curl -k -u "elastic:8*jidDJpxKBs0=aQ*9CS" -X PUT "https://localhost:9200/products" -H "Content-Type: application/json" -d '{"mappings":{"properties":{"id":{"type":"integer"},"name":{"type":"text","analyzer":"standard","fields":{"keyword":{"type":"keyword"}}},"product_variant":{"type":"nested","properties":{"id":{"type":"integer"},"name":{"type":"text","analyzer":"standard","fields":{"keyword":{"type":"keyword"}}}}}}}}'

making schema for users
curl -k -u "elastic:8*jidDJpxKBs0=aQ*9CS" -X PUT "https://localhost:9200/users" -H "Content-Type: application/json" -d '{"mappings":{"properties":{"id":{"type":"integer"},"name":{"type":"text","analyzer":"standard","fields":{"keyword":{"type":"keyword"}}},"age":{"type":"integer"},"attribute":{"type":"object"},"fav":{"type":"keyword"}}}}'

query for searching
curl -X POST "https://localhost:9200/products/_search" -u elastic:8*jidDJpxKBs0=aQ*9CS -H "Content-Type: application/json" -d '{"query":{"match":{"name":"arash"}}}'
curl -X POST "https://localhost:9200/products/_search" -u elastic:8*jidDJpxKBs0=aQ*9CS -H "Content-Type: application/json" -d '{"query":{"match":{"_id":"69382beffa9abc9b783dcd72"}}}'

query for first 10 records
curl -k -X POST "https://localhost:9200/users/_search" -u "elastic:8*jidDJpxKBs0=aQ*9CS" -H "Content-Type: application/json" -d '{"from":0,"size":10,"query":{"match_all":{}},"sort":[{"id.keyword":{"order":"asc"}}]}'

query for deleting all the data
curl -X POST "https://localhost:9200/users/_delete_by_query" -H 'Content-Type: application/json' -u "elastic:8*jidDJpxKBs0=aQ*9CS" -d '{"query": {"match_all": {}}}' -k


query for counting all the data
curl -k -u "elastic:8*jidDJpxKBs0=aQ*9CS" -X GET "https://localhost:9200/users/_count"
