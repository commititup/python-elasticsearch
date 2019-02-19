# one.com-python-elasticsearch

This Module does query to elasticsearch . Written on top of python-elasticsearch module

## Dependency
depends on python-elasticsearch ( > 5.4)
## How to Use:

Ex:-

```
from mod_elasticsearch import mod_elasticsearch

#create Object
test=mod_elasticsearch.ElasticQuery()

#create Object with index and config file
test=mod_elasticsearch.ElasticQuery('logstash-*','./config.json')

# Query To get the total hits count with relative time.
test.query("host:test* AND message",timeinterval='24h',count=True)

# Query To get the  result with relative time
test.query("host:test* AND message",'15m')

# Query To get the  result with a start and end time
test.query("host:test* AND message",timeinterval=['2019-02-05 05:00','2019-02-05 06:00'])
```

