# Java Z-Day: Kinesis Analytics News Consumer Demo

Sample Apache Flink application that can be deployed to Kinesis Analytics for Java. It reads news events from a Kinesis data stream, processes and aggregates them, and ingests the result to an Amazon Elasticsearch Service cluster for visualization with Kibana.


The prepared Kibana dashboard contains a heatmap and a line graph.

![Kibana Dashboard Screen Shot](misc/kibana-dashboard-screenshot.png?raw=true)

Create ES indices and Kibana dashboard
```shell script
export KDA_DEMO_ES_URL=https://search-kda-demo-du4rptpnts3v4mqyvbgxj2wqnm.eu-west-1.es.amazonaws.com
curl --location --request PUT $KDA_DEMO_ES_URL/news_position \
--header 'Content-Type: application/json' \
--data-binary '@/~/zday-kda-analytics-news-demo/misc/news-position-index.json'

curl --location --request PUT $KDA_DEMO_ES_URL/news_words \
--header 'Content-Type: application/json' \
--data-binary '@/~/zday-kda-analytics-news-demo/misc/news-words-index.json'


# create Kinaba visualizations and a dashboard
curl --location --request POST $KDA_DEMO_ES_URL/_plugin/kibana/api/saved_objects/_bulk_create \
--header 'Content-Type: application/json' \
--header 'kbn-xsrf: true' \
--data-binary '@/~/zday-kda-analytics-news-demo/misc/export.json'

# set default Kibana index pattern
curl --location --request POST 
$KDA_DEMO_ES_URL/_plugin/kibana/api/kibana/settings \
--header 'kbn-xsrf: true' \
--data '{"changes":{"defaultIndex":"news_position"}}'
```