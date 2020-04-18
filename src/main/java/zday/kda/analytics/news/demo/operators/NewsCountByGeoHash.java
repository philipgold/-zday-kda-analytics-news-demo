package zday.kda.analytics.news.demo.operators;

import zday.kda.analytics.news.demo.events.es.NewsGeoCount;
import zday.kda.analytics.news.demo.events.flink.NewsGeoHash;
import com.google.common.collect.Iterables;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class NewsCountByGeoHash implements WindowFunction<NewsGeoHash, NewsGeoCount, Tuple, TimeWindow> {
  @Override
  public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<NewsGeoHash> iterable, Collector<NewsGeoCount> collector) throws Exception {
    long count = Iterables.size(iterable);
    String position = Iterables.get(iterable, 0).geoHash;

    collector.collect(new NewsGeoCount(position, count, timeWindow.getEnd()));

  }


}