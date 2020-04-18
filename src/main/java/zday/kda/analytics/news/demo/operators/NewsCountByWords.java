package zday.kda.analytics.news.demo.operators;

import zday.kda.analytics.news.demo.events.es.NewsWordCount;
import zday.kda.analytics.news.demo.events.kinesis.NewsEvent;
import com.google.common.collect.Iterables;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class NewsCountByWords implements WindowFunction<NewsEvent, NewsWordCount, Tuple, TimeWindow> {
  @Override
  public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<NewsEvent> iterable, Collector<NewsWordCount> collector) throws Exception {
    long count = Iterables.size(iterable);
    String word = Iterables.get(iterable, 0).title;

    collector.collect(new NewsWordCount(word, count, timeWindow.getEnd()));

  }


}