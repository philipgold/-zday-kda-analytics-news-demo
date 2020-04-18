package zday.kda.analytics.news.demo.events;

import zday.kda.analytics.news.demo.events.kinesis.Event;
import zday.kda.analytics.news.demo.events.kinesis.WatermarkEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimestampAssigner implements AssignerWithPunctuatedWatermarks<Event> {
  private static final Logger LOG = LoggerFactory.getLogger(TimestampAssigner.class);

  @Override
  public long extractTimestamp(Event element, long previousElementTimestamp) {
    return element.getTimestamp();
  }

  @Override
  public Watermark checkAndGetNextWatermark(Event lastElement, long extractedTimestamp) {
    return lastElement instanceof WatermarkEvent ? new Watermark(extractedTimestamp) : null;
  }
}