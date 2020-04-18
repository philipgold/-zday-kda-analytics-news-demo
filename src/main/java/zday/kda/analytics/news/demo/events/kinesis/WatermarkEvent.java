package zday.kda.analytics.news.demo.events.kinesis;

import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WatermarkEvent extends Event {
  public final Instant watermark;

  private static final Logger LOG = LoggerFactory.getLogger(WatermarkEvent.class);

  public WatermarkEvent() {
    this.watermark = Instant.EPOCH;
  }

  @Override
  public long getTimestamp() {
    return watermark.toEpochMilli();
  }

  @Override
  public String toString() {
    return "WatermarkEvent{" +
            "watermark=" + watermark +
            '}';
  }
}
