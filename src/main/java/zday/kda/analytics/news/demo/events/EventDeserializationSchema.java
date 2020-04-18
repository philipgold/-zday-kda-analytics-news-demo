package zday.kda.analytics.news.demo.events;

import zday.kda.analytics.news.demo.events.kinesis.Event;
import zday.kda.analytics.news.demo.events.kinesis.WatermarkEvent;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventDeserializationSchema extends AbstractDeserializationSchema<Event> {

  private static final Logger LOG = LoggerFactory.getLogger(EventDeserializationSchema.class);

  @Override
  public Event deserialize(byte[] bytes) {
    try {
      Event event = Event.parseEvent(bytes);

      if (event instanceof WatermarkEvent) {
        LOG.debug("parsed WatermarkEvent: {}", ((WatermarkEvent) event).watermark);
      }

      return event;
    } catch (Exception e) {
      LOG.debug("cannot parse event '{}'", new String(bytes, StandardCharsets.UTF_8), e);
      return null;
    }
  }

  @Override
  public boolean isEndOfStream(Event event) {
    return false;
  }

  @Override
  public TypeInformation<Event> getProducedType() {
    return TypeExtractor.getForClass(Event.class);
  }

}
