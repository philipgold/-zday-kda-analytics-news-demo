package zday.kda.analytics.news.demo.events.es;

public class NewsWordCount extends Document {
    public final String word;
    public final long count;

    public NewsWordCount(String word, long count, long timestamp) {
        super(timestamp);

        this.word = word;
        this.count = count;
    }
}
