package trend.event;

public class GroupTopic {
    String urlkey;
    String topic_name;

    public GroupTopic(String urlkey, String topic_name) {
        this.urlkey = urlkey;
        this.topic_name = topic_name;
    }

    public String getUrlkey() {
        return urlkey;
    }

    public void setUrlkey(String urlkey) {
        this.urlkey = urlkey;
    }

    public String getTopic_name() {
        return topic_name;
    }

    public void setTopic_name(String topic_name) {
        this.topic_name = topic_name;
    }
}