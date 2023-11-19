package trend.event;

import java.util.ArrayList;
import java.util.List;

public class Group {
    List<GroupTopic> group_topics;
    String group_city;
    String group_country;
    int group_id;
    String group_name;
    double group_lon;
    String group_urlname;
    double group_lat;

    public Group() {

        this.group_topics = new ArrayList<>();
        this.group_topics.add(new GroupTopic("", ""));
    }

    public List<GroupTopic> getGroup_topics() {
        return group_topics;
    }

    public void setGroup_topics(List<GroupTopic> group_topics) {
        this.group_topics = group_topics;
    }

    public String getGroup_city() {
        return group_city;
    }

    public void setGroup_city(String group_city) {
        this.group_city = group_city;
    }

    public String getGroup_country() {
        return group_country;
    }

    public void setGroup_country(String group_country) {
        this.group_country = group_country;
    }

    public int getGroup_id() {
        return group_id;
    }

    public void setGroup_id(int group_id) {
        this.group_id = group_id;
    }

    public String getGroup_name() {
        return group_name;
    }

    public void setGroup_name(String group_name) {
        this.group_name = group_name;
    }

    public double getGroup_lon() {
        return group_lon;
    }

    public void setGroup_lon(double group_lon) {
        this.group_lon = group_lon;
    }

    public String getGroup_urlname() {
        return group_urlname;
    }

    public void setGroup_urlname(String group_urlname) {
        this.group_urlname = group_urlname;
    }

    public double getGroup_lat() {
        return group_lat;
    }

    public void setGroup_lat(double group_lat) {
        this.group_lat = group_lat;
    }
}