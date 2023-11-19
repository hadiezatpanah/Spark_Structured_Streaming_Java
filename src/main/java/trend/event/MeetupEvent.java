package trend.event;

public class MeetupEvent {
    Venue venue;
    String visibility;
    String response;
    int guests;
    Member member;
    long rsvp_id;
    long mtime;
    Event event;
    Group group;

    public MeetupEvent () {
        this.venue = new Venue();
        this.member = new Member();
        this.event = new Event();
        this.group = new Group();
    }

    public Venue getVenue() {
        return venue;
    }

    public void setVenue(Venue venue) {
        this.venue = venue;
    }

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public int getGuests() {
        return guests;
    }

    public void setGuests(int guests) {
        this.guests = guests;
    }

    public Member getMember() {
        return member;
    }

    public void setMember(Member member) {
        this.member = member;
    }

    public long getRsvp_id() {
        return rsvp_id;
    }

    public void setRsvp_id(long rsvp_id) {
        this.rsvp_id = rsvp_id;
    }

    public long getMtime() {
        return mtime;
    }

    public void setMtime(long mtime) {
        this.mtime = mtime;
    }

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }
}