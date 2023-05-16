package sd2223.trab1.mastodon.msgs;

public record GetStatusArgs(String status, String visibility) {

    public GetStatusArgs(String msg) {
        this(msg, "private");
    }
}