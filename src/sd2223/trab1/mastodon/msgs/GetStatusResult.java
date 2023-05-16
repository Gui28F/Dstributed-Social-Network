package sd2223.trab1.mastodon.msgs;

public record GetStatusResult(String id, String username) {

    public long getId() {
        return Long.valueOf(id);
    }

    public String toAccountName() {
        return username;
    }
}