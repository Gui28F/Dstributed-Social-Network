package sd2223.trab1.mastodon.msgs;

import sd2223.trab1.api.Message;

import java.time.Instant;

public record PostStatusResult(String id, String content, String created_at, MastodonAccount account) {

    public long getId() {
        return Long.valueOf(id);
    }

    long getCreationTime() {
        Instant instant = Instant.parse(created_at);
        return instant.toEpochMilli();
    }

    public String getText() {
        return content.substring(3, content.length() - 4);
    }

    public Message toMessage() {
        var m = new Message(getId(), account.username(), "ourorg0", getText());
        m.setCreationTime(getCreationTime());
        return m;
    }
}