package sd2223.trab1.mastodon.msgs;

import sd2223.trab1.api.Message;

public record PostStatusResult(String id, String content, String created_at, MastodonAccount account) {

    public long getId() {
        return Long.valueOf(id);
    }

    long getCreationTime() {
        return 0;
    }

    public String getText() {
        return content.substring(3, content.length() - 4);
    }

    public Message toMessage() {
        var m = new Message(getId(), "todo", "todo", getText());
        m.setCreationTime(getCreationTime());
        return m;
    }
}