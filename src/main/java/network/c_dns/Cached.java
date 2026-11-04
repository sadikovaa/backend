package network.c_dns;

import java.util.List;

public record Cached(List<String> answers, long expiresAt) {
}
