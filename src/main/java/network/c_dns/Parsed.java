package network.c_dns;

import java.util.List;

public record Parsed(int rcode, List<String> answers, int records, long minTtl) {
}
