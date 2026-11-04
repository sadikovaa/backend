package network.b_subnet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class Subnet {
  public static void main(String[] args) throws IOException {
    if (args[0].equals("subnet")) {
      subnet(args[1]);
    } else if (args[0].equals("route")) {
      route(args[1], args[2]);
    }
  }

  private static void subnet(String spec) {
    int slash = spec.indexOf('/');
    long address = parseIp(spec.substring(0, slash));
    int prefix = parsePrefix(spec.substring(slash + 1));
    long mask = mask(prefix);
    long network = address & mask;

    long first;
    long last;
    long hosts;
    String broadcast;
    if (prefix == 32) {
      first = network;
      last = network;
      hosts = 1;
      broadcast = "none";
    } else if (prefix == 31) {
      first = network;
      last = network + 1;
      hosts = 2;
      broadcast = "none";
    } else {
      long size = 1L << (32 - prefix);
      first = network + 1;
      last = network + size - 2;
      hosts = size - 2;
      broadcast = formatIp(network | (~mask & 0xFFFFFFFFL));
    }

    line("network", formatIp(network));
    line("broadcast", broadcast);
    line("netmask", formatIp(mask));
    line("prefix", prefix);
    line("first", formatIp(first));
    line("last", formatIp(last));
    line("hosts", hosts);
  }

  private static void route(String path, String destinationText) throws IOException {
    long destination = parseIp(destinationText);
    List<String> lines = Files.readAllLines(Path.of(path), StandardCharsets.UTF_8);

    int bestPrefix = -1;
    String bestVia = null;
    for (String raw : lines) {
      String line = raw.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      String[] fields = line.split("\\s+");
      int slash = fields[0].indexOf('/');
      long network = parseIp(fields[0].substring(0, slash));
      int prefix = Integer.parseInt(fields[0].substring(slash + 1));
      long mask = mask(prefix);
      if ((destination & mask) == (network & mask) && prefix > bestPrefix) {
        bestPrefix = prefix;
        bestVia = fields[1];
      }
    }

    if (bestVia == null) {
      line("unreachable", "true");
      System.exit(1);
    }
    line("via", bestVia);
    line("prefix", bestPrefix);
  }

  private static long parseIp(String text) {
    String[] parts = text.split("\\.", -1);
    if (parts.length != 4) {
      fail();
    }
    long ip = 0;
    for (String part : parts) {
      int octet;
      try {
        octet = Integer.parseInt(part);
      } catch (NumberFormatException e) {
        fail();
        return 0;
      }
      if (octet < 0 || octet > 255) {
        fail();
      }
      ip = (ip << 8) | octet;
    }
    return ip;
  }

  private static int parsePrefix(String text) {
    int prefix = Integer.parseInt(text);
    if (prefix < 0 || prefix > 32) {
      fail();
    }
    return prefix;
  }

  private static long mask(int prefix) {
    if (prefix == 0) {
      return 0;
    }
    return (0xFFFFFFFFL << (32 - prefix)) & 0xFFFFFFFFL;
  }

  private static String formatIp(long ip) {
    return ((ip >> 24) & 255) + "." + ((ip >> 16) & 255) + "." + ((ip >> 8) & 255) + "." + (ip & 255);
  }

  private static void line(String key, Object value) {
    System.out.println(key + " " + value);
  }

  private static void fail() {
    System.err.println("паника");
    System.exit(1);
  }
}
