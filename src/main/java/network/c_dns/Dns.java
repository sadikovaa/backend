package network.c_dns;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Dns {
  private static final long TIMEOUT_NS = 5_000_000_000L;
  private static final Map<String, Cached> cache = new HashMap<>();
  private static int nextId = 1;

  public static void main(String[] args) throws IOException {
    InetAddress server = InetAddress.getByName(args[0]);
    int port = Integer.parseInt(args[1]);
    try (DatagramSocket socket = new DatagramSocket();
         BufferedReader input = new BufferedReader(
           new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
      String line;
      while ((line = input.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = line.split("\\s+");
        ask(socket, server, port, parts[0], parts[1]);
      }
    }
  }

  private static void ask(DatagramSocket socket, InetAddress server, int port,
                          String name, String type) throws IOException {
    String key = name.toLowerCase(Locale.ROOT) + " " + type;
    Cached cached = cache.get(key);
    if (cached != null && System.nanoTime() < cached.expiresAt()) {
      printBlock(name, type, "NOERROR", cached.answers());
      return;
    }

    int id = nextId++ & 0xFFFF;
    byte[] query = buildQuery(id, name, typeCode(type));
    socket.send(new DatagramPacket(query, query.length, server, port));

    byte[] response = waitFor(socket, id);
    if (response == null) {
      printBlock(name, type, "TIMEOUT", List.of());
      System.exit(1);
    }

    Parsed parsed = parse(response);
    printBlock(name, type, statusName(parsed.rcode()), parsed.answers());
    if (parsed.rcode() == 0 && parsed.records() > 0 && parsed.minTtl() > 0) {
      long expiresAt = System.nanoTime() + parsed.minTtl() * 1_000_000_000L;
      cache.put(key, new Cached(parsed.answers(), expiresAt));
    }
  }

  private static byte[] buildQuery(int id, String name, int type) throws IOException {
    ByteArrayOutputStream raw = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(raw);
    out.writeShort(id);
    out.writeShort(0x0100);
    out.writeShort(1);
    out.writeShort(0);
    out.writeShort(0);
    out.writeShort(0);
    out.write(encodeName(name));
    out.writeShort(type);
    out.writeShort(1);
    return raw.toByteArray();
  }

  private static byte[] encodeName(String name) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (String label : name.split("\\.")) {
      byte[] bytes = label.getBytes(StandardCharsets.US_ASCII);
      out.write(bytes.length);
      out.write(bytes, 0, bytes.length);
    }
    out.write(0);
    return out.toByteArray();
  }

  private static byte[] waitFor(DatagramSocket socket, int id) throws IOException {
    long deadline = System.nanoTime() + TIMEOUT_NS;
    byte[] buffer = new byte[4096];
    while (true) {
      long left = deadline - System.nanoTime();
      if (left <= 0) {
        return null;
      }
      socket.setSoTimeout((int) Math.max(1, left / 1_000_000L));
      DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
      try {
        socket.receive(packet);
      } catch (SocketTimeoutException e) {
        return null;
      }
      if (u16(packet.getData(), packet.getOffset()) == id) {
        byte[] message = new byte[packet.getLength()];
        System.arraycopy(packet.getData(), packet.getOffset(), message, 0, message.length);
        return message;
      }
    }
  }

  private static Parsed parse(byte[] message) {
    int rcode = u16(message, 2) & 0x0F;
    int qdcount = u16(message, 4);
    int ancount = u16(message, 6);
    int[] cursor = {12};
    for (int i = 0; i < qdcount; i++) {
      readName(message, cursor[0], cursor);
      cursor[0] += 4;
    }

    List<String> answers = new ArrayList<>();
    long minTtl = Long.MAX_VALUE;
    for (int i = 0; i < ancount; i++) {
      readName(message, cursor[0], cursor);
      int type = u16(message, cursor[0]);
      long ttl = u32(message, cursor[0] + 4);
      int length = u16(message, cursor[0] + 8);
      int data = cursor[0] + 10;
      cursor[0] = data + length;
      minTtl = Math.min(minTtl, ttl);
      String value = formatValue(type, message, data, length);
      if (value != null) {
        answers.add(typeName(type) + " " + value + " " + ttl);
      }
    }
    return new Parsed(rcode, answers, ancount, minTtl);
  }

  private static String readName(byte[] message, int offset, int[] endOut) {
    StringBuilder labels = new StringBuilder();
    int end = -1;
    while (true) {
      int length = message[offset] & 0xFF;
      if ((length & 0xC0) == 0xC0) {
        if (end < 0) {
          end = offset + 2;
        }
        offset = u16(message, offset) & 0x3FFF;
        continue;
      }
      offset++;
      if (length == 0) {
        break;
      }
      if (labels.length() > 0) {
        labels.append('.');
      }
      labels.append(new String(message, offset, length, StandardCharsets.US_ASCII));
      offset += length;
    }
    if (end < 0) {
      end = offset;
    }
    endOut[0] = end;
    return labels.length() == 0 ? "." : labels + ".";
  }

  private static String formatValue(int type, byte[] message, int data, int length) {
    return switch (type) {
      case 1 -> (message[data] & 0xFF) + "." + (message[data + 1] & 0xFF) + "."
        + (message[data + 2] & 0xFF) + "." + (message[data + 3] & 0xFF);
      case 2, 5 -> readName(message, data, new int[1]);
      case 15 -> u16(message, data) + " " + readName(message, data + 2, new int[1]);
      case 16 -> readText(message, data, length);
      case 28 -> formatIpv6(message, data);
      default -> null;
    };
  }

  private static String readText(byte[] message, int data, int length) {
    StringBuilder text = new StringBuilder();
    int at = data;
    int end = data + length;
    while (at < end) {
      int size = message[at] & 0xFF;
      at++;
      text.append(new String(message, at, size, StandardCharsets.UTF_8));
      at += size;
    }
    return text.toString();
  }

  private static String formatIpv6(byte[] message, int data) {
    int[] groups = new int[8];
    for (int i = 0; i < 8; i++) {
      groups[i] = u16(message, data + i * 2);
    }
    int bestStart = -1;
    int bestLen = 0;
    for (int i = 0; i < 8; ) {
      if (groups[i] != 0) {
        i++;
        continue;
      }
      int j = i;
      while (j < 8 && groups[j] == 0) {
        j++;
      }
      if (j - i > bestLen) {
        bestStart = i;
        bestLen = j - i;
      }
      i = j;
    }

    StringBuilder out = new StringBuilder();
    for (int i = 0; i < 8; ) {
      if (bestLen >= 2 && i == bestStart) {
        out.append("::");
        i += bestLen;
        continue;
      }
      if (out.length() > 0 && !out.toString().endsWith(":")) {
        out.append(':');
      }
      out.append(Integer.toHexString(groups[i]));
      i++;
    }
    return out.toString();
  }

  private static int typeCode(String type) {
    return switch (type) {
      case "A" -> 1;
      case "NS" -> 2;
      case "CNAME" -> 5;
      case "MX" -> 15;
      case "TXT" -> 16;
      case "AAAA" -> 28;
      default -> 0;
    };
  }

  private static String typeName(int code) {
    return switch (code) {
      case 1 -> "A";
      case 2 -> "NS";
      case 5 -> "CNAME";
      case 15 -> "MX";
      case 16 -> "TXT";
      case 28 -> "AAAA";
      default -> null;
    };
  }

  private static String statusName(int rcode) {
    return switch (rcode) {
      case 0 -> "NOERROR";
      case 1 -> "FORMERR";
      case 2 -> "SERVFAIL";
      case 3 -> "NXDOMAIN";
      case 5 -> "REFUSED";
      default -> "RCODE" + rcode;
    };
  }

  private static void printBlock(String name, String type, String status, List<String> answers) {
    System.out.println("query " + name + " " + type);
    System.out.println("status " + status);
    for (String answer : answers) {
      System.out.println("answer " + answer);
    }
    System.out.println("end");
  }

  private static int u16(byte[] data, int at) {
    return ((data[at] & 0xFF) << 8) | (data[at + 1] & 0xFF);
  }

  private static long u32(byte[] data, int at) {
    return ((data[at] & 0xFFL) << 24) | ((data[at + 1] & 0xFFL) << 16) | ((data[at + 2] & 0xFFL) << 8) | (data[at + 3] & 0xFFL);
  }
}
