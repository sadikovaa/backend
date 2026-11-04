package network.a_packet;

import java.nio.charset.StandardCharsets;


public class Packet {
  public static void main(String[] args) throws Exception {
    byte[] data = readHex();

    int type = ethernet(data);
    if (type == 0x0800) {
      ipv4(data, 14);
    }
  }

  private static int ethernet(byte[] data) {
    require(data, 14);
    line("eth.dst", mac(data, 0));
    line("eth.src", mac(data, 6));
    int type = u16(data, 12);
    line("eth.ethertype", String.format("0x%04x", type));
    return type;
  }

  private static void ipv4(byte[] data, int at) {
    require(data, at + 20);
    int first = u8(data, at);
    int version = first >> 4;
    int header = (first & 0x0F) * 4;
    require(data, at + header);

    int totalLength = u16(data, at + 2);
    int flagsAndOffset = u16(data, at + 6);
    int protocol = u8(data, at + 9);

    line("ip.version", version);
    line("ip.ihl_bytes", header);
    line("ip.total_length", totalLength);
    line("ip.id", String.format("0x%04x", u16(data, at + 4)));
    line("ip.flags", ipFlags(flagsAndOffset));
    line("ip.frag_offset", (flagsAndOffset & 0x1FFF) * 8);
    line("ip.ttl", u8(data, at + 8));
    line("ip.protocol", protocol);
    line("ip.src", ip(data, at + 12));
    line("ip.dst", ip(data, at + 16));
    line("ip.checksum_valid", checksumOk(data, at, header) ? "true" : "false");

    int body = Math.max(0, totalLength - header);
    if (protocol == 6) {
      tcp(data, at + header, body);
    } else if (protocol == 17) {
      udp(data, at + header, body);
    } else {
      line("payload.length", body);
    }
  }

  private static void tcp(byte[] data, int at, int body) {
    require(data, at + 20);
    int header = (u8(data, at + 12) >> 4) * 4;
    require(data, at + (header == 0 ? 20 : Math.max(header, 20)));

    int bits = u16(data, at + 12) & 0x003F;
    String[] names = {"FIN", "SYN", "RST", "PSH", "ACK", "URG"};
    String flags = "";
    for (int i = 0; i < names.length; i++) {
      if ((bits & (1 << i)) != 0) {
        flags = flags.isEmpty() ? names[i] : flags + "," + names[i];
      }
    }

    line("tcp.src_port", u16(data, at));
    line("tcp.dst_port", u16(data, at + 2));
    line("tcp.seq", u32(data, at + 4));
    line("tcp.ack", u32(data, at + 8));
    line("tcp.data_offset_bytes", header);
    line("tcp.flags", flags.isEmpty() ? "none" : flags);
    line("tcp.window", u16(data, at + 14));
    line("payload.length", Math.max(0, body - header));
  }

  private static void udp(byte[] data, int at, int body) {
    require(data, at + 8);
    line("udp.src_port", u16(data, at));
    line("udp.dst_port", u16(data, at + 2));
    line("udp.length", u16(data, at + 4));
    line("payload.length", Math.max(0, body - 8));
  }

  private static String ipFlags(int flagsAndOffset) {
    String flags = "";
    if ((flagsAndOffset & 0x4000) != 0) {
      flags = "DF";
    }
    if ((flagsAndOffset & 0x2000) != 0) {
      flags = flags.isEmpty() ? "MF" : flags + ",MF";
    }
    return flags.isEmpty() ? "none" : flags;
  }

  private static boolean checksumOk(byte[] data, int at, int header) {
    int sum = 0;
    for (int i = 0; i < header; i += 2) {
      sum += u16(data, at + i);
    }
    while (sum > 0xFFFF) {
      sum = (sum & 0xFFFF) + (sum >>> 16);
    }
    return sum == 0xFFFF;
  }

  private static byte[] readHex() throws Exception {
    String text = new String(System.in.readAllBytes(), StandardCharsets.US_ASCII);
    StringBuilder digits = new StringBuilder();
    for (int i = 0; i < text.length(); i++) {
      char ch = text.charAt(i);
      if ((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')) {
        digits.append(ch);
      }
    }
    if (digits.length() % 2 != 0) {
      System.exit(1);
    }
    byte[] data = new byte[digits.length() / 2];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) Integer.parseInt(digits.substring(i * 2, i * 2 + 2), 16);
    }
    return data;
  }

  private static void require(byte[] data, int size) {
    if (size > data.length) {
      System.exit(1);
    }
  }

  private static void line(String key, Object value) {
    System.out.println(key + " " + value);
  }

  private static int u8(byte[] data, int at) {
    return data[at] & 0xFF;
  }

  private static int u16(byte[] data, int at) {
    return (u8(data, at) << 8) | u8(data, at + 1);
  }

  private static long u32(byte[] data, int at) {
    return ((long) u8(data, at) << 24) | ((long) u8(data, at + 1) << 16) | ((long) u8(data, at + 2) << 8) | u8(data, at + 3);
  }

  private static String mac(byte[] data, int at) {
    return String.format("%02x:%02x:%02x:%02x:%02x:%02x",
      u8(data, at), u8(data, at + 1), u8(data, at + 2),
      u8(data, at + 3), u8(data, at + 4), u8(data, at + 5));
  }

  private static String ip(byte[] data, int at) {
    return u8(data, at) + "." + u8(data, at + 1) + "." + u8(data, at + 2) + "." + u8(data, at + 3);
  }

}
