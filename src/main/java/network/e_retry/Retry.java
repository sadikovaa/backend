package network.e_retry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class Retry {
  public static void main(String[] args) throws Exception {
    String url = null;
    String method = "GET";
    int maxAttempts = 5;
    String key = null;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--method")) {
        method = args[++i];
      } else if (args[i].equals("--max-attempts")) {
        maxAttempts = Integer.parseInt(args[++i]);
      } else if (args[i].equals("--idempotency-key")) {
        key = args[++i];
      } else {
        url = args[i];
      }
    }

    URI uri = URI.create(url);
    String host = uri.getHost();
    int port = uri.getPort() == -1 ? 80 : uri.getPort();
    String path = uri.getRawPath();
    if (path == null || path.isEmpty()) {
      path = "/";
    }
    if (uri.getRawQuery() != null) {
      path = path + "?" + uri.getRawQuery();
    }

    int done = 0;
    boolean success = false;
    while (done < maxAttempts) {
      done++;
      Attempt attempt = request(host, port, path, method, key);
      if (attempt.error != null) {
        System.out.println("attempt " + done + " error " + attempt.error);
      } else {
        System.out.println("attempt " + done + " status " + attempt.status);
      }
      boolean retry = done < maxAttempts && mayRetry(method, key)
        && (attempt.error != null || retryStatus(attempt.status));
      if (!retry) {
        success = attempt.error == null && attempt.status >= 200 && attempt.status <= 399;
        break;
      }
      int pause = pause(done + 1, attempt.retryAfter);
      System.out.println("sleep_ms " + pause);
      Thread.sleep(pause);
    }
    System.out.println("result " + (success ? "success" : "failure") + " attempts " + done);
    System.exit(success ? 0 : 1);
  }

  private static Attempt request(String host, int port, String path, String method, String key) {
    Attempt attempt = new Attempt();
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port), 5000);
      socket.setSoTimeout(5000);
      OutputStream out = socket.getOutputStream();
      StringBuilder message = new StringBuilder();
      message.append(method).append(' ').append(path).append(" HTTP/1.1\r\n");
      message.append("Host: ").append(host).append(':').append(port).append("\r\n");
      if (key != null) {
        message.append("Idempotency-Key: ").append(key).append("\r\n");
      }
      message.append("\r\n");
      out.write(message.toString().getBytes(StandardCharsets.US_ASCII));
      out.flush();

      InputStream in = socket.getInputStream();
      String statusLine = readLine(in);
      if (statusLine == null) {
        throw new IOException("connection closed");
      }
      attempt.status = Integer.parseInt(statusLine.split(" ")[1]);
      int length = 0;
      String line;
      while ((line = readLine(in)) != null && !line.isEmpty()) {
        int colon = line.indexOf(':');
        String name = line.substring(0, colon).trim();
        String value = line.substring(colon + 1).trim();
        if (name.equalsIgnoreCase("content-length")) {
          length = Integer.parseInt(value);
        } else if (name.equalsIgnoreCase("retry-after") && digits(value)) {
          attempt.retryAfter = Integer.parseInt(value);
        }
      }
      in.readNBytes(length);
    } catch (IOException e) {
      attempt.status = -1;
      attempt.retryAfter = null;
      String text = e.getMessage();
      attempt.error = text == null || text.isBlank() ? e.getClass().getSimpleName() : text;
    }
    return attempt;
  }

  private static String readLine(InputStream in) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    int b;
    while ((b = in.read()) != -1) {
      if (b == '\n') {
        break;
      }
      if (b != '\r') {
        buf.write(b);
      }
    }
    if (b == -1 && buf.size() == 0) {
      return null;
    }
    return buf.toString(StandardCharsets.ISO_8859_1);
  }

  private static boolean retryStatus(int status) {
    return status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
  }

  private static boolean mayRetry(String method, String key) {
    if (key != null) {
      return true;
    }
    return method.equals("GET") || method.equals("HEAD") || method.equals("PUT")
      || method.equals("DELETE") || method.equals("OPTIONS") || method.equals("TRACE");
  }

  private static int pause(int nextAttempt, Integer retryAfterSeconds) {
    if (retryAfterSeconds != null) {
      return retryAfterSeconds * 1000;
    }
    long ceiling = Math.min(2000L, 200L << (nextAttempt - 1));
    return ThreadLocalRandom.current().nextInt((int) ceiling + 1);
  }

  private static boolean digits(String value) {
    if (value.isEmpty()) {
      return false;
    }
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  }

  private static final class Attempt {
    int status = -1;
    String error;
    Integer retryAfter;
  }
}
