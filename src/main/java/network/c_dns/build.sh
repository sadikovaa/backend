cd "$(dirname "$0")"
mkdir -p out
javac -encoding UTF-8 -d out Dns.java Parsed.java Cached.java
