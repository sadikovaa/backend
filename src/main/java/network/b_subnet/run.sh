#$env:PATH = "C:\Program Files\Git\bin;" + $env:PATH
# python -m unittest discover -s tests -v
cd "$(dirname "$0")"
exec java -cp out network.b_subnet.Subnet "$@"
