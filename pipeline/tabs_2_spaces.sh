find src/main/java -name '*.java' -exec bash -c 'expand -t 4 "$0" > "$0".xxx && mv "$0".xxx "$0"' {} \;
