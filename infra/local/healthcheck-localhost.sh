#!/bin/bash
# Uses the net-redirections feature in bash to send a request to a http endpoint on localhost
# Opens a file descriptor, redirects it to the specified port and performs a HTTP/GET request to
# the given endpoint.

# arg processing
if [ $# -lt 2 ]; then
  echo "Usage: $0 port endpoint [success_pattern]"
  echo
  echo "Send a GET request to an endpoint on port."
  echo
  echo "  success_pattern: specifies the pattern that the output must contain to be considered a success (default: 200 OK)"
  exit 1
fi
success_pattern=${3:-200 OK}

# run GET
exec 3<>/dev/tcp/127.0.0.1/"$1"
test $? -eq 0 || exit 1

echo -e "GET $2 HTTP/1.1
host: 127.0.0.1:$1
" >&3

response=$(timeout 1 cat <&3)
[[ "$response" == *"$success_pattern"* ]] || exit 1
