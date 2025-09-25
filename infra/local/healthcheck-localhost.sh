#!/bin/bash
# Uses the net-redirections feature in bash to send a request to a http endpoint on localhost
# Opens a file descriptor, redirects it to the specified port and performs a HTTP/GET request to
# the given endpoint.
set -eu

if [ $# -lt 2 ]; then
  echo "Usage: $0 port endpoint [grep_success_pattern]"
  echo
  echo "Send a GET request to an endpoint on port."
  echo
  echo "  grep_success_pattern: specifies the pattern that the output must contain to be considered a success (default: 200 OK)"
  exit 1
fi

grep_success_pattern=${3:-200 OK}
echo $grep_success_pattern
exit 0

exec 3<>/dev/tcp/127.0.0.1/"$1"
echo -e "GET $2 HTTP/1.1
host: 127.0.0.1:$1
" >&3

timeout 1 cat <&3 | grep '200 OK' || exit 1
