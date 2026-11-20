#!/bin/sh
# dbtrail: the command line of a DBTrail installed with Docker Compose.
#
# install.sh leaves this file as `dbtrail` beside docker-compose.yml. It runs
# DBTrail's own command inside the DBTrail container, so that
#
#   ./dbtrail server add -h db1.example.com -u dbtrail -p
#
# works from this machine with nothing else installed. To type just `dbtrail`
# from anywhere, link it into a folder of your PATH:
#
#   sudo ln -s "$PWD/dbtrail" /usr/local/bin/dbtrail
#
# The Linux packages need none of this: they install a real `dbtrail`.
set -eu

# The folder this file is in, links followed: docker-compose.yml is beside it.
self=$0
while [ -L "$self" ]; do
  link=$(readlink "$self")
  case "$link" in
    /*) self=$link ;;
    *)  self=$(dirname "$self")/$link ;;
  esac
done
dir=$(cd "$(dirname "$self")" && pwd)
[ -f "$dir/docker-compose.yml" ] || {
  echo "dbtrail: no docker-compose.yml in $dir. This file belongs beside DBTrail's docker-compose.yml." >&2
  exit 1
}

if docker compose version >/dev/null 2>&1; then
  set_compose() { set -- docker compose "$@"; "$@"; }
elif command -v docker-compose >/dev/null 2>&1; then
  set_compose() { set -- docker-compose "$@"; "$@"; }
else
  echo "dbtrail: Docker Compose was not found. DBTrail was installed with it: is Docker running?" >&2
  exit 1
fi

# --defaults-file names a file on THIS machine, which the container cannot
# see. It is sent on standard input instead (`--defaults-file -`).
file=""
n=$#
while [ "$n" -gt 0 ]; do
  a=$1
  shift
  n=$((n - 1))
  case "$a" in
    --defaults-file=-) set -- "$@" "$a" ;;
    --defaults-file=*) file=${a#--defaults-file=}; set -- "$@" --defaults-file=- ;;
    --defaults-file)
      if [ "$n" -eq 0 ]; then
        echo "dbtrail: --defaults-file needs a file" >&2
        exit 1
      fi
      if [ "$1" = - ]; then
        set -- "$@" --defaults-file=-
      else
        file=$1
        set -- "$@" --defaults-file=-
      fi
      shift
      n=$((n - 1))
      ;;
    *) set -- "$@" "$a" ;;
  esac
done

# The program in the container is bintrail-console. Told what to call
# itself (DBTRAIL_PROG), it uses that name in its help and in the commands it
# suggests: dbtrail when that word runs this file (it was linked into the
# PATH), else this file's absolute path, which works from any folder. A
# suggested "dbtrail snapshot run" before the link would be command not found.
realdir() {
  p=$1
  while [ -L "$p" ]; do
    l=$(readlink "$p")
    case "$l" in
      /*) p=$l ;;
      *)  p=$(dirname "$p")/$l ;;
    esac
  done
  (cd "$(dirname "$p")" 2>/dev/null && pwd -P)
}
prog="$dir/$(basename "$self")"
on_path=$(command -v dbtrail 2>/dev/null || true)
case "$on_path" in
  /*)
    if [ "$(realdir "$on_path")" = "$(cd "$dir" && pwd -P)" ] && [ "$(basename "$self")" = dbtrail ]; then
      prog=dbtrail
    fi
    ;;
esac
# A path the program would not take as one word (a quote, a backslash, a $
# or a backtick in a folder name) falls back to the word, as before.
case "$prog" in
  *[\'\"\\\$\`]*) prog=dbtrail ;;
esac

cd "$dir"
if [ -n "$file" ]; then
  # Read before anything runs, so a file that is missing is said in words.
  # A tilde the shell did not expand (--defaults-file=~/.my.cnf), on purpose.
  # shellcheck disable=SC2088
  case "$file" in "~/"*) file=$HOME/${file#"~/"} ;; esac
  [ -r "$file" ] || { echo "dbtrail: cannot read the option file $file" >&2; exit 1; }
  set_compose exec -T -e "DBTRAIL_PROG=$prog" bintrail bintrail-console "$@" < "$file"
elif [ -t 0 ] && [ -t 1 ]; then
  set_compose exec -e "DBTRAIL_PROG=$prog" bintrail bintrail-console "$@"
else
  # Not a terminal (a pipe, a script): no terminal is asked of Docker either,
  # which would refuse, and standard input is passed through as it is.
  set_compose exec -T -e "DBTRAIL_PROG=$prog" bintrail bintrail-console "$@"
fi
