# The option file: /etc/dbtrail/dbtrail.cnf

Every DBTrail program reads `/etc/dbtrail/dbtrail.cnf` when it starts, except
the standalone `bintrail-mcp`, which the AI client that starts it configures.
It is written the way MySQL writes `my.cnf`: options under a `[dbtrail]`
group, one per line.

With the Docker install, DBTrail runs inside a container and reads the file
there. The compose file does not mount one yet: add it to the `bintrail`
service yourself, and create the file first (a mount of a missing file makes
a directory, which DBTrail reports and does not read):

```yaml
    volumes:
      - ./dbtrail.cnf:/etc/dbtrail/dbtrail.cnf:ro
```

```ini
[dbtrail]
commit-within = 2s
rotate-retain = 7d
index-dsn     = 'dbtrail:secret@tcp(127.0.0.1:3306)/dbtrail_index'
```

An option is the name of its environment variable without `BINTRAIL_`, in
lower case, with dashes: `BINTRAIL_COMMIT_WITHIN` is `commit-within`.
`commit_within` is read the same way, as MySQL does.

## What wins

1. A command-line flag.
2. The environment the program was started with. A variable set to an empty
   value does not hide the file, so the compose file's `${NAME:-}` does not.
   It does still hide an env file, as it always has: with an empty variable
   in the environment, the env file's line for it is not used and this file's
   is.
3. `.bintrail.env` in the current folder, or `~/.config/bintrail/config.env`.
   The `dbtrail` commands that talk to a running DBTrail (`server`, `port`,
   `snapshot`) never read an env file for where it is and how to log in
   (`console-listen`, `console-token`, `console-auth`), so for them this file
   comes right after the environment.
4. `/etc/dbtrail/dbtrail.cnf`.
5. The default.

A change to the file is used at the next start. DBTrail never writes to the
file: what you save in the web interface stays where it is saved today.

## Lines that are not used

A program starts anyway and prints a warning with the line number. It never
prints a value, since a value may hold a password.

- An option DBTrail does not have: the warning names the nearest one. Put
  `loose-` in front (`loose-new-option = 1`) to keep a line for a newer version
  without the warning.
- An option with no value, or before any `[group]`.
- A name DBTrail sets for its own processes, one that no longer does anything,
  or one for its tests.
- An option the environment or an env file already sets.
- `!include` and `!includedir`: they are not followed.
- Other groups (`[client]`, `[mysqld]`) are left alone, so the file can hold
  them.

- Text after the `]` of a group (`[dbtrail] commit-within = 2s`).
- A file with no `[dbtrail]` group but a `[bintrail]` one, or one spelled
  nearly the same.

When the same option is written twice, the last line that can be used is
used.

## Values

They are read as MySQL reads them:

- A `#` outside quotes starts a comment, so `index-dsn = root:p#ss@...` stops
  at `root:p`. Put a value with a `#` in quotes. DBTrail warns when a `#` cuts
  a value that can hold a password.
- One pair of quotes around the value is taken off.
- A backslash starts an escape: `\n`, `\t`, `\r`, `\b` and `\s` (a space)
  become that character, and `\\` a backslash. Write a password with a
  backslash in it as `\\`. `.bintrail.env` does none of this, so a line copied
  from it may need changing.

## Permissions

- A file every user on the machine can change is **not used at all**, as
  MySQL does: it could send capture to another server. Fix it with
  `chmod o-w /etc/dbtrail/dbtrail.cnf`.
- A file every user can read that sets a DSN or another secret is used, with a
  warning. Fix it with `chmod o-r /etc/dbtrail/dbtrail.cnf`.

## Check it

```sh
dbtrail config check
```

It reads the file as a program would if it started now, with the environment
of the command, lists the settings it
sets with their lines, and every line that is not used, and why. It exits 1
when it finds one, so a deploy can stop on it. With the Docker install the
`dbtrail` command runs it inside the container, with DBTrail's environment.
Under systemd, the service's environment is not your shell's, so a line the
service's environment hides can read as used.

The file is read from `/etc/dbtrail/dbtrail.cnf` only, never from the current
folder: the `dbtrail` commands that talk to a running DBTrail send a password,
and a file in the folder you happen to be in must not decide where.
