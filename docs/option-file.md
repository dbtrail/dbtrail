# The option file: /etc/dbtrail/dbtrail.cnf

Every DBTrail program reads `/etc/dbtrail/dbtrail.cnf` when it starts. It is
written the way MySQL writes `my.cnf`: options under a `[dbtrail]` group, one
per line.

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
   value does not count, so the compose file's `${NAME:-}` does not hide the
   file.
3. `.bintrail.env` in the current folder, or `~/.config/bintrail/config.env`.
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

When the same option is written twice, the last line is used.

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

It reads the file as a program would if it started now, lists the settings it
sets with their lines, and every line that is not used, and why. It exits 1
when it finds one, so a deploy can stop on it.

The file is read from `/etc/dbtrail/dbtrail.cnf` only, never from the current
folder: the `dbtrail` commands that talk to a running DBTrail send a password,
and a file in the folder you happen to be in must not decide where.
