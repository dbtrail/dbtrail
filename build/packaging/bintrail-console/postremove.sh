#!/bin/sh
# Runs after the files are removed (deb postrm, rpm %postun). The account and
# /var/lib/bintrail always stay: they hold the servers, the login and the
# snapshots. The settings file and the option file stay too, except on a Debian purge, which is
# the operator asking for configuration to go.
set -e

if [ "$1" = purge ]; then
    rm -f /etc/bintrail/bintrail-console.env /etc/bintrail/.router-started /etc/bintrail/.pg-router-started
    rmdir /etc/bintrail 2>/dev/null || true
    # The option file goes too; a license or anything else in /etc/dbtrail
    # stays, and so does the folder then.
    rm -f /etc/dbtrail/dbtrail.cnf
    rmdir /etc/dbtrail 2>/dev/null || true
fi

[ -d /run/systemd/system ] || exit 0
systemctl daemon-reload || true
