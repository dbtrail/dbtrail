#!/bin/sh
# Runs after the files are removed (deb postrm, rpm %postun). The account and
# /var/lib/bintrail stay: they hold the servers, the login and the snapshots.
set -e

[ -d /run/systemd/system ] || exit 0
systemctl daemon-reload || true
