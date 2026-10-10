#!/bin/sh
# Runs before the files are removed (deb prerm, rpm %preun). Stops and disables
# the service on a real removal only: both formats also run this on an upgrade,
# where capture must keep running until the new binary is in place.
set -e

[ -d /run/systemd/system ] || exit 0

case "$1" in
    remove|0)  # deb: remove. rpm: 0 = last version removed, 1 = upgrade
        systemctl disable --now bintrail-console.service || true
        ;;
esac
