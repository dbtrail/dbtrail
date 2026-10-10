#!/bin/sh
# Runs after the files are unpacked (deb postinst, rpm %post).
#
# A first install neither enables nor starts the service: it cannot run until
# /etc/bintrail/bintrail-console.env names the index MySQL. An upgrade restarts
# the service only if it was running, so the new binary is the one capturing.
set -e

# No systemd running (a container build, a chroot): nothing to tell.
[ -d /run/systemd/system ] || exit 0

systemctl daemon-reload || true

upgrade=no
case "$1" in
    configure) [ -n "$2" ] && upgrade=yes ;;  # deb: $2 is the version replaced
    2) upgrade=yes ;;                         # rpm: 1 = install, 2 = upgrade
esac

if [ "$upgrade" = yes ]; then
    systemctl try-restart bintrail-console.service || true
else
    echo "DBTrail is installed but not started. Set BINTRAIL_INDEX_DSN in"
    echo "/etc/bintrail/bintrail-console.env, then run:"
    echo "  sudo systemctl enable --now bintrail-console"
fi
