#!/bin/sh
# Runs before the files are unpacked, on install and on upgrade (deb preinst,
# rpm %pre). Creates the account the service runs as. An account that already
# exists is left exactly as it is.
set -e

if ! getent group bintrail >/dev/null 2>&1; then
    groupadd --system bintrail
fi
if ! getent passwd bintrail >/dev/null 2>&1; then
    useradd --system --gid bintrail --home-dir /var/lib/bintrail \
        --shell /usr/sbin/nologin --comment "DBTrail" bintrail
fi
