#!/bin/sh
# Runs after the files are unpacked (deb postinst, rpm %post).
#
# The settings file is created here, from the copy the package ships, and only
# when there is none: it is not a file the package owns, so no later version
# can overwrite it, prompt about it, or leave a second copy beside it.
#
# A first install neither enables nor starts the service: it cannot run until
# the settings file names the index MySQL. An upgrade restarts the service only
# if it was running, so the new binary is the one capturing.
set -e

settings=/etc/bintrail/bintrail-console.env
if [ ! -e "$settings" ]; then
    install -d -m 0755 /etc/bintrail
    install -m 0600 /usr/share/bintrail-console/bintrail-console.env "$settings"
fi

# No systemd running (a container build, a chroot): nothing to tell.
[ -d /run/systemd/system ] || exit 0

systemctl daemon-reload || true

upgrade=no
case "$1" in
    configure) [ -n "$2" ] && upgrade=yes ;;  # deb: $2 is the version replaced
    2) upgrade=yes ;;                         # rpm: 1 = install, 2 = upgrade
esac

if [ "$upgrade" = yes ] && systemctl is-active --quiet bintrail-console.service; then
    systemctl try-restart bintrail-console.service || true
    # The restart returns as soon as the process exists. Look once more, so an
    # upgrade that left nothing capturing does not end in silence.
    sleep 2
    if ! systemctl is-active --quiet bintrail-console.service; then
        echo "WARNING: bintrail-console did not come back after the upgrade."
        echo "Nothing is capturing. To see why:"
        echo "  journalctl -u bintrail-console -n 50"
    fi
elif ! systemctl is-enabled --quiet bintrail-console.service 2>/dev/null; then
    router_hint=yes
    echo "DBTrail is installed but not started. Set BINTRAIL_INDEX_DSN in"
    echo "$settings, then run:"
    echo "  sudo systemctl enable --now bintrail-console"
fi

# The router is a second service of this package, never started by an install.
# One that runs is restarted by an upgrade on its own account: it does not
# depend on the capture service, and may run where capture is stopped.
if [ "$upgrade" = yes ] && systemctl is-active --quiet bintrail-router.service; then
    systemctl try-restart bintrail-router.service || true
    sleep 2
    if ! systemctl is-active --quiet bintrail-router.service; then
        echo "WARNING: bintrail-router did not come back after the upgrade."
        echo "Applications that connect through it cannot. To see why:"
        echo "  journalctl -u bintrail-router -n 50"
    fi
elif [ "${router_hint:-no}" = yes ]; then
    echo "The MySQL port with read routing is a service of its own, off until"
    echo "you start it: sudo systemctl enable --now bintrail-router"
fi
