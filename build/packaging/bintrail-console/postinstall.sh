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
    echo "DBTrail is installed but not started. Set BINTRAIL_INDEX_DSN in"
    echo "$settings, then run:"
    echo "  sudo systemctl enable --now bintrail-console"
fi

# The MySQL port (3309) is answered by a second service of this package,
# bintrail-router, and by nothing else: the capture service does not open it.
# So this one is enabled and started here, on a first install and on the first
# upgrade that brings it. It needs no settings to start, and it accepts no
# connection until the port is turned on in the web interface. The marker says
# it was enabled once, so a later upgrade leaves a router its operator stopped
# alone; it is written only when enabling worked, so one that did not is tried
# again by the next upgrade, and removing the package removes it. One that runs is restarted on its own account: it does not depend on
# the capture service, and may run where capture is stopped.
marker=${BINTRAIL_ROUTER_MARKER:-/etc/bintrail/.router-started}
router_check=no
if [ "$upgrade" = yes ] && systemctl is-active --quiet bintrail-router.service; then
    systemctl try-restart bintrail-router.service || true
    router_check=yes
    touch "$marker" 2>/dev/null || true
elif [ ! -e "$marker" ]; then
    # The folder both services work in. The capture unit creates it too, but
    # the router may be the first of the two to start.
    [ -d /var/lib/bintrail ] || install -d -o bintrail -g bintrail -m 0750 /var/lib/bintrail
    if systemctl enable --now bintrail-router.service; then
        router_check=yes
        touch "$marker" 2>/dev/null || true
        echo "The MySQL port (3309) is answered by the bintrail-router service,"
        echo "started now. It accepts no connection until you turn the port on in"
        echo "the web interface."
    else
        echo "WARNING: the bintrail-router service could not be enabled."
        echo "Nothing answers on the MySQL port (3309) until it runs:"
        echo "  sudo systemctl enable --now bintrail-router"
    fi
fi
if [ "$router_check" = yes ]; then
    sleep 2
    if ! systemctl is-active --quiet bintrail-router.service; then
        echo "WARNING: bintrail-router is not running."
        echo "Nothing answers on the MySQL port (3309). To see why:"
        echo "  journalctl -u bintrail-router -n 50"
    fi
fi
