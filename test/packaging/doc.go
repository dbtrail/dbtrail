// Package packaging tests what the bintrail-console .deb and .rpm install
// around the binary: the systemd unit, its settings file and the four
// maintainer scripts (#2291). The scripts decide what an install, an upgrade
// and a removal do to a running capture, so they are tested as scripts, with
// stand-ins for systemctl and the account tools.
package packaging
