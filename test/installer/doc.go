// Package installer tests install.sh, the one-command installer, by running
// it for real against stand-ins for docker, curl and lsof: the port handling
// is decided by the script, so it is tested as a script (#1768).
package installer
