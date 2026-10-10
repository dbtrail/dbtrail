//go:build !linux

package sqlsandbox

import "errors"

// fenceReason is whether a host without a fence says why: memory cgroups are
// a Linux feature, and elsewhere there is nothing for an operator to set up.
const fenceReason = false

func procSelfCgroup() ([]byte, error) { return nil, errors.ErrUnsupported }

func cgroupDelegated(string) bool { return false }
