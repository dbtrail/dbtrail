package settings

import "slices"

// Builtin is this repository's own list, without what Register added.
func Builtin() []Entry { return slices.Clone(table) }
