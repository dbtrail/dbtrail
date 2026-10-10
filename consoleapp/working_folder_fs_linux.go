package consoleapp

import "syscall"

// memoryFSMagic are the statfs types (include/uapi/linux/magic.h) of the
// filesystems whose files are held in RAM.
var memoryFSMagic = map[int64]string{
	0x01021994: "tmpfs",
	0x858458F6: "ramfs",
}

// fsKind classifies the filesystem holding path. Network storage is what the
// job journal already calls network storage (networkFSMagic): one list, so
// the two cannot disagree about a mount.
func fsKind(path string) (fsClass, string, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return fsLocal, "", err
	}
	// Through uint32: the field is signed and 32 bits wide on some
	// architectures, where a magic above 0x7FFFFFFF (cifs, smb2, ramfs)
	// would otherwise widen to a negative number and match nothing.
	magic := int64(uint32(st.Type)) //nolint:unconvert // the field's type differs per architecture
	if kind, ok := networkFSMagic(magic); ok {
		return fsNetwork, kind, nil
	}
	if kind, ok := memoryFSMagic[magic]; ok {
		return fsMemory, kind, nil
	}
	return fsLocal, "", nil
}
