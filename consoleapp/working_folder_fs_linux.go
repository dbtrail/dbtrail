package consoleapp

import "syscall"

// networkFSMagic are the statfs types (include/uapi/linux/magic.h) of
// filesystems that are not a disk of this host. FUSE is in the list although
// some FUSE mounts are local: what it fronts cannot be told from here, and
// staying in the temp folder is what every install did before.
var networkFSMagic = map[uint32]string{
	0x6969:     "nfs",
	0xFF534D42: "cifs",
	0xFE534D42: "smb2",
	0x517B:     "smb",
	0x00C36400: "ceph",
	0x01021997: "9p",
	0x65735546: "fuse",
}

// memoryFSMagic are the filesystems whose files are held in RAM.
var memoryFSMagic = map[uint32]string{
	0x01021994: "tmpfs",
	0x858458F6: "ramfs",
}

func fsKind(path string) (fsClass, string, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return fsLocal, "", err
	}
	magic := uint32(st.Type) //nolint:unconvert // the field's type differs per architecture
	if kind, ok := networkFSMagic[magic]; ok {
		return fsNetwork, kind, nil
	}
	if kind, ok := memoryFSMagic[magic]; ok {
		return fsMemory, kind, nil
	}
	return fsLocal, "", nil
}
