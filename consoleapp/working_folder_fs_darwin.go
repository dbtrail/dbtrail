package consoleapp

import "syscall"

var networkFSNames = map[string]bool{"nfs": true, "smbfs": true, "afpfs": true, "webdav": true}

func fsKind(path string) (fsClass, string, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return fsLocal, "", err
	}
	b := make([]byte, 0, len(st.Fstypename))
	for _, c := range st.Fstypename {
		if c == 0 {
			break
		}
		b = append(b, byte(c))
	}
	kind := string(b)
	if networkFSNames[kind] {
		return fsNetwork, kind, nil
	}
	return fsLocal, kind, nil
}
