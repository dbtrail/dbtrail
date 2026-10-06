//go:build !linux && !darwin

package consoleapp

func bootID() string { return "" }

func networkFS(string) (string, bool) { return "", false }
