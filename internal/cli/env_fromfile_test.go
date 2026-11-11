package cli

import (
	"os"
	"testing"
)

// A variable the process was started with is not "from the env file", even
// when the file names it too; one the file added is, until it is taken back.
func TestFromEnvFile(t *testing.T) {
	t.Setenv("BT_TEST_OWN", "mine")
	os.Unsetenv("BT_TEST_ADDED")
	undo := ApplyEnvFileForTest("BT_TEST_OWN=theirs\nBT_TEST_ADDED=theirs\n")
	if os.Getenv("BT_TEST_OWN") != "mine" || FromEnvFile("BT_TEST_OWN") {
		t.Errorf("a variable of the process: value %q, from file %v", os.Getenv("BT_TEST_OWN"), FromEnvFile("BT_TEST_OWN"))
	}
	if os.Getenv("BT_TEST_ADDED") != "theirs" || !FromEnvFile("BT_TEST_ADDED") {
		t.Errorf("a variable the file added: value %q, from file %v", os.Getenv("BT_TEST_ADDED"), FromEnvFile("BT_TEST_ADDED"))
	}
	undo()
	if _, set := os.LookupEnv("BT_TEST_ADDED"); set || FromEnvFile("BT_TEST_ADDED") {
		t.Error("undo left the added variable behind")
	}
	if os.Getenv("BT_TEST_OWN") != "mine" {
		t.Error("undo touched a variable of the process")
	}
}
