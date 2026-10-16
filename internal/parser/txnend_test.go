package parser

import "testing"

func TestIsTransactionTerminator(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  bool
	}{
		// What the server writes.
		{"COMMIT", true},
		{"ROLLBACK", true},
		{"XA COMMIT X'31',X'',1", true},
		{"XA ROLLBACK X'31',X'',1", true},
		{"XA PREPARE X'31',X'',1", true},
		// Case, padding, a trailing semicolon, line breaks, a leading comment.
		{"commit", true},
		{"  COMMIT  ", true},
		{"COMMIT;", true},
		{"\nCOMMIT\n", true},
		{"/* app */ COMMIT", true},
		{"xa   commit X'31'", true},
		// Mid-transaction statements that share a prefix or a word.
		{"ROLLBACK TO SAVEPOINT s1", false},
		{"ROLLBACK TO s1", false},
		{"SAVEPOINT s1", false},
		{"RELEASE SAVEPOINT s1", false},
		{"BEGIN", false},
		{"XA START X'31'", false},
		{"XA END X'31'", false},
		{"XA RECOVER", false},
		// Something that merely starts with the letters.
		{"COMMITTED", false},
		{"COMMIT_LOG", false},
		// Empty and comment-only.
		{"", false},
		{"   ", false},
		{"/* COMMIT */", false},
		// DML and DDL.
		{"INSERT INTO commit VALUES (1)", false},
		{"CREATE TABLE xa (id INT)", false},
	} {
		if got := isTransactionTerminator(tc.query); got != tc.want {
			t.Errorf("isTransactionTerminator(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}
}
