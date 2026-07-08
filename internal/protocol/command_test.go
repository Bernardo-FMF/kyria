package protocol

import (
	"errors"
	"testing"
)

// TestValue_Command extracts the command word and arguments from a well-formed
// request (an array of bulk strings).
func TestValue_Command(t *testing.T) {
	v := Array(
		BulkString([]byte("SET")),
		BulkString([]byte("foo")),
		BulkString([]byte("bar")),
	)

	cmd, err := v.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if cmd.Name != "SET" {
		t.Errorf("Name = %q, want %q", cmd.Name, "SET")
	}
	if len(cmd.Args) != 2 {
		t.Fatalf("len(Args) = %d, want 2", len(cmd.Args))
	}
	if string(cmd.Args[0]) != "foo" || string(cmd.Args[1]) != "bar" {
		t.Errorf("Args = [%q %q], want [foo bar]", cmd.Args[0], cmd.Args[1])
	}
}

// TestValue_Command_SingleWord: a command with no arguments (e.g. PING) yields the
// name and an empty argument list.
func TestValue_Command_SingleWord(t *testing.T) {
	cmd, err := Array(BulkString([]byte("PING"))).Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if cmd.Name != "PING" {
		t.Errorf("Name = %q, want PING", cmd.Name)
	}
	if len(cmd.Args) != 0 {
		t.Errorf("len(Args) = %d, want 0", len(cmd.Args))
	}
}

// TestValue_Command_Errors: anything that isn't a non-empty array of bulk strings
// is malformed, so Command returns a *ProtocolError (which the server can tell
// apart from a transport error).
func TestValue_Command_Errors(t *testing.T) {
	tests := map[string]Value{
		"not an array":     SimpleString("PING"),
		"empty array":      Array(),
		"non-bulk element": Array(BulkString([]byte("GET")), Integer(5)),
	}
	for name, v := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := v.Command()
			if err == nil {
				t.Fatalf("Command() = nil error, want an error")
			}
			var perr *ProtocolError
			if !errors.As(err, &perr) {
				t.Errorf("error %v is not a *ProtocolError", err)
			}
		})
	}
}
