package protocol

// Command is a parsed client request: the command word (Name) and its arguments
// (Args). The fields are exported so the server package can read them.
type Command struct {
	Name string
	Args [][]byte
}

// Command reads a request out of a decoded Value. A request is a RESP array of
// bulk strings - SET foo bar arrives as ["SET", "foo", "bar"] — so Command
// returns its command word and arguments, leaving the case as-is for the
// dispatcher to normalize. If the Value isn't a non-empty array of bulk strings,
// it returns a *ProtocolError.
func (v Value) Command() (command Command, err error) {
	if len(v.array) < 1 {
		err = ProtoErrorf("command must be a non-empty array")
		return
	}

	if v.array[0].typeTag != typeBulkString {
		err = ProtoErrorf("command name must be a bulk string, got type %q", v.array[0].typeTag)
		return
	}
	command.Name = string(v.array[0].bulk)

	for _, val := range v.array[1:] {
		if val.typeTag != typeBulkString {
			err = ProtoErrorf("command argument must be a bulk string, got type %q", val.typeTag)
			return
		}

		command.Args = append(command.Args, val.bulk)
	}

	return command, err
}
