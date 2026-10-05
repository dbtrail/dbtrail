package shim

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// Session is the command loop of one client connection: it reads a command,
// hands it to the Handler and writes the answer. It replaces go-mysql's own
// server.Conn.HandleCommand for one reason (#2036): that loop decodes the
// arguments of COM_STMT_EXECUTE only when the client re-sends their types,
// and hands the handler nil for every argument when it does not. Clients
// that send the types once (Connector/J with server prepares, the C API,
// PHP's mysqlnd) would run their second execution with every argument NULL:
// an empty result, or a row stored with NULLs, and no error. The session
// keeps each statement's types and decodes the values on every execution.
// It also hands the handler's error to the writer as it is, so an EXECUTE
// error keeps its MySQL code (the library wraps it first, which turns every
// one into 1105).
//
// Every other command is answered the way the library answers it, with one
// addition: before each answer is written the session's status flags are put
// on it (stampStatus, status.go), so the client is told the state its session
// really has.
type Session struct {
	conn  *server.Conn
	h     server.Handler
	stmts map[uint32]*sessionStmt
	next  uint32
	// longBytes is the long data held across the connection's statements.
	longBytes int
}

// sessionStmt is one prepared statement of the connection.
type sessionStmt struct {
	query  string
	params int
	ctx    any
	// types are the two bytes per parameter (type, flags) of the last
	// execution that sent them; nil until one does.
	types []byte
	// long holds the values sent ahead with COM_STMT_SEND_LONG_DATA, by
	// parameter; they belong to the next execution only.
	long map[int][]byte
	// longBytes is how much of it this statement was sent; longOver says
	// the connection's bound was passed while sending it, so the execution
	// it belongs to is refused rather than run with a cut value.
	longBytes int
	longOver  bool
}

// Bounds on what one connection may make the process hold; variables so a
// test can lower them. maxLongData is the long data pending across all of
// the connection's statements, maxSessionStmts the statements it may keep
// open (MySQL's own max_prepared_stmt_count default).
var (
	maxLongData     = 64 << 20
	maxSessionStmts = 16382
)

// NewSession serves conn's commands to h. conn is past its handshake.
func NewSession(conn *server.Conn, h server.Handler) *Session {
	return &Session{conn: conn, h: h, stmts: map[uint32]*sessionStmt{}}
}

// noReply marks a command that is not answered.
type noReply struct{}

// HandleCommand reads one command and answers it. An error means the
// connection is over.
func (s *Session) HandleCommand() error {
	c := s.conn
	if c.Conn == nil {
		return errors.New("connection closed")
	}
	data, err := c.ReadPacket()
	if err != nil {
		c.Close()
		c.Conn = nil
		return err
	}
	if len(data) == 0 {
		err = c.WriteValue(mysql.ErrMalformPacket)
	} else if data[0] == mysql.COM_QUIT {
		c.Close()
		c.Conn = nil
		return nil
	} else if v := s.dispatch(data[0], data[1:]); v != (noReply{}) {
		// The session's status flags as they are now that the command ran
		// (#2110): on this answer, and on the connection for the EOF and OK
		// packets the library writes from it.
		s.stampStatus(v)
		err = c.WriteValue(v)
	}
	if c.Conn != nil {
		c.ResetSequence()
	}
	if err != nil {
		c.Close()
		c.Conn = nil
	}
	return err
}

// dispatch answers one command: nil is OK, an error is sent as an error
// packet, noReply as nothing.
func (s *Session) dispatch(cmd byte, data []byte) any {
	if src, ok := s.h.(sourceSession); ok {
		switch cmd {
		case mysql.COM_STMT_CLOSE, mysql.COM_STMT_SEND_LONG_DATA:
			// No answer to put the loss in; a close still frees the statement.
		default:
			statement := cmd == mysql.COM_QUERY || cmd == mysql.COM_STMT_PREPARE || cmd == mysql.COM_STMT_EXECUTE
			if err := src.SourceLost(statement); err != nil {
				return err
			}
		}
	}
	switch cmd {
	case mysql.COM_QUERY:
		r, err := s.h.HandleQuery(string(data))
		if err != nil {
			return err
		}
		return r
	case mysql.COM_PING:
		if src, ok := s.h.(sourceSession); ok {
			if err := src.PingSource(); err != nil {
				return err
			}
		}
		return nil
	case mysql.COM_INIT_DB:
		if err := s.h.UseDB(string(data)); err != nil {
			return err
		}
		return nil
	case mysql.COM_FIELD_LIST:
		table, wildcard, _ := bytes.Cut(data, []byte{0x00})
		fs, err := s.h.HandleFieldList(string(table), string(wildcard))
		if err != nil {
			return err
		}
		return fs
	case mysql.COM_STMT_PREPARE:
		return s.prepare(string(data))
	case mysql.COM_STMT_EXECUTE:
		r, err := s.execute(data)
		if err != nil {
			return err
		}
		return r
	case mysql.COM_STMT_CLOSE:
		s.closeStmt(data)
		return noReply{}
	case mysql.COM_STMT_SEND_LONG_DATA:
		s.sendLongData(data)
		return noReply{}
	case mysql.COM_STMT_RESET:
		st, _, err := s.lookup(data, "stmt_reset")
		if err != nil {
			return err
		}
		s.takeLong(st)
		return mysql.NewResultReserveResultset(0)
	}
	if err := s.h.HandleOtherCommand(cmd, data); err != nil {
		return err
	}
	return nil
}

func (s *Session) prepare(query string) any {
	if len(s.stmts) >= maxSessionStmts {
		return mysql.NewError(mysql.ER_MAX_PREPARED_STMT_COUNT_REACHED, fmt.Sprintf("Can't create more than %d prepared statements on one connection; close some", maxSessionStmts))
	}
	params, columns, ctx, err := s.h.HandleStmtPrepare(query)
	if err != nil {
		return err
	}
	s.next++
	s.stmts[s.next] = &sessionStmt{query: query, params: params, ctx: ctx}
	out := &server.Stmt{Query: query, Context: ctx}
	out.ID, out.Params, out.Columns = s.next, params, columns
	if f, ok := ctx.(interface{ stmtFields() ([][]byte, [][]byte) }); ok {
		// A statement prepared on the source answers with the source's own
		// parameter and column definitions.
		out.RawParamFields, out.RawColumnFields = f.stmtFields()
	}
	return out
}

// lookup reads the statement id that opens every statement command.
func (s *Session) lookup(data []byte, command string) (*sessionStmt, uint32, error) {
	if len(data) < 4 {
		return nil, 0, mysql.ErrMalformPacket
	}
	id := binary.LittleEndian.Uint32(data[0:4])
	st, ok := s.stmts[id]
	if !ok {
		return nil, id, mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER, 5, strconv.FormatUint(uint64(id), 10), command)
	}
	return st, id, nil
}

// COM_STMT_EXECUTE flag bits this port does not serve: a cursor of any kind.
const stmtCursorFlags = 0x01 | 0x02 | 0x04

func (s *Session) execute(data []byte) (*mysql.Result, error) {
	// id (4), flags (1), iteration count (4).
	if len(data) < 9 {
		return nil, mysql.ErrMalformPacket
	}
	st, _, err := s.lookup(data, "stmt_execute")
	if err != nil {
		return nil, err
	}
	// Long data belongs to this execution, whatever its outcome: a refused
	// one must not leave it behind for the next (MySQL clears it too).
	long, tooLong := s.takeLong(st)
	if tooLong {
		return nil, mysql.NewError(mysql.ER_NET_PACKET_TOO_LARGE, fmt.Sprintf("prepared statement long data exceeds the %d bytes one connection may hold", maxLongData))
	}
	if flags := data[4]; flags&stmtCursorFlags != 0 || flags>>4 != 0 {
		return nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("prepared statement cursors are not supported (flags 0x%x)", flags))
	}
	pos := 9
	args := make([]any, st.params)
	if st.params > 0 {
		bitmapLen := (st.params + 7) >> 3
		if len(data) < pos+bitmapLen+1 {
			return nil, mysql.ErrMalformPacket
		}
		nullBitmap := data[pos : pos+bitmapLen]
		pos += bitmapLen
		newTypes := data[pos] == 1
		pos++
		if newTypes {
			if len(data) < pos+st.params*2 {
				return nil, mysql.ErrMalformPacket
			}
			// Copied: the packet buffer is the connection's.
			st.types = bytes.Clone(data[pos : pos+st.params*2])
			pos += st.params * 2
		}
		if st.types == nil {
			return nil, mysql.NewError(mysql.ER_WRONG_ARGUMENTS, "prepared statement executed without its argument types")
		}
		if err := decodeStmtArgs(args, nullBitmap, st.types, data[pos:], long); err != nil {
			return nil, err
		}
	}
	return s.h.HandleStmtExecute(st.ctx, st.query, args)
}

func (s *Session) closeStmt(data []byte) {
	st, id, err := s.lookup(data, "stmt_close")
	if err != nil {
		return
	}
	s.takeLong(st)
	// COM_STMT_CLOSE has no answer, so there is nowhere to report this.
	_ = s.h.HandleStmtClose(st.ctx)
	delete(s.stmts, id)
}

// takeLong hands over the statement's pending long data and forgets it,
// returning its bytes to the connection's budget. over says the data was cut
// by the bound.
func (s *Session) takeLong(st *sessionStmt) (long map[int][]byte, over bool) {
	long, over = st.long, st.longOver
	s.longBytes -= st.longBytes
	st.long, st.longBytes, st.longOver = nil, 0, false
	return long, over
}

// sendLongData appends a chunk to one parameter's value. The command has no
// answer; a chunk for an unknown statement or parameter is dropped, as MySQL
// drops it.
func (s *Session) sendLongData(data []byte) {
	st, _, err := s.lookup(data, "stmt_send_long_data")
	if err != nil || len(data) < 6 {
		return
	}
	param := int(binary.LittleEndian.Uint16(data[4:6]))
	if param >= st.params {
		return
	}
	if st.long == nil {
		st.long = map[int][]byte{}
	}
	if st.longOver || s.longBytes+len(data)-6 > maxLongData {
		// Not kept: the execution it belongs to is refused, and what the
		// statement already held is given back.
		long, _ := s.takeLong(st)
		clear(long)
		st.longOver = true
		return
	}
	st.longBytes += len(data) - 6
	s.longBytes += len(data) - 6
	st.long[param] = append(st.long[param], data[6:]...)
}

// decodeStmtArgs reads the binary-protocol values of one execution into
// args, the Go values go-mysql's server produces for the same bytes: sized
// integers, floats, nil for NULL, mysql.TypedBytes for everything carried as
// a length-prefixed string (temporal values included, in their binary form).
// A parameter sent ahead as long data has no value in the packet.
func decodeStmtArgs(args []any, nullBitmap, types, values []byte, long map[int][]byte) error {
	pos := 0
	need := func(n int) bool { return len(values) >= pos+n }
	for i := range args {
		tp := types[i<<1]
		unsigned := types[(i<<1)+1]&mysql.PARAM_UNSIGNED != 0
		if v, ok := long[i]; ok {
			// Long data is a string whatever type the client declared for
			// the parameter (MySQL reads it so): relayed under a numeric or
			// temporal type it would be decoded as that type's bytes.
			if !isLenEncType(tp) {
				tp = mysql.MYSQL_TYPE_BLOB
			}
			args[i] = mysql.TypedBytes{Type: tp, Bytes: v}
			continue
		}
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) != 0 {
			args[i] = nil
			continue
		}
		switch tp {
		case mysql.MYSQL_TYPE_NULL:
			args[i] = nil
		case mysql.MYSQL_TYPE_TINY:
			if !need(1) {
				return mysql.ErrMalformPacket
			}
			if unsigned {
				args[i] = values[pos]
			} else {
				args[i] = int8(values[pos])
			}
			pos++
		case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR:
			if !need(2) {
				return mysql.ErrMalformPacket
			}
			v := binary.LittleEndian.Uint16(values[pos:])
			if unsigned {
				args[i] = v
			} else {
				args[i] = int16(v)
			}
			pos += 2
		case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
			if !need(4) {
				return mysql.ErrMalformPacket
			}
			v := binary.LittleEndian.Uint32(values[pos:])
			if unsigned {
				args[i] = v
			} else {
				args[i] = int32(v)
			}
			pos += 4
		case mysql.MYSQL_TYPE_LONGLONG:
			if !need(8) {
				return mysql.ErrMalformPacket
			}
			v := binary.LittleEndian.Uint64(values[pos:])
			if unsigned {
				args[i] = v
			} else {
				args[i] = int64(v)
			}
			pos += 8
		case mysql.MYSQL_TYPE_FLOAT:
			if !need(4) {
				return mysql.ErrMalformPacket
			}
			args[i] = math.Float32frombits(binary.LittleEndian.Uint32(values[pos:]))
			pos += 4
		case mysql.MYSQL_TYPE_DOUBLE:
			if !need(8) {
				return mysql.ErrMalformPacket
			}
			args[i] = math.Float64frombits(binary.LittleEndian.Uint64(values[pos:]))
			pos += 8
		case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_BIT,
			mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_SET, mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB,
			mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING,
			mysql.MYSQL_TYPE_GEOMETRY, mysql.MYSQL_TYPE_VECTOR, mysql.MYSQL_TYPE_JSON,
			mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE,
			mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIME:
			if !need(1) {
				return mysql.ErrMalformPacket
			}
			v, isNull, n, err := mysql.LengthEncodedString(values[pos:])
			if err != nil {
				return mysql.ErrMalformPacket
			}
			pos += n
			if isNull {
				args[i] = nil
			} else {
				// Copied: the packet buffer is the connection's.
				args[i] = mysql.TypedBytes{Type: tp, Bytes: bytes.Clone(v)}
			}
		default:
			return mysql.NewError(mysql.ER_WRONG_ARGUMENTS, fmt.Sprintf("prepared statement argument %d has an unsupported type (%d)", i+1, tp))
		}
	}
	return nil
}
