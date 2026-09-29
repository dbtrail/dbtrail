package metadata

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

// MariaDB's UUID (10.7+), INET4 (10.10+) and INET6 (10.5+) are stored as fixed
// binary values, but every way a person or a tool reads them gives TEXT: a
// SELECT, mydumper (which writes `_binary "123e4567-…"`), and so the baseline
// Parquet built from it. The capture side keeps the bytes (MapRow pads them to
// full width, #1944) and the index stores those bytes base64-encoded, like a
// BLOB. The readers that hand values back (reconstruct, the shim, verify) turn
// them into the server's own text with FormatMariaDBFixed, so a restored value
// equals the source value and matches the baseline's.

// MariaDBFixedWidth returns the storage width in bytes of a MariaDB UUID,
// INET4 or INET6 column (16, 4, 16), or 0 for any other data type.
func MariaDBFixedWidth(dataType string) int {
	return mariaDBFixedWidth[strings.ToLower(strings.TrimSpace(dataType))]
}

// FormatMariaDBFixed renders the bytes of a UUID/INET4/INET6 value as MariaDB
// prints it. ok is false when dataType is not one of the three or b is not
// exactly the type's width: a value of another length is not a value of the
// type (a damaged pre-#1944 event, say), and guessing a rendering for it would
// hide that.
//
// The rules below were measured against MariaDB 11.8 (CAST(… AS CHAR) of the
// column) and are guarded against the server by an integration test:
//   - UUID: lowercase 8-4-4-4-12 hex, bytes in text order.
//   - INET4: dotted decimal.
//   - INET6: lowercase hex words without leading zeros; the LONGEST run of
//     zero words becomes "::" (the first run wins a tie, and a single zero word
//     is compressed too, unlike RFC 5952); when that run is exactly words 0-5
//     the last 32 bits print as a.b.c.d ("::1.2.3.4"), and when it is words 0-4
//     followed by ffff, as "::ffff:a.b.c.d".
func FormatMariaDBFixed(dataType string, b []byte) (string, bool) {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	w := mariaDBFixedWidth[dt]
	if w == 0 || len(b) != w {
		return "", false
	}
	switch dt {
	case "uuid":
		h := hex.EncodeToString(b)
		return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32], true
	case "inet4":
		return dotted(b), true
	default: // inet6
		return formatInet6(b), true
	}
}

func dotted(b []byte) string {
	return strconv.Itoa(int(b[0])) + "." + strconv.Itoa(int(b[1])) + "." +
		strconv.Itoa(int(b[2])) + "." + strconv.Itoa(int(b[3]))
}

func formatInet6(b []byte) string {
	var words [8]uint16
	for i := range words {
		words[i] = uint16(b[2*i])<<8 | uint16(b[2*i+1])
	}
	bestStart, bestLen := -1, 0
	for i := 0; i < 8; {
		if words[i] != 0 {
			i++
			continue
		}
		j := i
		for j < 8 && words[j] == 0 {
			j++
		}
		if j-i > bestLen {
			bestStart, bestLen = i, j-i
		}
		i = j
	}
	if bestStart == 0 && bestLen == 6 {
		return "::" + dotted(b[12:])
	}
	if bestStart == 0 && bestLen == 5 && words[5] == 0xffff {
		return "::ffff:" + dotted(b[12:])
	}
	var sb strings.Builder
	for i := 0; i < 8; i++ {
		if i == bestStart {
			sb.WriteString("::")
			i += bestLen - 1
			continue
		}
		if i > 0 && i != bestStart+bestLen {
			sb.WriteByte(':')
		}
		sb.WriteString(strconv.FormatUint(uint64(words[i]), 16))
	}
	return sb.String()
}

// ParseMariaDBFixed turns the text form of a UUID/INET4/INET6 value into its
// full-width bytes. It is for input a person types (`reconstruct --pk`) and for
// the text a baseline holds, so it accepts what MariaDB itself accepts in the
// cases measured, and refuses anything else rather than guess:
//   - UUID: 32 hex digits in either case; '-' may appear between digits
//     anywhere, but not first or last.
//   - INET4: four decimal parts 0-255, one to three digits each.
//   - INET6: any IPv6 text Go's netip parses, without a zone; a plain IPv4
//     address becomes the IPv4-mapped ::ffff:a.b.c.d, as MariaDB does.
func ParseMariaDBFixed(dataType, s string) ([]byte, error) {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "uuid":
		if s == "" || s[0] == '-' || s[len(s)-1] == '-' {
			return nil, fmt.Errorf("%q is not a UUID", s)
		}
		h := strings.ReplaceAll(s, "-", "")
		b, err := hex.DecodeString(h)
		if err != nil || len(b) != 16 {
			return nil, fmt.Errorf("%q is not a UUID", s)
		}
		return b, nil
	case "inet4":
		parts := strings.Split(s, ".")
		if len(parts) != 4 {
			return nil, fmt.Errorf("%q is not an INET4 address", s)
		}
		b := make([]byte, 4)
		for i, p := range parts {
			if len(p) == 0 || len(p) > 3 || strings.Trim(p, "0123456789") != "" {
				return nil, fmt.Errorf("%q is not an INET4 address", s)
			}
			n, err := strconv.Atoi(p)
			if err != nil || n > 255 {
				return nil, fmt.Errorf("%q is not an INET4 address", s)
			}
			b[i] = byte(n)
		}
		return b, nil
	case "inet6":
		a, err := netip.ParseAddr(s)
		if err != nil || a.Zone() != "" {
			return nil, fmt.Errorf("%q is not an INET6 address", s)
		}
		b16 := a.As16()
		return b16[:], nil
	}
	return nil, errors.New("not a MariaDB UUID/INET4/INET6 column: " + dataType)
}

// ParseMariaDBFixedKey is ParseMariaDBFixed for a key a person typed, which may
// also be the value's bytes in hex: "0x" (either case) followed by exactly the
// type's width in hex digits. That is how query and recover print such a key
// (the pk_values spelling), so a key copied from their output works as input
// too. It is only for typed keys: a baseline holds the text form, and its
// readers keep using ParseMariaDBFixed.
func ParseMariaDBFixedKey(dataType, s string) ([]byte, error) {
	if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		w := MariaDBFixedWidth(dataType)
		if w == 0 {
			return nil, errors.New("not a MariaDB UUID/INET4/INET6 column: " + dataType)
		}
		b, err := hex.DecodeString(s[2:])
		if err != nil || len(b) != w {
			return nil, fmt.Errorf("%q is not %d bytes in hex, the width of a %s", s, w, strings.ToUpper(strings.TrimSpace(dataType)))
		}
		return b, nil
	}
	return ParseMariaDBFixed(dataType, s)
}

// RenderStoredMariaDBFixed turns a UUID/INET4/INET6 value as the index stores
// it in an event image (base64 of the full-width bytes, since #1944) into
// MariaDB's text form. ok is false, and v comes back unchanged, for anything
// else: NULL, a non-string, or a string that does not decode to exactly the
// type's width. That last case is how an event captured before #1944 looks
// (the raw bytes as text, damaged where they were not UTF-8); no valid
// rendering exists for it, and the caller must not treat it as one.
func RenderStoredMariaDBFixed(dataType string, v any) (any, bool) {
	s, isStr := v.(string)
	w := MariaDBFixedWidth(dataType)
	if !isStr || w == 0 || base64.StdEncoding.EncodedLen(w) != len(s) {
		return v, false
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return v, false
	}
	text, ok := FormatMariaDBFixed(dataType, b)
	if !ok {
		return v, false
	}
	return text, true
}

// IsMariaDBFixedText reports whether s is exactly the text MariaDB prints for
// some value of the type. verify uses it to tell a rendered value (comparable
// with the source) from one the render could not produce.
func IsMariaDBFixedText(dataType, s string) bool {
	b, err := ParseMariaDBFixed(dataType, s)
	if err != nil {
		return false
	}
	text, ok := FormatMariaDBFixed(dataType, b)
	return ok && text == s
}
