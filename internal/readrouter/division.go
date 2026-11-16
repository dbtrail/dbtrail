package readrouter

import (
	"regexp"
	"strings"
)

// Three kinds of number the copy types another way than MySQL (#2806),
// measured through the MySQL port on MySQL 8.0.46 and DuckDB 1.5.6.
const (
	// vetoDivision: MySQL's / over two exact numbers (integers, DECIMAL, a
	// decimal literal) is a DECIMAL with div_precision_increment more
	// digits than the dividend, rounded there: k / 7 over an INT is 0.1429
	// and 1.0000, dc / 3 over a DECIMAL(10,2) 0.500000. The copy divides as
	// a DOUBLE: 0.14285714285714285 and 1, another type and another value.
	// FLOOR and CEIL of a quotient are a BIGINT on MySQL and a DOUBLE on
	// the copy, and a quotient compared or sorted is compared at another
	// precision. A / over a DOUBLE is a DOUBLE on both, but which operand
	// is one cannot be read from the text: every / stays on MySQL, but
	// one inside the argument of FROM_UNIXTIME, which UnixTimeVeto reads
	// whole (#2493: FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts) / 3600) * 3600)).
	vetoDivision = "/ (a DECIMAL with div_precision_increment more digits than the dividend on MySQL, a DOUBLE on the copy)"
	// vetoAvg: AVG is a sum divided by a count, with the same types as /:
	// AVG(k) over an INT is 2.8571 on MySQL and 2.857142857142857 on the
	// copy. MariaDB's MEDIAN interpolates between two rows the same way and
	// is kept with it; it was not measured.
	//
	// Under CopyOptions.MySQLAvg (the routing port, #2920) AVG is written as
	// the copy's exact average instead, and only MEDIAN is kept here: what
	// AVG may be given is AvgVeto's to decide once the columns' types are
	// known.
	vetoAvg = "AVG or MEDIAN (a DECIMAL with div_precision_increment more digits than its argument on MySQL, a DOUBLE on the copy)"
	// vetoMedian is vetoAvg once AVG is the copy's macro: MariaDB's MEDIAN
	// alone.
	vetoMedian = "MEDIAN (a DECIMAL with div_precision_increment more digits than its argument on MariaDB, a DOUBLE on the copy)"
	// vetoBigInteger: a whole number written in the statement from 2^63 up
	// is a BIGINT UNSIGNED on MySQL up to 2^64-1 and a DECIMAL past it; the
	// copy reads it as a 128-bit integer (sent as a DECIMAL, applyDuckType
	// in internal/shim) up to 2^127 and as a DOUBLE past it. Measured:
	// SELECT 9223372036854775808 is a BIGINT UNSIGNED on MySQL and was text
	// on the copy. Every one from 2^63 up stays on MySQL, the minus sign
	// read apart: -9223372036854775808 is the same digits after it.
	vetoBigInteger = "a whole number written from 9223372036854775808 up (a BIGINT UNSIGNED or a DECIMAL on MySQL, another type on the copy)"
)

// avgCall is AVG or MEDIAN called, as MySQL reads a call: the white space it
// takes before the parenthesis (callOpen). A column called avg is not
// matched; a qualified name (x.avg(...), a stored function on MySQL) is,
// which costs a statement the copy could have answered, never a wrong one.
var avgCallOpen = regexp.MustCompile(`(?i)\b(?:avg|median)` + callOpen)

// medianCallOpen is MEDIAN called, read as avgCallOpen reads it.
var medianCallOpen = regexp.MustCompile(`(?i)\bmedian` + callOpen)

// divisionVeto says why the copy must not answer blanked (the statement as
// the vetoes read it: string literals blanked, comments removed, backticks
// written as double quotes), or "": a / outside the argument of a
// FROM_UNIXTIME call and outside a quoted name, an AVG or a MEDIAN, or a
// whole number written from 2^63 up. One pass over the text. With
// avgWritten (CopyOptions.MySQLAvg, every AVG written as the copy's macro)
// an AVG is left to AvgVeto and only a MEDIAN is kept.
func divisionVeto(blanked string, avgWritten bool) string {
	if strings.IndexByte(blanked, '/') >= 0 && slashOutsideFromUnixTime(blanked) {
		return vetoDivision
	}
	if avgWritten {
		if medianCallOpen.MatchString(blanked) {
			return vetoMedian
		}
	} else if avgCallOpen.MatchString(blanked) {
		return vetoAvg
	}
	if bigWholeNumber(blanked) {
		return vetoBigInteger
	}
	return ""
}

// bigWholeNumber reports a run of digits in blanked that stands as a number
// of its own (not inside a word or a quoted name, not the fraction of a
// decimal, not followed by a dot, an exponent or a letter) and spells at
// least 2^63.
func bigWholeNumber(blanked string) bool {
	glued := func(c byte) bool { return wordByte(c) || c == '.' || c == '"' }
	for i := 0; i < len(blanked); {
		if blanked[i] < '0' || blanked[i] > '9' || (i > 0 && glued(blanked[i-1])) {
			i++
			continue
		}
		j := i
		for j < len(blanked) && blanked[j] >= '0' && blanked[j] <= '9' {
			j++
		}
		if (j == len(blanked) || !glued(blanked[j])) && atLeastTwoTo63(blanked[i:j]) {
			return true
		}
		i = j
	}
	return false
}

// slashOutsideFromUnixTime reports a / in blanked that is neither inside a
// quoted name nor inside the parentheses of a FROM_UNIXTIME call: what is
// there UnixTimeVeto reads, and it lets / through only under FLOOR or CEIL
// over literals and UNIX_TIMESTAMP calls (wholeParser). An unbalanced
// parenthesis is a statement both sides refuse; a ) with none open ends
// nothing.
func slashOutsideFromUnixTime(blanked string) bool {
	opens := map[int]bool{}
	for _, m := range fromUnixTimeCall.FindAllStringIndex(blanked, -1) {
		opens[m[1]-1] = true
	}
	depth, exemptAt := 0, -1
	for i := 0; i < len(blanked); i++ {
		switch blanked[i] {
		case '"':
			// A quoted name: "" inside it is one quote.
			for i++; i < len(blanked); i++ {
				if blanked[i] == '"' {
					if i+1 < len(blanked) && blanked[i+1] == '"' {
						i++
						continue
					}
					break
				}
			}
		case '(':
			depth++
			if exemptAt < 0 && opens[i] {
				exemptAt = depth
			}
		case ')':
			if depth == exemptAt {
				exemptAt = -1
			}
			if depth > 0 {
				depth--
			}
		case '/':
			if exemptAt < 0 {
				return true
			}
		}
	}
	return false
}

// atLeastTwoTo63 reports whether the digits spell a number of at least
// 2^63 (9223372036854775808), leading zeros left out.
func atLeastTwoTo63(digits string) bool {
	const twoTo63 = "9223372036854775808"
	digits = strings.TrimLeft(digits, "0")
	if len(digits) != len(twoTo63) {
		return len(digits) > len(twoTo63)
	}
	return digits >= twoTo63
}
