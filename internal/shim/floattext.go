package shim

import (
	"math"
	"strconv"
	"strings"
)

// mysqlFloatText is the text MySQL's text protocol carries for a FLOAT cell
// declared without digits (FLOAT, not FLOAT(M,D)): six significant digits,
// as my_gcvt(MY_GCVT_ARG_FLOAT) prints the value widened to a double. 3.14
// stored in a FLOAT is 3.140000104904175, and MySQL sends "3.14"; 0.3333333
// and 0.3333334 are both "0.333333" (#2829).
func mysqlFloatText(f float32) string {
	// Six digits, correctly rounded, ties to even: what dtoa mode 4 gives
	// for a widened float, whose shortest double text is longer than six.
	return gcvtText(float64(f), 5)
}

// mysqlDoubleText is the text MySQL's text protocol carries for a DOUBLE
// cell declared without digits: the shortest text that reads back as the
// same double, as my_gcvt(MY_GCVT_ARG_DOUBLE) prints it.
func mysqlDoubleText(d float64) string {
	return gcvtText(d, -1)
}

// maxDecptForF is MySQL's MAX_DECPT_FOR_F_FORMAT (DBL_DIG, 15).
const maxDecptForF = 15

// gcvtText is my_gcvt (strings/dtoa.cc, MySQL 8.0) for the width the text
// protocol gives it (FLOATING_POINT_BUFFER, 342 characters). prec is
// strconv's: the digits after the first for a FLOAT, -1 for the shortest
// text of a DOUBLE.
//
// With that width every finite double has room for all its digits in the
// fixed form, so my_gcvt's choice comes down to MAX_DECPT_FOR_F_FORMAT
// alone: the fixed form when the point sits at most 14 places left of the
// first digit and, for a value of 1 or more, at most 15 places right of it
// unless digits follow the point; the exponent form otherwise. The exponent
// is written without a plus sign and without padding: 1e38, 1.23457e-38,
// 5e-324.
//
// The caller passes a finite value: MySQL never sends NaN or an infinity.
func gcvtText(x float64, prec int) string {
	var b strings.Builder
	if math.Signbit(x) {
		// dtoa reports the sign of -0 too, and MySQL sends "-0".
		b.WriteByte('-')
		x = -x
	}
	if x == 0 {
		b.WriteByte('0')
		return b.String()
	}
	e := strconv.FormatFloat(x, 'e', prec, 64) // d.ddddde±XX
	mant, exp, _ := strings.Cut(e, "e")
	digits := strings.Replace(mant, ".", "", 1)
	digits = strings.TrimRight(digits, "0") // dtoa drops trailing zeros
	n, _ := strconv.Atoi(exp)
	decpt := n + 1 // where the point goes, counted from the first digit
	l := len(digits)
	if decpt >= -maxDecptForF+1 && (decpt <= maxDecptForF || l > decpt) {
		switch {
		case decpt <= 0:
			b.WriteString("0.")
			b.WriteString(strings.Repeat("0", -decpt))
			b.WriteString(digits)
		case decpt < l:
			b.WriteString(digits[:decpt])
			b.WriteByte('.')
			b.WriteString(digits[decpt:])
		default:
			b.WriteString(digits)
			b.WriteString(strings.Repeat("0", decpt-l))
		}
		return b.String()
	}
	b.WriteByte(digits[0])
	if l > 1 {
		b.WriteByte('.')
		b.WriteString(digits[1:])
	}
	b.WriteByte('e')
	b.WriteString(strconv.Itoa(decpt - 1))
	return b.String()
}

// exactFloatText is a FLOAT or DOUBLE cell's value as the shortest text that
// reads back as exactly that value under its type. It is what the binary
// protocol is encoded from (encodeBinaryCell): MySQL sends a FLOAT's four
// bytes and a DOUBLE's eight there, not its printed text, so 0.3333333 and
// 0.3333334 stay two values.
func exactFloatText(x float64, single bool) string {
	if single {
		return strconv.FormatFloat(float64(float32(x)), 'g', -1, 32)
	}
	return strconv.FormatFloat(x, 'g', -1, 64)
}
