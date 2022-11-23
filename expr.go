package cron

import (
	"errors"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
	"time"
)

type fieldType int

const (
	fieldMinutes fieldType = iota
	fieldHours
	fieldDaysOfMonth
	fieldMonths
	fieldDaysOfWeek
)

func (t fieldType) String() string {
	switch t {
	case fieldMinutes:
		return "minutes"
	case fieldHours:
		return "hours"
	case fieldDaysOfMonth:
		return "days of month"
	case fieldMonths:
		return "months"
	case fieldDaysOfWeek:
		return "days of week"
	default:
		return strconv.FormatInt(int64(t), 10)
	}
}

type Expr struct {
	expr string
	m    uint64 // 0-59
	h    uint32 // 0-23
	dom  uint32 // 1-31
	mon  uint16 // 1-12
	dow  uint8  // 0-6 (0=Sunday)
}

func MustParse(expr string) Expr {
	e, err := Parse(expr)
	if err != nil {
		panic(err)
	}
	return e
}

func Parse(expr string) (e Expr, err error) {
	m, h, dom, mon, dow := splitFields(expr)

	parseField := func(groups string, typ fieldType, min, max int) (field uint64) {
		if err != nil {
			return
		}
		if field, err = parseField(groups, typ, min, max); err != nil {
			err = fmt.Errorf("cron: parsing %q: %v", expr, err)
		}
		return
	}
	e.m = parseField(m, fieldMinutes, 0, 59)
	e.h = uint32(parseField(h, fieldHours, 0, 23))
	e.dom = uint32(parseField(dom, fieldDaysOfMonth, 1, 31))
	e.mon = uint16(parseField(mon, fieldMonths, 1, 12))
	e.dow = uint8(parseField(dow, fieldDaysOfWeek, 0, 6))
	if err != nil {
		return e, err
	}

	// Detect impossible combinations of month/day pairs, e.g., February 30th.
	const monthsWith31Days = 1<<1 | 1<<3 | 1<<5 | 1<<7 | 1<<8 | 1<<10 | 1<<12
	const domRange29 = (uint32(1)<<29 - 1) << 1
	const domRange30 = (uint32(1)<<30 - 1) << 1
	if e.mon&monthsWith31Days == 0 {
		domAllowed := domRange30
		onlyFeb := e.mon == 1<<2
		if onlyFeb {
			domAllowed = domRange29
		}
		if e.dom&domAllowed == 0 && onlyFeb {
			return e, fmt.Errorf("cron: field %q doesn't match any day of month 2", fieldDaysOfMonth)
		} else if e.dom&domAllowed == 0 {
			return e, fmt.Errorf("cron: field %q doesn't match any day of months 4, 6, 9 or 11", fieldDaysOfMonth)
		}
	}

	e.expr = expr

	return e, nil
}

func splitFields(expr string) (m, h, dom, mon, dow string) {
	m, expr, _ = strings.Cut(expr, " ")
	h, expr, _ = strings.Cut(expr, " ")
	dom, expr, _ = strings.Cut(expr, " ")
	mon, expr, _ = strings.Cut(expr, " ")
	dow = expr
	return
}

/*
parseField implements the following BNF:

	groups     ::= group ( ',' group )*
	group      ::= '*' | rangeOrNum ( '/' step )?
	rangeOrNum ::= number ( '-' number )?
	step       ::= number
	number     ::= digit+
	digit      ::= '0'..'9'
*/
func parseField(groups string, typ fieldType, min, max int) (field uint64, err error) {
	if groups == "" {
		return field, &parseError{typ: typ, err: errors.New("field is empty")}
	}
	for groups != "" {
		group, groupsRest, found := strings.Cut(groups, ",")
		if found && groupsRest == "" {
			return field, &parseError{typ, errors.New("trailing comma found")}
		}
		groups = groupsRest

		if from, to, step, err := parseGroup(typ, group, min, max); err != nil {
			return field, err
		} else if step == 1 {
			field |= uint64(1)<<(to+1) - uint64(1)<<from
		} else {
			for i := from; i <= to; i += step {
				field |= uint64(1) << i
			}
		}
	}
	return field, nil
}

func parseGroup(typ fieldType, expr string, min, max int) (from, to, step int, err error) {
	if expr == "*" {
		return min, max, 1, nil
	}

	rangeOrNum, rangeStep, foundStep := strings.Cut(expr, "/")
	rangeFrom, rangeTo, foundTo := strings.Cut(rangeOrNum, "-")
	if foundStep && rangeStep == "" {
		return from, to, step, &parseError{typ, errors.New("trailing slash found")}
	}
	if foundTo && rangeTo == "" {
		return from, to, step, &parseError{typ, errors.New("trailing dash found")}
	}

	from, err = parseAliasOrNumber(typ, rangeFrom, min, max)

	if rangeTo == "" && rangeStep == "" {
		to = from
	} else if rangeTo == "" {
		to = max
	} else if err == nil {
		to, err = parseAliasOrNumber(typ, rangeTo, from, max)
	}

	if rangeStep == "" {
		step = 1
	} else if err == nil {
		step, err = parseNumber(typ, rangeStep, 1, max-min+1)
	}

	return from, to, step, err
}

func parseAliasOrNumber(typ fieldType, s string, min, max int) (n int, err error) {
	switch typ {
	case fieldMonths:
		if n, ok := monFromName(s); ok {
			return n, nil
		}
	case fieldDaysOfWeek:
		if n, ok := dowFromName(s); ok {
			return n, nil
		}
	}
	return parseNumber(typ, s, min, max)
}

func monFromName(name string) (n int, ok bool) {
	if len(name) != 3 {
		return 0, false
	}

	switch n0, n1, n2 := toLower(name[0]), toLower(name[1]), toLower(name[2]); {
	case n0 == 'a' && n1 == 'p' && n2 == 'r': // apr
		return 4, true
	case n0 == 'a' && n1 == 'u' && n2 == 'g': // aug
		return 8, true
	case n0 == 'd' && n1 == 'e' && n2 == 'c': // dec
		return 12, true
	case n0 == 'f' && n1 == 'e' && n2 == 'b': // feb
		return 2, true
	case n0 == 'j' && n1 == 'a' && n2 == 'n': // jan
		return 1, true
	case n0 == 'j' && n1 == 'u' && n2 == 'l': // jul
		return 7, true
	case n0 == 'j' && n1 == 'u' && n2 == 'n': // jun
		return 6, true
	case n0 == 'm' && n1 == 'a' && n2 == 'r': // mar
		return 3, true
	case n0 == 'm' && n1 == 'a' && n2 == 'y': // may
		return 5, true
	case n0 == 'n' && n1 == 'o' && n2 == 'v': // nov
		return 11, true
	case n0 == 'o' && n1 == 'c' && n2 == 't': // oct
		return 10, true
	case n0 == 's' && n1 == 'e' && n2 == 'p': // sep
		return 9, true
	}

	return 0, false
}

func dowFromName(name string) (int, bool) {
	if len(name) != 3 {
		return 0, false
	}

	switch n0, n1, n2 := toLower(name[0]), toLower(name[1]), toLower(name[2]); {
	case n0 == 'f' && n1 == 'r' && n2 == 'i': // fri
		return 5, true
	case n0 == 'm' && n1 == 'o' && n2 == 'n': // mon
		return 1, true
	case n0 == 's' && n1 == 'a' && n2 == 't': // sat
		return 6, true
	case n0 == 's' && n1 == 'u' && n2 == 'n': // sun
		return 0, true
	case n0 == 't' && n1 == 'h' && n2 == 'u': // thu
		return 4, true
	case n0 == 't' && n1 == 'u' && n2 == 'e': // tue
		return 2, true
	case n0 == 'w' && n1 == 'e' && n2 == 'd': // wed
		return 3, true
	}

	return 0, false
}

func toLower(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + 'a' - 'A'
	}
	return b
}

func parseNumber(typ fieldType, s string, min, max int) (n int, err error) {
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		return 0, &parseError{typ, errors.New("leading sign found")}
	}
	if n, err = strconv.Atoi(s); err != nil {
		return n, &parseError{typ, err}
	}
	if n < min || n > max {
		return n, &parseError{
			typ, fmt.Errorf("value out of range [%d, %d] found", min, max)}
	}
	return n, nil
}

type parseError struct {
	typ fieldType
	err error
}

func (e *parseError) Error() string {
	return fmt.Sprintf("field %q: %v", e.typ, e.err)
}

func (e *Expr) Prev(from time.Time) time.Time {
	t := from.Truncate(time.Minute).Add(-time.Minute)
	m, h, dom, mon, dow := e.m, e.h, e.dom, e.mon, e.dow

	var dateY int
	var dateMon time.Month
	var dateDom int
	var dateDow time.Weekday
	var dateH, dateM int
day:
	for {
		dateY, dateMon, dateDom = t.Date()
		dateDow = t.Weekday()
		switch {
		case mon&(1<<dateMon) == 0:
			dateMon = prev(dateMon, time.January, mon) + 1
			dateDom = 0
		case dom&(1<<dateDom) == 0:
			dateDom = prev(dateDom, 1, dom)
		case dow&(1<<dateDow) == 0:
			dowPrev := prev(dateDow, time.Sunday, dow)
			dateDom -= int(dateDow - dowPrev)
		default:
			break day
		}
		t = time.Date(dateY, dateMon, dateDom, 23, 59, 0, 0, t.Location())
	}
	doy := t.YearDay()
hour:
	for {
		dateH, dateM, _ = t.Clock()
		switch {
		case h&(1<<dateH) == 0:
			dateH = prev(dateH, 0, h) + 1
			dateM = -1
		case m&(uint64(1)<<dateM) == 0:
			dateM = prev(dateM, 0, m)
		default:
			break hour
		}
		t = time.Date(dateY, dateMon, dateDom, dateH, dateM, 0, 0, t.Location())
		if t.YearDay() != doy {
			// We hit a different day.
			goto day
		}
	}
	return t
}

func (e *Expr) Next(from time.Time) time.Time {
	t := from.Truncate(time.Minute).Add(time.Minute)
	m, h, dom, mon, dow := e.m, e.h, e.dom, e.mon, e.dow

	var dateY int
	var dateMon time.Month
	var dateDom int
	var dateDow time.Weekday
	var dateH, dateM int
day:
	for {
		dateY, dateMon, dateDom = t.Date()
		dateDow = t.Weekday()
		switch {
		case mon&(1<<dateMon) == 0:
			dateMon = next(dateMon, time.December, mon)
			dateDom = 1
		case dom&(1<<dateDom) == 0:
			dateDom = next(dateDom, maxDomForMon(dateY, dateMon), dom)
		case dow&(1<<dateDow) == 0:
			dowNext := next(dateDow, time.Saturday, dow)
			dateDom += int(dowNext - dateDow)
		default:
			break day
		}
		t = time.Date(dateY, dateMon, dateDom, 0, 0, 0, 0, t.Location())
	}
	doy := t.YearDay()
hour:
	for {
		dateH, dateM, _ = t.Clock()
		switch {
		case h&(1<<dateH) == 0:
			dateH = next(dateH, 23, h)
			dateM = 0
		case m&(uint64(1)<<dateM) == 0:
			dateM = next(dateM, 59, m)
		default:
			break hour
		}
		t = time.Date(dateY, dateMon, dateDom, dateH, dateM, 0, 0, t.Location())
		if t.YearDay() != doy {
			// We hit a different day.
			goto day
		}
	}
	return t
}

func maxDomForMon(y int, mon time.Month) int {
	switch mon {
	case time.February:
		if (y%4 == 0 && y%100 != 0) || y%400 == 0 {
			// Leap year.
			return 29
		}
		return 28
	case time.April, time.June, time.September, time.November:
		return 30
	default:
		return 31
	}
}

type timeUnit interface {
	int | time.Month | time.Weekday
}

type bitfield interface {
	uint8 | uint16 | uint32 | uint64
}

// prev returns the position of the first least-significant bit set in field
// before position i. The result, p, is such that p >= limit-1.
func prev[T timeUnit, F bitfield](i, limit T, field F) (p T) {
	i++
	mask := ^(^uint64(0) << i)
	prev := T(bits.Len64(uint64(field)&mask) - 1)
	if prev < limit {
		prev = limit - 1
	}
	return prev
}

// next returns the position of the first most-significant bit set in field
// after position i. The result, n, is such that n <= limit+1.
func next[T timeUnit, F bitfield](i, limit T, field F) (n T) {
	i++
	mask := ^uint64(0) << i
	next := T(bits.TrailingZeros64(uint64(field) & mask))
	if next > limit {
		next = limit + 1
	}
	return next
}

// String returns a string representation of the cron expression.
func (e *Expr) String() string {
	return e.expr
}

// MarshalText implements the encoding.TextMarshaler interface.
func (e *Expr) MarshalText() ([]byte, error) {
	return []byte(e.expr), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (e *Expr) UnmarshalText(text []byte) (err error) {
	*e, err = Parse(string(text))
	return err
}
