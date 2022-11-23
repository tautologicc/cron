package cron_test

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"fmrsn.com/cron"
)

var cronRe *regexp.Regexp

func init() {
	fieldRes := [5]string{
		`(?:[06-9]|[1-5][0-9]?)`,      // Match minutes 0-59.
		`(?:[03-9]|1[0-9]?|2[0-3]?)`,  // Match hours 0-23.
		`(?:[4-9]|[12][0-9]?|3[01]?)`, // Match days of month 1-31.
		`(?:[2-9]|1[0-2]?)`,           // Match months 1-12.
		`[0-6]`,                       // Match weekdays 0-6.
	}
	fieldStepRes := [5]string{
		`(?:[7-9]|[1-5][0-9]?|60?)`,   // Match 1-60.
		`(?:[3-9]|1[0-9]?|2[0-4]?)`,   // Match 1-24.
		`(?:[4-9]|[12][0-9]?|3[01]?)`, // Match 1-31.
		`(?:[2-9]|1[0-2]?)`,           // Match 1-12.
		`[1-7]`,                       // Match 1-7.
	}

	matchZeroPadded := func(s string) string {
		return `(?:0*(?:` + s + `))`
	}
	for i := range fieldRes {
		fieldRes[i] = matchZeroPadded(fieldRes[i])
		fieldStepRes[i] = matchZeroPadded(fieldStepRes[i])
	}

	// Match aliases
	fieldRes[3] = `(?:` + fieldRes[3] + `|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)`
	fieldRes[4] = `(?:` + fieldRes[4] + `|sun|mon|tue|wed|thu|fri|sat)`

	for i, fieldRe := range fieldRes {
		rangeRe := `(?:` + fieldRe + `(?:-` + fieldRe + `)?)` // Match range.
		stepRe := `(?:/` + fieldStepRes[i] + `)`              // Match range step.
		re := `(?:` + rangeRe + `)`                           // Match number or range or asterisk.
		re = `(?:\*|(?:` + re + stepRe + `?))`                // Match asterisk or repetition.
		re = re + `(?:,` + re + `)*`                          // Match lists of the above.
		fieldRes[i] = `(?:` + re + `)`
	}

	re := fieldRes[0]
	for _, fieldRe := range fieldRes[1:] {
		re += ` ` + fieldRe // Match fields separated by space.
	}

	re = `^(?:` + re + `)$` // Match whole content.

	cronRe = regexp.MustCompile(re)
}

func FuzzCron(f *testing.F) {
	seed := []string{
		"* * * * *",
		"0/2 * * * *",
		"1-59/2 * * * *",
		"0/3 * * * *",
		"0/30 * * * *",
		"30 * * * *",
		"0 * * * *",
		"0 0/2 * * *",
		"0 9-17 * * *",
		"0 0 * * *",
		"0 0 * * 0",
		"0 0 * * 1-5",
		"0 0 * * 1-",
		"0 0 * * 0,6",
		"0 0 * * 0,",
		"0 0 1 * * *",
		"0 0 1 1/6 *",
		"0 0 1 1 *",
		"0 12 * * *",
		"15 10 * * *",
		"* 14 * * *",
		"0/5 14 * * *",
		"0/5 14,18 * * *",
		"0-5 14 * * *",
		"10,44 14 * 3 3",
		"15 10 * * 1-5",
		"15 10 15 * *",
		"0 12 1/5 * *",
		"11 11 11 11 *",
		"0/5 14,18,3-39,52 * 1,3,9 1-5",
		"0 0 1 jan 1",
		"0 0 1 feb 1",
		"0 0 1 mar 1",
		"0 0 1 apr 1",
		"0 0 1 may 1",
		"0 0 1 jun 1",
		"0 0 1 jul 1",
		"0 0 1 aug 1",
		"0 0 1 sep 1",
		"0 0 1 oct 1",
		"0 0 1 nov 1",
		"0 0 1 dec 1",
		"0 0 1 1 sun",
		"0 0 1 1 mon",
		"0 0 1 1 tue",
		"0 0 1 1 wed",
		"0 0 1 1 thu",
		"0 0 1 1 fri",
		"0 0 1 1 sat",
		"0 0 1 XXX XXX",
	}
	for _, expr := range seed {
		f.Add(expr)
	}

	start := time.Date(2011, 12, 21, 0, 0, 0, 0, time.UTC)
	oneYearAfter := start.AddDate(1, 0, 0)

	f.Fuzz(func(t *testing.T, cronExpr string) {
		cron, err := cron.Parse(cronExpr)
		tcron, ok := parseRefCron(cronExpr)
		if err != nil && ok {
			t.Fatalf("expected Parse to accept %q\nerr: %v", cronExpr, err)
		} else if err == nil && !ok {
			t.Fatalf("expected Parse not to accept %q", cronExpr)
		}
		if err != nil || tcron.isZero() {
			return
		}

		from := start
		for next := cron.Next(from); next.Before(oneYearAfter); next = cron.Next(from) {
			if got, want := next, tcron.next(from); !got.Equal(want) {
				t.Errorf("wrong next\ngot:  %v\nwant: %v", got, want)
			}
			from = next
		}

		from = oneYearAfter
		for prev := cron.Prev(from); prev.After(start); prev = cron.Prev(from) {
			if got, want := prev, tcron.prev(from); !got.Equal(want) {
				t.Errorf("wrong prev\ngot:  %v\nwant: %v", got, want)
			}
			from = prev
		}
	})
}

func TestParseInvalidDom(t *testing.T) {
	tests := []struct {
		expr string
		fail bool
	}{{
		expr: "* * 30,31 2 *",
		fail: true,
	}, {
		expr: "* * 29,30,31 2 *",
		fail: false,
	}, {
		expr: "* * 31 4,6,9,11 *",
		fail: true,
	}, {
		expr: "* * 30,31 4,6,9,11 *",
		fail: false,
	}}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.expr, func(t *testing.T) {
			_, err := cron.Parse(tt.expr)
			if err == nil && tt.fail {
				t.Error("expected Parse to reject cron expression")
			} else if err != nil && !tt.fail {
				t.Errorf("expected Parse to accept cron expression\nerr: %v", err)
			}
		})
	}
}

func BenchmarkParse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		cron.Parse("1 2-3 4/5 6,jul SUN")
	}
}

func TestNextDow(t *testing.T) {
	const year = 2022
	from := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		dow      string
		wantYear int
	}{
		{"sun", 2017},
		{"mon", 2018},
		{"tue", 2019},
		{"wed", 2020},
		{"thu", 2015},
		{"fri", 2021},
		{"sat", 2011},
		{"sun", 2023},
		{"mon", 2024},
		{"tue", 2030},
		{"wed", 2025},
		{"thu", 2026},
		{"fri", 2027},
		{"sat", 2028},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.dow, func(t *testing.T) {
			expr := cron.MustParse("0 0 1 1 " + tt.dow)
			want := time.Date(tt.wantYear, 1, 1, 0, 0, 0, 0, time.UTC)
			var got time.Time
			if tt.wantYear < year {
				got = expr.Prev(from)
			} else {
				got = expr.Next(from)
			}
			if got != want {
				t.Errorf("wrong year\ngot:  %v\nwant: %v", got, want)
			}
		})
	}
}

func BenchmarkNext(b *testing.B) {
	expr := cron.MustParse("0 0 1 1 *")
	from := time.Date(2011, 1, 1, 0, 0, 0, 0, time.UTC)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expr.Next(from)
	}
}

func BenchmarkPrev(b *testing.B) {
	expr := cron.MustParse("0 0 1 1 *")
	from := time.Date(2011, 1, 1, 0, 0, 0, 0, time.UTC)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expr.Prev(from)
	}
}

// refCron is a "gold standard" cron expression parser.
type refCron struct {
	expr   string
	fields [5][64]bool
}

func parseRefCron(expr string) (c refCron, ok bool) {
	expr = strings.ToLower(expr)
	if !validCron(expr) {
		return c, false
	}
	c.expr = expr
	fields := c.fields[:]
	for i, f := range strings.SplitN(expr, " ", 5) {
		if !parseCronField(fields[i][:], f, i) {
			return c, false
		}
	}
	return c, true
}

func (c *refCron) isZero() bool {
	return c.expr == ""
}

func (c *refCron) next(from time.Time) time.Time {
	t := from.Add(time.Minute)
	fm := c.fields[0]
	fh := c.fields[1]
	fdom := c.fields[2]
	fmon := c.fields[3]
	fdow := c.fields[4]

	var y int
	var mon time.Month
	var dom int
	var dow time.Weekday
	var h, m int
day:
	for {
		y, mon, dom = t.Date()
		dow = t.Weekday()
		switch {
		case !fmon[mon]:
			mon++
			dom = 1
		case !fdom[dom] || !fdow[dow]:
			dom++
		default:
			break day
		}
		t = time.Date(y, mon, dom, 0, 0, 0, 0, t.Location())
	}
	doy := t.YearDay()
hour:
	for {
		h, m, _ = t.Clock()
		switch {
		case !fh[h]:
			h++
			m = 0
		case !fm[m]:
			m++
		default:
			break hour
		}
		t = time.Date(y, mon, dom, h, m, 0, 0, t.Location())
		if t.YearDay() != doy {
			goto day
		}
	}
	return t
}

func (c *refCron) prev(from time.Time) time.Time {
	t := from.Add(-time.Minute)
	fm := c.fields[0]
	fh := c.fields[1]
	fdom := c.fields[2]
	fmon := c.fields[3]
	fdow := c.fields[4]

	var y int
	var mon time.Month
	var dom int
	var dow time.Weekday
	var h, m int
day:
	for {
		y, mon, dom = t.Date()
		dow = t.Weekday()
		switch {
		case !fmon[mon]:
			dom = 0
		case !fdom[dom] || !fdow[dow]:
			dom--
		default:
			break day
		}
		t = time.Date(y, mon, dom, 23, 59, 0, 0, t.Location())
	}
	doy := t.YearDay()
hour:
	for {
		h, m, _ = t.Clock()
		switch {
		case !fh[h]:
			m = -1
		case !fm[m]:
			m--
		default:
			break hour
		}
		t = time.Date(y, mon, dom, h, m, 0, 0, t.Location())
		if t.YearDay() != doy {
			// We hit a different day.
			goto day
		}
	}
	return t
}

func validCron(expr string) bool {
	if !cronRe.MatchString(expr) {
		return false
	}

	// Detect combinations of impossible month/day pairs.
	s := strings.SplitN(expr, " ", 5)
	var mon [32]bool
	if !parseCronField(mon[:], s[3], 3) {
		return false
	}
	if mon[1] || mon[3] || mon[5] || mon[7] || mon[8] || mon[10] || mon[12] {
		return true
	}
	var dom [32]bool
	if !parseCronField(dom[:], s[2], 2) {
		return false
	}
	maxDays := 29
	if mon[4] || mon[6] || mon[9] || mon[11] {
		maxDays = 30
	}
	for i := 1; i <= maxDays; i++ {
		if dom[i] {
			return true
		}
	}
	return false
}

func parseCronField(out []bool, field string, fieldIndex int) bool {
	for _, num := range strings.Split(field, ",") {
		from, to, step := 0, 0, 1

		switch {
		case num == "*":
			from, to, step = 0, len(out)-1, 1

		case strings.ContainsRune(num, '/'):
			s := strings.SplitN(num, "/", 2)
			if s0 := s[0]; strings.ContainsRune(s0, '-') {
				z := strings.SplitN(s0, "-", 2)
				from = aliasToNumber(z[0], fieldIndex)
				to = aliasToNumber(z[1], fieldIndex)
			} else {
				from = aliasToNumber(s0, fieldIndex)
				to = len(out) - 1
			}
			step, _ = strconv.Atoi(s[1])

		case strings.ContainsRune(num, '-'):
			s := strings.SplitN(num, "-", 2)
			from = aliasToNumber(s[0], fieldIndex)
			to = aliasToNumber(s[1], fieldIndex)
			step = 1

		default:
			from = aliasToNumber(num, fieldIndex)
			to, step = from, 1
		}

		if to < from || from < 0 || step <= 0 {
			return false
		}

		for i := from; i <= to; i += step {
			out[i] = true
		}
	}
	return true
}

func aliasToNumber(s string, fieldIndex int) int {
	switch fieldIndex {
	case 3: // month
		switch s {
		case "jan":
			return 1
		case "feb":
			return 2
		case "mar":
			return 3
		case "apr":
			return 4
		case "may":
			return 5
		case "jun":
			return 6
		case "jul":
			return 7
		case "aug":
			return 8
		case "sep":
			return 9
		case "oct":
			return 10
		case "nov":
			return 11
		case "dec":
			return 12
		}
	case 4: // weekday
		switch s {
		case "sun":
			return 0
		case "mon":
			return 1
		case "tue":
			return 2
		case "wed":
			return 3
		case "thu":
			return 4
		case "fri":
			return 5
		case "sat":
			return 6
		}
	}
	v, _ := strconv.Atoi(s)
	return v
}
