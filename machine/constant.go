package machine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

// A constant of machine notation is a JSON value. In this package it is one of
//
//	nil, bool, string, int64, uint64, float64, []any, map[string]any
//
// A number written without a fraction or an exponent is a whole number (int64,
// or uint64 above int64's range); any other number is a float64. `1` and `1.0`
// are therefore two different constants, as they are in the reference, whose
// JSON value keeps the same three kinds of number apart.

// decodeConstant reads one JSON value from its text.
func decodeConstant(text string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var raw any
	if err := decoder.Decode(&raw); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("more than one JSON value in %q", text)
	}
	return normalizeConstant(raw)
}

// normalizeConstant turns a value decoded with UseNumber into this package's
// constant form.
func normalizeConstant(raw any) (any, error) {
	switch value := raw.(type) {
	case nil, bool, string, int64, uint64, float64:
		return value, nil
	case json.Number:
		text := value.String()
		if !strings.ContainsAny(text, ".eE") {
			if whole, err := strconv.ParseInt(text, 10, 64); err == nil {
				return whole, nil
			}
			if whole, err := strconv.ParseUint(text, 10, 64); err == nil {
				return whole, nil
			}
		}
		float, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsInf(float, 0) || math.IsNaN(float) {
			return nil, fmt.Errorf("%q is not a number a constant can hold", text)
		}
		return float, nil
	case int:
		return int64(value), nil
	case []any:
		items := make([]any, len(value))
		for i, item := range value {
			normalized, err := normalizeConstant(item)
			if err != nil {
				return nil, err
			}
			items[i] = normalized
		}
		return items, nil
	case map[string]any:
		fields := make(map[string]any, len(value))
		for key, field := range value {
			normalized, err := normalizeConstant(field)
			if err != nil {
				return nil, err
			}
			fields[key] = normalized
		}
		return fields, nil
	default:
		return nil, fmt.Errorf("%T is not a JSON value", raw)
	}
}

// constantsEqual compares two constants as JSON values: a whole number and a
// float are never equal, an object's keys carry no order.
func constantsEqual(a, b any) bool {
	switch left := a.(type) {
	case nil:
		return b == nil
	case bool:
		right, ok := b.(bool)
		return ok && left == right
	case string:
		right, ok := b.(string)
		return ok && left == right
	case int64:
		switch right := b.(type) {
		case int64:
			return left == right
		case uint64:
			return left >= 0 && uint64(left) == right
		}
		return false
	case uint64:
		switch right := b.(type) {
		case uint64:
			return left == right
		case int64:
			return right >= 0 && uint64(right) == left
		}
		return false
	case float64:
		right, ok := b.(float64)
		return ok && left == right
	case []any:
		right, ok := b.([]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for i := range left {
			if !constantsEqual(left[i], right[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		right, ok := b.(map[string]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for key, field := range left {
			other, present := right[key]
			if !present || !constantsEqual(field, other) {
				return false
			}
		}
		return true
	}
	return false
}

// constantText writes a constant as notation writes it: compact JSON, an
// object's keys in order, numbers and strings spelled as the reference spells
// them. The same value has one text in every implementation, so canonical
// notation is the same text everywhere.
func constantText(value any) string {
	var out bytes.Buffer
	writeConstant(&out, value)
	return out.String()
}

func writeConstant(out *bytes.Buffer, value any) {
	switch v := value.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		out.WriteString(strconv.FormatBool(v))
	case string:
		writeConstantString(out, v)
	case int64:
		out.WriteString(strconv.FormatInt(v, 10))
	case uint64:
		out.WriteString(strconv.FormatUint(v, 10))
	case float64:
		out.WriteString(floatText(v))
	case []any:
		out.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				out.WriteByte(',')
			}
			writeConstant(out, item)
		}
		out.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			writeConstantString(out, key)
			out.WriteByte(':')
			writeConstant(out, v[key])
		}
		out.WriteByte('}')
	default:
		panic(fmt.Sprintf("%T is not a constant of machine notation", value))
	}
}

// writeConstantString writes a JSON string with exactly the escapes the
// reference writes: the quote, the backslash and the control characters.
// Everything else — `<`, `&`, any character outside ASCII — is written as it is.
func writeConstantString(out *bytes.Buffer, text string) {
	const hex = "0123456789abcdef"
	out.WriteByte('"')
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case c == '"':
			out.WriteString(`\"`)
		case c == '\\':
			out.WriteString(`\\`)
		case c == '\b':
			out.WriteString(`\b`)
		case c == '\f':
			out.WriteString(`\f`)
		case c == '\n':
			out.WriteString(`\n`)
		case c == '\r':
			out.WriteString(`\r`)
		case c == '\t':
			out.WriteString(`\t`)
		case c < 0x20:
			out.WriteString(`\u00`)
			out.WriteByte(hex[c>>4])
			out.WriteByte(hex[c&0xf])
		default:
			out.WriteByte(c)
		}
	}
	out.WriteByte('"')
}

// floatText writes a float as the shortest decimal that reads back as the same
// float, laid out as the reference lays it out: plain digits with a decimal
// point while the number has at most sixteen digits before the point and
// fewer than five zeros after it, an exponent (with its sign, `e+16` and
// `e-7`) otherwise. A float that is a whole number keeps its `.0`, so it reads
// back as a float.
func floatText(value float64) string {
	if value == 0 {
		if math.Signbit(value) {
			return "-0.0"
		}
		return "0.0"
	}
	sign := ""
	if value < 0 {
		sign = "-"
		value = -value
	}
	// The shortest digits and their decimal exponent: d.ddd e±x.
	scientific := strconv.FormatFloat(value, 'e', -1, 64)
	mantissa, exponentText, _ := strings.Cut(scientific, "e")
	exponent, err := strconv.Atoi(exponentText)
	if err != nil {
		panic(fmt.Sprintf("strconv wrote an exponent that is no number: %q", scientific))
	}
	digits := strings.Replace(mantissa, ".", "", 1)
	count := len(digits)
	// The value is 0.DIGITS × 10^point.
	point := exponent + 1
	switch {
	case count <= point && point <= 16:
		return sign + digits + strings.Repeat("0", point-count) + ".0"
	case 0 < point && point <= 16:
		return sign + digits[:point] + "." + digits[point:]
	case -5 < point && point <= 0:
		return sign + "0." + strings.Repeat("0", -point) + digits
	}
	// An exponent carries its sign, `+` included.
	power := strconv.Itoa(point - 1)
	if point > 0 {
		power = "+" + power
	}
	if count == 1 {
		return sign + digits + "e" + power
	}
	return sign + digits[:1] + "." + digits[1:] + "e" + power
}
