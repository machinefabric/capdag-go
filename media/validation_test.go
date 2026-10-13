package media

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST12835: the one decision of whether a value breaks a media definition's
// validation. A rule applies to the values it is about and passes the rest:
// min and max to numbers, the length rules to strings and arrays, pattern and
// allowed values to strings. The first rule broken is named; a pattern that is
// no regular expression is a defect of the definition, not of the value.
func Test12835_a_value_is_held_to_the_rules_that_are_about_it(t *testing.T) {
	number := func(value float64) *float64 { return &value }
	count := func(value int) *int { return &value }
	text := func(value string) *string { return &value }
	broken := func(validation MediaValidation, value any) string {
		rule, err := validation.Violation(value)
		require.NoError(t, err)
		return rule
	}

	rangeOf := MediaValidation{Min: number(1), Max: number(365)}
	assert.Equal(t, "", broken(rangeOf, int64(1)))
	assert.Equal(t, "", broken(rangeOf, float64(365)))
	assert.Equal(t, "minimum value 1", broken(rangeOf, int64(0)))
	assert.Equal(t, "maximum value 365", broken(rangeOf, 365.5))
	assert.Equal(t, "minimum value 1", broken(rangeOf, json.Number("0.5")), "a number however it was decoded")
	assert.Equal(t, "maximum value 365", broken(rangeOf, uint64(366)))
	// A range says nothing of what is not a number.
	assert.Equal(t, "", broken(rangeOf, "0"))
	assert.Equal(t, "", broken(rangeOf, []any{int64(0)}))
	assert.Equal(t, "", broken(rangeOf, nil))

	length := MediaValidation{MinLength: count(2), MaxLength: count(3)}
	assert.Equal(t, "", broken(length, "ab"))
	assert.Equal(t, "", broken(length, "abc"))
	assert.Equal(t, "minimum length 2", broken(length, "a"))
	assert.Equal(t, "maximum length 3", broken(length, "abcd"))
	// An array has a length too, and the rule says which it measured.
	assert.Equal(t, "", broken(length, []any{"a", "b"}))
	assert.Equal(t, "minimum array length 2", broken(length, []any{"a"}))
	assert.Equal(t, "maximum array length 3", broken(length, []any{1, 2, 3, 4}))
	// A number has none.
	assert.Equal(t, "", broken(length, int64(12345)))

	pattern := MediaValidation{Pattern: text(`^[a-z]+$`)}
	assert.Equal(t, "", broken(pattern, "abc"))
	assert.Equal(t, "pattern '^[a-z]+$'", broken(pattern, "abc1"))
	assert.Equal(t, "", broken(pattern, int64(1)), "a pattern is about strings")
	_, err := (&MediaValidation{Pattern: text(`(`)}).Violation("x")
	require.Error(t, err, "a pattern that is no regular expression")
	assert.Contains(t, err.Error(), "Invalid regex pattern '('")
	// The defect is the definition's only when the rule is applied.
	_, err = (&MediaValidation{Pattern: text(`(`)}).Violation(int64(1))
	assert.NoError(t, err)

	allowed := MediaValidation{AllowedValues: []string{"mean", "median"}}
	assert.Equal(t, "", broken(allowed, "median"))
	assert.Equal(t, `allowed values: ["mean", "median"]`, broken(allowed, "mode"))
	assert.Equal(t, "", broken(allowed, int64(3)), "allowed values are about strings")

	// The first rule broken is the one named: the range before the length,
	// the length before the pattern, the pattern before the allowed values.
	every := MediaValidation{MinLength: count(3), Pattern: text(`^x`), AllowedValues: []string{"xyz"}}
	assert.Equal(t, "minimum length 3", broken(every, "ab"))
	assert.Equal(t, "pattern '^x'", broken(every, "abc"))
	assert.Equal(t, `allowed values: ["xyz"]`, broken(every, "xab"))
	assert.Equal(t, "", broken(every, "xyz"))
	// No rule, nothing broken.
	assert.Equal(t, "", broken(MediaValidation{}, "anything"))
}
