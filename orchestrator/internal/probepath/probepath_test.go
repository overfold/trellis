package probepath

import (
	"regexp"
	"strings"
	"testing"
)

var pathCases = map[string]bool{
	"":                                     true,
	"/":                                    true,
	"/health":                              true,
	"/health/ready?verbose=1&probe=a+b":    true,
	"/v1/status%2Fready?q=%20":             true,
	"/@169.254.169.254/latest":             true,
	"//health":                             true,
	"/a:b;c=d,e!f$g'h(i)j*k~l":             true,
	"/" + strings.Repeat("a", MaxLength-1): true,

	"health":                             false,
	"@169.254.169.254/latest":            false,
	"http://169.254.169.254/latest":      false,
	"/health check":                      false,
	"/health\tcheck":                     false,
	"/health\r\nHost: evil":              false,
	"/health\x00":                        false,
	"/health\x7f":                        false,
	"/héalth":                            false,
	"/health#fragment":                   false,
	"/health%zz":                         false,
	"/health?x=%zz":                      false,
	"/health%":                           false,
	"/health%4":                          false,
	"/a|b":                               false,
	"/q{x}":                              false,
	"/a\\b":                              false,
	"/a\"b":                              false,
	"/a[0]":                              false,
	"/" + strings.Repeat("a", MaxLength): false,
}

func TestValidate(t *testing.T) {
	for path, valid := range pathCases {
		if err := Validate(path); (err == nil) != valid {
			t.Errorf("Validate(%q) = %v, want valid=%v", path, err, valid)
		}
	}
}

func TestPatternMatchesValidate(t *testing.T) {
	pattern := regexp.MustCompile(Pattern)
	for path, valid := range pathCases {
		if len(path) > MaxLength {
			continue // Length is enforced by the schema's maxLength.
		}
		if got := pattern.MatchString(path); got != valid {
			t.Errorf("Pattern matches %q = %v, want %v", path, got, valid)
		}
	}
}
