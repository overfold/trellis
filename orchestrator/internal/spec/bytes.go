package spec

import (
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strings"
)

// ByteSize is a byte count that accepts human-readable binary or decimal units
// when decoded from a YAML manifest. Its JSON representation remains a number
// so the HTTP API stays machine-oriented.
type ByteSize int64

var byteSizePattern = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]*)$`)

// ParseByteSize parses values such as 64MiB, 512Mi, 1GB, or a bare byte count.
func ParseByteSize(value string) (ByteSize, error) {
	value = strings.TrimSpace(value)
	matches := byteSizePattern.FindStringSubmatch(value)
	if matches == nil {
		return 0, fmt.Errorf("invalid byte size %q", value)
	}
	amount, ok := new(big.Rat).SetString(matches[1])
	if !ok {
		return 0, fmt.Errorf("invalid byte size %q", value)
	}
	units := map[string]int64{
		"": 1, "b": 1,
		"kb": 1_000, "mb": 1_000_000, "gb": 1_000_000_000, "tb": 1_000_000_000_000,
		"ki": 1 << 10, "kib": 1 << 10,
		"mi": 1 << 20, "mib": 1 << 20,
		"gi": 1 << 30, "gib": 1 << 30,
		"ti": 1 << 40, "tib": 1 << 40,
	}
	multiplier, ok := units[strings.ToLower(matches[2])]
	if !ok {
		return 0, fmt.Errorf("invalid byte-size unit %q", matches[2])
	}
	amount.Mul(amount, new(big.Rat).SetInt64(multiplier))
	if amount.Cmp(new(big.Rat).SetInt64(math.MaxInt64)) > 0 {
		return 0, fmt.Errorf("byte size %q is too large", value)
	}
	// Preserve nearest-byte rounding for human quantities without float64
	// rounding overflowing MaxInt64 or losing exact bare byte counts.
	amount.Add(amount, big.NewRat(1, 2))
	bytes := new(big.Int).Quo(amount.Num(), amount.Denom())
	return ByteSize(bytes.Int64()), nil
}

func (b ByteSize) String() string {
	value := int64(b)
	if value == 0 {
		return "0B"
	}
	units := []struct {
		size int64
		name string
	}{
		{1 << 40, "TiB"},
		{1 << 30, "GiB"},
		{1 << 20, "MiB"},
		{1 << 10, "KiB"},
	}
	for _, unit := range units {
		if value >= unit.size && value%unit.size == 0 {
			return fmt.Sprintf("%d%s", value/unit.size, unit.name)
		}
	}
	return fmt.Sprintf("%dB", value)
}
