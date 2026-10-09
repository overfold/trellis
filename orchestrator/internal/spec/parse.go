package spec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"

	"github.com/go-viper/mapstructure/v2"
	"gopkg.in/yaml.v3"
)

var byteSizeType = reflect.TypeFor[ByteSize]()

// ParseYAML decodes a YAML job specification.
func ParseYAML(raw []byte) (*JobSpec, error) {
	var document yaml.Node
	yamlDecoder := yaml.NewDecoder(bytes.NewReader(raw))
	err := yamlDecoder.Decode(&document)
	if err != nil {
		return nil, err
	}
	var extra any
	if err := yamlDecoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("job manifest must contain exactly one YAML document")
	}
	if err := rejectYAMLNulls(&document); err != nil {
		return nil, err
	}
	var data map[string]any
	if err := document.Decode(&data); err != nil {
		return nil, err
	}

	var job *JobSpec
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		ErrorUnused:      true,
		WeaklyTypedInput: false,
		MatchName:        func(mapKey, fieldName string) bool { return mapKey == fieldName },
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			stringToByteSizeHook,
			strictIntegerHook,
		),
		Result:  &job,
		TagName: "yaml",
	})
	if err != nil {
		return nil, err
	}
	return job, decoder.Decode(data)
}

func rejectYAMLNulls(node *yaml.Node) error {
	if node.Tag == "!!null" {
		return fmt.Errorf("YAML null at line %d, column %d is not allowed; omit optional fields instead", node.Line, node.Column)
	}
	// Anchored values are visited where declared. Do not follow aliases, which
	// can form cycles; their targets are already part of the document tree.
	for _, child := range node.Content {
		if err := rejectYAMLNulls(child); err != nil {
			return err
		}
	}
	return nil
}

func stringToByteSizeHook(from reflect.Type, to reflect.Type, data any) (any, error) {
	if from.Kind() != reflect.String || to != byteSizeType {
		return data, nil
	}
	return ParseByteSize(data.(string))
}

func strictIntegerHook(_ reflect.Type, to reflect.Type, data any) (any, error) {
	if to.Kind() < reflect.Int || to.Kind() > reflect.Uint64 {
		return data, nil
	}
	value := reflect.ValueOf(data)
	if value.Kind() < reflect.Int || value.Kind() > reflect.Uint64 {
		return nil, fmt.Errorf("must be an integer in range")
	}
	var decimal string
	if value.Kind() <= reflect.Int64 {
		decimal = strconv.FormatInt(value.Int(), 10)
	} else {
		decimal = strconv.FormatUint(value.Uint(), 10)
	}
	if to.Kind() <= reflect.Int64 {
		parsed, err := strconv.ParseInt(decimal, 10, to.Bits())
		if err != nil {
			return nil, fmt.Errorf("must be an integer in range")
		}
		return parsed, nil
	}
	parsed, err := strconv.ParseUint(decimal, 10, to.Bits())
	if err != nil {
		return nil, fmt.Errorf("must be an integer in range")
	}
	return parsed, nil
}
