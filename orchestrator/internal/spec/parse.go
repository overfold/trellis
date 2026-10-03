package spec

import (
	"reflect"

	"github.com/go-viper/mapstructure/v2"
	"gopkg.in/yaml.v3"
)

var byteSizeType = reflect.TypeFor[ByteSize]()

// ParseYAML decodes a YAML job specification.
func ParseYAML(raw []byte) (*JobSpec, error) {
	var data map[string]any
	err := yaml.Unmarshal(raw, &data)
	if err != nil {
		return nil, err
	}

	var job *JobSpec
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		ErrorUnused:      true,
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			stringToByteSizeHook,
		),
		Result:  &job,
		TagName: "yaml",
	})
	if err != nil {
		return nil, err
	}
	return job, decoder.Decode(data)
}

func stringToByteSizeHook(from reflect.Type, to reflect.Type, data any) (any, error) {
	if from.Kind() != reflect.String || to != byteSizeType {
		return data, nil
	}
	return ParseByteSize(data.(string))
}
