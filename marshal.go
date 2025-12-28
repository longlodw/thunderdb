package thunderdb

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"github.com/vmihailenco/msgpack/v5"
	"rsc.io/ordered"
)

type Marshaler interface {
	Marshal(v any) (data []byte, err error)
}

type Unmarshaler interface {
	Unmarshal(data []byte, v any) error
}

type MarshalUnmarshaler interface {
	Marshaler
	Unmarshaler
}

var (
	JsonMaUn    = jsonMarshalUnmarshaler{}
	GobMaUn     = gobMarshalUnmarshaler{}
	MsgpackMaUn = msgpackMarshalUnmarshaler{}
	orderedMa   = orderedMarshaler{}
)

type jsonMarshalUnmarshaler struct{}

func (j *jsonMarshalUnmarshaler) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (j *jsonMarshalUnmarshaler) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

type gobMarshalUnmarshaler struct{}

func (g *gobMarshalUnmarshaler) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (g *gobMarshalUnmarshaler) Unmarshal(data []byte, v any) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(v)
}

type msgpackMarshalUnmarshaler struct{}

func (m *msgpackMarshalUnmarshaler) Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (m *msgpackMarshalUnmarshaler) Unmarshal(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}

type orderedMarshaler struct{}

func (o *orderedMarshaler) Marshal(v []any) ([]byte, error) {
	if !ordered.CanEncode(v...) {
		return nil, ErrCannotMarshal(v)
	}
	return ordered.Encode(v...), nil
}

func (o *orderedMarshaler) Unmarshal(data []byte, v *[]any) error {
	decoded, err := ordered.DecodeAny(data)
	if err != nil {
		return err
	}
	*v = decoded
	return nil
}
