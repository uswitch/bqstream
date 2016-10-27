package bigquery

import (
	"fmt"
	bq "google.golang.org/api/bigquery/v2"
)

func row(id RowIdentity, m map[string]bq.JsonValue) (*bq.TableDataInsertAllRequestRows, error) {
	identity, err := id.Identity(m)
	if err != nil {
		return nil, err
	}
	row := &bq.TableDataInsertAllRequestRows{
		InsertId: identity,
		Json:     m,
	}
	return row, nil
}

type RowIdentity interface {
	Identity(map[string]bq.JsonValue) (string, error)
}

type AttributeIdentity struct{ name string }

func NewAttributeIdentity(name string) *AttributeIdentity {
	return &AttributeIdentity{name}
}

func (a *AttributeIdentity) Identity(m map[string]bq.JsonValue) (string, error) {
	val, ok := m[a.name]
	if !ok {
		return "", fmt.Errorf("no value for insertId attribute %s in record.", a.name)
	}
	return val.(string), nil
}

type EmptyIdentity struct{}

func (e *EmptyIdentity) Identity(m map[string]bq.JsonValue) (string, error) {
	return "", nil
}

func NewEmptyIdentity() *EmptyIdentity {
	return &EmptyIdentity{}
}
