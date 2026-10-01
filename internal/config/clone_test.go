package config

import (
	"reflect"
	"testing"
)

// fillValue sets every field reachable from v to a non-zero value.
func fillValue(v reflect.Value, depth int) {
	switch v.Kind() {
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fillValue(v.Elem(), depth)
	case reflect.Struct:
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				fillValue(v.Field(i), depth+1)
			}
		}
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fillValue(v.Index(0), depth+1)
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
		key := reflect.New(v.Type().Key()).Elem()
		fillValue(key, depth+1)
		value := reflect.New(v.Type().Elem()).Elem()
		fillValue(value, depth+1)
		v.SetMapIndex(key, value)
	case reflect.String:
		v.SetString("x")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(3)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(3)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1.5)
	}
}

// Every config update saves and publishes a Clone. A setting that Clone drops
// (a field that is not written to JSON, or that reads back differently) would
// be reset by the next save of any other setting.
func TestCloneKeepsEverySetting(t *testing.T) {
	var original Config
	fillValue(reflect.ValueOf(&original).Elem(), 0)

	clone, err := original.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if clone.Auth == original.Auth {
		t.Fatal("the clone shares the Auth pointer")
	}
	root := reflect.ValueOf(original)
	copied := reflect.ValueOf(*clone)
	for i := range root.NumField() {
		name := root.Type().Field(i).Name
		if !reflect.DeepEqual(root.Field(i).Interface(), copied.Field(i).Interface()) {
			t.Errorf("Clone changed %s:\n was %+v\n now %+v", name, root.Field(i).Interface(), copied.Field(i).Interface())
		}
	}

	// No unexported field anywhere in the config: Clone could not carry one.
	var walk func(reflect.Type, string, map[reflect.Type]bool)
	walk = func(typ reflect.Type, path string, seen map[reflect.Type]bool) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || seen[typ] {
			return
		}
		seen[typ] = true
		for i := range typ.NumField() {
			field := typ.Field(i)
			if !field.IsExported() {
				t.Errorf("%s.%s is unexported: Clone drops it", path, field.Name)
				continue
			}
			walk(field.Type, path+"."+field.Name, seen)
		}
	}
	walk(reflect.TypeOf(original), "Config", map[reflect.Type]bool{})
}
