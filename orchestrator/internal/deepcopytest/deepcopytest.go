// Package deepcopytest checks that Clone methods produce deep copies. Fill
// sets every reachable field of a value, exported or not, to a non-zero value
// so a clone that forgets a field no longer compares equal; AssertDisjoint
// reports any map, slice, or pointer a clone still shares with its original.
package deepcopytest

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"
)

var (
	timeType    = reflect.TypeFor[time.Time]()
	mutexType   = reflect.TypeFor[sync.Mutex]()
	rwMutexType = reflect.TypeFor[sync.RWMutex]()
)

// Fill sets every field reachable from ptr to a non-zero value. Maps and
// slices get one element; pointers get a filled target. Mutexes stay zero.
func Fill(t testing.TB, ptr any) {
	t.Helper()
	value := reflect.ValueOf(ptr)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		t.Fatalf("Fill needs a non-nil pointer, got %T", ptr)
	}
	counter := 0
	if err := fill(value.Elem(), &counter, 0); err != nil {
		t.Fatal(err)
	}
}

func settable(value reflect.Value) reflect.Value {
	if value.CanSet() {
		return value
	}
	return reflect.NewAt(value.Type(), unsafe.Pointer(value.UnsafeAddr())).Elem()
}

func fill(value reflect.Value, counter *int, depth int) error {
	if depth > 16 {
		return fmt.Errorf("deepcopytest: %s nests too deeply", value.Type())
	}
	value = settable(value)
	*counter++
	n := *counter
	switch value.Type() {
	case timeType:
		value.Set(reflect.ValueOf(time.Date(2026, 1, 2, 3, 4, 5, n, time.UTC)))
		return nil
	case mutexType, rwMutexType:
		return nil
	}
	switch value.Kind() {
	case reflect.Bool:
		value.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value.SetInt(int64(n % 100))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		value.SetUint(uint64(n % 100))
	case reflect.Float32, reflect.Float64:
		value.SetFloat(float64(n%100) / 128)
	case reflect.String:
		value.SetString(fmt.Sprintf("value-%d", n))
	case reflect.Pointer:
		target := reflect.New(value.Type().Elem())
		if err := fill(target.Elem(), counter, depth+1); err != nil {
			return err
		}
		value.Set(target)
	case reflect.Slice:
		slice := reflect.MakeSlice(value.Type(), 1, 1)
		if err := fill(slice.Index(0), counter, depth+1); err != nil {
			return err
		}
		value.Set(slice)
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := fill(value.Index(i), counter, depth+1); err != nil {
				return err
			}
		}
	case reflect.Map:
		key := reflect.New(value.Type().Key()).Elem()
		if err := fill(key, counter, depth+1); err != nil {
			return err
		}
		element := reflect.New(value.Type().Elem()).Elem()
		if err := fill(element, counter, depth+1); err != nil {
			return err
		}
		m := reflect.MakeMap(value.Type())
		m.SetMapIndex(key, element)
		value.Set(m)
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if err := fill(value.Field(i), counter, depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("deepcopytest: cannot fill %s", value.Type())
	}
	return nil
}

// AssertDisjoint fails when original and clone, two pointers to values of
// the same type, share any map, slice backing array, or pointer target.
func AssertDisjoint(t testing.TB, original, clone any) {
	t.Helper()
	a, b := reflect.ValueOf(original), reflect.ValueOf(clone)
	if a.Type() != b.Type() || a.Kind() != reflect.Pointer {
		t.Fatalf("AssertDisjoint needs two pointers of one type, got %T and %T", original, clone)
	}
	if a.Pointer() == b.Pointer() {
		t.Fatalf("clone is the original %T", original)
	}
	for _, shared := range disjoint(a.Elem(), b.Elem(), a.Elem().Type().String()) {
		t.Errorf("clone shares %s with the original", shared)
	}
}

func disjoint(a, b reflect.Value, path string) []string {
	switch a.Type() {
	case timeType, mutexType, rwMutexType:
		return nil
	}
	var shared []string
	switch a.Kind() {
	case reflect.Pointer:
		if a.IsNil() || b.IsNil() {
			return nil
		}
		if a.Pointer() == b.Pointer() {
			return []string{path}
		}
		return disjoint(a.Elem(), b.Elem(), "*"+path)
	case reflect.Map:
		if a.IsNil() || b.IsNil() {
			return nil
		}
		if a.Pointer() == b.Pointer() {
			return []string{path}
		}
		iterator := a.MapRange()
		for iterator.Next() {
			if other := b.MapIndex(iterator.Key()); other.IsValid() {
				shared = append(shared, disjoint(iterator.Value(), other, fmt.Sprintf("%s[%v]", path, iterator.Key()))...)
			}
		}
	case reflect.Slice:
		if a.Len() == 0 || b.Len() == 0 {
			return nil
		}
		if a.Pointer() == b.Pointer() {
			return []string{path}
		}
		for i := 0; i < min(a.Len(), b.Len()); i++ {
			shared = append(shared, disjoint(a.Index(i), b.Index(i), fmt.Sprintf("%s[%d]", path, i))...)
		}
	case reflect.Array:
		for i := 0; i < a.Len(); i++ {
			shared = append(shared, disjoint(a.Index(i), b.Index(i), fmt.Sprintf("%s[%d]", path, i))...)
		}
	case reflect.Struct:
		for i := 0; i < a.NumField(); i++ {
			shared = append(shared, disjoint(a.Field(i), b.Field(i), path+"."+a.Type().Field(i).Name)...)
		}
	case reflect.Interface:
		if !a.IsNil() && !b.IsNil() {
			shared = append(shared, disjoint(a.Elem(), b.Elem(), path)...)
		}
	}
	return shared
}
