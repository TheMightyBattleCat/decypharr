package config

import (
	"bytes"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"sort"
)

// MergeJSON returns base's JSON object with patch's object laid over it: a
// key present in both whose values are both objects is merged the same way,
// key by key; any other value in patch (a string, number, bool, array or
// null) replaces base's. Keys patch leaves out keep base's value.
//
// Settings saves decode the request through this onto the live config, so a
// form that has no field for a setting cannot reset it. On a production install every
// general Settings save reset ffprobe_path, ffprobe_timeout and
// cleanup_superseded (and re-defaulted decode_verify_ttl,
// import_availability_check and par2_urgent_concurrency), because the handler
// rebuilt the whole Repair block from the form.
func MergeJSON(base, patch []byte) ([]byte, error) {
	baseObj, err := decodeObject(base, true)
	if err != nil {
		return nil, fmt.Errorf("base: %w", err)
	}
	patchObj, err := decodeObject(patch, false)
	if err != nil {
		return nil, err
	}
	return stdjson.Marshal(mergeObjects(baseObj, patchObj))
}

func decodeObject(data []byte, emptyOK bool) (map[string]any, error) {
	if emptyOK && len(bytes.TrimSpace(data)) == 0 {
		return map[string]any{}, nil
	}
	dec := stdjson.NewDecoder(bytes.NewReader(data))
	dec.UseNumber() // keep int64 sizes exact
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("expected a JSON object")
	}
	return obj, nil
}

func mergeObjects(base, patch map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(patch))
	for k, v := range base {
		out[k] = v
	}
	for k, pv := range patch {
		if pm, ok := pv.(map[string]any); ok {
			if bm, ok := out[k].(map[string]any); ok {
				out[k] = mergeObjects(bm, pm)
				continue
			}
		}
		out[k] = pv
	}
	return out
}

// ChangedJSONKeys lists, sorted, the dotted key paths whose values differ
// between two JSON objects. Objects are walked key by key; an array or scalar
// is compared whole and reported at its own path. Only key names are returned,
// never values, so the result is safe to log for a config holding secrets.
func ChangedJSONKeys(before, after []byte) ([]string, error) {
	b, err := decodeObject(before, true)
	if err != nil {
		return nil, fmt.Errorf("before: %w", err)
	}
	a, err := decodeObject(after, true)
	if err != nil {
		return nil, fmt.Errorf("after: %w", err)
	}
	fb, fa := map[string]string{}, map[string]string{}
	flattenJSON("", b, fb)
	flattenJSON("", a, fa)
	var changed []string
	for k, v := range fb {
		if av, ok := fa[k]; !ok || av != v {
			changed = append(changed, k)
		}
	}
	for k := range fa {
		if _, ok := fb[k]; !ok {
			changed = append(changed, k)
		}
	}
	sort.Strings(changed)
	return changed, nil
}

func flattenJSON(prefix string, v any, out map[string]string) {
	if obj, ok := v.(map[string]any); ok {
		for k, child := range obj {
			p := k
			if prefix != "" {
				p = prefix + "." + k
			}
			flattenJSON(p, child, out)
		}
		return
	}
	enc, _ := stdjson.Marshal(v)
	out[prefix] = string(enc)
}
