package backup

import (
	"context"
	"errors"
	"testing"
)

func TestPreActionNoOpWithoutServiceOrHook(t *testing.T) {
	SetPreActionHook(nil)
	if id, err := PreAction(context.Background(), nil, ActionDeleteEntry); id != "" || err != nil {
		t.Fatalf("got %q, %v", id, err)
	}
	if id, err := PreAction(context.Background(), NotImplemented(), ActionDeleteEntry); id != "" || err != nil {
		t.Fatalf("stub service: got %q, %v", id, err)
	}
}

func TestPreActionUsesHookWhenNoRealService(t *testing.T) {
	t.Cleanup(func() { SetPreActionHook(nil) })
	want := errors.New("boom")
	SetPreActionHook(func(_ context.Context, action string) (string, error) { return action, want })
	id, err := PreAction(context.Background(), NotImplemented(), "x")
	if id != "x" || !errors.Is(err, want) {
		t.Fatalf("got %q, %v", id, err)
	}
}
