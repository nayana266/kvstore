package partition

import (
	"context"
	"testing"

	"google.golang.org/grpc"
)

func TestBlockListSetGet(t *testing.T) {
	b := NewBlockList("n1")
	if b.Blocked("n2") {
		t.Fatal("empty list should not block")
	}
	b.Set([]string{"n2", "n1", ""}) // self ignored
	if !b.Blocked("n2") || b.Blocked("n1") || b.Blocked("n3") {
		t.Fatalf("blocked=%v", b.Get())
	}
	b.Set(nil)
	if b.Blocked("n2") {
		t.Fatal("cleared list should not block")
	}
}

func TestAdminMethodsBypassInterceptor(t *testing.T) {
	b := NewBlockList("n1")
	b.Set([]string{"n2"})
	interceptor := b.UnaryServerInterceptor()
	called := false
	_, err := interceptor(
		context.Background(),
		nil,
		&grpc.UnaryServerInfo{FullMethod: "/kvstore.admin.Admin/SetBlockList"},
		func(ctx context.Context, req any) (any, error) {
			called = true
			return nil, nil
		},
	)
	if err != nil || !called {
		t.Fatalf("admin should bypass: err=%v called=%v", err, called)
	}
}
