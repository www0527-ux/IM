package messagecache

import "testing"

func TestKeysUseIndependentNamespaces(t *testing.T) {
	content := ContentKey(12, 34)
	idempotency := IdempotencyKey(56, "client-1")

	if content != "im:message-cache:{12}:34" {
		t.Fatalf("unexpected content key: %q", content)
	}
	if idempotency != "im:message:idempotency:v3:56:client-1" {
		t.Fatalf("unexpected idempotency key: %q", idempotency)
	}
	if content == idempotency {
		t.Fatal("cache namespaces must not overlap")
	}
}
