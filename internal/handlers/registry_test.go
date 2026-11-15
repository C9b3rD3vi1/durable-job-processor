package handlers

import "testing"

func TestDefaultRegistry(t *testing.T) {
	registry := DefaultRegistry()
	for _, jobType := range []string{"report.generate", "email.send", "webhook.deliver", "data.import"} {
		if !registry.Allows(jobType) {
			t.Errorf("Allows(%q) = false, want true", jobType)
		}
	}
	if registry.Allows("unknown.job") {
		t.Error(`Allows("unknown.job") = true, want false`)
	}
}

func TestRegistryIgnoresEmptyTypes(t *testing.T) {
	registry := NewRegistry("report.generate", "", "   ")
	if !registry.Allows("report.generate") {
		t.Error("report.generate should be allowed")
	}
	if got := len(registry.Types()); got != 1 {
		t.Errorf("len(Types()) = %d, want 1", got)
	}
}

func TestRegistryTypesSorted(t *testing.T) {
	registry := NewRegistry("z.job", "a.job", "m.job")
	types := registry.Types()
	want := []string{"a.job", "m.job", "z.job"}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("Types() = %v, want %v", types, want)
		}
	}
}

func TestNilRegistry(t *testing.T) {
	var registry *Registry
	if registry.Allows("report.generate") {
		t.Error("nil registry should not allow jobs")
	}
	if registry.Types() != nil {
		t.Error("nil registry Types() should be nil")
	}
}
