package dedup

import (
	"bytes"
	"testing"
)

func TestTemplateAware_Registered(t *testing.T) {
	c, err := New(KindTemplateAware)
	if err != nil {
		t.Fatalf("template-aware должен создаваться: %v", err)
	}
	if _, ok := c.(*TemplateAwareChunker); !ok {
		t.Fatalf("New(template-aware) вернул %T", c)
	}
}

func TestHasTemplateID(t *testing.T) {
	if HasTemplateID(nil) {
		t.Errorf("nil attrs -> ожидали false")
	}
	if HasTemplateID(map[string]any{"gen_ai.request.model": "gpt-4o-mini"}) {
		t.Errorf("без prompt.name -> ожидали false")
	}
	if !HasTemplateID(map[string]any{AttrPromptName: "antispam.system"}) {
		t.Errorf("с prompt.name -> ожидали true")
	}
}

// Пока делегирует в fallback: результат Split бит-в-бит совпадает с FastCDC, и
// конкатенация фрагментов равна исходному значению (обратимость сохранена).
func TestTemplateAware_DelegatesToFallback(t *testing.T) {
	fast := NewFastCDC(DefaultFastCDCConfig())
	tmpl := NewTemplateAware(fast)
	val := string(pseudoRandom(30*1024, 9))
	attrs := map[string]any{AttrPromptName: "antispam.system", AttrPromptVersion: "v3"}

	got := tmpl.Split(val, attrs)
	want := fast.Split(val, attrs)
	if len(got) != len(want) {
		t.Fatalf("фрагментов %d, у FastCDC %d", len(got), len(want))
	}
	var reasm []byte
	for i := range got {
		if !bytes.Equal(got[i].Hash, want[i].Hash) {
			t.Fatalf("фрагмент[%d] != FastCDC", i)
		}
		reasm = append(reasm, got[i].Data...)
	}
	if string(reasm) != val {
		t.Fatalf("реассембл != оригинал")
	}
}
