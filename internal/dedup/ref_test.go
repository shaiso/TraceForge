package dedup

import (
	"bytes"
	"strings"
	"testing"
)

func TestRef_RoundTrip(t *testing.T) {
	chunks := []Chunk{
		wholeFieldChunk("первый фрагмент"),
		wholeFieldChunk("второй фрагмент"),
	}
	ref := MakeRef(chunks)
	if !IsRef(ref) {
		t.Fatalf("IsRef(%q) = false", ref)
	}
	hashes, ok := ParseRef(ref)
	if !ok {
		t.Fatalf("ParseRef(%q) не распарсился", ref)
	}
	if len(hashes) != 2 {
		t.Fatalf("хэшей %d, ожидали 2", len(hashes))
	}
	for i, h := range hashes {
		if !bytes.Equal(h, chunks[i].Hash) {
			t.Errorf("хэш[%d] не совпал", i)
		}
	}
}

func TestRef_NotARef(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected bool
	}{
		{"обычный текст промта", "обычный текст промта", false},
		{"пустая строка", "", false},
		{"префикс без тела", RefPrefix, false},
		{"префикс с невалидным hex", RefPrefix + "zzzz", false},
		{"префикс с коротким хешем (не 32 байта)", RefPrefix + "ab", false},
		{"валидная ссылка (64 hex = 32 байта)", RefPrefix + strings.Repeat("ab", 32), true},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := ParseRef(tt.input)
			if ok != tt.expected {
				t.Errorf("ParseRef(%q) = %v, ожидали %v", tt.input, ok, tt.expected)
			}
		})
	}

	if IsRef("не ссылка") {
		t.Errorf("IsRef на обычной строке = true")
	}
}
