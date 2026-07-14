package dedup

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestWholeFieldChunker_Split(t *testing.T) {
	c := WholeFieldChunker{}

	got := c.Split("привет", nil)
	if len(got) != 1 {
		t.Fatalf("ожидали 1 фрагмент, получили %d", len(got))
	}

	want := sha256.Sum256([]byte("привет"))
	if !bytes.Equal(got[0].Hash, want[:]) {
		t.Errorf("hash не равен sha256 поля")
	}
	if string(got[0].Data) != "привет" {
		t.Errorf("Data искажена: %q", got[0].Data)
	}
}

func TestWholeFieldChunker_Deterministic(t *testing.T) {
	c := WholeFieldChunker{}
	// Один и тот же вход → байт-в-байт одинаковый хэш (основа дедупа).
	a := c.Split("одинаковая системка", nil)
	b := c.Split("одинаковая системка", nil)
	if !bytes.Equal(a[0].Hash, b[0].Hash) {
		t.Errorf("одинаковый вход дал разные хэши")
	}
	// Разный вход → разный хэш.
	d := c.Split("другой текст", nil)
	if bytes.Equal(a[0].Hash, d[0].Hash) {
		t.Errorf("разный вход дал одинаковый хэш")
	}
}

func TestNew(t *testing.T) {
	if _, err := New(KindWholeField); err != nil {
		t.Errorf("whole-field должен создаваться: %v", err)
	}
	if _, err := New(KindFastCDC); err != nil {
		t.Errorf("fastcdc должен создаваться: %v", err)
	}
	if _, err := New("bogus"); err == nil {
		t.Errorf("неизвестный чанкер должен возвращать ошибку")
	}
}
