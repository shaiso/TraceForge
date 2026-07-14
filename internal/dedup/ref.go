package dedup

import (
	"encoding/hex"
	"strings"
)

// RefPrefix помечает строковое значение, вынесенное в CAS. В тонком спане тяжёлый
// контент заменяется на "ref:sha256:<hex>[,<hex>...]": один хэш для whole-field,
// несколько (по порядку) для FastCDC, где поле разбито на фрагменты. Restore
// склеивает байты фрагментов из CAS в исходную строку.
const RefPrefix = "ref:sha256:"

// MakeRef строит ссылку на список фрагментов (в порядке следования в поле).
func MakeRef(chunks []Chunk) string {
	var b strings.Builder
	b.WriteString(RefPrefix)
	for i, c := range chunks {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(hex.EncodeToString(c.Hash))
	}
	return b.String()
}

// IsRef сообщает, что строка — это ссылка на CAS, а не исходный контент.
func IsRef(s string) bool {
	return strings.HasPrefix(s, RefPrefix)
}

// ParseRef разбирает ссылку в упорядоченный список хэшей. ok=false — строка не
// ссылка. Для restore: склеить CAS[h] для каждого h в исходное значение поля.
func ParseRef(s string) (hashes [][]byte, ok bool) {
	if !IsRef(s) {
		return nil, false
	}
	body := strings.TrimPrefix(s, RefPrefix)
	if body == "" {
		return nil, false
	}
	for _, part := range strings.Split(body, ",") {
		h, err := hex.DecodeString(part)
		if err != nil || len(h) != 32 {
			return nil, false
		}
		hashes = append(hashes, h)
	}
	return hashes, true
}
