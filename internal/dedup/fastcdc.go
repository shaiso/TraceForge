package dedup

import (
	"crypto/sha256"
	"encoding/binary"
	"math/bits"
)

// FastCDC — content-defined chunking: границы фрагментов определяются содержимым
// (rolling gear-hash), а не фиксированным смещением. Это ловит частичные совпадения
// и общие префиксы: вставка/сдвиг в начале поля не «съезжает» все границы, поэтому
// хвостовые фрагменты остаются теми же и дедуплицируются (в отличие от whole-field,
// где одна изменённая буква рушит дедуп всего поля).
//
// Реализация — нормализованный чанкинг (FastCDC, Xia et al. 2016): до среднего
// размера действует «строгая» маска (реже режет — тянет мелкие куски к среднему),
// после — «слабая» (чаще режет — не даёт куску разрастись). Границы min/avg/max
// зажимают дисперсию размеров.

// FastCDCConfig задаёт границы размеров фрагмента (байты).
type FastCDCConfig struct {
	MinSize int // ниже — не режем (первые MinSize байт куска не хэшируются)
	AvgSize int // целевой средний размер; задаёт число бит масок (степень двойки)
	MaxSize int // выше — режем принудительно
}

// DefaultFastCDCConfig — согласованный диапазон ~2–8 КБ.
func DefaultFastCDCConfig() FastCDCConfig {
	return FastCDCConfig{MinSize: 2 * 1024, AvgSize: 4 * 1024, MaxSize: 8 * 1024}
}

// FastCDCChunker реализует Chunker через content-defined chunking.
type FastCDCChunker struct {
	cfg   FastCDCConfig
	maskS uint64 // строгая маска (больше единичных бит -> реже совпадение fp&mask==0)
	maskL uint64 // слабая маска (меньше бит -> чаще режет)
}

// gearTable — детерминированная таблица gear-хэша: байт -> случайное 64-битное
// число. Строится из sha256 с фиксированным сидом, поэтому одинакова во всех
// процессах и версиях сборки (критично: разная таблица = несовместимый CAS).
var gearTable = buildGearTable()

func buildGearTable() [256]uint64 {
	var g [256]uint64
	for i := 0; i < 256; i++ {
		h := sha256.Sum256([]byte{byte(i), 0x9e, 0x37, 0x79, 0xb9}) // фикс. сид
		g[i] = binary.BigEndian.Uint64(h[:8])
	}
	return g
}

// NewFastCDC создаёт чанкер. Число бит масок = log2(AvgSize); строгая маска на 2
// бита длиннее, слабая — на 2 короче (нормализация уровня 2).
func NewFastCDC(cfg FastCDCConfig) *FastCDCChunker {
	b := bits.Len(uint(cfg.AvgSize)) - 1 // log2(AvgSize)
	return &FastCDCChunker{
		cfg:   cfg,
		maskS: topMask(b + 2),
		maskL: topMask(b - 2),
	}
}

// topMask возвращает k старших единичных бит (проверяем именно старшие: в gear-hash
// они лучше перемешаны, чем младшие).
func topMask(k int) uint64 {
	switch {
	case k <= 0:
		return 0
	case k >= 64:
		return ^uint64(0)
	default:
		return ^uint64(0) << (64 - k)
	}
}

// Split режет значение на последовательные фрагменты. Инвариант обратимости:
// конкатенация Data всех фрагментов по порядку == исходное значение (restore
// склеивает их из CAS в том же порядке через ref:sha256:h1,h2,...).
func (c *FastCDCChunker) Split(value string, _ map[string]any) []Chunk {
	data := []byte(value)
	var chunks []Chunk
	for len(data) > 0 {
		n := c.cutpoint(data)
		seg := data[:n]
		sum := sha256.Sum256(seg)
		chunks = append(chunks, Chunk{
			Hash: append([]byte(nil), sum[:]...),
			Data: append([]byte(nil), seg...),
		})
		data = data[n:]
	}
	return chunks
}

// cutpoint ищет смещение границы в data: сканирует gear-хэшем от MinSize, сперва
// строгой маской (до AvgSize), затем слабой (до MaxSize); если не нашёл — режет по
// границе окна (MaxSize или конец data).
func (c *FastCDCChunker) cutpoint(data []byte) int {
	n := len(data)
	if n <= c.cfg.MinSize {
		return n
	}
	if n > c.cfg.MaxSize {
		n = c.cfg.MaxSize
	}
	normal := c.cfg.AvgSize
	if normal > n {
		normal = n
	}

	var fp uint64
	i := c.cfg.MinSize
	for ; i < normal; i++ {
		fp = (fp << 1) + gearTable[data[i]]
		if fp&c.maskS == 0 {
			return i
		}
	}
	for ; i < n; i++ {
		fp = (fp << 1) + gearTable[data[i]]
		if fp&c.maskL == 0 {
			return i
		}
	}
	return n
}
