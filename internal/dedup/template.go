package dedup

// Стандартные semconv-ключи идентичности промпт-шаблона. Инструментации их уже
// проставляют (см. gen_ai.prompt.name/version); дополнительно добавит
// template_id + vars на своей стороне (формат согласуется — см. запрос в docs).
const (
	AttrPromptName    = "gen_ai.prompt.name"
	AttrPromptVersion = "gen_ai.prompt.version"
)

// TemplateAwareChunker (v3, фаза 2) — заготовка шаблонного дедупа. Замысел: когда
// у спана есть идентичность шаблона (template_id/vars или name+version), хранить
// ШАБЛОН один раз на тысячи трейсов, а в спане держать лишь hash(шаблона) + мелкие
// переменные — это бьёт остаточные дубли, которые FastCDC не схлопывает (одинаковый
// каркас с разными подстановками режется на разные фрагменты по границам вставок).
//
// Формат template_id/vars ещё не зафиксирован, поэтому пока делегируем в
// FastCDC: он и так ловит общий текст шаблона как общие фрагменты, так что регресса
// нет, а интерфейс и проводка attrs уже готовы под настоящую реализацию.
type TemplateAwareChunker struct {
	fallback Chunker
}

// NewTemplateAware создаёт шаблонный чанкер с запасным (обычно FastCDC).
func NewTemplateAware(fallback Chunker) *TemplateAwareChunker {
	return &TemplateAwareChunker{fallback: fallback}
}

// HasTemplateID сообщает, проставила ли инструментация идентичность шаблона —
// сигнал, что в фазе 2 можно применить шаблонный путь вместо fallback.
func HasTemplateID(attrs map[string]any) bool {
	if attrs == nil {
		return false
	}
	_, ok := attrs[AttrPromptName]
	return ok
}

// Split пока полностью делегирует в fallback (см. TODO ниже). attrs уже содержит
// атрибуты спана, поэтому подключение шаблонного пути не потребует менять конвейер.
func (c *TemplateAwareChunker) Split(value string, attrs map[string]any) []Chunk {
	// TODO(phase2): при HasTemplateID(attrs) — отделить шаблон от vars по формату
	// научрука, хэшировать шаблон отдельно (hash(template)) и класть vars компактно.
	return c.fallback.Split(value, attrs)
}
