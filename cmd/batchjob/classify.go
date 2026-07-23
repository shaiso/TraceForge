package main

import (
	"strings"

	"traceforge/internal/tf"
)

// Причины завершения диалога, которые перечислялись на созвоне.
const (
	reasonConnLost   = "connection_lost"  // связь оборвалась (error-статус спана)
	reasonSpammerBye = "spammer_hung_up"  // спамер отработал скрипт/автоинформатор и бросил
	reasonExhausted  = "dialog_exhausted" // диалог исчерпан (дожил до предела удержания)
	reasonAgentDumb  = "agent_confused"   // агент сказал глупость / вышел из роли
	reasonUnknown    = "unknown"
)

// Classifier определяет причину завершения по диалогу и сигналам последнего хода.
// СЕЙЧАС — эвристики; ТОЧКА ПОДМЕНЫ НА LLM: реализовать этот же интерфейс
// llmClassifier'ом (промпт «почему диалог закончился?» + вызов модели), контур
// батча (LoadRange -> BuildDialog -> Classify -> WriteResults) при этом не меняется.
type Classifier interface {
	Classify(dialog []tf.Message, sig Signals) string
}

// Signals — дешёвые сигналы последнего хода (со спанов, без разворота контента).
type Signals struct {
	Turns        int    // число ходов (реплик пользователя)
	LastStatus   string // UNSET | OK | ERROR
	LastFinish   string // gen_ai.response.finish_reasons[0]
	LastUserText string // последняя реплика собеседника
}

// heuristicClassifier — простые правила по последним репликам/таймингам. на его место встаёт LLM без изменения остального.
type heuristicClassifier struct{}

// прощально-обрывающие маркеры в реплике спамера (он «закруглился»).
var byeMarkers = []string{
	"до свидания", "всего доброго", "перезвон", "не интересно", "спасибо, не надо",
	"кладу трубку", "отключаюсь", "хорошего дня",
}

func (heuristicClassifier) Classify(dialog []tf.Message, sig Signals) string {
	if len(dialog) == 0 {
		return reasonUnknown
	}
	// Связь: последний ход завершился ошибкой.
	if sig.LastStatus == "ERROR" {
		return reasonConnLost
	}
	// Агент вышел из роли: модель оборвала генерацию не по 'stop'.
	if sig.LastFinish == "content_filter" || sig.LastFinish == "length" {
		return reasonAgentDumb
	}
	// Спамер закруглился — по маркерам в его последней реплике.
	low := strings.ToLower(sig.LastUserText)
	for _, m := range byeMarkers {
		if strings.Contains(low, m) {
			return reasonSpammerBye
		}
	}
	// Короткий диалог — спамер бросил рано; длинный — удержание сработало, диалог исчерпан.
	if sig.Turns <= 3 {
		return reasonSpammerBye
	}
	return reasonExhausted
}
