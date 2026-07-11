package seminaive_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// benchRecord builds a jsonfacts-shaped telemetry record. Every 10th record
// is "suspicious" so the detection rules have work to do.
func benchRecord(i int) map[string]any {
	name := "sh"
	if i%10 == 0 {
		name = fmt.Sprintf("x%d.tmp.exe", i)
	}
	return map[string]any{
		"pid":     i,
		"ppid":    i % 7,
		"name":    name,
		"cmdline": fmt.Sprintf("/bin/%s -c task-%d", name, i),
		"host":    fmt.Sprintf("host-%d", i%50),
		"labels":  []any{"prod", fmt.Sprintf("zone-%d", i%4)},
	}
}

// setupDestructure loads records as whole composites; the rule destructures
// each record per fact via a pattern.
func setupDestructure(b *testing.B, n int) (datalog.Database, datalog.Transformer) {
	b.Helper()
	builder := memory.NewBuilder()
	for i := range n {
		c, err := datalog.NewComposite(benchRecord(i))
		if err != nil {
			b.Fatal(err)
		}
		builder.AddFact(datalog.Fact{Name: "event", Terms: []datalog.Constant{datalog.ID(i), c}})
	}
	rs, err := syntax.ParseAll(`
		suspicious(Id) :- event(Id, {name: Name, ppid: 3}), @ends_with(Name, ".tmp.exe").
	`)
	if err != nil {
		b.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		b.Fatal(err)
	}
	return builder.Build(), tr
}

// setupPreFlattened loads the same workload flattened at load time, the way
// a jsonfacts mapping would emit it.
func setupPreFlattened(b *testing.B, n int) (datalog.Database, datalog.Transformer) {
	b.Helper()
	builder := memory.NewBuilder()
	for i := range n {
		rec := benchRecord(i)
		builder.AddFact(datalog.Fact{Name: "process", Terms: []datalog.Constant{
			datalog.ID(i),
			datalog.Integer(rec["ppid"].(int)),
			datalog.String(rec["name"].(string)),
		}})
	}
	rs, err := syntax.ParseAll(`
		suspicious(Id) :- process(Id, 3, Name), @ends_with(Name, ".tmp.exe").
	`)
	if err != nil {
		b.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		b.Fatal(err)
	}
	return builder.Build(), tr
}

func runDetection(b *testing.B, input datalog.Database, tr datalog.Transformer) {
	b.Helper()
	for b.Loop() {
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for range output.Facts("suspicious", 1) {
			count++
		}
		if count == 0 {
			b.Fatal("expected suspicious facts")
		}
	}
}

// BenchmarkPatternDestructure quantifies the lazy-destructure side of the
// flatten-vs-lazy trade-off: rules pay a per-fact getter cost and patterns
// do not use column indexes.
func BenchmarkPatternDestructure10k(b *testing.B) {
	input, tr := setupDestructure(b, 10000)
	b.ResetTimer()
	runDetection(b, input, tr)
}

// BenchmarkPreFlattened is the same detection on load-time flattened facts,
// the recommended shape for hot predicates.
func BenchmarkPreFlattened10k(b *testing.B) {
	input, tr := setupPreFlattened(b, 10000)
	b.ResetTimer()
	runDetection(b, input, tr)
}

// BenchmarkCompositeIntern measures canonicalization plus interning cost per
// record at load time; b.SetBytes reports throughput per MB of JSON input.
func BenchmarkCompositeIntern(b *testing.B) {
	records := make([]map[string]any, 1000)
	var totalBytes int64
	for i := range records {
		records[i] = benchRecord(i)
		data, err := json.Marshal(records[i])
		if err != nil {
			b.Fatal(err)
		}
		totalBytes += int64(len(data))
	}
	b.SetBytes(totalBytes)
	b.ResetTimer()
	for b.Loop() {
		builder := memory.NewBuilder()
		for i, rec := range records {
			c, err := datalog.NewComposite(rec)
			if err != nil {
				b.Fatal(err)
			}
			if err := builder.AddFact(datalog.Fact{Name: "event", Terms: []datalog.Constant{datalog.ID(i), c}}); err != nil {
				b.Fatal(err)
			}
		}
		builder.Build()
	}
}
