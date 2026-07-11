package interned

import (
	"testing"

	"swdunlop.dev/pkg/datalog"
)

func mustComposite(t *testing.T, v any) *datalog.Composite {
	t.Helper()
	c, err := datalog.NewComposite(v)
	if err != nil {
		t.Fatalf("NewComposite: %v", err)
	}
	return c
}

func TestInternCompositeHashCons(t *testing.T) {
	d := NewDict()
	a := mustComposite(t, map[string]any{"pid": 1.0, "name": "sh"})
	b := mustComposite(t, map[string]any{"name": "sh", "pid": 1})
	idA := d.Intern(a)
	idB := d.Intern(b)
	if idA != idB {
		t.Errorf("structurally equal composites got different IDs: %d vs %d", idA, idB)
	}
	// Resolution is pointer-identical within one dict: the first instance wins.
	if got := d.ResolveConstant(idA); got != datalog.Constant(a) {
		t.Errorf("ResolveConstant returned %v, want the first interned instance", got)
	}
}

func TestInternCompositeDistinctFromString(t *testing.T) {
	d := NewDict()
	c := mustComposite(t, map[string]any{"a": int64(1)})
	idC := d.InternConstant(c)
	idS := d.Intern(c.Canonical()) // a plain string with the same text
	if idC == idS {
		t.Error("composite collided with string of its canonical form")
	}
	if _, ok := d.ResolveConstant(idC).(*datalog.Composite); !ok {
		t.Errorf("expected *Composite, got %T", d.ResolveConstant(idC))
	}
	if _, ok := d.ResolveConstant(idS).(datalog.String); !ok {
		t.Errorf("expected String, got %T", d.ResolveConstant(idS))
	}
}

func TestInternBoolNull(t *testing.T) {
	d := NewDict()
	idTrue := d.InternConstant(datalog.Bool(true))
	idFalse := d.InternConstant(datalog.Bool(false))
	idNull := d.InternConstant(datalog.Null{})
	idStrTrue := d.Intern("true")
	idStrNull := d.Intern("null")
	ids := map[uint64]bool{idTrue: true, idFalse: true, idNull: true, idStrTrue: true, idStrNull: true}
	if len(ids) != 5 {
		t.Errorf("bool/null constants collided: true=%d false=%d null=%d \"true\"=%d \"null\"=%d",
			idTrue, idFalse, idNull, idStrTrue, idStrNull)
	}
	if got := d.ResolveConstant(idTrue); got != datalog.Bool(true) {
		t.Errorf("resolve true: got %v", got)
	}
	if got := d.ResolveConstant(idNull); got != (datalog.Null{}) {
		t.Errorf("resolve null: got %v", got)
	}
}

func TestFreezeWithComposites(t *testing.T) {
	d := NewDict()
	c1 := mustComposite(t, []any{int64(2)})
	c2 := mustComposite(t, []any{int64(1)})
	id1 := d.Intern(c1)
	id2 := d.Intern(c2)
	d.Intern("zzz")
	d.Intern(int64(7))

	remap := d.Freeze()

	// Composites sort by canonical string among themselves.
	new1, new2 := remap[id1], remap[id2]
	if !(new2 < new1) {
		t.Errorf("composite ordering after freeze: [1]=%d should sort before [2]=%d", new2, new1)
	}
	// Index still finds them after freeze.
	if id, ok := d.Has(c1); !ok || id != new1 {
		t.Errorf("Has(c1) after freeze: got %d %v, want %d", id, ok, new1)
	}
	if got := d.ResolveConstant(new2); got != datalog.Constant(c2) {
		t.Errorf("resolve after freeze: got %v", got)
	}
}

func TestHasComposite(t *testing.T) {
	d := NewDict()
	c := mustComposite(t, map[string]any{"k": "v"})
	if _, ok := d.Has(c); ok {
		t.Error("Has reported an uninterned composite")
	}
	id := d.Intern(c)
	// A different instance with equal structure is found.
	other := mustComposite(t, map[string]any{"k": "v"})
	got, ok := d.Has(other)
	if !ok || got != id {
		t.Errorf("Has(equal composite): got %d %v, want %d", got, ok, id)
	}
}

func TestInternFactWithComposite(t *testing.T) {
	d := NewDict()
	c := mustComposite(t, map[string]any{"k": "v"})
	fact := datalog.Fact{Name: "event", Terms: []datalog.Constant{datalog.ID(1), c}}
	ifact, err := d.InternFact(fact)
	if err != nil {
		t.Fatalf("InternFact: %v", err)
	}
	back := d.DeInternFact(ifact)
	if back.Name != "event" || len(back.Terms) != 2 {
		t.Fatalf("round trip: %v", back)
	}
	if back.Terms[1] != datalog.Constant(c) {
		t.Errorf("composite did not round trip pointer-identically: %v", back.Terms[1])
	}
}
