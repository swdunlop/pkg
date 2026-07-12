package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"

	flag "github.com/spf13/pflag"
)

var (
	configPath = flag.StringP("config", "c", "", "path to a JSON or YAML jsonfacts config file")
	dataDir    = flag.StringP("data-dir", "d", "", "directory containing JSONL data files (defaults to config file's directory)")
	cpuProfile = flag.String("cpuprofile", "", "write CPU profile to file")
	memProfile = flag.String("memprofile", "", "write memory profile to file")
)

func main() {
	// datalog mcp dispatches to the MCP server before pflag.Parse touches
	// any global state: mcp owns its own flag.FlagSet (see runMCP) so that
	// registering its flags never interferes with bare mode's pflag
	// registrations above, and vice versa. Checking os.Args directly (not
	// flag.Args() after Parse) is required for that separation to hold.
	if len(os.Args) > 1 && os.Args[1] == "mcp" {
		runMCP(os.Args[2:])
		return
	}

	// datalog serve dispatches to the web workbench the same way; see
	// runServe.
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		runServe(os.Args[2:])
		return
	}

	flag.Parse()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	r := newREPL()

	if *configPath != "" {
		dir := *dataDir
		if dir == "" {
			dir = filepath.Dir(*configPath)
		}
		r.setDataSource(*configPath, dir)
		if err := r.loadData(); err != nil {
			log.Fatal(err)
		}
	}

	for _, path := range flag.Args() {
		data, err := os.ReadFile(path)
		if err != nil {
			log.Fatalf("reading %s: %v", path, err)
		}
		if err := r.loadProgram(string(data)); err != nil {
			log.Fatalf("%s: %v", path, err)
		}
		fmt.Fprintf(os.Stderr, "Loaded %s\n", path)
	}

	if err := r.run(); err != nil {
		log.Fatal(err)
	}

	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal(err)
		}
	}
}
