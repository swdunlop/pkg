package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	flag "github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/seminaive"
)

var (
	configPath = flag.StringP("config", "c", "", "path to a JSON or YAML jsonfacts config file")
	dataDir    = flag.StringP("data-dir", "d", "", "directory containing JSONL data files (defaults to config file's directory)")
)

func main() {
	flag.Parse()

	r := newREPL(seminaive.New())

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
}

// loadConfig reads a jsonfacts.Config from a JSON or YAML file.
func loadConfig(path string) (jsonfacts.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return jsonfacts.Config{}, err
	}

	ext := filepath.Ext(path)
	if ext == ".yaml" || ext == ".yml" {
		var raw any
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return jsonfacts.Config{}, fmt.Errorf("parsing YAML: %w", err)
		}
		data, err = json.Marshal(raw)
		if err != nil {
			return jsonfacts.Config{}, fmt.Errorf("converting YAML to JSON: %w", err)
		}
	}

	var cfg jsonfacts.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return jsonfacts.Config{}, fmt.Errorf("parsing config: %w", err)
	}
	return cfg, nil
}
