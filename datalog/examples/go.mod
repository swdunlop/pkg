module swdunlop.dev/pkg/datalog/examples

go 1.26.4

require (
	gopkg.in/yaml.v3 v3.0.1
	swdunlop.dev/pkg/datalog v0.0.0
)

require github.com/expr-lang/expr v1.17.8 // indirect

// Until swdunlop.dev/pkg/datalog is published; then require its version and drop this.
replace swdunlop.dev/pkg/datalog => ..
