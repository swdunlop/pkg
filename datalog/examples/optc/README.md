# OpTC: Enterprise-Scale APT Detection

This example targets the [DARPA Operationally Transparent Cyber (OpTC)](https://github.com/FiveDirections/OpTC-data)
dataset: roughly a terabyte (compressed) of endpoint and network telemetry
collected from ~500 Windows 10 hosts over two weeks, with three multi-day
red-team APT scenarios embedded in benign background activity. It is the
natural step up from the [mordor example](../mordor/): instead of 506
attack-only events, you get billions of mostly-benign events with a few
labeled needles.

The data is **not** included in this repository and cannot be — see
"Getting the data" below. Nothing under `data/` is committed.

## The Dataset

OpTC telemetry is published as **eCAR** (extended Cyber Analytics
Repository) records: gzipped NDJSON, one JSON object per line, each with
`hostname`, `object` (PROCESS, FILE, FLOW, MODULE, REGISTRY, ...),
`action` (CREATE, OPEN, TERMINATE, ...), `actorID`/`objectID` (UUIDs that
chain events into provenance), `pid`, `ppid`, `principal`, `timestamp`,
and an object-specific `properties` map. The format is documented in
`ecar.md` in the dataset's Drive folder.

The Google Drive release ([folder](https://drive.google.com/drive/folders/1n3kkS3KR31KUegn42yk3-e6JkZvf0Caa))
is laid out as:

```
OpTCNCR/
├── OpTCRedTeamGroundTruth.pdf   # red-team diary: who attacked what, when
├── errata/                      # known data-quality issues — read this
├── ecar/                        # endpoint telemetry (this example uses only ecar/)
│   ├── benign/                  # pre-attack benign collection period
│   ├── evaluation/              # the attack window, 23–25 Sep 2019
│   │   ├── 23Sep19-red/         # day 1: PowerShell Empire
│   │   ├── 24Sep19/             # day 2: custom PowerShell Empire
│   │   ├── 25Sept/              # day 3: malicious update campaign
│   │   └── 23Sep-night/ ...     # overnight gaps between the above
│   ├── short/                   # incomplete captures; skip
├── ecar-bro/                    # flow-start events annotated with bro UIDs
└── bro/                         # raw network sensor logs by date
```

Each evaluation day folder is split into host buckets of 25 machines
(`AIA-1-25/`, `AIA-51-75/`, ..., `AIA-951-975/`), each holding one or two
`*.ecar*.json.gz` files covering `SysClient0201.systemia.com` through
`SysClient0225` and so on for that bucket's range.

## Getting the data

**Start small.** The full release is ~1 TB compressed; a single host
bucket for a single day is ~2 GB, and while data stays compressed on
disk, the loader materializes mapped facts in memory — RAM, not disk, is
what limits how big a slice you can take. The recommended starter slice is `AIA-201-225` for day 1
(23Sep19-red), because per `OpTCRedTeamGroundTruth.pdf` the day-1
scenario begins on **SysClient0201** — this one bucket contains the
initial compromise plus 24 benign neighbor hosts.

The starter slice is ~2.2 GB compressed, and the loader reads the `.gz`
files directly — nothing gets decompressed on disk.

### Option A: the fetch command

From this directory (needs only the Go toolchain — it handles Google
Drive's virus-scan interstitial itself and verifies the gzips):

```
go run ./fetch
```

### Option B: browser

Navigate the [Drive folder](https://drive.google.com/drive/folders/1n3kkS3KR31KUegn42yk3-e6JkZvf0Caa)
to `ecar/evaluation/23Sep19-red/AIA-201-225/` and download both files:

- `AIA-201-225.ecar-2019-12-08T11-05-10.046.json.gz`
- `AIA-201-225.ecar-last.json.gz`

Place them in `data/` here, keeping their original names, then run
`go run ./fetch` to verify them.

### Option C: rclone (for bigger slices)

For whole-day or whole-evaluation pulls, set up an
[rclone Google Drive remote](https://rclone.org/drive/) and copy by path,
e.g.:

```
rclone copy "optc:OpTCNCR/ecar/evaluation/23Sep19-red" data/ --drive-shared-with-me
```

## Placement

After setup, the layout this example expects is:

```
examples/optc/
├── data/
│   ├── AIA-201-225.ecar-2019-12-08T11-05-10.046.json.gz
│   └── AIA-201-225.ecar-last.json.gz
├── fetch/          # go run ./fetch — downloads and verifies the slice
├── optc.yaml       # jsonfacts schema: eCAR records → flat predicates
├── rules.dl        # day-1 Empire kill-chain detection rules
└── README.md
```

The loader decompresses `.gz` sources transparently, so the files stay
as downloaded. One caveat: the schema references sources by exact
filename, not glob, so if you pull additional host buckets or days, each
new file needs its own `file:` entry in the (forthcoming) `optc.yaml`.

Also download `OpTCRedTeamGroundTruth.pdf` from the top of the Drive
folder and keep it beside the data; it is the answer key for validating
any detection rules you write.

## Running it

```
datalog -c optc.yaml rules.dl
```

`optc.yaml` maps process/flow/file/module/shell/thread/registry/task/
session events to flat predicates keyed by eCAR's actor/object UUIDs,
and seeds matchers for Empire command-line tells, encoded-command
payloads, script drops, autorun keys, and internal-vs-external flow
destinations. `rules.dl` finds the Empire stager by its stacked
command-line tells, then propagates taint along the UUID provenance
chain (child processes and remote-thread injection targets) so C2
beaconing, payload drops, persistence, and shell activity are
attributed by ancestry rather than per-event signatures.

**RAM is the real ceiling.** `--max-facts` (default 10M) bounds what one
evaluation may derive and `--eval-timeout` (default 5m) bounds how long
it may run, but neither bounds the base load itself — the ~8.7 GB below
is mostly interned base facts, and the only backstops past your RAM are
Stop and the OS OOM killer. If memory is tight, `--provenance=false`
drops the why? drawer's per-derived-fact bookkeeping for headroom.
`datalog serve` starts its listener immediately and loads the dataset in
the background — watch the console for "loading dataset…" and the
"dataset loaded" notice (~6 minutes on the starter slice).

On the starter slice (~6 minutes, ~8.7 GB peak RSS) the rules
reconstruct the documented day-1 scenario: four stager launches as
`zleazer`, 317 tainted processes, 2,331 C2 flows — all to the
documented Empire C2 at `132.197.158.98` — the `Updater` run-key
persistence, and lateral spread from SysClient0201 to SysClient0205.
Useful queries once the REPL is up:

```
stager_launch(Host, Proc, Pid, User, Cmd, Time)?
c2_channel(Host, Ip, N)?
empire_autorun(Host, Action, Key, Data, Time)?
empire_kill_chain(Host, User, Proc, C2Ip)?
compromised_host(Host)?
```
