# Mordor: Lateral Movement Detection

This example uses Datalog to detect a lateral movement attack in real Windows security telemetry from the [OTRF Security Datasets](https://github.com/OTRF/Security-Datasets) project (formerly "Mordor").

## The Dataset

The included `covenant_copy_smb.zip` (25 KB) contains 506 Windows event log entries captured during an attack simulation where:

1. An attacker running the [Covenant C2](https://github.com/cobbr/Covenant) implant `GruntHTTP.exe` on **WORKSTATION5** (`172.18.39.5`) initiates an SMB connection to **WORKSTATION6** (`172.18.39.6`) on port 445.
2. Kerberos authentication is performed against the domain controller **MORDORDC** (`172.18.38.5`).
3. User `pgustavo` logs on remotely to WORKSTATION6 (LogonType 3) with elevated privileges (SeDebugPrivilege, SeBackupPrivilege, etc.).
4. The attacker accesses the admin shares `\\*\IPC$` and `\\*\C$`.
5. `GruntHTTP.exe` is written to `C:\ProgramData\GruntHTTP.exe` on WORKSTATION6 via the C$ share.

This corresponds to [MITRE ATT&CK T1021.002](https://attack.mitre.org/techniques/T1021/002/) (Remote Services: SMB/Windows Admin Shares).

The dataset was published by the [Open Threat Research Forge](https://github.com/OTRF) under the `covenant_copy_smb_CreateRequest` scenario. Events include Sysmon operational logs (network connections, file creates, image loads, registry activity, process access) and Windows Security logs (logon, privilege assignment, share access).

## Running It

Build the CLI and point it at the zip directly:

```
go build -o datalog ./datalog/cmd/datalog

datalog \
  -c datalog/examples/mordor/mordor.yaml \
  -d datalog/examples/mordor/covenant_copy_smb.zip \
  datalog/examples/mordor/rules.dl
```

The `-d` flag accepts a `.zip` file as well as a directory.

## Exploring in the REPL

### List loaded predicates

```
?> .list
  ci_ends_with/2  (2 facts)
  cidr_match/2  (2 facts)
  contains/2  (4 facts)
  file_create/3  (5 facts)
  image_load/5  (1 facts)
  logon/7  (2 facts)
  net_conn/8  (3 facts)
  proc_access/4  (10 facts)
  proc_terminate/3  (3 facts)
  reg_key/3  (123 facts)
  reg_value/4  (59 facts)
  share_access/4  (2 facts)
  share_file/6  (3 facts)
  special_priv/4  (2 facts)
```

There are 506 raw events but only 215 distinct facts after deduplication. The `reg_key` and `reg_value` predicates dominate -- most of the event log is routine registry noise. The signal is in the handful of `net_conn`, `logon`, `share_access`, `share_file`, and `file_create` facts.

### Inspect raw facts

Look at network connections:

```
?> .facts net_conn/8
  net_conn("WORKSTATION5.theshire.local", "C:\\Windows\\System32\\lsass.exe", "172.18.39.5", "62783", "172.18.38.5", "88", "tcp", "NT AUTHORITY\\SYSTEM")
  net_conn("WORKSTATION5.theshire.local", "System", "172.18.39.5", "62782", "172.18.39.6", "445", "tcp", "NT AUTHORITY\\SYSTEM")
  net_conn("WORKSTATION6.theshire.local", "System", "172.18.39.5", "62782", "172.18.39.6", "445", "tcp", "NT AUTHORITY\\SYSTEM")
```

Two SMB connections to port 445, and one Kerberos authentication to port 88 -- the three network events tell the whole story.

Look at what was written through the admin share:

```
?> .facts share_file/6
  share_file("WORKSTATION6.theshire.local", "pgustavo", "\\\\*\\C$", "ProgramData", "172.18.39.5", "0x80")
  share_file("WORKSTATION6.theshire.local", "pgustavo", "\\\\*\\C$", "ProgramData\\GruntHTTP.exe", "172.18.39.5", "0x17019f")
  share_file("WORKSTATION6.theshire.local", "pgustavo", "\\\\*\\C$", "ProgramData\\GruntHTTP.exe", "172.18.39.5", "0x2")
```

### Query intermediate detections

Each rule in `rules.dl` derives a focused predicate. Try them bottom-up:

```
?> smb_conn(Host, Src, Dst)?
  Host = "WORKSTATION5.theshire.local", Src = "172.18.39.5", Dst = "172.18.39.6"
  Host = "WORKSTATION6.theshire.local", Src = "172.18.39.5", Dst = "172.18.39.6"
  (2 results)

?> remote_logon(Host, User, Ip, Auth)?
  Host = "MORDORDC.theshire.local", User = "MORDORDC$", Ip = "::1", Auth = "Kerberos"
  Host = "WORKSTATION6.theshire.local", User = "pgustavo", Ip = "172.18.39.5", Auth = "Kerberos"
  (2 results)

?> admin_share(Host, User, Share, Ip)?
  Host = "WORKSTATION6.theshire.local", User = "pgustavo", Share = "\\\\*\\C$", Ip = "172.18.39.5"
  Host = "WORKSTATION6.theshire.local", User = "pgustavo", Share = "\\\\*\\IPC$", Ip = "172.18.39.5"
  (2 results)

?> exe_drop(Host, User, Share, Path, Ip)?
  Host = "WORKSTATION6.theshire.local", User = "pgustavo", Share = "\\\\*\\C$", Path = "ProgramData\\GruntHTTP.exe", Ip = "172.18.39.5"
  (1 results)
```

### Query the kill chain

The `lateral_movement` rule joins all four intermediate detections together by IP address and username:

```
?> lateral_movement(User, Src, Target, Path)?
  User = "pgustavo", Src = "172.18.39.5", Target = "WORKSTATION6.theshire.local", Path = "ProgramData\\GruntHTTP.exe"
  (1 results)
```

Was it an elevated session?

```
?> elevated_lateral_movement(User, Src, Target, Path)?
  User = "pgustavo", Src = "172.18.39.5", Target = "WORKSTATION6.theshire.local", Path = "ProgramData\\GruntHTTP.exe"
  (1 results)
```

Can we corroborate the share write with Sysmon's independent observation of the file appearing on disk?

```
?> confirmed_drop(User, Host, SharePath, DiskPath)?
  User = "pgustavo", Host = "WORKSTATION6.theshire.local", SharePath = "ProgramData\\GruntHTTP.exe", DiskPath = "C:\\ProgramData\\GruntHTTP.exe"
  (1 results)
```

## How the Schema Works

The `mordor.yaml` schema handles the fact that Windows event log entries have different structures depending on `EventID`. Each mapping uses a `filter` to select events by type:

```yaml
- predicate: net_conn
  args: ["value.Hostname", "value.Image", "value.SourceIp", ...]
  filter: "value.EventID == 3"

- predicate: logon
  args: ["value.Hostname", "value.TargetUserName", ...]
  filter: "value.EventID == 4624"
```

This decomposes the heterogeneous JSONL into typed relational predicates -- `net_conn/8`, `logon/7`, `share_file/6`, etc. -- that Datalog rules can join cleanly.

The schema also defines **matchers** that scan loaded facts and emit derived match predicates:

- `contains(DstPort, "445")` -- flags SMB port connections
- `cidr_match(Ip, "172.18.39.0/24")` -- flags IPs in the lab subnet
- `contains(Share, "C$")` -- flags admin share access
- `ci_ends_with(Path, ".exe")` -- flags executable file paths (case-insensitive)

Rules reference these match predicates to express detection logic declaratively rather than embedding string operations in rule bodies.

## How the Rules Work

The rules in `rules.dl` are organized as a pipeline of increasingly specific detections:

```
net_conn ──→ smb_conn             ─┐
logon ──→ remote_logon             ├──→ lateral_movement ──→ elevated_lateral_movement
share_access ──→ admin_share       │
share_file ──→ exe_drop ───────────┘
                    │
file_create ──→ exe_on_disk ──→ confirmed_drop
```

Each intermediate predicate captures a single observable indicator. The `lateral_movement` rule joins them on the shared IP address and username. The `confirmed_drop` rule cross-references two independent data sources (Security share audit and Sysmon file monitoring) to corroborate the finding.

## Exercise

The current rules detect the completed lateral movement. Try extending them to answer these questions:

1. **Kerberos ticket request**: The dataset includes a Security EventID 4769 (Kerberos Service Ticket Operation). Add a mapping for it in `mordor.yaml` and write a rule that joins the Kerberos ticket request with the remote logon to show the full authentication chain.

2. **Process lifecycle**: The dataset has Sysmon EventID 7 (Image Loaded) showing `GruntHTTP.exe` loading `ntmarta.dll` on the source host. Write a rule that connects the image load on the source to the file drop on the target -- i.e., "the process that loaded libraries on Host A is the same executable that was later dropped on Host B."

3. **Negative detection**: Write a rule for `unexpected_file_create` that finds file-create events where the creating process is *not* `svchost.exe` or `System`. Use negation (`not`) or a comparison (`Image != "..."`) to filter out routine system activity. What does it find?

4. **Counting**: Use an aggregate query to count how many distinct registry keys each process image touched. Which process has the most registry activity, and is it suspicious?

## Running the Tests

```
go test -v ./datalog/examples/mordor/
```

The tests verify both fact loading (correct counts and matcher output) and rule evaluation (full kill chain materialization).
