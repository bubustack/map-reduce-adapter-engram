# 🧩 Map/Reduce Adapter Engram

Scales a Story over thousands of items by spinning up per-item StoryRuns and
optionally reducing their outputs back into a summary.

## 🌟 Highlights

- Converts each item into a `StoryRun` via template-driven map logic.
- Stores per-item outputs in shared storage and indexes them for downstream reduce steps.
- Optional reduce story can summarize items or compute aggregates.
- Maintains execution safety with retries, conditionals, and storage-based backpressure.

## 🚀 Quick Start

```bash
go test ./...
```

Apply `Engram.yaml` and create a Story that references the template. Provide a
runtime `items` array (inline or storage-backed) plus a `map.storyRef` or inline
`map.story` definition.

## ⚙️ Template Defaults (`Engram.spec.with`)

| Field | Description |
| --- | --- |
| `defaultBatchSize` | Default number of items per batch when `map.batchSize` is omitted. |
| `defaultConcurrency` | Default maximum number of in-flight StoryRuns when `map.concurrency` is omitted. |
| `maxBatchSize` | Hard cap applied to batch size overrides. |
| `maxConcurrency` | Hard cap applied to concurrency overrides. |
| `defaultFailFast` | Default fail-fast behavior for the map stage. |
| `defaultAllowFailures` | Default behavior for tolerating map-item failures without failing the adapter step. |
| `defaultInlineResultsLimit` | Maximum number of per-item results to inline in `output.results` by default (`0` disables inlining). |
| `pollInterval` | Default polling interval for child StoryRun completion. |
| `defaultStoryRunTTLSeconds` | Default TTL for child StoryRun resources. |
| `defaultStoryRunRetentionSeconds` | Default retention window for child StoryRuns. |

These values set bounds and defaults for runtime requests. The actual fan-out
payload is supplied in the StepRun input.

## 📥 Inputs

| Field | Description |
| --- | --- |
| `items` | Required batch to process. Supports inline arrays, `$bubuStorageRef`, or `$bubuConfigMapRef`. |
| `context` | Optional shared context injected into every mapped StoryRun. |
| `inputs` | Optional shared inputs merged into every mapped StoryRun. |
| `map.storyRef` | Reference to the Story template executed for each item. |
| `map.story` | Inline Story spec alternative to `map.storyRef`. |
| `map.inputs` | Additional per-item inputs merged into the map payload. |
| `map.batchSize` | Per-request override for batch size. |
| `map.concurrency` | Per-request override for maximum concurrent child StoryRuns. |
| `map.failFast` | Stop the adapter after the first failed map item. |
| `map.allowFailures` | Allow failed items without failing the parent step. |
| `map.inlineResultsLimit` | Inline small per-item results directly in the adapter output. |
| `map.pollInterval` | Override the polling interval for map child StoryRuns. |
| `map.storyRunTTLSeconds` | TTL override for map child StoryRuns. |
| `map.storyRunRetentionSeconds` | Retention override for map child StoryRuns. |
| `reduce.storyRef` | Optional Story template reference for the reduce stage. |
| `reduce.story` | Inline Story spec for the reduce stage. |
| `reduce.inputs` | Additional inputs merged into the reduce payload. |
| `reduce.pollInterval` | Polling interval override for the reduce StoryRun. |
| `reduce.storyRunTTLSeconds` | TTL override for the reduce StoryRun. |
| `reduce.storyRunRetentionSeconds` | Retention override for the reduce StoryRun. |

## 📤 Outputs

| Field | Description |
| --- | --- |
| `manifest` | Storage-backed manifest describing each mapped item, its child StoryRun, and the aggregate stats. |
| `stats` | Object containing `total`, `succeeded`, and `failed` counters. |
| `resultsPrefix` | Storage prefix that contains per-item outputs. |
| `resultsIndex` | Storage-backed index of per-item status and output refs. |
| `results` | Optional inline results when `inlineResultsLimit` allows them. |
| `reduce` | Optional reduce-stage result metadata (`storyRun`, `phase`, `outputRef`, `error`). |

## 🧪 Local Development

- `go test ./...` – Unit and integration tests for batching and reduce flows.
- `go vet ./...` – Ensure schema compliance before release.

## 🤝 Community & Support

- [Contributing](./CONTRIBUTING.md)
- [Support](./SUPPORT.md)
- [Security Policy](./SECURITY.md)
- [Code of Conduct](./CODE_OF_CONDUCT.md)
- [Discord](https://discord.gg/dysrB7D8H6)

## 📄 License

Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
