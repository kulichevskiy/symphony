# Maintainability — `kulichevskiy/EXP-CF2251760E6A-A1`

> Functional grade is intentionally not included; maintainability is a separate axis.

## Static diagnostics

- Production source: 6 files, 1471 lines
- Functions: 45
- Cyclomatic complexity median/p95/max: 2/5/59
- Largest function/file: 318/657 lines
- Large functions/files: 1/2
- Duplicated six-line blocks: 2
- Import cycles: 0

## Mutation testing — 17/24 killed

- `python:boolean_connector:support_queue/db.py:72:2a2bd956`: killed
- `python:boolean_literal:support_queue/db.py:135:00f80dbc`: survived
- `python:comparison:support_queue/db.py:118:1b0f9a69`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:87:3f91df91`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:101:08505f99`: killed
- `typescript:comparison:frontend/src/App.tsx:44:44a11c6d`: killed
- `python:boolean_connector:support_queue/main.py:57:e915e76f`: killed
- `python:boolean_literal:support_queue/db.py:135:00f80dbc`: killed
- `python:comparison:support_queue/db.py:126:a896c3d6`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:136:082c9885`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:102:5dc51014`: killed
- `typescript:comparison:frontend/src/App.tsx:87:5352d181`: killed
- `python:boolean_connector:support_queue/main.py:76:9058e2ab`: killed
- `python:boolean_literal:support_queue/db.py:156:a556e8cb`: survived
- `python:comparison:support_queue/db.py:132:6eedec29`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:137:caf4d427`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:131:f11240fc`: survived
- `typescript:comparison:frontend/src/App.tsx:87:5352d181`: killed
- `python:boolean_connector:support_queue/main.py:82:6083acca`: killed
- `python:boolean_literal:support_queue/db.py:189:39cc3f70`: survived
- `python:comparison:support_queue/db.py:134:a0426adc`: survived
- `typescript:boolean_connector:frontend/src/App.tsx:163:08c31a73`: survived
- `typescript:boolean_literal:frontend/src/App.tsx:170:4ca9b12b`: survived
- `typescript:comparison:frontend/src/App.tsx:136:406c62f1`: killed

## Maintenance probe

- Status: passed
- Successful repetitions: 1/1
- Wall time: 274.52s
- Raw tokens: 2073380
- Cost: $2.2004
- Change surface: 9 files, 309 lines
