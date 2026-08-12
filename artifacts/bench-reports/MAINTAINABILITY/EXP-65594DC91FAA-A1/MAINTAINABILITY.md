# Maintainability — `kulichevskiy/EXP-65594DC91FAA-A1`

> Functional grade is intentionally not included; maintainability is a separate axis.

## Static diagnostics

- Production source: 4 files, 988 lines
- Functions: 40
- Cyclomatic complexity median/p95/max: 2/13/15
- Largest function/file: 70/525 lines
- Large functions/files: 2/2
- Duplicated six-line blocks: 4
- Import cycles: 0

## Mutation testing — 16/24 killed

- `python:boolean_connector:support_queue/main.py:93:c9c272e5`: killed
- `python:comparison:support_queue/main.py:85:41c35189`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:58:b5483dc4`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:64:df9f29cf`: survived
- `typescript:comparison:frontend/src/App.tsx:58:ef5f3e6e`: killed
- `python:boolean_connector:support_queue/main.py:104:bfae11dc`: killed
- `python:comparison:support_queue/main.py:87:16895c97`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:62:c98ee1e7`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:65:3e55e0fb`: survived
- `typescript:comparison:frontend/src/App.tsx:62:254886e6`: killed
- `python:boolean_connector:support_queue/main.py:188:b568a107`: survived
- `python:comparison:support_queue/main.py:93:d45763fb`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:109:ebcc0700`: survived
- `typescript:boolean_literal:frontend/src/App.tsx:69:088144fc`: killed
- `typescript:comparison:frontend/src/App.tsx:98:24296f74`: killed
- `python:boolean_connector:support_queue/main.py:201:00e76aa2`: survived
- `python:comparison:support_queue/main.py:104:83a89687`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:110:92edf8ca`: survived
- `typescript:boolean_literal:frontend/src/App.tsx:70:b1f7cb03`: killed
- `typescript:comparison:frontend/src/App.tsx:109:4d022302`: killed
- `python:boolean_connector:support_queue/main.py:245:9267f216`: killed
- `python:comparison:support_queue/main.py:104:d53dfc52`: survived
- `typescript:boolean_connector:frontend/src/App.tsx:111:a0e4229c`: survived
- `typescript:boolean_literal:frontend/src/App.tsx:74:f8351eac`: killed

## Maintenance probe

- Status: passed
- Successful repetitions: 1/1
- Wall time: 243.19s
- Raw tokens: 1099464
- Cost: $1.4203
- Change surface: 4 files, 241 lines
