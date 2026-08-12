# Maintainability — `kulichevskiy/EXP-BAC81AD7F9F8-A1`

> Functional grade is intentionally not included; maintainability is a separate axis.

## Static diagnostics

- Production source: 4 files, 1253 lines
- Functions: 37
- Cyclomatic complexity median/p95/max: 2/30/83
- Largest function/file: 544/633 lines
- Large functions/files: 4/2
- Duplicated six-line blocks: 7
- Import cycles: 0

## Mutation testing — 17/24 killed

- `python:boolean_connector:support_queue/main.py:105:14afcdd3`: killed
- `python:boolean_literal:support_queue/main.py:47:139cd514`: killed
- `python:comparison:support_queue/main.py:105:729282b2`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:80:01cdc8b7`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:94:4b1afbc5`: killed
- `typescript:comparison:frontend/src/App.tsx:80:cfff83d8`: killed
- `python:boolean_connector:support_queue/main.py:107:0bab69e0`: killed
- `python:boolean_literal:support_queue/main.py:50:0ad2c3ab`: killed
- `python:comparison:support_queue/main.py:107:21492696`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:84:804afea9`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:95:0951b6d7`: survived
- `typescript:comparison:frontend/src/App.tsx:84:9cf36571`: killed
- `python:boolean_connector:support_queue/main.py:109:df34352b`: killed
- `python:boolean_literal:support_queue/main.py:54:99eae4f4`: killed
- `python:comparison:support_queue/main.py:109:45ddf723`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:161:dbd873cf`: survived
- `typescript:boolean_literal:frontend/src/App.tsx:102:5dc51014`: killed
- `typescript:comparison:frontend/src/App.tsx:161:dc4b6be7`: survived
- `python:boolean_connector:support_queue/main.py:188:b568a107`: survived
- `python:boolean_literal:support_queue/main.py:156:d0df3ed9`: survived
- `python:comparison:support_queue/main.py:188:7d7aac19`: survived
- `typescript:boolean_connector:frontend/src/App.tsx:223:2eb1cf57`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:103:868c66e7`: killed
- `typescript:comparison:frontend/src/App.tsx:162:6b9d2e79`: survived

## Maintenance probe

- Status: passed
- Successful repetitions: 1/1
- Wall time: 338.31s
- Raw tokens: 2441803
- Cost: $2.4478
- Change surface: 4 files, 384 lines
