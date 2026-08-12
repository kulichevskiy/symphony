# Maintainability — `kulichevskiy/EXP-A105BD7797F3-A1`

> Functional grade is intentionally not included; maintainability is a separate axis.

## Static diagnostics

- Production source: 4 files, 817 lines
- Functions: 34
- Cyclomatic complexity median/p95/max: 2/15/91
- Largest function/file: 216/544 lines
- Large functions/files: 2/1
- Duplicated six-line blocks: 20
- Import cycles: 0

## Mutation testing — 14/24 killed

- `python:boolean_connector:support_queue/main.py:113:3a2477c1`: killed
- `python:boolean_literal:support_queue/main.py:79:803fd505`: survived
- `python:comparison:support_queue/main.py:113:4dbff7bd`: survived
- `typescript:boolean_connector:frontend/src/App.tsx:43:a85da0ce`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:70:b1f7cb03`: survived
- `typescript:comparison:frontend/src/App.tsx:86:8b789973`: killed
- `python:boolean_connector:support_queue/main.py:178:3cd1d2d6`: survived
- `python:boolean_literal:support_queue/main.py:90:27ec33c6`: survived
- `python:comparison:support_queue/main.py:178:6d93a70f`: survived
- `typescript:boolean_connector:frontend/src/App.tsx:150:cb6f47c4`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:107:185195b0`: survived
- `typescript:comparison:frontend/src/App.tsx:89:83b21758`: killed
- `python:boolean_connector:support_queue/main.py:272:647094a1`: killed
- `python:boolean_literal:support_queue/main.py:209:1806b3d3`: survived
- `python:comparison:support_queue/main.py:197:105305db`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:160:34905bea`: survived
- `typescript:comparison:frontend/src/App.tsx:103:9f73150e`: killed
- `python:boolean_connector:support_queue/main.py:398:fc982574`: killed
- `python:boolean_literal:support_queue/main.py:395:3be561db`: killed
- `python:comparison:support_queue/main.py:203:62a810aa`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:167:6146f5a8`: survived
- `typescript:comparison:frontend/src/App.tsx:121:8451723b`: killed
- `python:boolean_connector:support_queue/main.py:400:07466b37`: killed
- `python:comparison:support_queue/main.py:309:a3c9e4d1`: killed

## Maintenance probe

- Status: passed
- Successful repetitions: 1/1
- Wall time: 342.92s
- Raw tokens: 2189355
- Cost: $2.2851
- Change surface: 5 files, 404 lines
