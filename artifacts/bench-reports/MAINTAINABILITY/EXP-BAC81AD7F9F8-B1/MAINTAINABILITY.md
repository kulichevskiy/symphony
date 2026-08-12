# Maintainability — `kulichevskiy/EXP-BAC81AD7F9F8-B1`

> Functional grade is intentionally not included; maintainability is a separate axis.

## Static diagnostics

- Production source: 6 files, 1905 lines
- Functions: 50
- Cyclomatic complexity median/p95/max: 2/16/181
- Largest function/file: 1054/1202 lines
- Large functions/files: 3/2
- Duplicated six-line blocks: 16
- Import cycles: 0

## Mutation testing — 19/24 killed

- `python:boolean_connector:support_queue/database.py:46:63c4dbc1`: survived
- `python:boolean_literal:support_queue/database.py:110:b5f3cb15`: survived
- `python:comparison:support_queue/database.py:46:9e1a06ce`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:90:43354e09`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:56:05f1a4d2`: killed
- `typescript:comparison:frontend/src/App.tsx:90:adb865e6`: killed
- `python:boolean_connector:support_queue/main.py:152:ccc76180`: killed
- `python:boolean_literal:support_queue/main.py:128:5649d053`: survived
- `python:comparison:support_queue/database.py:47:68de4587`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:94:7e34b284`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:60:81b7e46b`: killed
- `typescript:comparison:frontend/src/App.tsx:94:d84fb17d`: killed
- `python:boolean_connector:support_queue/main.py:231:b4e40907`: killed
- `python:boolean_literal:support_queue/models.py:89:7feebaf5`: killed
- `python:comparison:support_queue/database.py:48:ca680133`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:110:92edf8ca`: survived
- `typescript:boolean_literal:frontend/src/App.tsx:171:6b692311`: killed
- `typescript:comparison:frontend/src/App.tsx:110:d1a6559d`: survived
- `python:boolean_connector:support_queue/main.py:234:86b8e937`: killed
- `python:comparison:support_queue/database.py:49:8ac42ced`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:146:0e137e09`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:173:10ed929a`: killed
- `typescript:comparison:frontend/src/App.tsx:209:cd86f492`: killed
- `python:boolean_connector:support_queue/main.py:247:3fae912d`: killed

## Maintenance probe

- Status: passed
- Successful repetitions: 1/1
- Wall time: 333.26s
- Raw tokens: 2930834
- Cost: $2.7261
- Change surface: 7 files, 529 lines
