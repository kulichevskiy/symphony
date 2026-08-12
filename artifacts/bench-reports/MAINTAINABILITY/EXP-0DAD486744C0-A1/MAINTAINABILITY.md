# Maintainability — `kulichevskiy/EXP-0DAD486744C0-A1`

> Functional grade is intentionally not included; maintainability is a separate axis.

## Static diagnostics

- Production source: 6 files, 1247 lines
- Functions: 34
- Cyclomatic complexity median/p95/max: 2/6/21
- Largest function/file: 229/598 lines
- Large functions/files: 1/1
- Duplicated six-line blocks: 3
- Import cycles: 0

## Mutation testing — 16/24 killed

- `python:boolean_connector:support_queue/main.py:144:b73ec603`: killed
- `python:boolean_literal:support_queue/db.py:67:ccfc8f5a`: survived
- `python:comparison:support_queue/db.py:52:389f70ab`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:41:3881df53`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:132:c9593c78`: survived
- `typescript:comparison:frontend/src/App.tsx:29:102d7253`: killed
- `python:boolean_connector:support_queue/main.py:153:417a5fc6`: killed
- `python:boolean_literal:support_queue/db.py:67:ccfc8f5a`: killed
- `python:comparison:support_queue/db.py:66:72fc8ea2`: survived
- `typescript:boolean_connector:frontend/src/App.tsx:172:eae6bf15`: survived
- `typescript:boolean_literal:frontend/src/App.tsx:133:d1a71763`: survived
- `typescript:comparison:frontend/src/App.tsx:41:4a5ed7b2`: killed
- `python:boolean_connector:support_queue/main.py:153:417a5fc6`: killed
- `python:boolean_literal:support_queue/db.py:81:2570ff54`: survived
- `python:comparison:support_queue/main.py:171:5e26153d`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:172:eae6bf15`: survived
- `typescript:boolean_literal:frontend/src/App.tsx:136:930deea7`: killed
- `typescript:comparison:frontend/src/App.tsx:41:4a5ed7b2`: killed
- `python:boolean_connector:support_queue/main.py:154:0e0bc802`: killed
- `python:boolean_literal:support_queue/main.py:45:ea56acc7`: killed
- `python:comparison:support_queue/main.py:171:90451449`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:172:eae6bf15`: survived
- `typescript:boolean_literal:frontend/src/App.tsx:137:f1c7f614`: killed
- `typescript:comparison:frontend/src/App.tsx:55:70d1491f`: killed

## Maintenance probe

- Status: passed
- Successful repetitions: 1/1
- Wall time: 234.51s
- Raw tokens: 1496297
- Cost: $1.7839
- Change surface: 7 files, 155 lines
