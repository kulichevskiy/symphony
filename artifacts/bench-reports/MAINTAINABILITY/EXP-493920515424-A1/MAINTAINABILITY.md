# Maintainability — `kulichevskiy/EXP-493920515424-A1`

> Functional grade is intentionally not included; maintainability is a separate axis.

## Static diagnostics

- Production source: 6 files, 1956 lines
- Functions: 48
- Cyclomatic complexity median/p95/max: 2/16/184
- Largest function/file: 1064/1247 lines
- Large functions/files: 5/2
- Duplicated six-line blocks: 7
- Import cycles: 0

## Mutation testing — 19/24 killed

- `python:boolean_connector:support_queue/database.py:114:a33c57ba`: survived
- `python:boolean_literal:support_queue/database.py:56:1f78c013`: killed
- `python:comparison:support_queue/database.py:49:5bc521db`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:60:82119eb7`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:137:f1c7f614`: survived
- `typescript:comparison:frontend/src/App.tsx:60:b3ec891b`: killed
- `python:boolean_connector:support_queue/database.py:263:4bbd3511`: killed
- `python:boolean_literal:support_queue/database.py:136:58751231`: survived
- `python:comparison:support_queue/database.py:114:6b1055bc`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:64:029de028`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:145:9364b18a`: killed
- `typescript:comparison:frontend/src/App.tsx:64:ec9074e5`: killed
- `python:boolean_connector:support_queue/database.py:265:526ac5b3`: killed
- `python:boolean_literal:support_queue/schemas.py:32:80fcbcd9`: killed
- `python:comparison:support_queue/database.py:114:6b1055bc`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:99:6b041a6c`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:189:df0ee3de`: survived
- `typescript:comparison:frontend/src/App.tsx:82:71928508`: killed
- `python:boolean_connector:support_queue/database.py:266:73fa7308`: killed
- `python:boolean_literal:support_queue/schemas.py:40:09ecffeb`: killed
- `python:comparison:support_queue/database.py:120:2eb31050`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:100:2f274038`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:190:9bbd9106`: survived
- `typescript:comparison:frontend/src/App.tsx:91:aa2c3301`: killed

## Maintenance probe

- Status: passed
- Successful repetitions: 1/1
- Wall time: 376.99s
- Raw tokens: 3024572
- Cost: $2.5944
- Change surface: 5 files, 269 lines
