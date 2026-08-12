# Maintainability — `kulichevskiy/EXP-26E49A750672-A1`

> Functional grade is intentionally not included; maintainability is a separate axis.

## Static diagnostics

- Production source: 4 files, 707 lines
- Functions: 31
- Cyclomatic complexity median/p95/max: 2/27/62
- Largest function/file: 252/386 lines
- Large functions/files: 2/0
- Duplicated six-line blocks: 2
- Import cycles: 0

## Mutation testing — 22/24 killed

- `python:boolean_connector:support_queue/main.py:79:dcca25b6`: killed
- `python:boolean_literal:support_queue/main.py:136:107aae0b`: killed
- `python:comparison:support_queue/main.py:93:d45763fb`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:45:9e9f4768`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:77:d2d579f5`: killed
- `typescript:comparison:frontend/src/App.tsx:38:8234ec4b`: killed
- `python:boolean_connector:support_queue/main.py:93:c9c272e5`: killed
- `python:comparison:support_queue/main.py:128:e965a1bd`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:45:c309d8ed`: survived
- `typescript:boolean_literal:frontend/src/App.tsx:82:d0747917`: killed
- `typescript:comparison:frontend/src/App.tsx:44:44a11c6d`: killed
- `python:boolean_connector:support_queue/main.py:93:f082835d`: killed
- `python:comparison:support_queue/main.py:136:63c9935e`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:47:37bebac6`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:83:db0dc3db`: killed
- `typescript:comparison:frontend/src/App.tsx:45:e3657b45`: killed
- `python:boolean_connector:support_queue/main.py:120:f1c24f6b`: killed
- `python:comparison:support_queue/main.py:184:23048df3`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:47:999c20af`: killed
- `typescript:boolean_literal:frontend/src/App.tsx:130:a14651d2`: killed
- `typescript:comparison:frontend/src/App.tsx:47:934e9fb6`: killed
- `python:boolean_connector:support_queue/main.py:128:93bfaea8`: killed
- `python:comparison:support_queue/main.py:187:48969336`: killed
- `typescript:boolean_connector:frontend/src/App.tsx:52:d7d6c583`: survived

## Maintenance probe

- Status: passed
- Successful repetitions: 1/1
- Wall time: 330.28s
- Raw tokens: 2277496
- Cost: $2.3416
- Change surface: 6 files, 306 lines
