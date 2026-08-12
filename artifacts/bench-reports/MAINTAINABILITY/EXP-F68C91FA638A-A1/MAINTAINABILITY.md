# Maintainability — `kulichevskiy/EXP-F68C91FA638A-A1`

> Functional grade is intentionally not included; maintainability is a separate axis.

## Static diagnostics

- Production source: 4 files, 1843 lines
- Functions: 54
- Cyclomatic complexity median/p95/max: 3/41/125
- Largest function/file: 674/1192 lines
- Large functions/files: 5/2
- Duplicated six-line blocks: 22
- Import cycles: 0

## Mutation testing — 0/0 killed

- Not run.

## Maintenance probe

- Status: passed
- Successful repetitions: 1/1
- Wall time: 401.01s
- Raw tokens: 3278071
- Cost: $3.1923
- Change surface: 6 files, 528 lines

## Errors

- typescript mutation baseline failed on repetition 1/2: vents.maxEventTargetListenersWarned): false,
+       Symbol(kHandlers): undefined,
+       Symbol(kAborted): false,
+       Symbol(kReason): undefined,
+       Symbol(kComposite): false,
+     },
    },
  ]

 ❯ src/App.test.tsx:1935:19
    1933|
    1934|     await waitFor(() => expect(patchRequests).toBe(3));
    1935|     expect(fetch).toHaveBeenLastCalledWith(
       |                   ^
    1936|       "/tickets/7",
    1937|       expect.objectContaining({

⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯[2/2]⎯
