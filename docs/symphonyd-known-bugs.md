# symphonyd-known-bugs

## How to apply

### `$retry` on `implement_failed` wait is consumed without resuming

If `$retry` on an `implement_failed` parked issue has no effect, `operator_waits`
still has a row for the issue, and `comment_events` shows the `$retry` comment
IDs as seen, remove the stale wait and restart the daemon so in-memory run
bindings are rebuilt:

```sh
sqlite3 state.sqlite "DELETE FROM operator_waits WHERE issue_id='<uuid>';"
```

Then restart `symphonyd`. On the next poll, the issue can be picked up through
the normal ready-lane scan.
