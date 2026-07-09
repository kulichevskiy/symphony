import { authHeaders } from "@/lib/auth";

/** One readable event parsed from an agent's stream-json, as emitted by the
 *  `/api/runs/{run_id}/stream` NDJSON endpoint. `cursor`/`end` are control
 *  frames the transport uses for reconnect + clean completion. */
export type LiveEvent =
  | { kind: "message"; text: string }
  | { kind: "tool_call"; tool: string; detail: string }
  | { kind: "file_edit"; tool?: string; files: string[] }
  | {
      kind: "tokens";
      input_tokens: number;
      output_tokens: number;
      cache_write_tokens: number;
      cache_read_tokens: number;
      cost_usd: number;
    }
  | { kind: "cursor"; offset: number }
  | { kind: "end" };

export interface StreamResult {
  /** Byte offset reached in the run log — pass back as `offset` to resume. */
  offset: number;
  /** True once the server signalled the run finished (`end` frame). */
  ended: boolean;
}

/**
 * Tail a run's live log via `fetch` + `ReadableStream` (NOT `EventSource`, so
 * the request carries the Auth0 `Authorization: Bearer` header through the
 * gate). Each parsed event is handed to `onEvent`; `cursor`/`end` frames are
 * consumed here to track the resume offset and completion, and are not
 * forwarded. Resolves when the body closes — cleanly on `end`, or on an abort
 * / dropped connection (the caller reconnects from the returned `offset`).
 */
export async function streamRun(
  runId: string,
  {
    offset = 0,
    signal,
    onEvent,
    onCursor,
  }: {
    offset?: number;
    signal?: AbortSignal;
    onEvent: (event: LiveEvent) => void;
    /** Invoked with the latest byte offset as `cursor` frames are consumed,
     *  so the caller can track resume progress even if the read loop later
     *  errors out (e.g. a real mid-stream connection drop). */
    onCursor?: (offset: number) => void;
  },
): Promise<StreamResult> {
  const params = new URLSearchParams({ offset: String(offset) });
  const response = await fetch(
    `/api/runs/${encodeURIComponent(runId)}/stream?${params.toString()}`,
    {
      headers: { Accept: "application/x-ndjson", ...(await authHeaders()) },
      signal,
    },
  );
  if (!response.ok || !response.body) {
    throw new Error(`Failed to open live stream (${response.status})`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let cursor = offset;
  let ended = false;

  const drain = (chunk: string): void => {
    buffer += chunk;
    let newline = buffer.indexOf("\n");
    while (newline >= 0) {
      const raw = buffer.slice(0, newline).trim();
      buffer = buffer.slice(newline + 1);
      newline = buffer.indexOf("\n");
      if (!raw) continue;
      let event: LiveEvent;
      try {
        event = JSON.parse(raw) as LiveEvent;
      } catch {
        continue;
      }
      if (event.kind === "cursor") {
        cursor = event.offset;
        onCursor?.(cursor);
      } else if (event.kind === "end") {
        ended = true;
      } else {
        onEvent(event);
      }
    }
  };

  try {
    for (;;) {
      let step: ReadableStreamReadResult<Uint8Array>;
      try {
        step = await reader.read();
      } catch {
        // Mid-stream drop (not a clean `done`) — report progress made so far
        // instead of throwing, so the caller can resume from `cursor`.
        break;
      }
      if (step.done) break;
      drain(decoder.decode(step.value, { stream: true }));
    }
    drain(decoder.decode());
  } finally {
    reader.releaseLock();
  }

  return { offset: cursor, ended };
}
