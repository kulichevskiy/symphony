import { Icon } from "@/components/ui/icon";
import type { TokenSplit } from "@/lib/api";
import { exactInt, formatTokens } from "@/lib/format";
import { cn } from "@/lib/utils";

/** Abbreviated token count with the exact value in the title attribute. */
export function Tk({ value }: { value: number | null | undefined }) {
  return <span title={exactInt(value)}>{formatTokens(value)}</span>;
}

const MIX_SEGMENTS = [
  { key: "input_tokens", tint: "bg-sky-500", label: "in" },
  { key: "output_tokens", tint: "bg-emerald-500", label: "out" },
  { key: "cache_write_tokens", tint: "bg-amber-500", label: "cache-write" },
  { key: "cache_read_tokens", tint: "bg-violet-500", label: "cache-read" },
] as const;

/**
 * Per-row stacked token mix-bar. Every bar is the same full width; its segments
 * are sized by this row's *own* raw-token proportions of in / out / cache-write
 * / cache-read. Length never encodes magnitude — that comes from the explicit
 * numbers and the list's sort order.
 */
export function MixBar({
  split,
  className,
}: {
  split: TokenSplit;
  className?: string;
}) {
  const total =
    split.input_tokens +
    split.output_tokens +
    split.cache_write_tokens +
    split.cache_read_tokens;
  const title = MIX_SEGMENTS.map(
    (s) => `${s.label} ${formatTokens(split[s.key])}`,
  ).join(" · ");
  return (
    <div
      className={cn(
        "flex h-1.5 w-full overflow-hidden rounded-full bg-secondary",
        className,
      )}
      title={title}
    >
      {total > 0
        ? MIX_SEGMENTS.map((s) => {
            const value = split[s.key];
            if (value <= 0) return null;
            return (
              <div
                key={s.key}
                className={s.tint}
                style={{ width: `${(value / total) * 100}%` }}
              />
            );
          })
        : null}
    </div>
  );
}

/** The four explicit token figures (in / out / cache-w / cache-r). */
export function TokenFigures({
  split,
  className,
}: {
  split: TokenSplit;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "flex flex-wrap gap-x-3 gap-y-0.5 font-mono text-xs tabular-nums text-muted-foreground",
        className,
      )}
    >
      <span>in <Tk value={split.input_tokens} /></span>
      <span>out <Tk value={split.output_tokens} /></span>
      <span>cache-w <Tk value={split.cache_write_tokens} /></span>
      <span>cache-r <Tk value={split.cache_read_tokens} /></span>
    </div>
  );
}

export type Checks = { passing: number; failing: number; pending: number };

export function CheckSummary({ checks }: { checks?: Checks | null }) {
  if (!checks) {
    return <span className="text-muted-foreground">—</span>;
  }
  const { passing, failing, pending } = checks;
  const tone =
    failing > 0
      ? "text-red-600 dark:text-red-400"
      : pending > 0
        ? "text-amber-600 dark:text-amber-400"
        : "text-green-600 dark:text-green-400";
  return (
    <span className={cn("inline-flex items-center gap-1.5 font-mono text-xs", tone)}>
      <Icon
        name={failing > 0 ? "x" : pending > 0 ? "clock" : "check"}
        size={13}
        strokeWidth={2}
      />
      {passing}✓ {failing}✕ {pending}⋯
    </span>
  );
}
