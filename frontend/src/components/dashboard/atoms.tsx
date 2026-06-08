import { Icon } from "@/components/ui/icon";
import type { TokenSplit } from "@/lib/api";
import { exactInt, formatTokens } from "@/lib/format";
import { cn } from "@/lib/utils";

/** Abbreviated token count with the exact value in the title attribute. */
export function Tk({ value }: { value: number | null | undefined }) {
  return <span title={exactInt(value)}>{formatTokens(value)}</span>;
}

/**
 * Token categories — one shared palette + legend used by the stat rail, the mix
 * legend, and every breakdown bar, so the whole overview speaks one visual
 * language (Dashboard v2). `label` doubles as the short legend caption.
 */
export const TOKEN_CATS = [
  { key: "input_tokens", label: "in", swatch: "bg-blue-500" },
  { key: "output_tokens", label: "out", swatch: "bg-violet-500" },
  { key: "cache_write_tokens", label: "cache-write", swatch: "bg-cyan-500" },
  {
    key: "cache_read_tokens",
    label: "cache-read",
    swatch: "bg-slate-300 dark:bg-slate-600",
  },
] as const satisfies ReadonlyArray<{
  key: keyof TokenSplit;
  label: string;
  swatch: string;
}>;

/**
 * Provider dot palette (Dashboard v2). Shared so every consumer speaks one
 * color language. Unknown providers fall back to the slate dot at the call site.
 */
export const PROVIDER_TINT: Record<string, string> = {
  codex: "bg-blue-500",
  claude: "bg-violet-500",
};

/**
 * Per-row stacked token mix-bar. Segments are always sized by this row's own
 * raw-token proportions of in / out / cache-write / cache-read.
 *
 * - `mode="composition"` (default): every bar fills the full track, so only the
 *   segment proportions vary. Used where magnitude is read from the numbers.
 * - `mode="magnitude"`: the bar's *total length* encodes this row's token total
 *   relative to `maxTotal` (the largest row), so length = sum of all tokens.
 */
export function MixBar({
  split,
  mode = "composition",
  maxTotal,
  className,
}: {
  split: TokenSplit;
  mode?: "composition" | "magnitude";
  maxTotal?: number;
  className?: string;
}) {
  const total =
    split.input_tokens +
    split.output_tokens +
    split.cache_write_tokens +
    split.cache_read_tokens;
  const denom = total || 1;
  const scale =
    mode === "magnitude" && maxTotal && maxTotal > 0 ? total / maxTotal : 1;
  const title = TOKEN_CATS.map(
    (s) => `${s.label} ${formatTokens(split[s.key])}`,
  ).join(" · ");
  return (
    <div
      className={cn(
        "h-2.5 w-full overflow-hidden rounded-full bg-secondary/70",
        className,
      )}
      title={title}
    >
      <div className="flex h-full" style={{ width: `${scale * 100}%` }}>
        {TOKEN_CATS.map((s) => {
          const value = split[s.key];
          if (value <= 0) return null;
          return (
            <div
              key={s.key}
              className={s.swatch}
              style={{ width: `${(value / denom) * 100}%` }}
            />
          );
        })}
      </div>
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
