import { Icon } from "@/components/ui/icon";
import { exactInt, formatTokens } from "@/lib/format";
import { cn } from "@/lib/utils";

/** Abbreviated token count with the exact value in the title attribute. */
export function Tk({ value }: { value: number | null | undefined }) {
  return <span title={exactInt(value)}>{formatTokens(value)}</span>;
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
