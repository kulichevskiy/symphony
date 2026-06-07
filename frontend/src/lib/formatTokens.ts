export function formatTokens(value: number | null | undefined): string {
  const count = value ?? 0;
  const absCount = Math.abs(count);

  if (absCount >= 1_000_000_000) {
    return `${formatCompact(count / 1_000_000_000)}B`;
  }
  if (absCount >= 1_000_000) {
    return `${formatCompact(count / 1_000_000)}M`;
  }
  if (absCount >= 1_000) {
    return `${formatCompact(count / 1_000)}k`;
  }
  return String(count);
}

function formatCompact(value: number): string {
  const rounded = value >= 100 ? Math.round(value) : Number(value.toFixed(1));
  return String(rounded);
}
