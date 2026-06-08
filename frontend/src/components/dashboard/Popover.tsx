import { useEffect, useRef, useState, type ReactNode } from "react";

import { Icon } from "@/components/ui/icon";
import { cn } from "@/lib/utils";

/** A filter-chip button: label, optional active-value summary, and a caret.
 *  Shared across FilterBar filters so they read as one control family. */
export function FilterTrigger({
  label,
  value,
  active = false,
  open = false,
  onClick,
}: {
  label: string;
  value?: string;
  active?: boolean;
  open?: boolean;
  onClick?: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-expanded={open}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-md border px-2.5 py-1 text-xs font-medium transition-colors",
        active
          ? "border-border bg-background text-foreground shadow-sm"
          : "border-border bg-secondary/60 text-muted-foreground hover:text-foreground",
      )}
    >
      <span>{label}</span>
      {value ? <span className="text-foreground">{value}</span> : null}
      <Icon
        name="chevronRight"
        size={12}
        className="transition-transform"
        style={{ transform: open ? "rotate(-90deg)" : "rotate(90deg)" }}
      />
    </button>
  );
}

/** A trigger + a floating panel that closes on outside click / Escape. The
 *  trigger is a render prop so callers reuse `FilterTrigger` with live state. */
export function Popover({
  trigger,
  children,
  align = "start",
}: {
  trigger: (state: { open: boolean; toggle: () => void }) => ReactNode;
  children: ReactNode;
  align?: "start" | "end";
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const onPointer = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", onPointer);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onPointer);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  return (
    <div ref={ref} className="relative">
      {trigger({ open, toggle: () => setOpen((o) => !o) })}
      {open ? (
        <div
          className={cn(
            "absolute z-30 mt-1.5 min-w-[12rem] rounded-md border border-border bg-background p-1.5 shadow-md",
            align === "end" ? "right-0" : "left-0",
          )}
        >
          {children}
        </div>
      ) : null}
    </div>
  );
}
