import { Link, Route, Routes } from "react-router";

import { FilterBar } from "@/components/dashboard/FilterBar";
import { LiveDot } from "@/components/dashboard/StatusBadge";
import { ThemeToggle } from "@/components/dashboard/ThemeToggle";
import { FiltersProvider } from "@/lib/filters";
import { useTheme } from "@/lib/useTheme";
import { HomePage } from "@/pages/HomePage";
import { IssuePage } from "@/pages/IssuePage";

function Wordmark() {
  return (
    <div className="flex items-center gap-2">
      <span className="flex h-7 w-7 items-center justify-center rounded-lg bg-blue-600 text-white">
        <svg
          width="15"
          height="15"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
        >
          <path d="M5 14v-4M9.5 18V6M14 16V8M18.5 13v-2" />
        </svg>
      </span>
      <span className="text-sm font-semibold tracking-tight">symphony</span>
    </div>
  );
}

export function App() {
  const { dark, toggle } = useTheme();

  return (
    <FiltersProvider>
      <div className="min-h-screen bg-background text-foreground">
        <header className="sticky top-0 z-20 border-b border-border bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80">
          <div className="mx-auto flex h-14 w-full max-w-[1200px] items-center justify-between gap-4 px-4 sm:px-6 lg:px-8">
            <Link to="/" className="transition-opacity hover:opacity-80">
              <Wordmark />
            </Link>
            <div className="flex items-center gap-2">
              <span className="hidden items-center gap-1.5 rounded-md border border-border px-2 py-1 text-xs text-muted-foreground sm:inline-flex">
                <LiveDot tone="bg-green-500" /> daemon · loopback
              </span>
              <ThemeToggle dark={dark} onToggle={toggle} />
            </div>
          </div>
        </header>

        <FilterBar />

        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/issue/:id" element={<IssuePage />} />
        </Routes>
      </div>
    </FiltersProvider>
  );
}
