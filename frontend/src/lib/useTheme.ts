import { useEffect, useState } from "react";

const STORAGE_KEY = "sym-theme";

function initialDark(): boolean {
  if (typeof localStorage !== "undefined") {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) {
      return saved === "dark";
    }
  }
  return true; // dark by default
}

export function useTheme(): { dark: boolean; toggle: () => void } {
  const [dark, setDark] = useState(initialDark);

  useEffect(() => {
    const root = document.documentElement;
    root.classList.toggle("dark", dark);
    root.style.colorScheme = dark ? "dark" : "light";
    localStorage.setItem(STORAGE_KEY, dark ? "dark" : "light");
  }, [dark]);

  return { dark, toggle: () => setDark((v) => !v) };
}
