import { QueryClient } from "@tanstack/react-query";

export const queryClient = new QueryClient({
  // A small default staleTime keeps a tab refocus from refiring the ~6
  // dashboard queries that were just polled — each query still refetches on
  // its own refetchInterval.
  defaultOptions: { queries: { staleTime: 5_000 } },
});
