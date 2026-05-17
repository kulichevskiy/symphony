import { useParams } from "react-router";

export function IssuePage() {
  const { id } = useParams();

  return (
    <main className="flex min-h-screen items-center justify-center bg-background px-6 text-foreground">
      <h1 className="text-2xl font-semibold tracking-normal">Issue {id}</h1>
    </main>
  );
}
