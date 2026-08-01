import { FormEvent, useCallback, useEffect, useState } from "react";

type Event = {
  id: number;
  name: string;
  capacity: number;
  created_at: string;
};

export function App() {
  const [events, setEvents] = useState<Event[]>([]);
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    const response = await fetch("/events");
    if (!response.ok) throw new Error("Could not load events");
    setEvents(await response.json());
  }, []);

  useEffect(() => {
    load().catch((reason: Error) => setError(reason.message));
  }, [load]);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    const data = new FormData(event.currentTarget);
    const response = await fetch("/events", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: data.get("name"), capacity: Number(data.get("capacity")) }),
    });
    if (!response.ok) {
      setError("Could not create event");
      return;
    }
    event.currentTarget.reset();
    await load();
  }

  return (
    <main>
      <h1>EventDesk</h1>
      <form onSubmit={submit}>
        <label>
          Event name
          <input name="name" required />
        </label>
        <label>
          Capacity
          <input name="capacity" type="number" min="1" required />
        </label>
        <button type="submit">Create event</button>
      </form>
      {error && <p role="alert">{error}</p>}
      <ul>
        {events.map((event) => (
          <li key={event.id}>
            <strong>{event.name}</strong> — {event.capacity} seats
          </li>
        ))}
      </ul>
    </main>
  );
}
