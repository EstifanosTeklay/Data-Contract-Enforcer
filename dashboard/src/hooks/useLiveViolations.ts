import { useEffect, useRef, useState } from "react";
import { Violation } from "../types";

export interface LiveEvent {
  id: string;
  violation: Violation;
}

export function useLiveViolations() {
  const [events, setEvents] = useState<LiveEvent[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const sourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    const source = new EventSource("http://localhost:8000/api/live-violations");
    sourceRef.current = source;

    source.onopen = () => setIsConnected(true);
    source.onerror = () => setIsConnected(false);

    source.onmessage = (event) => {
      try {
        const parsed = JSON.parse(event.data) as Violation;
        setEvents((prev) => [
          {
            id: `${parsed.violation_id}-${Date.now()}`,
            violation: parsed
          },
          ...prev
        ].slice(0, 25));
      } catch {
        // Ignore invalid payloads.
      }
    };

    return () => {
      source.close();
    };
  }, []);

  return {
    events,
    latest: events[0]?.violation,
    isConnected
  };
}
