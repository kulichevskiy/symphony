import { Route, Routes } from "react-router";

import { HomePage } from "@/pages/HomePage";
import { IssuePage } from "@/pages/IssuePage";

export function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/issue/:id" element={<IssuePage />} />
    </Routes>
  );
}
