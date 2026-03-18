import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./style.css";
import App from "./App.jsx";
import colors from "./theme.js";

Object.entries(colors).forEach(([k, v]) =>
    document.documentElement.style.setProperty(`--color-${k}`, v),
);

createRoot(document.getElementById("root")).render(
    <StrictMode>
        <App />
    </StrictMode>,
);
