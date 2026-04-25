import "@testing-library/jest-dom";

Object.assign(import.meta.env, {
  VITE_SOURCE_MAX_ZOOM: "14",
  VITE_BASE_ZOOM_TRACK: "14",
  VITE_BASE_ZOOM_ALBUM: "11",
  VITE_BASE_ZOOM_ARTIST: "11",
  VITE_BASE_ZOOM_LABEL: "12",
});

Object.defineProperty(window.HTMLElement.prototype, "scrollIntoView", {
  value: () => {},
  writable: true,
});
