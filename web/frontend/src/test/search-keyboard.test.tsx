import {
  act,
  createEvent,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import Search from "../Search.tsx";

const SEARCH_RESULTS = [
  {
    entity_type: "artist" as const,
    rowid: 1,
    artist_name: "Result Artist",
    lon: 10,
    lat: 20,
    logcount: 3,
  },
  {
    entity_type: "label" as const,
    rowid: 2,
    label: "Result Label",
    lon: 30,
    lat: 40,
    logcount: 4,
  },
];

function stubSearchFetch() {
  vi.stubGlobal(
    "fetch",
    vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve(SEARCH_RESULTS),
      }),
    ),
  );
}

describe("Search", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  async function renderOpenSearch() {
    vi.useFakeTimers();
    const navigate = vi.fn();
    stubSearchFetch();

    render(<Search navigate={navigate} panelOpen={false} />);

    const input = screen.getByRole("combobox");
    fireEvent.change(input, { target: { value: "res" } });

    await act(async () => {
      vi.advanceTimersByTime(300);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByRole("listbox")).toBeInTheDocument();
    return { input, navigate };
  }

  it("prevents caret-moving arrow behavior when dropdown navigation handles the keys", async () => {
    const { input } = await renderOpenSearch();

    const arrowDown = createEvent.keyDown(input, { key: "ArrowDown" });
    fireEvent(input, arrowDown);
    expect(arrowDown.defaultPrevented).toBe(true);

    const arrowUp = createEvent.keyDown(input, { key: "ArrowUp" });
    fireEvent(input, arrowUp);
    expect(arrowUp.defaultPrevented).toBe(true);
  });

  it("exposes combobox and listbox semantics for the active option", async () => {
    const { input } = await renderOpenSearch();

    expect(input).toHaveAttribute("aria-expanded", "true");
    const listbox = screen.getByRole("listbox");
    expect(input).toHaveAttribute("aria-controls", listbox.id);

    fireEvent.keyDown(input, { key: "ArrowDown" });

    const firstOption = screen.getByRole("option", { name: /result artist/i });
    const secondOption = screen.getByRole("option", { name: /result label/i });
    expect(input).toHaveAttribute("aria-activedescendant", firstOption.id);
    expect(firstOption).toHaveAttribute("aria-selected", "true");
    expect(secondOption).toHaveAttribute("aria-selected", "false");
  });

  it("closes the popup when focus leaves the search container", async () => {
    const { input } = await renderOpenSearch();

    const outsideButton = document.createElement("button");
    outsideButton.textContent = "Outside";
    document.body.appendChild(outsideButton);

    fireEvent.blur(input, { relatedTarget: outsideButton });

    expect(screen.queryByRole("listbox")).not.toBeInTheDocument();
    expect(input).toHaveAttribute("aria-expanded", "false");

    outsideButton.remove();
  });
});
