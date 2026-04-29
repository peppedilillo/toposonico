import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import App from "../App.tsx";
import type { Entity } from "../types.ts";

vi.mock("../MapView.tsx", () => ({
  default: ({
    onFeatureSelect,
  }: {
    onFeatureSelect: (entity: Entity) => void;
  }) => {
    return (
      <>
        <button
          type="button"
          onClick={() =>
            onFeatureSelect({
              entity_type: "artist",
              rowid: 1,
              lon: 10,
              lat: 15,
              logcount: 3,
            })
          }
        >
          Select artist 1
        </button>
        <button
          type="button"
          onClick={() =>
            onFeatureSelect({
              entity_type: "artist",
              rowid: 2,
              lon: 20,
              lat: 30,
              logcount: 4,
            })
          }
        >
          Select artist 2
        </button>
      </>
    );
  },
}));

// Lets the test release each response payload at a specific moment, so we can
// force one selection to settle after a newer one has already become current.
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

type ArtistPayload = {
  entity_type: "artist";
  rowid: number;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number;
  ntrack: number;
  nalbum: number;
  nrepr: number;
  artist_genre: string | null;
  reprs: [];
};

describe("App", () => {
  beforeEach(() => {
    window.location.hash = "";
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("does not let a late selection overwrite a newer one", async () => {
    const user = userEvent.setup();
    const firstJson = deferred<ArtistPayload>();
    const secondJson = deferred<ArtistPayload>();

    const fetchMock = vi.fn((input: string | URL | Request) => {
      const url = String(input);
      if (url.includes("rowid=1")) {
        return Promise.resolve({
          ok: true,
          statusText: "OK",
          json: () => firstJson.promise,
        });
      }
      if (url.includes("rowid=2")) {
        return Promise.resolve({
          ok: true,
          statusText: "OK",
          json: () => secondJson.promise,
        });
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    // Start selection 1, then replace it with selection 2 before selection 1's
    // delayed JSON payload is allowed to settle.
    await user.click(screen.getByRole("button", { name: "Select artist 1" }));
    await user.click(screen.getByRole("button", { name: "Select artist 2" }));

    // Selection 2 settles first and becomes the visible current selection.
    await act(async () => {
      secondJson.resolve({
        entity_type: "artist",
        rowid: 2,
        artist_name: "Current Artist",
        lon: 20,
        lat: 30,
        logcount: 4,
        ntrack: 8,
        nalbum: 3,
        nrepr: 0,
        artist_genre: null,
        reprs: [],
      });
      await secondJson.promise;
    });

    expect(screen.getByText("Current Artist")).toBeInTheDocument();
    expect(screen.queryByText("Stale Artist")).not.toBeInTheDocument();

    // A late settlement from selection 1 must not overwrite the already-visible
    // newer selection.
    await act(async () => {
      firstJson.resolve({
        entity_type: "artist",
        rowid: 1,
        artist_name: "Stale Artist",
        lon: 10,
        lat: 15,
        logcount: 3,
        ntrack: 5,
        nalbum: 1,
        nrepr: 0,
        artist_genre: null,
        reprs: [],
      });
      await firstJson.promise;
    });

    expect(screen.getByText("Current Artist")).toBeInTheDocument();
    expect(screen.queryByText("Stale Artist")).not.toBeInTheDocument();
  });
});
