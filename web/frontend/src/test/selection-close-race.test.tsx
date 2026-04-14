import {act, render, screen} from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import App from '../App.tsx'

vi.mock('../MapView.tsx', () => ({
  default: ({onFeatureSelect}: {onFeatureSelect: (entityType: 'artist', rowid: number) => void}) => (
    <button type="button" onClick={() => onFeatureSelect('artist', 1)}>
      Select artist
    </button>
  ),
}))

// Lets the test hold and later release one async step, so we can reproduce
// "user closed the panel before the selection payload settled" deterministically.
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return {promise, resolve, reject}
}

describe('App', () => {
  beforeEach(() => {
    window.location.hash = ''
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('does not reopen the panel when a late selection settles after close', async () => {
    const user = userEvent.setup()
    // `fetch()` resolves immediately, but `response.json()` stays pending until
    // the test decides to release it. That creates the stale-commit window.
    const jsonDeferred = deferred<{
      artist_rowid: number
      artist_name: string
      lon: number
      lat: number
      logcount: number
      ntrack: number
      nalbum: number
      nrepr: number
      artist_genre: string | null
      reprs: []
    }>()

    const fetchMock = vi.fn(() =>
      Promise.resolve({
        ok: true,
        statusText: 'OK',
        json: () => jsonDeferred.promise,
      })
    )
    vi.stubGlobal('fetch', fetchMock)

    render(<App/>)

    // Open a selection, then close the panel before the delayed JSON payload is
    // allowed to settle.
    await user.click(screen.getByRole('button', {name: 'Select artist'}))
    expect(screen.getByRole('button', {name: 'Close'})).toBeInTheDocument()

    await user.click(screen.getByRole('button', {name: 'Close'}))
    expect(screen.queryByRole('button', {name: 'Close'})).not.toBeInTheDocument()

    // A late settlement must not recreate the closed panel.
    await act(async () => {
      jsonDeferred.resolve({
        artist_rowid: 1,
        artist_name: 'Late Artist',
        lon: 12.34,
        lat: 45.67,
        logcount: 3,
        ntrack: 10,
        nalbum: 2,
        nrepr: 0,
        artist_genre: null,
        reprs: [],
      })
      await jsonDeferred.promise
    })

    expect(screen.queryByRole('button', {name: 'Close'})).not.toBeInTheDocument()
    expect(screen.queryByText('Late Artist')).not.toBeInTheDocument()
  })
})
