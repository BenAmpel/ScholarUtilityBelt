# Contributing

Thanks for your interest in Scholar Utility Belt.

## How to contribute

- Open an issue for bug reports, regressions, feature requests, or questions about extension behavior.
- Include the Scholar page type involved (`results`, `author profile`, `library`, `options`, or `popup`) and the browser you tested with.
- For UI bugs, screenshots and a short reproduction sequence are especially helpful.
- For data-source updates, please cite the upstream source and the date/version you used.

## Development expectations

- Keep changes local-first when possible. Features that can run from the current DOM or packaged data should not be moved to network-dependent implementations without a strong reason.
- Bound external requests with caching, timeouts, and graceful failure paths.
- Preserve existing user-facing behavior unless the change is explicitly intended to alter it.
- Add or update smoke-test coverage when touching major Scholar flows.

## Pull requests

- Keep pull requests focused and explain the user-facing effect of the change.
- Mention any new external data source, permission, or network dependency.
- Note any manual verification performed, especially for Scholar result pages and author profiles.

## Security

Please report security-sensitive issues privately rather than opening a public issue with a working exploit.
