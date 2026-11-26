# Releases

stratus follows [semantic versioning](https://semver.org). Releases are made by pushing a tag,
which only maintainers can do.

## The emulator

| Push | Publishes |
|---|---|
| a commit to `main` | the image tagged `edge` |
| a tag `v1.2.3` | the images `1.2.3`, `1.2`, `1` and `latest`, and a GitHub release with generated notes |

The `release` workflow builds for `linux/amd64` and `linux/arm64` with an SBOM and provenance.
It pushes to GHCR (`ghcr.io/stratus-emulator/stratus`), and to Docker Hub
(`stratusemulator/stratus`) once the `DOCKERHUB_TOKEN` secret is set. It also copies
`.github/dockerhub.md` to the Docker Hub page. Image tags are versions only, never commit hashes.

```sh
git tag -a v0.2.0 -m "stratus 0.2.0"
git push origin v0.2.0
```

Then edit the GitHub release: put a short summary of what changed for users above the generated
notes.

## The Testcontainers module

`testcontainers/` is a Go module of its own, with tags `testcontainers/vX.Y.Z`. Its `DefaultImage`
names the emulator release it is tested with. After an emulator release that the module should
default to, bump `DefaultImage` in a pull request, then tag the module:

```sh
git tag -a testcontainers/v0.2.0 -m "testcontainers module 0.2.0"
git push origin testcontainers/v0.2.0
```

## This site

The `docs` workflow builds the site with `mkdocs build --strict` on every pull request that
touches `docs/` or `mkdocs.yml`. From `main` it publishes the site to GitHub Pages, at
[stratus-emulator.github.io/stratus](https://stratus-emulator.github.io/stratus/). The site
describes `main`, so a feature merged before its release is marked *Since x.y.z*.
