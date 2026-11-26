# Releases

stratus follows [semantic versioning](https://semver.org). Only maintainers make releases, by
running the `release` workflow by hand or by pushing a tag.

## The emulator

| Push | Publishes |
|---|---|
| a commit to `main` | the image tagged `edge` |
| a tag `v1.2.3`, or a run by hand | the images `1.2.3`, `1.2`, `1` and `latest`, and a GitHub release with generated notes |

The `release` workflow builds for `linux/amd64` and `linux/arm64` with an SBOM and provenance.
It pushes to GHCR (`ghcr.io/stratus-emulator/stratus`), and to Docker Hub
(`stratusemulator/stratus`) once the `DOCKERHUB_TOKEN` secret is set. It also copies
`.github/dockerhub.md` to the Docker Hub page. Image tags are versions only, never commit hashes.

### By hand

In **Actions → release → Run workflow**, on `main`, choose which part of the newest version to bump
(`patch`, `minor` or `major`) and write a short summary of what changed for users. The workflow
publishes the images first, then creates the tag on the commit it built and the GitHub release: the
summary, then the notes generated from the merged pull requests, grouped as `.github/release.yml`
says.

### With a tag

```sh
git tag -a v0.2.0 -m "stratus 0.2.0"
git push origin v0.2.0
```

Then edit the GitHub release: put a short summary of what changed for users above the generated
notes. A tag with a suffix, such as `v1.0.0-rc.1`, makes a pre-release and does not move `latest`.

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
