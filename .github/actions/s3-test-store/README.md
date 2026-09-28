# S3-compatible test store

The integration tests check the S3 code against a real S3-compatible server,
MinIO. This action starts it, and `../s3-test-store-check` fails the job when
that leg did not run. `ci.yml` (the `integration` job) and `release.yaml` (the
`integration-mysql` job) both use the two actions, so the image is named in
one place: the `image` default in `action.yml`.

When the store cannot start, the other integration tests still run, and the
job is still red. A missing store is never a silent skip.

## The image

| | |
|---|---|
| What | MinIO server `RELEASE.2025-10-15T17-29-55Z`, linux/amd64 and linux/arm64 |
| Digest | `sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9` (the pin in `action.yml` is what CI uses) |
| Built by | `ghcr.io/coollabsio/minio` (a community build of the upstream server) |
| Our copy | `ghcr.io/dbtrail/ci-minio`, same tag, same digest |

The upstream images were withdrawn in September 2026 (Docker Hub first,
then quay.io). The copy in this project's registry
namespace keeps CI working if the community build goes away too.

## Licence

MinIO is licensed under the GNU AGPL v3.0 (the image says so in its
`org.opencontainers.image.licenses` label). The copy is the community build,
unmodified, byte for byte: the digest is the same. It exists only so this
project's CI can run tests against it. bintrail does not ship it, link to it,
or include it in any release artifact.

Source code for this exact build:

- MinIO server: https://github.com/minio/minio at tag
  `RELEASE.2025-10-15T17-29-55Z`, commit
  `da2e68be8d27b8fc647e4849048e9b39990407c6` (the image's
  `org.opencontainers.image.source` and `org.opencontainers.image.revision`
  labels).
- The base layer is Red Hat Universal Base Image 9 Micro, under the
  [UBI end user licence](https://www.redhat.com/en/about/red-hat-end-user-license-agreements#UBI),
  which allows redistribution.

## Refreshing the copy

The copy is made by `.github/workflows/mirror-ci-images.yml`. It runs only by
hand:

```sh
gh workflow run mirror-ci-images.yml -R dbtrail/dbtrail --ref main
```

It copies the pinned digest, checks the copy has the same digest and both
architectures, and checks that anybody can pull it without logging in (pull
requests from forks need that). It never moves a tag that already exists.

A new package on ghcr.io starts private. If the last check fails, set the
`ci-minio` package to Public in the organization's package settings, link it
to this repository, and run the workflow again.

To move to another MinIO build:

1. On a branch, change `TAG` and `DIGEST` in `mirror-ci-images.yml`, and the
   `image` default in `action.yml` to `ghcr.io/dbtrail/ci-minio:<tag>@<digest>`.
   The tag and digest must be the same in both files, or the
   `workflow-lockstep` job fails (`scripts/check-s3-store-lockstep.sh`).
2. Copy it before the pull request's CI needs it:
   `gh workflow run mirror-ci-images.yml -R dbtrail/dbtrail --ref <branch>`
3. Re-run the pull request's CI.
4. Update the table and the source links in this file.

Read the tag's digest without Docker:
`crane digest ghcr.io/coollabsio/minio:<tag>`.
