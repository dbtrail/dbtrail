#!/usr/bin/env bash
# Attach syft-generated SBOMs to a published GHCR image as cosign
# attestations (#976). Invoked by the sbom-attestation entry under
# `docker_signs` in build/.goreleaser.yaml, during GoReleaser's publish phase,
# with the manifest reference and digest GoReleaser just pushed:
#
#   attest-image-sbom.sh <manifest-ref> <sha256:digest>
#
# One SPDX SBOM is generated per architecture present in the manifest list
# and attested against the manifest-list DIGEST. Cosign attestations are
# digest-addressed, so `cosign verify-attestation` on any tag that resolves
# to that digest (:X.Y.Z and :latest alike) finds them.
#
# Requires: docker (with buildx and registry credentials), syft, cosign —
# all present in the release workflow before GoReleaser runs.
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <manifest-ref> <sha256:digest>" >&2
  exit 1
fi
ref="$1"
digest="$2"

# The :latest manifest lists the same per-arch images as the version
# manifest — same digest — so attesting it again would only upload duplicate
# attestation layers to the same .att tag. Skip it; verification against
# :latest still succeeds because it resolves to the already-attested digest.
case "$ref" in
  *:latest)
    echo "attest-image-sbom: skipping ${ref} (same digest as the version manifest, already attested)"
    exit 0
    ;;
esac

# Discover the platforms actually present in the manifest list instead of
# hardcoding the arch matrix, so an arch added in build/.goreleaser.yaml gets an
# SBOM here without touching this script. Attestation manifests (platform
# "unknown/unknown") are filtered out; empty lines come from the template's
# trailing newline.
platforms=$(docker buildx imagetools inspect "${ref}@${digest}" \
  --format '{{range .Manifest.Manifests}}{{.Platform.OS}}/{{.Platform.Architecture}}{{"\n"}}{{end}}' \
  | grep -v -e '^unknown/' -e '^$' || true)
if [ -z "$platforms" ]; then
  echo "attest-image-sbom: no platform manifests found in ${ref}@${digest}" >&2
  exit 1
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

for platform in $platforms; do
  sbom="${tmpdir}/sbom-${platform//\//-}.spdx.json"
  echo "attest-image-sbom: generating SPDX SBOM for ${ref}@${digest} (${platform})"
  syft scan "registry:${ref}@${digest}" --platform "$platform" -o "spdx-json=${sbom}"
  echo "attest-image-sbom: attesting SBOM (${platform}) on ${ref}@${digest}"
  cosign attest --yes --type spdxjson --predicate "$sbom" "${ref}@${digest}"
done
