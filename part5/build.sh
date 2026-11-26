#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
work="$(mktemp -d)"
trap 'rm -rf -- "$work"' EXIT
export GNUPGHOME="$work/gnupg"
mkdir -m 700 "$GNUPGHOME"
mkdir "$work/context"
package="https://downloads.aws.com/aws-emulator/packages/aws-emulator/LTS/aws-emulator-1.0.490-1-aarch64.rpm"
fingerprint=98BCF336A3BE53B734E967C52E5BF49BD940F624
checksum=49051da4bd32268e4cbff87690bc05bab7ab342c8e6a71e1803fbf59c0062246
curl_flags=(--fail --silent --show-error --location --proto '=https' --proto-redir '=https')
curl "${curl_flags[@]}" "$package" -o "$work/context/aws-emulator.rpm"
curl "${curl_flags[@]}" "$package.sig" -o "$work/aws-emulator.rpm.sig"
curl "${curl_flags[@]}" https://downloads.aws.com/keys/gpg/current.asc -o "$work/downloaded.asc"
(cd "$work/context"; printf '%s  aws-emulator.rpm\n' "$checksum" | shasum -a 256 -c -)
gpg --batch --import "$work/downloaded.asc"
gpg --batch --armor --export "$fingerprint" > "$work/context/aws-publishing.asc"
test -s "$work/context/aws-publishing.asc"
gpg --batch --yes --output "$work/aws-publishing.gpg" --dearmor "$work/context/aws-publishing.asc"
gpgv --keyring "$work/aws-publishing.gpg" "$work/aws-emulator.rpm.sig" "$work/context/aws-emulator.rpm"
cp build/Dockerfile "$work/context/Dockerfile"
docker --host "unix://$HOME/.docker/run/docker.sock" build --no-cache --platform linux/arm64 \
  --tag local-aws-lab-part5-emulator:1.0.490 "$work/context"
docker --host "unix://$HOME/.docker/run/docker.sock" image inspect \
  --format '{{.Id}}' local-aws-lab-part5-emulator:1.0.490
