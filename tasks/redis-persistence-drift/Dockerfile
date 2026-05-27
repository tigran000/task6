FROM us-central1-docker.pkg.dev/bespokelabs/nebula-devops-registry/nebula-devops:1.1.0

# Pre-cache kubectl image at build time so setup.sh can import it into
# k3s with no network at test time. v55/v57 rollouts confirmed the
# runtime-pull approach failed 100% — Bitnami deprecated free Docker
# Hub images in 2024 (`bitnami/kubectl:1.28` returns 404/401 from
# docker.io), and the air-gapped k3s node can't reach the registry
# anyway. Their free archive lives at `bitnamilegacy/`; pull from
# there at build time when the registry IS reachable.
# Download crane (~10MB self-contained Go binary, no daemon) if absent,
# pull the image to a local tarball, then setup.sh imports via
# `k3s ctr images import`.
RUN set -eux; \
    install -d /workdir/images; \
    if ! command -v crane >/dev/null 2>&1; then \
      timeout 120 curl --connect-timeout 10 --max-time 90 --retry 2 --retry-delay 5 -fsSL \
        https://github.com/google/go-containerregistry/releases/download/v0.16.1/go-containerregistry_Linux_x86_64.tar.gz \
        | tar -xzC /usr/local/bin/ crane; \
    fi; \
    timeout 300 crane pull bitnamilegacy/kubectl:1.28 /workdir/images/bitnami-kubectl.tar

ENV DISPLAY_NUM=1
ENV COMPUTER_HEIGHT_PX=768
ENV COMPUTER_WIDTH_PX=1024

ENV ALLOWED_NAMESPACES="bleater,monitoring,argocd,gitea"
