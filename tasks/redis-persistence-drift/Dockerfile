FROM us-central1-docker.pkg.dev/bespokelabs/nebula-devops-registry/nebula-devops:1.1.0

# Pre-cache bitnami/kubectl:1.28 at build time so setup.sh can import it
# into k3s with no network at test time. v55 rollouts confirmed the
# previous runtime-pull approach (kubectl run kubectl-prepull) failed
# 100% in the air-gapped test environment, leaving the
# redis-config-watchdog CronJob dead-on-arrival and silently reducing
# a1's variance signal (one of the planted reverters never actually ran).
# Download crane (~10MB self-contained Go binary, no daemon) if absent,
# pull the image to a local tarball, then setup.sh imports via
# `k3s ctr images import`.
RUN set -eux; \
    install -d /workdir/images; \
    if ! command -v crane >/dev/null 2>&1; then \
      curl -sL https://github.com/google/go-containerregistry/releases/download/v0.16.1/go-containerregistry_Linux_x86_64.tar.gz \
        | tar -xzC /usr/local/bin/ crane; \
    fi; \
    crane pull bitnami/kubectl:1.28 /workdir/images/bitnami-kubectl.tar

ENV DISPLAY_NUM=1
ENV COMPUTER_HEIGHT_PX=768
ENV COMPUTER_WIDTH_PX=1024

ENV ALLOWED_NAMESPACES="bleater,monitoring,argocd,gitea"
