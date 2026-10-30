# The workflow supplies the previously tested Linux binaries as the context.
FROM gcr.io/distroless/static-debian12:nonroot
ARG TARGETARCH
COPY --chmod=0755 linux-${TARGETARCH}/xray /usr/local/bin/xray
COPY licenses/ /usr/share/licenses/xray-shine/
ENV XRAY_LOCATION_ASSET=/usr/local/share/xray
ENTRYPOINT ["/usr/local/bin/xray"]
CMD ["run", "-config", "/etc/xray/config.json"]
