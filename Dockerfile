# Single-image pipeline: go-music-dl (acquire+web) + um (decrypt) + music_tag (metadata) + orchestrator.
# State lives only in ./data volume + the /music mount.
FROM alpine:3.22

RUN apk --no-cache add ca-certificates tzdata ffmpeg gcompat python3 py3-pip \
    && ffmpeg -version >/dev/null \
    && ffprobe -version >/dev/null

ENV TZ=Asia/Shanghai

# go-music-dl static binary
COPY --from=guohuiyuan/go-music-dl:latest /home/appuser/music-dl /usr/local/bin/music-dl

# um (unlock-music CLI) + orchestrator
COPY bin/um /usr/local/bin/um
COPY scripts/music_pipeline.py /opt/music_pipeline.py

RUN python3 -m pip install --no-cache-dir --break-system-packages music_tag

WORKDIR /home/appuser
EXPOSE 8080
CMD ["music-dl", "web", "--port", "8080", "--no-browser"]
