CREATE TABLE videos (
  id TEXT PRIMARY KEY,
  width BIGINT NOT NULL,
  height BIGINT NOT NULL,
  fps BIGINT NOT NULL
);

CREATE TABLE images (
  id TEXT PRIMARY KEY,
  width BIGINT NOT NULL,
  height BIGINT NOT NULL,
  video_id TEXT,
  frame BIGINT DEFAULT 0,
  FOREIGN KEY(video_id) REFERENCES videos(id)
);

CREATE TABLE pixels (
  id BIGSERIAL PRIMARY KEY,
  image_id TEXT NOT NULL,
  x BIGINT NOT NULL,
  y BIGINT NOT NULL,
  r BIGINT NOT NULL CHECK(r >= 0 AND r <= 255),
  g BIGINT NOT NULL CHECK(g >= 0 AND g <= 255),
  b BIGINT NOT NULL CHECK(b >= 0 AND b <= 255),
  FOREIGN KEY(image_id) REFERENCES images(id),
  UNIQUE(image_id, x, y)
);

CREATE TABLE audio_tracks (
  id TEXT PRIMARY KEY,
  sample_rate BIGINT NOT NULL,
  format TEXT NOT NULL
);
CREATE TABLE audio_track_samples (
  value BIGINT NOT NULL,
  position BIGINT NOT NULL,
  audio_track_id TEXT,
  FOREIGN KEY(audio_track_id) REFERENCES audio_tracks(id)
);
