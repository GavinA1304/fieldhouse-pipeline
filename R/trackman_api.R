# TrackMan Data API client + conversion of its nested JSON into the same
# column names as TrackMan CSVs, so API rows line up with your history.
# Endpoints per TrackMan's "Data API Quick Start" guide.

TM_LOGIN <- "https://login.trackmanbaseball.com/connect/token"
TM_BASE  <- "https://dataapi.trackmanbaseball.com"
.tm <- new.env()

tm_token <- function() {
  if (!is.null(.tm$token) && Sys.time() < .tm$expires) return(.tm$token)
  body <- request(TM_LOGIN) |>
    req_body_form(client_id = env("TRACKMAN_CLIENT_ID"),
                  client_secret = env("TRACKMAN_CLIENT_SECRET"),
                  grant_type = "client_credentials") |>
    with_retry() |>
    req_perform() |>
    resp_body_json()
  .tm$token <- mask_secret(body$access_token)
  .tm$expires <- Sys.time() + (body$expires_in %||% 3600) - 300  # refresh 5 min early
  .tm$token
}

tm_get_text <- function(path) {
  request(TM_BASE) |>
    req_url_path_append(path) |>
    req_auth_bearer_token(tm_token()) |>
    with_retry() |>
    req_perform() |>
    resp_body_string()
}

# Session discovery. The API refuses windows longer than 30 dates, so we
# walk the range in 29-day chunks.
tm_discover <- function(from, to, kind = "game", session_type = "All") {
  from <- as.Date(from); to <- as.Date(to)
  starts <- seq(from, to, by = 29)
  out <- list()
  for (i in seq_along(starts)) {
    s <- starts[i]
    e <- min(s + 28, to)
    body <- toJSON(list(sessionType = session_type,
                        utcDateFrom = paste0(format(s), "T00:00:00.000Z"),
                        utcDateTo = paste0(format(e), "T23:59:59.999Z")), auto_unbox = TRUE)
    txt <- request(TM_BASE) |>
      req_url_path_append("api", "v1", "discovery", kind, "sessions") |>
      req_auth_bearer_token(tm_token()) |>
      req_headers(accept = "text/plain") |>
      req_body_raw(body, type = "application/json-patch+json") |>
      with_retry() |>
      req_perform() |>
      resp_body_string()
    sess <- fromJSON(txt, simplifyVector = FALSE)
    out <- c(out, sess)
  }
  # de-dupe sessions that straddle chunk edges
  ids <- vapply(out, function(s) s$sessionId %||% NA_character_, "")
  out[!duplicated(ids) & !is.na(ids)]
}

# ---- JSON -> CSV-style columns ---------------------------------------------
# Each target lists candidate flattened API paths, first match wins.
# Game balls flatten as "pitch.release.relSpeed"; practice balls as "release.relSpeed".
TM_MAP <- list(
  RelSpeed = c("pitch.release.relSpeed", "release.relSpeed"),
  SpinRate = c("pitch.release.spinRate", "release.spinRate"),
  Extension = c("pitch.release.extension", "release.extension"),
  VertRelAngle = c("pitch.release.vertRelAngle", "release.vertRelAngle"),
  HorzRelAngle = c("pitch.release.horzRelAngle", "release.horzRelAngle"),
  RelHeight = c("pitch.release.relHeight", "release.relHeight"),
  RelSide = c("pitch.release.relSide", "release.relSide"),
  HorzBreak = c("pitch.movement.horzBreak", "trajectory.horzBreak"),
  VertBreak = c("pitch.movement.vertBreak", "trajectory.vertBreak"),
  InducedVertBreak = c("pitch.movement.inducedVertBreak", "trajectory.inducedVertBreak"),
  SpinAxis = c("pitch.movement.spinAxis", "release.spinAxis"),
  Tilt = c("pitch.movement.tilt", "release.tilt"),
  ZoneTime = c("pitch.location.zoneTime", "location.zoneTime"),
  PlateLocHeight = c("pitch.location.plateLocHeight", "location.plateLocHeight"),
  PlateLocSide = c("pitch.location.plateLocSide", "location.plateLocSide"),
  ZoneSpeed = c("pitch.location.zoneSpeed", "location.zoneSpeed"),
  VertApprAngle = c("pitch.location.vertApprAngle", "location.vertApprAngle"),
  HorzApprAngle = c("pitch.location.horzApprAngle", "location.horzApprAngle"),
  SpeedDrop = c("pitch.speedDrop"),
  EffectiveVelo = c("pitch.effectiveVelo", "effVelocity"),
  pfxx = c("pitch.nineP.pfxx", "pfxData.pfxx"), pfxz = c("pitch.nineP.pfxz", "pfxData.pfxz"),
  x0 = c("pitch.nineP.x0.x", "pfxData.x0.x"), y0 = c("pitch.nineP.x0.y", "pfxData.x0.y"),
  z0 = c("pitch.nineP.x0.z", "pfxData.x0.z"),
  vx0 = c("pitch.nineP.v0.x", "pfxData.v0.x"), vy0 = c("pitch.nineP.v0.y", "pfxData.v0.y"),
  vz0 = c("pitch.nineP.v0.z", "pfxData.v0.z"),
  ax0 = c("pitch.nineP.a0.x", "pfxData.a0.x"), ay0 = c("pitch.nineP.a0.y", "pfxData.a0.y"),
  az0 = c("pitch.nineP.a0.z", "pfxData.a0.z"),
  SpinAxis3dTilt = c("pitch.release.spinAxis3dTilt", "release.spinAxis3dTilt"),
  SpinAxis3dActiveSpinRate = c("pitch.release.spinAxis3dActiveSpinRate", "release.spinAxis3dActiveSpinRate"),
  SpinAxis3dSpinEfficiency = c("pitch.release.spinAxis3dSpinEfficiency", "release.spinAxis3dSpinEfficiency"),
  SpinAxis3dTransverseAngle = c("pitch.release.spinAxis3dTransverseAngle", "release.spinAxis3dTransverseAngle"),
  SpinAxis3dLongitudinalAngle = c("pitch.release.spinAxis3dLongitudinalAngle", "release.spinAxis3dLongitudinalAngle"),
  ExitSpeed = c("hit.launch.exitSpeed", "launch.exitSpeed"),
  Angle = c("hit.launch.angle", "launch.angle"),
  Direction = c("hit.launch.direction", "launch.direction"),
  HitSpinRate = c("launch.spinRate"),
  ContactPositionX = c("hit.launch.contactPosition.x", "launch.contactPosition.x"),
  ContactPositionY = c("hit.launch.contactPosition.y", "launch.contactPosition.y"),
  ContactPositionZ = c("hit.launch.contactPosition.z", "launch.contactPosition.z"),
  Distance = c("hit.landingFlat.distance", "landing.distance"),
  Bearing = c("hit.landingFlat.bearing", "landing.bearing"),
  HangTime = c("hit.landingFlat.hangTime", "landing.hangTime"),
  MaxHeight = c("hit.maxHeight"),
  LastTrackedDistance = c("lastTrackedDistance"),
  ThrowSpeed = c("catcherThrow.throw.throwSpeed"),
  TimeToBase = c("catcherThrow.location.timeToBase"),
  # tagged play data
  PitchUID = c("pitchUID"), PlayID = c("playID", "playId"),
  Inning = c("gameState.inning"), `Top/Bottom` = c("gameState.topBottom"),
  Outs = c("gameState.outs"), Balls = c("gameState.balls"), Strikes = c("gameState.strikes"),
  TaggedPitchType = c("pitchTag.taggedPitchType"), AutoPitchType = c("pitchTag.autoPitchType"),
  PitchCall = c("pitchTag.pitchCall"), KorBB = c("korBB"), PlayResult = c("playResult"),
  TaggedHitType = c("hitTag.taggedHitType", "hitTag.hitType"), Notes = c("notes"),
  Pitcher = c("pitcher.name", "pitcher.fullName", "pitcher"),
  PitcherId = c("pitcher.id", "pitcher.playerId", "pitcher.trackmanId"),
  PitcherThrows = c("pitcher.throws", "pitcher.hand"),
  PitcherTeam = c("pitcher.team", "pitcher.teamShortName", "pitcher.team.shortName"),
  Batter = c("batter.name", "batter.fullName", "batter"),
  BatterId = c("batter.id", "batter.playerId", "batter.trackmanId"),
  BatterSide = c("batter.side", "batter.bats"),
  BatterTeam = c("batter.team", "batter.teamShortName", "batter.team.shortName"),
  Catcher = c("catcher.name", "catcher.fullName", "catcher"),
  CatcherId = c("catcher.id", "catcher.playerId", "catcher.trackmanId"),
  CatcherTeam = c("catcher.team", "catcher.teamShortName", "catcher.team.shortName")
)

flatten_json <- function(txt) {
  x <- fromJSON(txt, flatten = TRUE)
  if (!is.data.frame(x) || nrow(x) == 0) return(data.frame())
  # drop array columns (trajectories) - they stay in the raw JSON archive
  x[, !vapply(x, is.list, TRUE), drop = FALSE]
}

apply_map <- function(df) {
  if (!nrow(df)) return(df)
  out <- data.frame(row.names = seq_len(nrow(df)))
  used <- character()
  for (target in names(TM_MAP)) {
    hit <- intersect(TM_MAP[[target]], names(df))[1]
    if (!is.na(hit)) { out[[target]] <- df[[hit]]; used <- c(used, hit) }
  }
  # keep every unmapped scalar too, so nothing the API sends is lost
  rest <- setdiff(names(df), c(used, "version", "kind"))
  for (nm in rest) out[[paste0("api_", gsub("[.]", "_", nm))]] <- df[[nm]]
  out
}

# Merge one session's balls (pitch/hit/throw tracks) with its tagged plays.
tm_session_frame <- function(session, balls_txt, plays_txt, kind = "game") {
  balls <- flatten_json(balls_txt)
  plays <- flatten_json(plays_txt)
  key_b <- intersect(c("playId", "playID", "pitchUID"), names(balls))[1]

  frame <- NULL
  if (nrow(balls) && kind == "game" && "kind" %in% names(balls)) {
    pieces <- lapply(c("Pitch", "Hit", "CatcherThrow"), function(k) {
      d <- balls[balls$kind == k, , drop = FALSE]
      if (!nrow(d)) return(NULL)
      d <- d[, colSums(!is.na(d)) > 0, drop = FALSE]
      for (nm in intersect(c("trackId", "trackStartTime"), names(d))) {
        names(d)[names(d) == nm] <- paste0(tolower(k), ".", nm)
      }
      d[!duplicated(d[[key_b]]), setdiff(names(d), c("version", "kind")), drop = FALSE]
    })
    pieces <- Filter(Negate(is.null), pieces)
    frame <- Reduce(function(a, b) merge(a, b, by = key_b, all = TRUE), pieces)
  } else if (nrow(balls)) {
    frame <- balls
  }

  if (nrow(plays)) {
    key_p <- if (kind == "game") intersect(c("playID", "playId"), names(plays))[1] else "playID"
    if (is.null(frame)) {
      frame <- plays
    } else if (!is.na(key_p) && !is.na(key_b)) {
      frame <- merge(plays, frame, by.x = key_p, by.y = key_b, all = TRUE)
    }
  }
  if (is.null(frame) || !nrow(frame)) return(data.frame())

  df <- apply_map(frame)
  ldt <- intersect(c("localDateTime"), names(frame))
  df$Date <- if (length(ldt)) substr(frame[[ldt]], 1, 10) else substr(session$gameDateLocal %||% NA, 1, 10)
  df$Date[is.na(df$Date)] <- substr(session$gameDateLocal %||% session$gameDateUtc %||% NA, 1, 10)
  df$Time <- if (length(ldt)) substr(frame[[ldt]], 12, 23) else NA_character_
  df$GameUID <- session$sessionId
  df$HomeTeam <- session$homeTeam$shortName %||% NA_character_
  df$AwayTeam <- session$awayTeam$shortName %||% NA_character_
  df$Stadium <- session$location$venue$name %||% session$location$field$name %||% NA_character_
  df$Level <- session$level$name %||% NA_character_
  df$League <- session$league$shortName %||% session$league$name %||% NA_character_
  df$Verified <- isTRUE(session$verified)
  df$Source <- paste0("trackman_api_", kind)
  df$PulledAtUtc <- format(Sys.time(), tz = "UTC", usetz = FALSE)
  df
}

tm_pull_session <- function(session, kind = "game", raw_json_dir = NULL) {
  id <- session$sessionId
  balls <- tm_get_text(file.path("api/v1/data", kind, "balls", id))
  plays <- tm_get_text(file.path("api/v1/data", kind, "plays", id))
  if (!is.null(raw_json_dir)) {
    con <- gzfile(ensure_dir(file.path(raw_json_dir, paste0(id, ".json.gz"))), "w")
    writeLines(toJSON(list(session = session, balls = fromJSON(balls, simplifyVector = FALSE),
                           plays = fromJSON(plays, simplifyVector = FALSE)), auto_unbox = TRUE, null = "null"), con)
    close(con)
  }
  tm_session_frame(session, balls, plays, kind)
}
