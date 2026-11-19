# Season compaction, summaries, dropdown lookups, health checks.

# Collections keep separate datasets apart: "trackman" = college (the main
# sheet), "ccbl" = Cape League. Each has its own folders, summaries and
# manifest keys, so models and leaderboards never mix them.
season_path  <- function(s, coll = "trackman") sprintf("%s/season=%s/pitches.parquet", coll, s)
delta_dir    <- function(s, coll = "trackman") sprintf("%s/delta/season=%s", coll, s)
summary_key  <- function(coll = "trackman") if (coll == "trackman") "summary" else paste0(coll, "_summary")
summary_path <- function(name, s, coll = "trackman") sprintf("%s/%s/season=%s.parquet", summary_key(coll), name, s)

COLLECTION_LABELS <- list(trackman = "College", ccbl = "Cape League")
# Registry the app reads to offer the College / Cape League switch.
register_collection <- function(manifest, coll, cfg = NULL) {
  reg <- manifest$collections
  if (!length(reg)) reg <- setNames(list(), character())
  label <- cfg$collections[[coll]]$label %||% COLLECTION_LABELS[[coll]] %||% coll
  reg[[coll]] <- list(label = label, summary = summary_key(coll))
  manifest$collections <- reg
  manifest
}

# Merge Parquet files (oldest first) into one sorted, de-duplicated file.
# Used two ways:
#   live:    existing live.parquet + newly pulled sessions -> new live.parquet
#   nightly: season file + live.parquet -> new season file
# Safety rule: the new file must open and pass a row
# check BEFORE anything is marked for deletion. If it fails, we stop and the
# old files stay untouched on Hugging Face.
merge_files <- function(con, cfg, label, files, out_file) {
  files <- files[!is.na(files) & file.exists(files)]
  if (!length(files)) return(NULL)
  # Duplicate pitch (same PitchUID): verified beats unverified, then the
  # newer file wins, then the newer pull.
  rank <- paste(sprintf("WHEN %s THEN %d", sql_str(files), seq_along(files)), collapse = " ")
  src <- sprintf("read_parquet(%s, union_by_name = true, filename = true)", sql_list(files))
  dedup <- sprintf("
    SELECT * EXCLUDE (filename, _rk) FROM (
      SELECT *, row_number() OVER (
        PARTITION BY coalesce(PitchUID, uuid()::VARCHAR)
        ORDER BY coalesce(Verified, true) DESC, (CASE filename %s END) DESC,
                 PulledAtUtc DESC NULLS LAST) AS _rk
      FROM %s) WHERE _rk = 1", rank, src)
  first_rows <- dbGetQuery(con, sprintf("SELECT count(*) n FROM read_parquet(%s)", sql_str(files[1])))$n
  ensure_dir(out_file)
  tmp_file <- paste0(out_file, ".tmp")
  copy_sorted(con, dedup, "PitcherTeam NULLS LAST, Pitcher NULLS LAST, GameDate, PitchNo",
              tmp_file, cfg$build$row_group_size)
  new_rows <- dbGetQuery(con, sprintf("SELECT count(*) n FROM read_parquet(%s)", sql_str(tmp_file)))$n
  # Replacing unverified with verified can legitimately drop a few rows, but
  # never a big chunk.
  if (new_rows < floor(first_rows * 0.98)) {
    stop(sprintf("Merge check failed for %s: %d rows -> %d. Nothing uploaded.", label, first_rows, new_rows))
  }
  file.rename(tmp_file, out_file)
  log_msg("%s merged: %d files -> %d rows", label, length(files), new_rows)
  new_rows
}

SWING <- "PitchCall IN ('StrikeSwinging','FoulBall','FoulBallNotFieldable','FoulBallFieldable','InPlay')"
INZONE <- "(abs(PlateLocSide) <= 0.83 AND PlateLocHeight BETWEEN 1.5 AND 3.5)"
PTYPE <- "coalesce(nullif(TaggedPitchType, 'Undefined'), nullif(AutoPitchType, 'Undefined'), 'Undefined')"
PA_END <- "(KorBB IN ('Strikeout','Walk') OR PitchCall IN ('HitByPitch','InPlay'))"
HIT <- "PlayResult IN ('Single','Double','Triple','HomeRun')"
TB <- "CASE PlayResult WHEN 'Single' THEN 1 WHEN 'Double' THEN 2 WHEN 'Triple' THEN 3 WHEN 'HomeRun' THEN 4 ELSE 0 END"

summary_sql <- list(
  pitcher_arsenal = sprintf("
    SELECT Season, Pitcher, PitcherId, PitcherTeam, PitcherThrows, %s AS PitchType,
      count(*) AS Pitches,
      round(count(*) / sum(count(*)) OVER (PARTITION BY Season, Pitcher, PitcherId, PitcherTeam), 3) AS Usage,
      round(avg(RelSpeed), 1) AS Velo, round(max(RelSpeed), 1) AS MaxVelo,
      round(avg(SpinRate)) AS Spin, round(avg(InducedVertBreak), 1) AS IVB, round(avg(HorzBreak), 1) AS HB,
      round(avg(Extension), 2) AS Ext, round(avg(RelHeight), 2) AS RelHeight, round(avg(RelSide), 2) AS RelSide,
      round(avg(VertApprAngle), 2) AS VAA,
      round(sum((PitchCall = 'StrikeSwinging')::INT) / nullif(sum((%s)::INT), 0), 3) AS WhiffPct,
      round(avg((%s)::INT), 3) AS ZonePct,
      round(sum(((%s) AND NOT (%s))::INT) / nullif(sum((NOT (%s))::INT), 0), 3) AS ChasePct,
      round(avg((PitchCall IN ('StrikeCalled','StrikeSwinging'))::INT), 3) AS CSWPct,
      round(avg(CASE WHEN PitchCall = 'InPlay' THEN ExitSpeed END), 1) AS EVAllowed
    FROM pitches WHERE Pitcher IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6", PTYPE, SWING, INZONE, SWING, INZONE, INZONE),

  pitcher_season = sprintf("
    SELECT Season, Pitcher, PitcherId, PitcherTeam, PitcherThrows,
      count(*) AS Pitches, count(DISTINCT GameUID) AS Games,
      sum((%s)::INT) AS PA,
      sum((KorBB = 'Strikeout')::INT) AS K, sum((KorBB = 'Walk')::INT) AS BB,
      round(sum((KorBB = 'Strikeout')::INT) / nullif(sum((%s)::INT), 0), 3) AS KPct,
      round(sum((KorBB = 'Walk')::INT) / nullif(sum((%s)::INT), 0), 3) AS BBPct,
      round(sum((PitchCall = 'StrikeSwinging')::INT) / nullif(sum((%s)::INT), 0), 3) AS WhiffPct,
      round(sum(((%s) AND NOT (%s))::INT) / nullif(sum((NOT (%s))::INT), 0), 3) AS ChasePct,
      round(avg((%s)::INT), 3) AS ZonePct,
      round(avg(CASE WHEN %s IN ('FourSeamFastBall','Sinker') THEN RelSpeed END), 1) AS FBVelo,
      round(avg(CASE WHEN PitchCall = 'InPlay' THEN ExitSpeed END), 1) AS EVAllowed,
      round(avg(CASE WHEN PitchCall = 'InPlay' THEN (ExitSpeed >= 95)::INT END), 3) AS HardHitPct,
      round(avg(CASE WHEN PitchCall = 'InPlay' THEN (TaggedHitType = 'GroundBall')::INT END), 3) AS GBPct,
      max(GameDate) AS LastGame
    FROM pitches WHERE Pitcher IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5", PA_END, PA_END, PA_END, SWING, SWING, INZONE, INZONE, INZONE, PTYPE),

  batter_season = sprintf("
    SELECT Season, Batter, BatterId, BatterTeam, BatterSide,
      count(*) AS Pitches, count(DISTINCT GameUID) AS Games,
      sum((%s)::INT) AS PA,
      sum((%s)::INT) AS H, sum((PlayResult = 'HomeRun')::INT) AS HR,
      sum((KorBB = 'Strikeout')::INT) AS K, sum((KorBB = 'Walk')::INT) AS BB,
      round(sum((%s)::INT) / nullif(sum((%s)::INT) - sum((KorBB = 'Walk')::INT)
            - sum((PitchCall = 'HitByPitch')::INT) - sum((PlayResult = 'Sacrifice')::INT), 0), 3) AS AVG,
      round(sum(%s) / nullif(sum((%s)::INT) - sum((KorBB = 'Walk')::INT)
            - sum((PitchCall = 'HitByPitch')::INT) - sum((PlayResult = 'Sacrifice')::INT), 0), 3) AS SLG,
      round(sum((KorBB = 'Strikeout')::INT) / nullif(sum((%s)::INT), 0), 3) AS KPct,
      round(sum((KorBB = 'Walk')::INT) / nullif(sum((%s)::INT), 0), 3) AS BBPct,
      round(avg(CASE WHEN PitchCall = 'InPlay' THEN ExitSpeed END), 1) AS AvgEV,
      round(max(CASE WHEN PitchCall = 'InPlay' THEN ExitSpeed END), 1) AS MaxEV,
      round(quantile_cont(CASE WHEN PitchCall = 'InPlay' THEN ExitSpeed END, 0.9), 1) AS EV90,
      round(avg(CASE WHEN PitchCall = 'InPlay' THEN (ExitSpeed >= 95)::INT END), 3) AS HardHitPct,
      round(avg(CASE WHEN PitchCall = 'InPlay' THEN (Angle BETWEEN 8 AND 32)::INT END), 3) AS SweetSpotPct,
      round(avg(CASE WHEN PitchCall = 'InPlay' THEN Angle END), 1) AS AvgLA,
      round(sum((PitchCall = 'StrikeSwinging')::INT) / nullif(sum((%s)::INT), 0), 3) AS WhiffPct,
      round(sum(((%s) AND NOT (%s))::INT) / nullif(sum((NOT (%s))::INT), 0), 3) AS ChasePct,
      max(GameDate) AS LastGame
    FROM pitches WHERE Batter IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5", PA_END, HIT, HIT, PA_END, TB, PA_END, PA_END, PA_END, SWING, SWING, INZONE, INZONE)
)

# Everything the app's dropdowns need, precomputed - the fix for slow
# SELECT DISTINCT, but rebuilt automatically instead of hardcoded.
lookup_sql <- "
  SELECT Season, 'Pitcher' AS Role, Pitcher AS Name, PitcherId AS Id, PitcherTeam AS Team, count(*) AS Pitches
    FROM pitches WHERE Pitcher IS NOT NULL GROUP BY 1, 2, 3, 4, 5
  UNION ALL
  SELECT Season, 'Batter', Batter, BatterId, BatterTeam, count(*) FROM pitches WHERE Batter IS NOT NULL GROUP BY 1, 2, 3, 4, 5
  UNION ALL
  SELECT Season, 'Catcher', Catcher, CatcherId, CatcherTeam, count(*) FROM pitches WHERE Catcher IS NOT NULL GROUP BY 1, 2, 3, 4, 5"

build_season_outputs <- function(con, season, pitches_file, cfg = NULL, coll = "trackman") {
  # Bullpen/practice rows live in the same files (Source = trackman_api_practice)
  # but never count toward game stats.
  dbExecute(con, sprintf("CREATE OR REPLACE VIEW pitches AS SELECT * FROM read_parquet(%s)
                          WHERE Source IS NULL OR Source NOT LIKE '%%practice%%'", sql_str(pitches_file)))
  # Model scores (Stuff+ etc.) averaged into the summaries, when present.
  have <- dbGetQuery(con, "DESCRIBE SELECT * FROM pitches")$column_name
  avg_cols <- function(cols) {
    cols <- intersect(cols, have)
    if (!length(cols)) return("")
    paste0(",\n      ", paste(sprintf("round(avg(%s), 1) AS %s", sql_id(cols), sql_id(cols)), collapse = ", "))
  }
  extra <- list(pitcher = if (!is.null(cfg)) avg_cols(model_summary_cols(cfg, "pitcher")) else "",
                batter = if (!is.null(cfg)) avg_cols(model_summary_cols(cfg, "batter")) else "")
  paths <- list()
  for (nm in names(summary_sql)) {
    p <- summary_path(nm, season, coll)
    sql <- summary_sql[[nm]]
    who <- if (startsWith(nm, "pitcher")) "pitcher" else "batter"
    anchor <- if (who == "pitcher") "\n    FROM pitches WHERE Pitcher" else "\n    FROM pitches WHERE Batter"
    sql <- sub(anchor, paste0(extra[[who]], anchor), sql, fixed = TRUE)
    dbExecute(con, sprintf("COPY (%s) TO %s (FORMAT parquet, COMPRESSION zstd)",
                           sql, sql_str(ensure_dir(file.path(OUT, p)))))
    paths[[nm]] <- p
  }
  p <- summary_path("players", season, coll)
  dbExecute(con, sprintf("COPY (%s) TO %s (FORMAT parquet, COMPRESSION zstd)", lookup_sql,
                         sql_str(ensure_dir(file.path(OUT, p)))))
  paths$players <- p
  stats <- dbGetQuery(con, "SELECT count(*) AS n, max(GameDate) AS last_game,
                              max(CASE WHEN IsOurGame THEN GameDate END) AS last_our_game FROM pitches")
  list(paths = paths, rows = stats$n, last_game = as.character(stats$last_game),
       last_our_game = as.character(stats$last_our_game))
}

# Stale-data alarm: catches a silently stopped feed the day it starts,
# instead of weeks later. Each watch in config.yml (health.watches)
# applies only in its months, e.g. your team's games in summer, any college game
# in spring.
health_check <- function(cfg, manifest) {
  problems <- character()
  today <- Sys.Date()
  month <- as.integer(format(today, "%m"))
  cur <- manifest$trackman[[format(today, "%Y")]] %||% list()
  stale <- cfg$health$stale_days %||% 3
  for (w in cfg$health$watches) {
    if (!(month %in% unlist(w$months))) next
    last <- (if (isTRUE(w$our_team_only)) cur$last_our_game else cur$last_game) %||% NA
    if (is.na(last)) {
      problems <- c(problems, sprintf("%s: none loaded yet this year (check config.yml and the TrackMan source).", w$name))
    } else if (as.numeric(today - as.Date(last)) > stale) {
      problems <- c(problems, sprintf("%s: nothing new since %s (%d days). Pipeline or source may be broken.",
                                      w$name, last, as.integer(today - as.Date(last))))
    }
  }
  unk <- manifest$trackman[["unknown"]]$rows %||% 0
  if (unk > 0) problems <- c(problems, sprintf("%d rows have no parseable game date (season=unknown).", unk))
  problems
}

# Model output column names come from the private config. The models
# themselves run in the private repo; this side only averages their columns.
models_enabled <- function(cfg) Filter(function(m) !isFALSE(m$enabled), cfg$models$list %||% list())
model_summary_cols <- function(cfg, who) unique(unlist(lapply(models_enabled(cfg), function(m) m$summarize[[who]])))
