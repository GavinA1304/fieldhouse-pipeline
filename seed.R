#!/usr/bin/env Rscript
# One-time load of historical data (a combined Parquet file from a
# database export) into the Hugging Face layout.
#
#   Rscript seed.R path/to/history.parquet            # DRY RUN: prints counts only
#   Rscript seed.R path/to/history.parquet --apply    # writes build/out, then run:
#   python scripts/hf_upload.py
#
#   Rscript seed.R ccbl_2026_master.parquet --collection=ccbl [--apply]
#     loads into a SEPARATE collection (Cape League), never mixed with the
#     college sheet. Uses collections.ccbl.include from config.yml instead of
#     the college include rules, and only replaces that collection's seasons.
#
# Needs HF_REPO and HF_TOKEN env vars (to merge with the existing manifest).
# Models aren't run here: the private scoring job scores seeded seasons
# automatically, a couple per night (config: models.rescore_seasons_per_night).
# Same pattern as your cleaning passes: look at the numbers, then apply.

args <- commandArgs(TRUE)
SRC <- args[!startsWith(args, "--")][1]
APPLY <- "--apply" %in% args
COLL <- sub("^--collection=", "", grep("^--collection=", args, value = TRUE)[1])
if (is.na(COLL) || !nzchar(COLL)) COLL <- "trackman"
if (!grepl("^[a-z0-9_]+$", COLL)) stop("--collection must be lowercase letters/numbers, e.g. ccbl")
if (is.na(SRC) || !file.exists(SRC)) stop("usage: Rscript seed.R path/to/history.parquet [--collection=ccbl] [--apply]")
Sys.setenv(TZ = "UTC")
file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
HERE <- if (length(file_arg)) dirname(normalizePath(sub("^--file=", "", file_arg))) else getwd()
for (f in c("utils.R", "clean.R", "build.R")) source(file.path(HERE, "R", f))
hub <- Hub(env("HF_REPO"), env("HF_TOKEN"))
cfg <- load_config(hub, HERE)   # local config.yml if present, else config/config.yml in the dataset
if (COLL != "trackman") {
  # A separate collection gets its own include rules (no college level/month
  # filters). Missing block -> sensible defaults for a summer league.
  cfg$include <- cfg$collections[[COLL]]$include %||%
    list(require_two_teams = TRUE, require_date = TRUE, exclude_practice = TRUE,
         drop_columns = cfg$include$drop_columns)
  log_msg("collection: %s (separate from the college data)", COLL)
}

con <- duck_connect()
dbExecute(con, sprintf("CREATE VIEW src AS SELECT * FROM read_parquet(%s, union_by_name = true)", sql_str(SRC)))
n_src <- dbGetQuery(con, "SELECT count(*) n FROM src")$n
log_msg("source rows: %s", format(n_src, big.mark = ","))

# What the categorical cleanup would change, value by value.
cols <- dbGetQuery(con, "DESCRIBE src")$column_name
for (col in intersect(names(VOCAB), cols)) {
  vals <- dbGetQuery(con, sprintf("SELECT CAST(%s AS VARCHAR) v, count(*) n FROM src GROUP BY 1", sql_id(col)))
  canon <- VOCAB[[col]]
  map <- c(setNames(canon, norm_key(canon)), ALIASES[[col]] %||% character())
  map <- map[!duplicated(names(map))]
  new <- unname(map[norm_key(vals$v)])
  changed <- !is.na(vals$v) & !is.na(new) & new != vals$v
  unknown <- !is.na(vals$v) & is.na(new)
  if (any(changed)) {
    cat(sprintf("\n%s changes:\n", col))
    for (i in which(changed)) cat(sprintf("  %-28s -> %-20s %s rows\n", vals$v[i], new[i], format(vals$n[i], big.mark = ",")))
  }
  if (any(unknown)) cat(sprintf("%s values left as-is (not in vocabulary): %s\n", col,
                                paste(sprintf("%s (%d)", vals$v[unknown], vals$n[unknown]), collapse = ", ")))
}

# ---- What's in this file, so you can decide what Brewster gets -------------
desc <- dbGetQuery(con, "DESCRIBE src")
have <- setNames(desc$column_type, desc$column_name)
show <- function(title, sql) { cat(sprintf("\n%s\n", title)); print(dbGetQuery(con, sql), row.names = FALSE) }
for (col in intersect(c("Level", "League"), cols)) {
  show(sprintf("%s values (set include.%s in config.yml to the ones to keep):", col, tolower(paste0(col, "s"))),
       sprintf("SELECT CAST(%s AS VARCHAR) AS %s, count(*) AS rows FROM src GROUP BY 1 ORDER BY 2 DESC LIMIT 40", sql_id(col), col))
}
gd <- if ("Date" %in% cols) date_expr(toupper(have[["Date"]])) else "CAST(NULL AS DATE)"
show("Rows by month of the year (fall ball / scrimmages usually show up in Sep-Nov):",
     sprintf("SELECT month(%s) AS month, count(*) AS rows FROM src GROUP BY 1 ORDER BY 1", gd))
if (all(c("HomeTeam", "AwayTeam") %in% cols)) {
  show("Same team on both sides (intrasquads, bullpens, practices):",
       "SELECT count(*) AS rows FROM src WHERE HomeTeam = AwayTeam OR HomeTeam IS NULL OR AwayTeam IS NULL")
}
rules <- include_conditions(cfg, have, gd)
if (length(rules)) {
  cat("\nRows each include rule removes (a row can fail more than one):\n")
  for (nm in names(rules)) {
    n <- dbGetQuery(con, sprintf("SELECT count(*) AS n FROM src WHERE NOT coalesce((%s), false)", rules[[nm]]))$n
    cat(sprintf("  %-55s %s\n", nm, format(n, big.mark = ",")))
  }
} else {
  cat("\nNo include rules set yet: EVERYTHING in the file would be loaded. Set include: in config.yml.\n")
}
# Borderline games to eyeball: teams that barely appear (alumni games,
# exhibitions vs. pro/JUCO/club teams usually show up here). Add any that
# aren't real D1-D3 opponents to include.exclude_teams.
if (length(rules) && all(c("HomeTeam", "AwayTeam") %in% cols)) {
  keep <- paste(sprintf("coalesce((%s), false)", rules), collapse = " AND ")
  show("Team codes that look like practice/exhibition/national teams (check; add to exclude_teams):", sprintf("
    WITH t AS (SELECT HomeTeam AS team FROM src WHERE %s UNION ALL SELECT AwayTeam FROM src WHERE %s)
    SELECT team, count(*) AS pitches FROM t
    WHERE regexp_matches(team, '(?i)(PRA|PRAC|SCR|EXH|ALU|WBC|NAT|TEAM|USA|BLU|RED|WHI|GRAY|GREY)$')
       OR regexp_matches(team, '(?i)(WBC|_US$|^USA|ALUM|SCRIM)')
    GROUP BY 1 ORDER BY 2 DESC LIMIT 100", keep, keep))
  show("Teams that appear on only ONE date in the whole file (one-off opponents; most are real schools):", sprintf("
    WITH g AS (SELECT DISTINCT %s AS d, HomeTeam AS team, Level FROM src WHERE %s
               UNION SELECT DISTINCT %s, AwayTeam, Level FROM src WHERE %s)
    SELECT team, min(d) AS date, any_value(Level) AS level FROM g GROUP BY 1
    HAVING count(DISTINCT d) = 1 ORDER BY 2 LIMIT 400", gd, keep, gd, keep))
  if ("Date" %in% cols) show("Earliest games kept each season (should start on/after opening day for D1):", sprintf("
    SELECT year(%s) AS season, Level, min(%s) AS first_date, count(DISTINCT %s) FILTER (WHERE month(%s) = 2) AS feb_game_dates
    FROM src WHERE %s GROUP BY 1, 2 ORDER BY 1, 2", gd, gd, gd, gd, keep))
}

# ---- Does it have everything? ---------------------------------------------
key_cols <- c("Date", "PitchUID", "GameUID", "HomeTeam", "AwayTeam", "Level", "League",
              "Pitcher", "PitcherId", "PitcherThrows", "PitcherTeam", "Batter", "BatterId", "BatterSide", "BatterTeam",
              "Inning", "Balls", "Strikes", "Outs", "TaggedPitchType", "AutoPitchType", "PitchCall", "KorBB",
              "PlayResult", "TaggedHitType", "RelSpeed", "SpinRate", "SpinAxis", "InducedVertBreak", "HorzBreak",
              "RelHeight", "RelSide", "Extension", "VertApprAngle", "PlateLocHeight", "PlateLocSide",
              "ExitSpeed", "Angle", "Direction", "Distance")
cat("\nKey columns the app uses (present? and % of rows filled in):\n")
fill <- vapply(key_cols, function(k) {
  if (!(k %in% cols)) return("MISSING")
  pct <- dbGetQuery(con, sprintf("SELECT round(100.0 * count(%s) / count(*), 1) AS p FROM src", sql_id(k)))$p
  sprintf("%5.1f%%", pct)
}, "")
cat(sprintf("  %-18s %s", names(fill), fill), sep = "\n")
if (any(fill == "MISSING")) cat("  -> MISSING columns will be empty in the app.\n")
cat("  (Hit columns like ExitSpeed/Angle are only filled on balls in play, so ~15-20% is normal.)\n")

if (length(rules)) {
  keep <- paste(sprintf("coalesce((%s), false)", rules), collapse = " AND ")
  show("After the filter: game dates per month in each season (gaps = missing data):", sprintf("
    PIVOT (SELECT year(%s) AS season, month(%s) AS m, %s AS d FROM src WHERE %s)
    ON m USING count(DISTINCT d) GROUP BY season ORDER BY season", gd, gd, gd, keep))
  show("After the filter: most recent games in the file:", sprintf("
    SELECT %s AS date, count(DISTINCT HomeTeam || AwayTeam) AS games, count(*) AS pitches
    FROM src WHERE %s GROUP BY 1 ORDER BY 1 DESC LIMIT 10", gd, keep))
  show("After the filter: teams per season (D1 alone is ~300):", sprintf("
    SELECT year(%s) AS season, count(DISTINCT HomeTeam) AS home_teams, count(DISTINCT Pitcher) AS pitchers,
           count(DISTINCT Batter) AS batters, count(*) AS pitches
    FROM src WHERE %s GROUP BY 1 ORDER BY 1", gd, keep))
}

cat(sprintf("\nAll %d columns (list any that must not be shared under include.drop_columns):\n", length(cols)))
cat(strwrap(paste(cols, collapse = ", "), width = 110, prefix = "  "), sep = "\n")

dbExecute(con, sprintf("CREATE VIEW cleaned AS %s", clean_select_sql(con, "src", cfg)))
if (all(c("HomeTeam", "AwayTeam") %in% cols)) {
  tc <- dbGetQuery(con, "SELECT t AS team_code, count(*) AS rows FROM
                         (SELECT HomeTeam t FROM src UNION ALL SELECT AwayTeam FROM src)
                         WHERE t IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 60")
  cat("\nTeam codes in this data (put yours in config.yml -> team.codes):\n")
  print(tc, row.names = FALSE)
}
by_season <- dbGetQuery(con, "
  SELECT coalesce(CAST(Season AS VARCHAR), 'unknown') AS season, count(*) AS rows,
         sum(IsOurGame::INT) AS our_rows, min(GameDate) AS first, max(GameDate) AS last,
         max(CASE WHEN IsOurGame THEN GameDate END) AS last_our_game
  FROM cleaned GROUP BY 1 ORDER BY 1")
cat("\nAfter cleaning, by season (from the real game date):\n")
print(by_season, row.names = FALSE)
n_clean <- sum(by_season$rows)
cat(sprintf("\nRows NOT loaded (include rules + impossible rows): %s of %s\n",
            format(n_src - n_clean, big.mark = ","), format(n_src, big.mark = ",")))
if (any(by_season$season == "unknown")) {
  cat("NOTE: 'unknown' season = rows with no parseable date (check these: often misfiled appearances from other leagues).\n")
}
if (!APPLY) { cat("\nDry run only. Re-run with --apply to write files.\n"); quit(status = 0) }

dir.create(WORK, recursive = TRUE, showWarnings = FALSE)
writeLines(hub_sha(hub), file.path(WORK, "base_sha.txt"))
manifest <- hub_read_json(hub, "manifest.json", list(version = 1))
empty_obj <- function() setNames(list(), character())
SK <- summary_key(COLL)
for (k in c("trackman", "summary", "trumedia", COLL, SK)) if (!length(manifest[[k]])) manifest[[k]] <- empty_obj()
manifest <- register_collection(register_collection(manifest, "trackman", cfg), COLL, cfg)
unlink(OUT, recursive = TRUE)

for (i in seq_len(nrow(by_season))) {
  s <- by_season$season[i]
  if (!is.null(manifest[[COLL]][[s]]$pitches)) log_msg("WARNING: %s season %s already exists on the hub and will be replaced", COLL, s)
  out_file <- file.path(OUT, season_path(s, COLL))
  where <- if (s == "unknown") "Season IS NULL" else sprintf("Season = %s", s)
  dbExecute(con, sprintf(
    "COPY (SELECT * REPLACE (coalesce(Source, 'history_seed') AS Source) FROM cleaned WHERE %s
           ORDER BY PitcherTeam NULLS LAST, Pitcher NULLS LAST, GameDate, PitchNo)
     TO %s (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE %d)",
    where, sql_str(ensure_dir(out_file)), as.integer(cfg$build$row_group_size)))
  rows <- dbGetQuery(con, sprintf("SELECT count(*) n FROM read_parquet(%s)", sql_str(out_file)))$n
  if (rows != by_season$rows[i]) stop(sprintf("Row check failed for %s: expected %d, wrote %d", s, by_season$rows[i], rows))
  built <- build_season_outputs(con, s, out_file, cfg, COLL)
  manifest[[COLL]][[s]] <- list(pitches = season_path(s, COLL), rows = rows,
                                 deltas = manifest[[COLL]][[s]]$deltas %||% list(),
                                 models = setNames(list(), character()),  # scored by the nightly run
                                 last_game = built$last_game, last_our_game = built$last_our_game,
                                 updated_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"))
  for (nm in names(built$paths)) {
    cur <- manifest[[SK]][[nm]] %||% empty_obj()
    cur[[s]] <- built$paths[[nm]]
    manifest[[SK]][[nm]] <- cur
  }
  log_msg("season %s: %s rows, %.0f MB", s, format(rows, big.mark = ","), file.size(out_file) / 1e6)
}
manifest$updated_utc <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
write_json_file(manifest, file.path(OUT, "manifest.json"))
writeLines(character(), file.path(WORK, "delete_paths.txt"))
writeLines(sprintf("seed %s seasons", COLL), file.path(WORK, "commit_message.txt"))
log_msg("staged in %s. Verify the numbers above, then run: python scripts/hf_upload.py", OUT)
