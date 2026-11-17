#!/usr/bin/env Rscript
# One-time load of historical data (a combined Parquet file from a
# database export) into the Hugging Face layout.
#
#   Rscript seed.R path/to/history.parquet            # DRY RUN: prints counts only
#   Rscript seed.R path/to/history.parquet --apply    # writes build/out, then run:
#   python scripts/hf_upload.py
#
# Needs HF_REPO and HF_TOKEN env vars (to merge with the existing manifest).
# Models aren't run here: the private scoring job scores seeded seasons
# automatically, a couple per night (config: models.rescore_seasons_per_night).
# Same pattern as your cleaning passes: look at the numbers, then apply.

args <- commandArgs(TRUE)
SRC <- args[!startsWith(args, "--")][1]
APPLY <- "--apply" %in% args
if (is.na(SRC) || !file.exists(SRC)) stop("usage: Rscript seed.R path/to/history.parquet [--apply]")
Sys.setenv(TZ = "UTC")
file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
HERE <- if (length(file_arg)) dirname(normalizePath(sub("^--file=", "", file_arg))) else getwd()
for (f in c("utils.R", "clean.R", "build.R")) source(file.path(HERE, "R", f))
hub <- Hub(env("HF_REPO"), env("HF_TOKEN"))
cfg <- load_config(hub, HERE)   # local config.yml if present, else config/config.yml in the dataset

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
cat(sprintf("\nImpossible rows dropped: %s\n", format(n_src - n_clean, big.mark = ",")))
if (any(by_season$season == "unknown")) {
  cat("NOTE: 'unknown' season = rows with no parseable date (check these: often misfiled appearances from other leagues).\n")
}
if (!APPLY) { cat("\nDry run only. Re-run with --apply to write files.\n"); quit(status = 0) }

dir.create(WORK, recursive = TRUE, showWarnings = FALSE)
writeLines(hub_sha(hub), file.path(WORK, "base_sha.txt"))
manifest <- hub_read_json(hub, "manifest.json", list(version = 1))
empty_obj <- function() setNames(list(), character())
for (k in c("trackman", "summary", "trumedia")) if (!length(manifest[[k]])) manifest[[k]] <- empty_obj()
unlink(OUT, recursive = TRUE)

for (i in seq_len(nrow(by_season))) {
  s <- by_season$season[i]
  if (!is.null(manifest$trackman[[s]]$pitches)) log_msg("WARNING: season %s already exists on the hub and will be replaced", s)
  out_file <- file.path(OUT, season_path(s))
  where <- if (s == "unknown") "Season IS NULL" else sprintf("Season = %s", s)
  dbExecute(con, sprintf(
    "COPY (SELECT * REPLACE (coalesce(Source, 'history_seed') AS Source) FROM cleaned WHERE %s
           ORDER BY PitcherTeam NULLS LAST, Pitcher NULLS LAST, GameDate, PitchNo)
     TO %s (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE %d)",
    where, sql_str(ensure_dir(out_file)), as.integer(cfg$build$row_group_size)))
  rows <- dbGetQuery(con, sprintf("SELECT count(*) n FROM read_parquet(%s)", sql_str(out_file)))$n
  if (rows != by_season$rows[i]) stop(sprintf("Row check failed for %s: expected %d, wrote %d", s, by_season$rows[i], rows))
  built <- build_season_outputs(con, s, out_file, cfg)
  manifest$trackman[[s]] <- list(pitches = season_path(s), rows = rows,
                                 deltas = manifest$trackman[[s]]$deltas %||% list(),
                                 models = setNames(list(), character()),  # scored by the nightly run
                                 last_game = built$last_game, last_our_game = built$last_our_game,
                                 updated_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"))
  for (nm in names(built$paths)) {
    cur <- manifest$summary[[nm]] %||% empty_obj()
    cur[[s]] <- built$paths[[nm]]
    manifest$summary[[nm]] <- cur
  }
  log_msg("season %s: %s rows, %.0f MB", s, format(rows, big.mark = ","), file.size(out_file) / 1e6)
}
manifest$updated_utc <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
write_json_file(manifest, file.path(OUT, "manifest.json"))
writeLines(character(), file.path(WORK, "delete_paths.txt"))
writeLines("seed historical seasons", file.path(WORK, "commit_message.txt"))
log_msg("staged in %s. Verify the numbers above, then run: python scripts/hf_upload.py", OUT)
