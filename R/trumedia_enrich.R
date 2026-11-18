# TruMedia fielder identity (TeamPlays), joined onto TrackMan pitches.
# Rebuilt from the proven Neon-era scripts:
#   * team list: ONE AllTeams request, filtered to D1
#   * per team: TeamPlays with statEvent [BIP|PIT] and the confirmed fields
#   * pace: 45 requests/min + 30 s pause per 20 teams; on 429, wait the FULL
#     Retry-After the server asks for (up to an hour), never a capped guess
#   * join: game (from SourceFile) + half inning + inning + PA + pitch in PA
#   * only fills BLANKS; values already present are never overwritten

TM_DQ <- function(cfg) sub("/$", "", cfg$trumedia$base_url %||%
                           "https://api.trumedianetworks.com/v1/mlbapi/custom/baseball/DirectedQuery")

TEAMPLAYS_FIELDS <- c(
  "[FirstBaseman]", "[SecondBaseman]", "[ThirdBaseman]", "[Shortstop]",
  "[LeftFielder]", "[CenterFielder]", "[RightFielder]",
  "[HangTime|EVENT]", "event.battedBallDistance", "[SprayAng|EVENT]", "[PO]",
  "lookup(event.fieldersFirstOfficialFielder,fullName)",
  "lookup(event.fieldersBestChanceFielder,fullName)",
  "event.fieldersBestChancePosition", "event.fieldersFirstOfficialFielderPosition",
  "event.fieldersFirstAssistPosition", "event.fieldersFirstPutOutPosition", "event.fieldersTotalErrors"
)
TEAMPLAYS_KEYS <- c("trackmanGameId", "inning", "side", "abNumInSide", "pitchNumInAB")

tm_get_full_wait <- function(url, max_attempts = 5, max_wait = 3600) {
  for (attempt in seq_len(max_attempts)) {
    resp <- tryCatch(request(url) |> req_timeout(120) |> req_error(is_error = function(r) FALSE) |> req_perform(),
                     error = function(e) NULL)
    if (is.null(resp)) { Sys.sleep(30); next }
    if (resp_status(resp) != 429) return(resp)
    ra <- suppressWarnings(as.numeric(resp_header(resp, "retry-after")))
    wait <- if (!is.na(ra)) ra else 60 * 2^attempt
    if (wait > max_wait) stop(sprintf("TruMedia rate limit asks for %.0f min; stopping this pull (next run resumes)", wait / 60))
    log_msg("TruMedia rate limited: waiting the full %.0f s it asked for", wait)
    Sys.sleep(wait)
  }
  stop("TruMedia still rate limited after repeated full waits")
}

tm_read_csv_text <- function(txt) {
  if (!nzchar(txt)) return(data.frame())
  utils::read.csv(text = txt, colClasses = "character", check.names = TRUE, na.strings = c("", "NA"))
}

tm_d1_team_ids <- function(cfg, season) {
  url <- sprintf("%s/AllTeams.csv?seasonYear=%s&filters=%s&token=%s", TM_DQ(cfg), season,
                 utils::URLencode("((team.game.gameLeague = 'D1'))", reserved = TRUE), trumedia_token())
  resp <- tm_get_full_wait(url)
  if (resp_status(resp) != 200) stop("TruMedia AllTeams failed: HTTP ", resp_status(resp))
  d <- tm_read_csv_text(resp_body_string(resp))
  if (!"teamId" %in% names(d)) stop("AllTeams response had no teamId column")
  unique(d$teamId[!is.na(d$teamId)])
}

# Pull TeamPlays for every D1 team in a season -> one de-duplicated table
# (each play comes back twice, once per team, so it's keyed and de-duped).
tm_pull_teamplays <- function(cfg, season) {
  ids <- tm_d1_team_ids(cfg, season)
  log_msg("TruMedia TeamPlays %s: %d D1 teams", season, length(ids))
  cols <- utils::URLencode(paste(TEAMPLAYS_FIELDS, collapse = ","), reserved = TRUE)
  ev <- utils::URLencode("[BIP|PIT]", reserved = TRUE)
  per_min <- as.numeric(cfg$trumedia_enrich$requests_per_minute %||% 45)
  out <- list(); failed <- 0
  for (i in seq_along(ids)) {
    url <- sprintf("%s/TeamPlays.csv?seasonYear=%s&teamId=%s&statEvent=%s&columns=%s&token=%s",
                   TM_DQ(cfg), season, ids[i], ev, cols, trumedia_token())
    resp <- tm_get_full_wait(url)
    if (resp_status(resp) == 200) {
      d <- tryCatch(tm_read_csv_text(resp_body_string(resp)), error = function(e) NULL)
      if (!is.null(d) && nrow(d)) out[[length(out) + 1]] <- d
    } else failed <- failed + 1
    Sys.sleep(60 / per_min)
    if (i %% 20 == 0 && i < length(ids)) Sys.sleep(30)
  }
  log_msg("TruMedia TeamPlays %s: %d teams returned data, %d failed", season, length(out), failed)
  if (!length(out)) return(NULL)
  all_names <- unique(unlist(lapply(out, names)))
  out <- lapply(out, function(d) { for (n in setdiff(all_names, names(d))) d[[n]] <- NA_character_; d[all_names] })
  tp <- do.call(rbind, out)
  if (length(setdiff(TEAMPLAYS_KEYS, names(tp)))) stop("TeamPlays missing join columns: ",
                                                       paste(setdiff(TEAMPLAYS_KEYS, names(tp)), collapse = ", "))
  tp[!duplicated(tp[TEAMPLAYS_KEYS]), , drop = FALSE]
}

# Fill blanks in a season file from a TruMedia table `tp` (with columns
# trackmanGameId, inning, side, abNumInSide, pitchNumInAB). Every column in
# `allow` (or already in the file) is filled where it's currently empty.
# TrackMan's own columns are never touched.
tm_fill_file <- function(con, cfg, season, file, tp, allow, label) {
  duckdb::duckdb_register(con, "tm_fill", tp, overwrite = TRUE)
  on.exit(try(duckdb::duckdb_unregister(con, "tm_fill"), silent = TRUE), add = TRUE)
  have <- dbGetQuery(con, sprintf("DESCRIBE SELECT * FROM read_parquet(%s)", sql_str(file)))
  types <- setNames(have$column_type, have$column_name)
  fill <- setdiff(intersect(names(tp), unique(c(allow, names(types)))), c(TEAMPLAYS_KEYS, names(CORE_SCHEMA)))
  if (!length(fill) || !"SourceFile" %in% names(types)) {
    log_msg("TruMedia %s %s: nothing to fill", label, season); return(invisible(0))
  }
  exprs <- vapply(fill, function(c) {
    tcol <- sprintf("t.%s", sql_id(c))
    if (c %in% names(types)) sprintf("coalesce(p.%s, TRY_CAST(%s AS %s)) AS %s", sql_id(c), tcol, types[[c]], sql_id(c))
    else sprintf("%s AS %s", tcol, sql_id(c))
  }, "")
  present <- intersect(fill, names(types))
  base <- if (length(present)) sprintf("p.* EXCLUDE (%s)", paste(sql_id(present), collapse = ", ")) else "p.*"
  game <- "regexp_replace(regexp_replace(regexp_replace(p.\"SourceFile\", '\\.csv$', '', 'i'), '_playerpositioning.*$', '', 'i'), '_unverified$', '', 'i')"
  tmp <- paste0(file, ".tm.tmp")
  dbExecute(con, sprintf("
    COPY (SELECT %s, %s FROM read_parquet(%s) p LEFT JOIN tm_fill t
          ON %s = t.trackmanGameId
         AND upper(left(CAST(p.\"Top/Bottom\" AS VARCHAR), 1)) = upper(left(t.side, 1))
         AND CAST(p.\"Inning\" AS VARCHAR) = t.inning
         AND CAST(p.\"PAofInning\" AS VARCHAR) = t.abNumInSide
         AND CAST(p.\"PitchofPA\" AS VARCHAR) = t.pitchNumInAB
         ORDER BY PitcherTeam NULLS LAST, Pitcher NULLS LAST, GameDate, PitchNo)
    TO %s (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE %d)",
    base, paste(exprs, collapse = ", "), sql_str(file), game, sql_str(tmp), as.integer(cfg$build$row_group_size)))
  n_in <- dbGetQuery(con, sprintf("SELECT count(*) n FROM read_parquet(%s)", sql_str(file)))$n
  n_out <- dbGetQuery(con, sprintf("SELECT count(*) n FROM read_parquet(%s)", sql_str(tmp)))$n
  if (n_in != n_out) stop(sprintf("TruMedia %s join changed the row count (%d -> %d); nothing uploaded", label, n_in, n_out))
  filled <- dbGetQuery(con, sprintf("SELECT count(*) n FROM read_parquet(%s) WHERE %s IS NOT NULL", sql_str(tmp), sql_id(fill[1])))$n
  file.rename(tmp, file)
  log_msg("TruMedia %s %s: %d of %d pitches now have %s", label, season, filled, n_out, fill[1])
  invisible(filled)
}

FIELDER_COLS <- c("FirstBaseman", "SecondBaseman", "ThirdBaseman", "Shortstop", "LeftFielder", "CenterFielder",
                  "RightFielder", "fieldersBestChancePosition", "fieldersFirstOfficialFielderPosition",
                  "fieldersFirstAssistPosition", "fieldersFirstPutOutPosition", "fieldersTotalErrors")

tm_enrich_file <- function(con, cfg, season, file) {
  tp <- tm_pull_teamplays(cfg, season)
  if (is.null(tp)) return(invisible(0))
  tm_fill_file(con, cfg, season, file, tp, FIELDER_COLS, "fielders")
}

# ---- GamePitchesTrackman: runners, outs, steals, PB/WP, blocking, team IDs ----
# One game per request: batching several games was proven to silently drop
# rows once a response got big. Pacing copied from the proven script.
GAMEPITCHES_COLS <- c("manOnFirst", "manOnSecond", "manOnThird", "totalOuts",
  "stolenBaseAttempt2B", "stolenBaseAttempt3B", "stolenBaseAttemptHome",
  "stolenBase2B", "stolenBase3B", "stolenBaseHome", "catcherBackpickAttemptBase",
  "passedBall", "wildPitch", "pickoffResult", "probPBWP",
  "battingTeamid", "battingTeamName", "pitchingTeamid", "pitchingTeamName",
  "catcher_blocked_ball", "blocking_value_above_expected")

tm_fetch_game_pitches <- function(cfg, gid) {
  url <- sprintf("%s/GamePitchesTrackman.csv?trackmanGameId=%s&token=%s",
                 TM_DQ(cfg), utils::URLencode(gid, reserved = TRUE), trumedia_token())
  resp <- tm_get_full_wait(url)
  if (resp_status(resp) != 200) return(NULL)
  d <- tryCatch(tm_read_csv_text(resp_body_string(resp)), error = function(e) NULL)
  if (is.null(d) || !nrow(d) || !"trackmanGameID" %in% names(d)) return(NULL)
  keep <- intersect(c("trackmanGameID", "inning", "side", "abNumInSide", "pitchNumInAB", GAMEPITCHES_COLS), names(d))
  d[, keep, drop = FALSE]
}

# Pull every game in `file` that isn't done yet (per state$tm_games), up to
# a per-run cap; fill the file; return the updated game state.
tm_enrich_pitches <- function(con, cfg, season, file, games_state) {
  e <- cfg$trumedia_enrich
  cap <- as.integer(e$max_games_per_run %||% 1500)
  retry_days <- as.numeric(e$retry_empty_after_days %||% 3)
  gids <- dbGetQuery(con, sprintf("
    SELECT DISTINCT regexp_replace(regexp_replace(regexp_replace(\"SourceFile\", '\\.csv$', '', 'i'),
           '_playerpositioning.*$', '', 'i'), '_unverified$', '', 'i') AS g
    FROM read_parquet(%s) WHERE \"SourceFile\" IS NOT NULL", sql_str(file)))$g
  need <- Filter(function(g) {
    st <- games_state[[g]]
    if (is.null(st)) return(TRUE)
    identical(st$status, "empty") && (st$tries %||% 1) < 3 &&
      as.numeric(difftime(Sys.time(), as.POSIXct(st$last, tz = "UTC"), units = "days")) >= retry_days
  }, gids)
  attr(games_state, "filled") <- 0
  if (!length(need)) { log_msg("TruMedia pitches %s: all %d games already done", season, length(gids)); return(games_state) }
  todo <- head(need, cap)
  log_msg("TruMedia pitches %s: %d games to fetch (%d this run)", season, length(need), length(todo))
  got <- list()
  for (i in seq_along(todo)) {
    g <- todo[i]
    d <- tryCatch(tm_fetch_game_pitches(cfg, g), error = function(err) { log_msg("  stopped: %s", redact(conditionMessage(err))); "STOP" })
    if (identical(d, "STOP")) break                     # rate limit too long: resume next run
    prev <- games_state[[g]]
    if (!is.null(d)) { got[[length(got) + 1]] <- d
                       games_state[[g]] <- list(status = "ok", last = format(Sys.time(), tz = "UTC")) }
    else games_state[[g]] <- list(status = "empty", tries = (prev$tries %||% 0) + 1, last = format(Sys.time(), tz = "UTC"))
    Sys.sleep(2)
    if (i %% 25 == 0) Sys.sleep(60)
    if (i %% 250 == 0) Sys.sleep(300)
  }
  if (!length(got)) return(games_state)
  all_names <- unique(unlist(lapply(got, names)))
  got <- lapply(got, function(d) { for (n in setdiff(all_names, names(d))) d[[n]] <- NA_character_; d[all_names] })
  tp <- do.call(rbind, got)
  tp$trackmanGameId <- tp$trackmanGameID; tp$trackmanGameID <- NULL
  # blocking columns, same definitions as before
  lg <- function(x) toupper(trimws(x)) %in% c("TRUE", "T", "1", "YES")
  if (all(c("passedBall", "wildPitch") %in% names(tp))) {
    pb <- lg(tp$passedBall); wp <- lg(tp$wildPitch)
    tp$catcher_blocked_ball <- as.character(!pb & !wp)
    if ("probPBWP" %in% names(tp)) tp$blocking_value_above_expected <-
      as.character(suppressWarnings(as.numeric(tp$probPBWP)) - as.numeric(pb | wp))
  }
  tp <- tp[!duplicated(tp[TEAMPLAYS_KEYS]), , drop = FALSE]
  attr(games_state, "filled") <- tm_fill_file(con, cfg, season, file, tp, GAMEPITCHES_COLS, "pitches")
  games_state
}

# GamePitches: every nightly run for the current season (it only fetches
# games not done yet), and every backfill.
tm_pitches_due <- function(cfg, mode, season) {
  e <- cfg$trumedia_enrich
  if (!isTRUE(e$enabled) || season == "unknown") return(FALSE)
  mode == "backfill" || (mode == "nightly" && season == format(Sys.Date(), "%Y"))
}

# TeamPlays (fielders): weekly for the current season, and every backfill.
tm_enrich_due <- function(cfg, mode, season) {
  e <- cfg$trumedia_enrich
  if (!isTRUE(e$enabled) || season == "unknown") return(FALSE)
  if (mode == "backfill") return(TRUE)
  if (mode != "nightly") return(FALSE)
  cur <- format(Sys.Date(), "%Y")
  if (season != cur) return(FALSE)
  as.integer(format(Sys.Date(), "%u")) %in% as.integer(unlist(e$weekdays %||% list(2)))
}
