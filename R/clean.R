# Schema + cleaning, expressed as one DuckDB SELECT so the exact same rules
# run on a 300-row live session and on a 9M-row historical seed.

# Core columns every file is guaranteed to have, with fixed types.
# Fixed types are what keep separately-pulled batches from disagreeing
# (the type-inference problem you hit with CSV handoffs).
CORE_SCHEMA <- c(
  PitchUID = "VARCHAR", GameUID = "VARCHAR", PlayID = "VARCHAR",
  Date = "VARCHAR", Time = "VARCHAR",
  HomeTeam = "VARCHAR", AwayTeam = "VARCHAR", Stadium = "VARCHAR", Level = "VARCHAR", League = "VARCHAR",
  Pitcher = "VARCHAR", PitcherId = "VARCHAR", PitcherThrows = "VARCHAR", PitcherTeam = "VARCHAR",
  Batter = "VARCHAR", BatterId = "VARCHAR", BatterSide = "VARCHAR", BatterTeam = "VARCHAR",
  Catcher = "VARCHAR", CatcherId = "VARCHAR", CatcherTeam = "VARCHAR",
  Inning = "INTEGER", `Top/Bottom` = "VARCHAR", Outs = "INTEGER", Balls = "INTEGER", Strikes = "INTEGER",
  PitchNo = "INTEGER", PAofInning = "INTEGER", PitchofPA = "INTEGER",
  TaggedPitchType = "VARCHAR", AutoPitchType = "VARCHAR", PitchCall = "VARCHAR", KorBB = "VARCHAR",
  TaggedHitType = "VARCHAR", PlayResult = "VARCHAR",
  RelSpeed = "DOUBLE", SpinRate = "DOUBLE", SpinAxis = "DOUBLE", Tilt = "VARCHAR",
  RelHeight = "DOUBLE", RelSide = "DOUBLE", Extension = "DOUBLE",
  VertRelAngle = "DOUBLE", HorzRelAngle = "DOUBLE",
  InducedVertBreak = "DOUBLE", VertBreak = "DOUBLE", HorzBreak = "DOUBLE",
  PlateLocHeight = "DOUBLE", PlateLocSide = "DOUBLE", ZoneSpeed = "DOUBLE",
  VertApprAngle = "DOUBLE", HorzApprAngle = "DOUBLE", ZoneTime = "DOUBLE",
  ExitSpeed = "DOUBLE", Angle = "DOUBLE", Direction = "DOUBLE", Distance = "DOUBLE",
  Bearing = "DOUBLE", HangTime = "DOUBLE",
  Verified = "BOOLEAN", Source = "VARCHAR", PulledAtUtc = "TIMESTAMP",
  SourceFile = "VARCHAR", PitcherConference = "VARCHAR", BatterConference = "VARCHAR"
)

# Non-core columns that look like measurements are cast to DOUBLE; IDs and
# everything else stay VARCHAR. Same rule every time = no type drift.
INT_PATTERN <- "^(PitchNo|Inning|PAofInning|PitchofPA|Outs|Balls|Strikes|OutsOnPlay|RunsScored)$"
NUM_PATTERN <- paste0(
  "(Speed|Velo|Rate|Angle|Height|Side|Break|Extension|Distance|Bearing|Time$|Duration|",
  "Position|Location[XYZ]?|Efficiency|Drop|^pfx[xz]$|^[xyz]0$|^v[xyz]0$|^a[xyz]0$|Measured[XYZ]$)"
)
NOT_NUMERIC <- "(Id$|UID$|^Tilt$|^Date$|^Time$|Team|Name|Type|Call|Result|Side$|Throws)"

column_type <- function(name) {
  if (name %in% names(CORE_SCHEMA)) return(CORE_SCHEMA[[name]])
  if (grepl(INT_PATTERN, name)) return("INTEGER")
  if (grepl(NUM_PATTERN, name) && !grepl(NOT_NUMERIC, name)) return("DOUBLE")
  "VARCHAR"
}

# Coerce an R data.frame to the schema before it ever touches Parquet.
cast_frame <- function(df) {
  for (nm in names(df)) {
    v <- df[[nm]]
    if (is.list(v)) { df[[nm]] <- NULL; next }   # drop arrays (trajectories live in raw JSON)
    df[[nm]] <- switch(column_type(nm),
      INTEGER = suppressWarnings(as.integer(round(as.numeric(v)))),
      DOUBLE = suppressWarnings(as.numeric(v)),
      BOOLEAN = as.logical(v),
      TIMESTAMP = as.POSIXct(v, tz = "UTC"),
      { x <- as.character(v); x[!is.na(x) & !nzchar(trimws(x))] <- NA; x })
  }
  df
}

# Canonical spellings. Matching ignores case, spaces and punctuation, so
# "Home Run", "homerun", "HOMERUN" all become "HomeRun".
VOCAB <- list(
  PitchCall = c("BallCalled", "BallinDirt", "BallIntentional", "StrikeCalled", "StrikeSwinging",
                "FoulBall", "FoulBallNotFieldable", "FoulBallFieldable", "InPlay", "HitByPitch", "Undefined",
                "CatchersInterference", "BattersInterference", "AutomaticStrike", "AutomaticBall"),
  PlayResult = c("Single", "Double", "Triple", "HomeRun", "Out", "Error", "FieldersChoice",
                 "Sacrifice", "StolenBase", "CaughtStealing", "Undefined"),
  TaggedHitType = c("GroundBall", "LineDrive", "FlyBall", "Popup", "Bunt", "Undefined"),
  TaggedPitchType = c("FourSeamFastBall", "Sinker", "Cutter", "Slider", "Sweeper", "Curveball",
                      "ChangeUp", "Splitter", "Knuckleball", "Other", "Undefined"),
  AutoPitchType = c("FourSeamFastBall", "Sinker", "Cutter", "Slider", "Sweeper", "Curveball",
                    "ChangeUp", "Splitter", "Knuckleball", "Other", "Undefined",
                    "Four-Seam", "Changeup"),
  KorBB = c("Strikeout", "Walk", "Undefined"),
  BatterSide = c("Right", "Left", "Undefined"),
  PitcherThrows = c("Right", "Left", "Both", "Undefined")
)
# Your consolidation rules, plus common aliases.
ALIASES <- list(
  TaggedPitchType = c(fastball = "FourSeamFastBall", fourseam = "FourSeamFastBall",
                      fourseamfastball = "FourSeamFastBall", twoseamfastball = "Sinker",
                      twoseam = "Sinker", oneseamfastball = "Other", change = "ChangeUp",
                      curve = "Curveball", split = "Splitter"),
  AutoPitchType = c(fourseam = "Four-Seam", changeup = "Changeup"),
  PlayResult = c(homer = "HomeRun", fielderschoice = "FieldersChoice", sac = "Sacrifice"),
  BatterSide = c(r = "Right", l = "Left"),
  PitcherThrows = c(r = "Right", l = "Left")
)

norm_key <- function(x) tolower(gsub("[^A-Za-z]", "", x))

categorical_expr <- function(col) {
  src <- sprintf("CAST(%s AS VARCHAR)", sql_id(col))
  canon <- VOCAB[[col]]
  map <- setNames(canon, norm_key(canon))
  if (!is.null(ALIASES[[col]])) map <- c(map, ALIASES[[col]])
  map <- map[!duplicated(names(map))]
  key <- sprintf("lower(regexp_replace(%s, '[^A-Za-z]', '', 'g'))", src)
  whens <- paste(sprintf("WHEN %s THEN %s", sql_str(names(map)), sql_str(map)), collapse = " ")
  sprintf("CASE %s %s ELSE nullif(trim(%s), '') END", key, whens, src)
}

date_expr <- function(type) {
  if (grepl("DATE|TIMESTAMP", type)) return('CAST("Date" AS DATE)')
  # Mixed formats in historical CSVs; Season comes from the real game date,
  # never a separate year column, which can disagree with the actual date.
  paste0('coalesce(TRY_CAST(left(trim("Date"), 10) AS DATE), ',
         'CAST(try_strptime(trim("Date"), \'%m/%d/%y\') AS DATE), ',  # 2-digit year first
         'CAST(try_strptime(trim("Date"), \'%m/%d/%Y\') AS DATE), ',
         'CAST(try_strptime(trim("Date"), \'%Y/%m/%d\') AS DATE))')
}

# Build the cleaning SELECT for a table/view already registered in DuckDB.
clean_select_sql <- function(con, relation, cfg) {
  desc <- dbGetQuery(con, sprintf("DESCRIBE SELECT * FROM %s", relation))
  have <- setNames(desc$column_type, desc$column_name)

  replace <- character()
  add <- character()

  # 1. enforce types on every existing column
  for (nm in names(have)) {
    target <- column_type(nm)
    if (nm == "Date") next
    if (toupper(have[[nm]]) != target) {
      replace[nm] <- sprintf("TRY_CAST(%s AS %s)", sql_id(nm), target)
    }
  }
  # 2. normalize categoricals (applied on the typed value)
  for (col in intersect(names(VOCAB), names(have))) {
    replace[col] <- categorical_expr(col)
  }
  # 3. position codes masquerading as names -> Unknown
  codes <- paste(sql_str(toupper(cfg$cleaning$position_code_names)), collapse = ", ")
  for (col in intersect(c("Pitcher", "Batter", "Catcher"), names(have))) {
    replace[col] <- sprintf("CASE WHEN upper(trim(%s)) IN (%s) THEN 'Unknown' ELSE nullif(trim(%s), '') END",
                            sql_id(col), codes, sql_id(col))
  }
  # 4. missing core columns -> typed NULLs, so downstream SQL never breaks
  for (nm in setdiff(names(CORE_SCHEMA), names(have))) {
    add <- c(add, sprintf("CAST(NULL AS %s) AS %s", CORE_SCHEMA[[nm]], sql_id(nm)))
  }
  # 5. derived columns
  gd <- if ("Date" %in% names(have)) date_expr(toupper(have[["Date"]])) else "CAST(NULL AS DATE)"
  teams <- paste(sql_str(cfg$team$codes), collapse = ", ")
  derived <- c(
    GameDate = gd,
    Season = sprintf("CAST(year(%s) AS INTEGER)", gd),
    IsOurGame = sprintf("(%s IN (%s) OR %s IN (%s))",
                        if ("HomeTeam" %in% names(have)) '"HomeTeam"' else "NULL", teams,
                        if ("AwayTeam" %in% names(have)) '"AwayTeam"' else "NULL", teams)
  )
  for (nm in names(derived)) {
    if (nm %in% names(have)) replace[nm] <- derived[[nm]]
    else add <- c(add, sprintf("%s AS %s", derived[[nm]], sql_id(nm)))
  }
  if ("Date" %in% names(have)) replace["Date"] <- sprintf("CAST(%s AS VARCHAR)", sql_id("Date"))

  # 6. physically impossible rows are dropped
  conds <- c()
  lim <- c(Outs = cfg$cleaning$max_outs, Balls = cfg$cleaning$max_balls, Strikes = cfg$cleaning$max_strikes)
  for (nm in intersect(names(lim), names(have))) {
    conds <- c(conds, sprintf("coalesce(TRY_CAST(%s AS INTEGER), 0) <= %d", sql_id(nm), as.integer(lim[[nm]])))
  }
  if ("Inning" %in% names(have)) conds <- c(conds, 'coalesce(TRY_CAST("Inning" AS INTEGER), 1) >= 1')

  # 7. what this dataset is allowed to contain (config: include). Applied to
  #    history AND every live pull, so nothing outside it ever lands.
  conds <- c(conds, include_conditions(cfg, have, gd))

  # 8. columns that must never be copied over (config: include.drop_columns)
  drop <- intersect(unlist(cfg$include$drop_columns), names(have))
  replace <- replace[setdiff(names(replace), drop)]

  sel <- "*"
  if (length(drop)) sel <- sprintf("* EXCLUDE (%s)", paste(sql_id(drop), collapse = ", "))
  if (length(replace)) {
    sel <- sprintf("%s REPLACE (%s)", sel, paste(sprintf("%s AS %s", replace, sql_id(names(replace))), collapse = ",\n  "))
  }
  if (length(add)) sel <- paste0(sel, ",\n  ", paste(add, collapse = ",\n  "))
  where <- if (length(conds)) paste("WHERE", paste(conds, collapse = " AND ")) else ""
  sprintf("SELECT %s\nFROM %s\n%s", sel, relation, where)
}

# The "include" rules from config.yml, as SQL conditions. Each rule only
# applies if its column exists. Returns a named character vector (names are
# rule labels, used by the seed dry run to report what each rule removes).
include_conditions <- function(cfg, have, gd) {
  inc <- cfg$include %||% list()
  ids <- function(x) paste(sql_str(unlist(x)), collapse = ", ")
  conds <- character()
  col <- function(nm) sprintf("trim(CAST(%s AS VARCHAR))", sql_id(nm))
  if (length(unlist(inc$levels)) && "Level" %in% names(have)) {
    # Match spelling variants: "D1", "DI", "Division I", "NCAA D1" all count as D1.
    alias <- list(D1 = c("D1", "DI", "DIVISIONI", "DIVISION1", "NCAAD1", "NCAADI", "NCAADIVISIONI"),
                  D2 = c("D2", "DII", "DIVISIONII", "DIVISION2", "NCAAD2", "NCAADII", "NCAADIVISIONII"),
                  D3 = c("D3", "DIII", "DIVISIONIII", "DIVISION3", "NCAAD3", "NCAADIII", "NCAADIVISIONIII"))
    keys <- unique(unlist(lapply(unlist(inc$levels), function(l) {
      k <- toupper(gsub("[^A-Za-z0-9]", "", l)); alias[[k]] %||% k })))
    conds["level not in include.levels"] <- sprintf(
      "upper(regexp_replace(%s, '[^A-Za-z0-9]', '', 'g')) IN (%s)", col("Level"), ids(keys))
  }
  # Exhibitions, scrimmages, fall/preseason games: keyword check across the
  # columns that describe the game (never pitch-level notes).
  kw <- unlist(inc$exclude_keywords)
  kcols <- intersect(c("Level", "League", "HomeTeam", "AwayTeam", "GameID", "Stadium", "GameType", "SessionType"), names(have))
  if (length(kw) && length(kcols)) {
    # whole words only, so "camp" never matches Campbell and "fall" never matches Fallon
    words <- gsub(" ", "[^a-z0-9]?", tolower(gsub("[^A-Za-z0-9 ]", "", kw)), fixed = TRUE)  # "blue white" matches Blue-White too
    pattern <- paste0("\\b(", paste(words, collapse = "|"), ")\\b")
    conds["game looks like exhibition/scrimmage/practice (include.exclude_keywords)"] <- sprintf(
      "NOT regexp_matches(lower(concat_ws(' ', %s)), %s)",
      paste(sprintf("CAST(%s AS VARCHAR)", sql_id(kcols)), collapse = ", "), sql_str(pattern))
  }
  if (length(unlist(inc$leagues)) && "League" %in% names(have))
    conds["league not in include.leagues"] <- sprintf("%s IN (%s)", col("League"), ids(inc$leagues))
  if (length(unlist(inc$exclude_leagues)) && "League" %in% names(have))
    conds["league in include.exclude_leagues"] <- sprintf("coalesce(%s, '') NOT IN (%s)", col("League"), ids(inc$exclude_leagues))
  if (length(unlist(inc$exclude_teams)) && all(c("HomeTeam", "AwayTeam") %in% names(have)))
    conds["team in include.exclude_teams"] <- sprintf("coalesce(%s, '') NOT IN (%s) AND coalesce(%s, '') NOT IN (%s)",
                                                       col("HomeTeam"), ids(inc$exclude_teams), col("AwayTeam"), ids(inc$exclude_teams))
  pats <- unlist(inc$exclude_team_patterns)
  if (length(pats) && all(c("HomeTeam", "AwayTeam") %in% names(have))) {
    rx <- sql_str(paste0("(?i)(", paste(pats, collapse = "|"), ")"))
    conds["team code matches include.exclude_team_patterns"] <- sprintf(
      "NOT regexp_matches(coalesce(%s, ''), %s) AND NOT regexp_matches(coalesce(%s, ''), %s)",
      col("HomeTeam"), rx, col("AwayTeam"), rx)
  }
  # D1 games before that season's official opening day are scrimmages/exhibitions.
  od <- inc$d1_opening_day
  if (length(od) && "Level" %in% names(have)) {
    cases <- paste(sprintf("WHEN %d THEN DATE %s", as.integer(names(od)), sql_str(unlist(od))), collapse = " ")
    d1 <- sprintf("upper(regexp_replace(%s, '[^A-Za-z0-9]', '', 'g')) IN ('D1','DI','DIVISIONI','DIVISION1','NCAAD1','NCAADI','NCAADIVISIONI')", col("Level"))
    conds["D1 game before opening day (include.d1_opening_day)"] <- sprintf(
      "NOT coalesce(%s AND (%s) < (CASE year(%s) %s END), false)", d1, gd, gd, cases)
  }
  if (isTRUE(inc$require_two_teams) && all(c("HomeTeam", "AwayTeam") %in% names(have)))
    conds["same team both sides (intrasquad/bullpen/practice)"] <- sprintf(
      "nullif(%s, '') IS NOT NULL AND nullif(%s, '') IS NOT NULL AND %s <> %s",
      col("HomeTeam"), col("AwayTeam"), col("HomeTeam"), col("AwayTeam"))
  if (isTRUE(inc$require_date))
    conds["no usable game date"] <- sprintf("(%s) IS NOT NULL", gd)
  if (length(unlist(inc$months)))
    conds["outside include.months"] <- sprintf("month(%s) IN (%s)", gd, paste(as.integer(unlist(inc$months)), collapse = ", "))
  if (isTRUE(inc$exclude_practice) && "Source" %in% names(have))
    conds["practice session"] <- "coalesce(CAST(\"Source\" AS VARCHAR), '') NOT LIKE '%practice%'"
  conds
}

# Clean an in-memory data.frame and write it as one Parquet file.
# Returns c(rows_in, rows_out).
write_clean_parquet <- function(con, df, path, cfg) {
  df <- cast_frame(df)
  duckdb::duckdb_register(con, "incoming_df", df, overwrite = TRUE)
  on.exit(duckdb::duckdb_unregister(con, "incoming_df"), add = TRUE)
  sql <- clean_select_sql(con, "incoming_df", cfg)
  ensure_dir(path)
  dbExecute(con, sprintf("COPY (%s) TO %s (FORMAT parquet, COMPRESSION zstd)", sql, sql_str(path)))
  n_out <- dbGetQuery(con, sprintf("SELECT count(*) AS n FROM read_parquet(%s)", sql_str(path)))$n
  c(rows_in = nrow(df), rows_out = n_out)
}
