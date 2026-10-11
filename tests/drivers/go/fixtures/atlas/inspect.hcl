table "players" {
  schema = schema.public
  column "id" {
    null = false
    type = bigserial
  }
  column "team_id" {
    null = false
    type = integer
  }
  column "nick" {
    null    = false
    type    = text
    comment = "in-game name"
  }
  column "mmr" {
    null = true
    type = integer
  }
  column "active" {
    null    = true
    type    = boolean
    default = true
  }
  column "joined" {
    null    = true
    type    = timestamptz
    default = sql("now()")
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "players_team_id_fkey" {
    columns     = [column.team_id]
    ref_columns = [table.teams.column.id]
    on_update   = NO_ACTION
    on_delete   = NO_ACTION
  }
  index "players_active_nick_idx" {
    columns = [column.active, column.nick]
  }
  index "players_mmr_idx" {
    on {
      desc   = true
      column = column.mmr
    }
  }
  check "players_mmr_check" {
    expr = "(mmr >= 0)"
  }
  unique "players_team_id_nick_key" {
    columns = [column.team_id, column.nick]
  }
}
table "teams" {
  schema  = schema.public
  comment = "pro teams"
  column "id" {
    null = false
    type = serial
  }
  column "name" {
    null = false
    type = character_varying(40)
  }
  column "founded" {
    null = true
    type = date
  }
  column "budget" {
    null    = true
    type    = numeric(12,2)
    default = 0
  }
  primary_key {
    columns = [column.id]
  }
  unique "teams_name_key" {
    columns = [column.name]
  }
}
schema "public" {
  comment = "standard public schema"
}
