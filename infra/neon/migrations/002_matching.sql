-- Migration 002: Deal-Matching MVP tables (additive)
BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Tables
CREATE TABLE IF NOT EXISTS projects (
  project_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sponsor_name     TEXT NOT NULL,
  country_iso3     CHAR(3) NOT NULL,
  lat              DOUBLE PRECISION,
  lng              DOUBLE PRECISION,
  technology       TEXT NOT NULL,
  capacity_value   DOUBLE PRECISION,
  capacity_unit    TEXT,
  expected_cod     DATE,
  stage            TEXT,
  capex_usd        NUMERIC,
  ticket_open_usd  NUMERIC,
  instrument_needed TEXT,
  currency         TEXT,
  tenor_years      NUMERIC,
  target_irr       NUMERIC,
  offtake_status   TEXT,
  permits_status   TEXT,
  land_rights_status TEXT,
  grid_interconnect_status TEXT,
  esia_status      TEXT,
  ifc_category     TEXT,
  community_risk   TEXT,
  pri_possible     BOOLEAN,
  eca_possible     BOOLEAN,
  sovereign_support BOOLEAN,
  policy_alignment TEXT[],
  china_plus_one_fit BOOLEAN,
  nda_signed       BOOLEAN DEFAULT FALSE,
  data_room_url    TEXT,
  free_text        TEXT,
  embedding        VECTOR(1536),
  last_updated     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS investors (
  investor_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name               TEXT NOT NULL,
  type               TEXT NOT NULL,
  mandate_regions    TEXT[],
  mandate_technologies TEXT[],
  use_of_proceeds_allowed TEXT[],
  instruments_offered TEXT[],
  min_ticket_usd     NUMERIC,
  max_ticket_usd     NUMERIC,
  min_tenor_years    NUMERIC,
  max_tenor_years    NUMERIC,
  target_irr_range   TEXT,
  risk_appetite      TEXT,
  ifc_category_allowed TEXT[],
  coal_exclusion     BOOLEAN,
  other_exclusions   TEXT[],
  lending_currencies TEXT[],
  local_currency_pref BOOLEAN,
  requires_site_visit BOOLEAN,
  requires_ifc_ps    BOOLEAN,
  avg_decision_time_days INTEGER,
  themes             TEXT[],
  contacts           JSONB,
  notes              TEXT,
  free_text          TEXT,
  embedding          VECTOR(1536),
  last_updated       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS interactions (
  interaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id     UUID REFERENCES projects(project_id) ON DELETE CASCADE,
  investor_id    UUID REFERENCES investors(investor_id) ON DELETE CASCADE,
  status         TEXT,
  rating_by_investor SMALLINT,
  rating_by_sponsor  SMALLINT,
  reasons_declined   TEXT[],
  notes          TEXT,
  ts            TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS matches (
  match_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id UUID REFERENCES projects(project_id) ON DELETE CASCADE,
  investor_id UUID REFERENCES investors(investor_id) ON DELETE CASCADE,
  score_numeric NUMERIC NOT NULL,
  drivers     TEXT[],
  blockers    TEXT[],
  next_action TEXT,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_projects_country_tech ON projects(country_iso3, technology);
CREATE INDEX IF NOT EXISTS ix_investors_type ON investors(type);
CREATE UNIQUE INDEX IF NOT EXISTS ux_projects_nk ON projects(sponsor_name, country_iso3, technology, expected_cod);
CREATE UNIQUE INDEX IF NOT EXISTS ux_investors_name ON investors(name);
CREATE UNIQUE INDEX IF NOT EXISTS ux_matches_pair ON matches(project_id, investor_id);

COMMIT;

