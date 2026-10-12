-- Ejecutar una sola vez en Supabase > SQL Editor.
-- Mantener estas tablas privadas; no exponer credenciales de Postgres al navegador.
create table if not exists public.observed_races (
    race_id bigint primary key,
    card_id bigint not null,
    first_seen timestamptz not null default now()
);
create table if not exists public.result_outbox (
    race_id bigint primary key,
    card_id bigint not null,
    message text not null,
    digest text not null,
    state text not null default 'queued'
      check (state in ('queued','sent','failed','manual_review')),
    queued_at timestamptz not null default now(),
    sent_at timestamptz
);
create index if not exists result_outbox_state_queued_idx
    on public.result_outbox (state, queued_at desc);
-- Proteger información interna: sin políticas, anon y authenticated no pueden acceder vía Data API.
alter table public.observed_races enable row level security;
alter table public.result_outbox enable row level security;
-- El worker se conecta exclusivamente mediante DATABASE_URL de Postgres en Railway.
-- Nunca colocar DATABASE_URL ni claves de servidor en Vercel público o navegador.
