create table if not exists public.payments (
    id text primary key,
    user_id text not null,
    user_name text not null,
    target_days integer not null,
    payment_method text not null,
    amount text not null,
    receipt_url text not null,
    file_name text not null,
    status text not null default 'pending' check (status in ('pending', 'approved', 'rejected')),
    status_token uuid,
    created_at timestamptz not null default now()
);

alter table public.payments add column if not exists status_token uuid;
update public.payments set status_token = gen_random_uuid() where status_token is null;
alter table public.payments alter column status_token set not null;
create unique index if not exists payments_status_token_key on public.payments (status_token);

insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values (
    'payment-receipts',
    'payment-receipts',
    false,
    5242880,
    array['image/jpeg', 'image/png', 'image/webp', 'image/gif', 'application/pdf']
)
on conflict (id) do update
set public = false,
    file_size_limit = excluded.file_size_limit,
    allowed_mime_types = excluded.allowed_mime_types;

alter table public.payments enable row level security;
revoke all on public.payments from anon, authenticated;
grant usage on schema public to service_role;
grant all on public.payments to service_role;

do $$
begin
    if not exists (
        select 1
        from pg_publication_tables
        where pubname = 'supabase_realtime'
          and schemaname = 'public'
          and tablename = 'payments'
    ) then
        alter publication supabase_realtime add table public.payments;
    end if;
end
$$;

notify pgrst, 'reload schema';
