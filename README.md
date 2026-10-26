# STOP Recovery app

## Run the app

1. Copy `.env.example` to `.env`.
2. Set `SUPABASE_URL`, `SUPABASE_SECRET_KEY`, and a strong, private `ADMIN_PASSWORD`.
   Get the secret key from your Supabase project's API settings. Never put it in
   `index.html` or share it publicly. To enable the AI Coach, set `GEMINI_API_KEY`
   to a private server key as well.
3. Run `supabase-setup.sql` in the Supabase SQL Editor.
4. Run `npm install`, then `npm start`, and open `http://localhost:3000`.

Set `TRUST_PROXY=1` only when the app is behind a trusted reverse proxy. In
production, set `NODE_ENV=production` and use HTTPS.

Payment receipts are stored in a private Supabase Storage bucket and registered
as pending in Supabase. On startup, the backend creates or secures the private
receipt bucket automatically. When the admin panel is open, Supabase Realtime
notifies it immediately; opening the panel also loads existing payments. The
admin can approve or reject each receipt, and the user's app checks for the
resulting status.

Ba9chich and D17 are currently manual payment methods: submitting a receipt does
not verify or charge a payment automatically. Automatic confirmation requires a
payment-provider webhook integration.

Keep `.env` private and out of Git. If a Gemini key was previously embedded in
the frontend or published, rotate it with Google AI Studio before adding the
replacement to `.env`.
