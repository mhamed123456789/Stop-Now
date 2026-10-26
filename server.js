require('dotenv').config();

const crypto = require('node:crypto');
const path = require('node:path');
const express = require('express');
const { createClient } = require('@supabase/supabase-js');

const requiredEnvironment = ['SUPABASE_URL', 'SUPABASE_SECRET_KEY', 'ADMIN_PASSWORD'];
const missingEnvironment = requiredEnvironment.filter(name => !process.env[name]);
if (missingEnvironment.length) {
    console.error(`Missing required environment variables: ${missingEnvironment.join(', ')}`);
    process.exit(1);
}
if (process.env.ADMIN_PASSWORD.length < 12) {
    console.error('ADMIN_PASSWORD must contain at least 12 characters.');
    process.exit(1);
}

const app = express();
app.set('trust proxy', process.env.TRUST_PROXY === '1');
const port = Number(process.env.PORT) || 3000;
const sessionDurationMs = 8 * 60 * 60 * 1000;
const receiptBucketName = 'payment-receipts';
const receiptBucketOptions = {
    public: false,
    fileSizeLimit: 5 * 1024 * 1024,
    allowedMimeTypes: ['image/jpeg', 'image/png', 'image/webp', 'image/gif', 'application/pdf']
};
const sessions = new Map();
const failedAdminLogins = new Map();
const adminEventStreams = new Set();
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SECRET_KEY, {
    auth: { persistSession: false, autoRefreshToken: false }
});

app.use(express.json({ limit: '7mb' }));
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.post('/api/chat', async (req, res) => {
    const { prompt, user_name, target_days, trigger } = req.body || {};
    if (typeof prompt !== 'string' || !prompt.trim() || prompt.length > 2000) {
        return res.status(400).json({ error: 'Message must be between 1 and 2000 characters.' });
    }
    if (typeof user_name !== 'string' || user_name.length > 100) {
        return res.status(400).json({ error: 'Invalid user name.' });
    }
    if (!Number.isInteger(target_days) || target_days < 1 || target_days > 365) {
        return res.status(400).json({ error: 'Invalid recovery target.' });
    }
    if (typeof trigger !== 'string' || trigger.length > 100) {
        return res.status(400).json({ error: 'Invalid recovery trigger.' });
    }
    if (!process.env.GEMINI_API_KEY) {
        return res.status(503).json({ error: 'AI Coach is not configured on the server yet.' });
    }

    const systemInstruction = `You are the official STOP Recovery Assistant, an empathetic, firm, and knowledgeable addiction coach.
User details: Name: ${user_name || 'User'}, Target Clean Goal: ${target_days} days, Primary Trigger: ${trigger || 'not specified'}.
Provide short, highly motivating, actionable advice. Support Arabic, Tunisian Darija, French, and English. Keep responses focused on urge surfing, dopamine restoration, and willpower.`;

    try {
        const response = await fetch('https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-goog-api-key': process.env.GEMINI_API_KEY
            },
            body: JSON.stringify({
                contents: [{ parts: [{ text: prompt.trim() }] }],
                systemInstruction: { parts: [{ text: systemInstruction }] }
            }),
            signal: AbortSignal.timeout(30000)
        });
        if (!response.ok) {
            console.error(`Gemini API request failed with status ${response.status}.`);
            return res.status(502).json({ error: 'AI Coach is temporarily unavailable. Please try again.' });
        }
        const result = await response.json();
        const reply = result.candidates?.[0]?.content?.parts?.[0]?.text;
        if (typeof reply !== 'string' || !reply.trim()) {
            console.error('Gemini API returned no text response.');
            return res.status(502).json({ error: 'AI Coach returned an empty response. Please try again.' });
        }
        return res.json({ reply });
    } catch (error) {
        console.error('Gemini API request could not be completed:', error.message);
        return res.status(502).json({ error: 'AI Coach is temporarily unavailable. Please try again.' });
    }
});

function getSession(req) {
    const cookie = req.headers.cookie || '';
    const token = cookie.split(';').map(part => part.trim()).find(part => part.startsWith('stop_admin_session='));
    if (!token) return null;

    const sessionId = token.slice('stop_admin_session='.length);
    const expiresAt = sessions.get(sessionId);
    if (!expiresAt || expiresAt <= Date.now()) {
        sessions.delete(sessionId);
        return null;
    }
    return sessionId;
}

function requireAdmin(req, res, next) {
    if (!getSession(req)) {
        return res.status(401).json({ error: 'Admin login required.' });
    }
    next();
}

function parseReceiptDataUrl(value) {
    if (typeof value !== 'string') return null;
    const match = value.match(/^data:(image\/(?:jpeg|png|webp|gif)|application\/pdf);base64,([A-Za-z0-9+/]+={0,2})$/);
    if (!match) return null;
    const buffer = Buffer.from(match[2], 'base64');
    if (buffer.length > 5 * 1024 * 1024) return null;
    const extensions = {
        'image/jpeg': 'jpg',
        'image/png': 'png',
        'image/webp': 'webp',
        'image/gif': 'gif',
        'application/pdf': 'pdf'
    };
    return { buffer, contentType: match[1], extension: extensions[match[1]] };
}

async function ensureReceiptBucket() {
    const { data: buckets, error: listError } = await supabase.storage.listBuckets();
    if (listError) throw new Error(`Could not check Supabase storage buckets: ${listError.message}`);

    const bucketExists = buckets.some(bucket => bucket.id === receiptBucketName);
    const { error } = bucketExists
        ? await supabase.storage.updateBucket(receiptBucketName, receiptBucketOptions)
        : await supabase.storage.createBucket(receiptBucketName, receiptBucketOptions);
    if (error) throw new Error(`Could not configure private receipt storage: ${error.message}`);
}

function sendPaymentChange() {
    for (const response of adminEventStreams) {
        response.write('event: payments-changed\ndata: {}\n\n');
    }
}

let paymentChannel = null;

function startPaymentRealtime() {
    paymentChannel = supabase
        .channel('admin-payment-notifications')
        .on('postgres_changes', { event: '*', schema: 'public', table: 'payments' }, sendPaymentChange)
        .subscribe((status, error) => {
            if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT') {
                console.error('Supabase Realtime subscription failed:', error || status);
            }
        });
}

app.post('/api/admin/login', (req, res) => {
    const password = req.body && req.body.password;
    if (typeof password !== 'string') {
        return res.status(400).json({ error: 'Password is required.' });
    }

    const ip = req.ip;
    const attempts = failedAdminLogins.get(ip);
    if (attempts && attempts.count >= 5 && attempts.startedAt + 15 * 60 * 1000 > Date.now()) {
        return res.status(429).json({ error: 'Too many login attempts. Try again in 15 minutes.' });
    }

    const supplied = Buffer.from(password);
    const expected = Buffer.from(process.env.ADMIN_PASSWORD);
    const valid = supplied.length === expected.length && crypto.timingSafeEqual(supplied, expected);
    if (!valid) {
        const withinWindow = attempts && attempts.startedAt + 15 * 60 * 1000 > Date.now();
        failedAdminLogins.set(ip, {
            count: withinWindow ? attempts.count + 1 : 1,
            startedAt: withinWindow ? attempts.startedAt : Date.now()
        });
        return res.status(401).json({ error: 'Incorrect admin password.' });
    }

    failedAdminLogins.delete(ip);
    const sessionId = crypto.randomBytes(32).toString('hex');
    sessions.set(sessionId, Date.now() + sessionDurationMs);
    const secureCookie = process.env.NODE_ENV === 'production' ? '; Secure' : '';
    res.setHeader('Set-Cookie', `stop_admin_session=${sessionId}; HttpOnly; SameSite=Strict; Path=/; Max-Age=28800${secureCookie}`);
    res.json({ ok: true });
});

app.post('/api/admin/logout', requireAdmin, (req, res) => {
    const sessionId = getSession(req);
    if (sessionId) sessions.delete(sessionId);
    res.setHeader('Set-Cookie', 'stop_admin_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0');
    res.json({ ok: true });
});

app.post('/api/payments', async (req, res) => {
    const { user_id, user_name, target_days, payment_method, receipt_url, file_name } = req.body || {};
    if (typeof user_id !== 'string' || user_id.length < 1 || user_id.length > 100) {
        return res.status(400).json({ error: 'Invalid user id.' });
    }
    if (typeof user_name !== 'string' || user_name.trim().length < 1 || user_name.length > 100) {
        return res.status(400).json({ error: 'Invalid user name.' });
    }
    if (!Number.isInteger(target_days) || target_days < 1 || target_days > 365) {
        return res.status(400).json({ error: 'Invalid recovery target.' });
    }
    if (payment_method !== 'ba9chich' && payment_method !== 'd17') {
        return res.status(400).json({ error: 'Invalid payment method.' });
    }
    const receiptFile = parseReceiptDataUrl(receipt_url);
    if (!receiptFile) {
        return res.status(400).json({ error: 'Receipt must be a supported image or PDF under 5 MB.' });
    }
    if (typeof file_name !== 'string' || file_name.trim().length < 1 || file_name.length > 255) {
        return res.status(400).json({ error: 'Invalid receipt file name.' });
    }

    const receiptPath = `${crypto.randomUUID()}.${receiptFile.extension}`;
    const payment = {
        id: `PAY-${crypto.randomUUID()}`,
        user_id,
        user_name: user_name.trim(),
        target_days,
        payment_method: payment_method === 'd17' ? 'D17 Card (90272020)' : 'Ba9chich Pay',
        amount: '50 TND (Promo 24H)',
        receipt_url: receiptPath,
        file_name: path.basename(file_name.replace(/\\/g, '/')),
        status: 'pending',
        status_token: crypto.randomUUID(),
        created_at: new Date().toISOString()
    };

    let receiptUploaded = false;
    try {
        const { error: uploadError } = await supabase.storage
            .from('payment-receipts')
            .upload(receiptPath, receiptFile.buffer, {
                contentType: receiptFile.contentType,
                upsert: false
            });
        if (uploadError) {
            console.error('Supabase receipt upload failed:', uploadError.message);
            return res.status(502).json({ error: 'Could not upload payment proof. Please try again.' });
        }
        receiptUploaded = true;

        const { error } = await supabase.from('payments').insert(payment);
        if (error) {
            console.error('Supabase payment insert failed:', error.message);
            const { error: cleanupError } = await supabase.storage.from('payment-receipts').remove([receiptPath]);
            if (cleanupError) console.error('Could not remove unlinked receipt:', cleanupError.message);
            return res.status(502).json({ error: 'Could not save payment proof. Please try again.' });
        }
        return res.status(201).json({ status_token: payment.status_token });
    } catch (error) {
        if (receiptUploaded) {
            try {
                const { error: cleanupError } = await supabase.storage.from('payment-receipts').remove([receiptPath]);
                if (cleanupError) console.error('Could not remove unlinked receipt:', cleanupError.message);
            } catch (cleanupError) {
                console.error('Could not remove unlinked receipt:', cleanupError);
            }
        }
        console.error('Payment submission failed:', error);
        return res.status(502).json({ error: 'Could not save payment proof. Please try again.' });
    }
});

app.get('/api/payments/status/:token', async (req, res) => {
    if (!/^[0-9a-f-]{36}$/i.test(req.params.token)) {
        return res.status(400).json({ error: 'Invalid payment status token.' });
    }
    try {
        const { data, error } = await supabase
            .from('payments')
            .select('status')
            .eq('status_token', req.params.token)
            .maybeSingle();
        if (error) {
            console.error('Supabase payment status query failed:', error.message);
            return res.status(502).json({ error: 'Could not check payment status.' });
        }
        if (!data) return res.status(404).json({ error: 'Payment not found.' });
        return res.json({ status: data.status });
    } catch (error) {
        console.error('Payment status check failed:', error);
        return res.status(502).json({ error: 'Could not check payment status.' });
    }
});

app.get('/api/admin/payments', requireAdmin, async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('payments')
            .select('id, user_id, user_name, target_days, payment_method, amount, receipt_url, file_name, status, created_at')
            .order('created_at', { ascending: false });
        if (error) {
            console.error('Supabase admin payment query failed:', error.message);
            return res.status(502).json({ error: 'Could not load payments.' });
        }
        const payments = data.map(payment => ({
            ...payment,
            receipt_url: payment.receipt_url.startsWith('data:')
                ? payment.receipt_url
                : `/api/admin/receipts/${encodeURIComponent(payment.receipt_url)}`
        }));
        return res.json({ payments });
    } catch (error) {
        console.error('Admin payment query failed:', error);
        return res.status(502).json({ error: 'Could not load payments.' });
    }
});

app.get('/api/admin/receipts/:path', requireAdmin, async (req, res) => {
    const receiptPath = req.params.path;
    if (!/^[0-9a-f-]{36}\.(?:jpg|png|webp|gif|pdf)$/i.test(receiptPath)) {
        return res.status(400).json({ error: 'Invalid receipt path.' });
    }
    try {
        const { data, error } = await supabase.storage.from('payment-receipts').download(receiptPath);
        if (error) {
            console.error('Supabase receipt download failed:', error.message);
            return res.status(404).json({ error: 'Receipt not found.' });
        }
        const contentType = {
            jpg: 'image/jpeg',
            png: 'image/png',
            webp: 'image/webp',
            gif: 'image/gif',
            pdf: 'application/pdf'
        }[path.extname(receiptPath).slice(1).toLowerCase()];
        res.setHeader('Content-Type', contentType);
        res.setHeader('Content-Disposition', 'inline');
        res.setHeader('X-Content-Type-Options', 'nosniff');
        return res.send(Buffer.from(await data.arrayBuffer()));
    } catch (error) {
        console.error('Receipt download failed:', error);
        return res.status(502).json({ error: 'Could not load receipt.' });
    }
});

app.patch('/api/admin/payments/:id/status', requireAdmin, async (req, res) => {
    const { status } = req.body || {};
    if (status !== 'approved' && status !== 'rejected') {
        return res.status(400).json({ error: 'Status must be approved or rejected.' });
    }
    try {
        const { data, error } = await supabase
            .from('payments')
            .update({ status })
            .eq('id', req.params.id)
            .select('id')
            .maybeSingle();
        if (error) {
            console.error('Supabase payment update failed:', error.message);
            return res.status(502).json({ error: 'Could not update payment.' });
        }
        if (!data) return res.status(404).json({ error: 'Payment not found.' });
        return res.json({ ok: true });
    } catch (error) {
        console.error('Admin payment update failed:', error);
        return res.status(502).json({ error: 'Could not update payment.' });
    }
});

app.get('/api/admin/payment-events', requireAdmin, (req, res) => {
    const sessionId = getSession(req);
    const expiresAt = sessions.get(sessionId);
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();
    res.write(': connected\n\n');
    adminEventStreams.add(res);
    const expiryTimer = setTimeout(() => {
        adminEventStreams.delete(res);
        res.end();
    }, Math.max(0, expiresAt - Date.now()));

    res.on('close', () => {
        clearTimeout(expiryTimer);
        adminEventStreams.delete(res);
    });
});

app.use((error, req, res, next) => {
    console.error('Request processing failed:', error);
    if (res.headersSent) return next(error);
    const status = error.status === 413 ? 413 : 400;
    const message = status === 413 ? 'Request is too large.' : 'Invalid request.';
    return res.status(status).json({ error: message });
});

setInterval(() => {
    for (const response of adminEventStreams) {
        response.write(': keep-alive\n\n');
    }
}, 25 * 1000).unref();

setInterval(() => {
    const now = Date.now();
    for (const [sessionId, expiresAt] of sessions) {
        if (expiresAt <= now) sessions.delete(sessionId);
    }
    for (const [ip, attempts] of failedAdminLogins) {
        if (attempts.startedAt + 15 * 60 * 1000 <= now) failedAdminLogins.delete(ip);
    }
}, 60 * 60 * 1000).unref();

async function startServer() {
    await ensureReceiptBucket();
    startPaymentRealtime();
    app.listen(port, () => {
        console.log(`STOP app server listening on port ${port}`);
    });
}

startServer().catch(error => {
    console.error('Could not start STOP app server:', error.message);
    process.exitCode = 1;
});

process.on('SIGTERM', async () => {
    if (paymentChannel) await supabase.removeChannel(paymentChannel);
    process.exit(0);
});
