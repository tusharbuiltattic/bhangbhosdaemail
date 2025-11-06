import io
import re
import time
import ssl
import csv
import smtplib
import socket
import uuid
from typing import Dict, Any, Optional, List

import pandas as pd
import streamlit as st
from jinja2 import Template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


# --------------------
# Helpers
# --------------------
def render_template(tpl: Optional[str], ctx: Dict[str, Any]) -> Optional[str]:
    if not tpl:
        return None
    return Template(tpl).render(**ctx)

def backoff(attempt: int, base: float = 1.5, cap: float = 60.0) -> float:
    wait = min(cap, (base ** (attempt - 1)))
    jitter = 0.2 * wait * (2 * (uuid.uuid4().int % 2) - 1)
    return max(0.0, wait + jitter)

def extract_email(addr: str) -> str:
    if not addr:
        return ""
    m = re.search(r"<([^>]+)>", addr)
    return (m.group(1) if m else addr).strip()


class SMTPSession:
    # keyword-only for timeout/from_addr (prevents mis-ordered args)
    def __init__(self, host, port, user, password, use_ssl, use_tls, *, timeout: float = 30.0, from_addr: str = ""):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.use_ssl = bool(use_ssl)
        self.use_tls = bool(use_tls)
        self.timeout = float(timeout)
        self.from_addr = from_addr
        self.server = None

    def __enter__(self):
        context = ssl.create_default_context()
        if self.use_ssl:
            self.server = smtplib.SMTP_SSL(self.host, self.port, timeout=self.timeout, context=context)
        else:
            self.server = smtplib.SMTP(self.host, self.port, timeout=self.timeout)
            self.server.ehlo()
            if self.use_tls:
                self.server.starttls(context=context)
        if self.user and self.password:
            self.server.login(self.user, self.password)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.server:
                self.server.quit()
        except Exception:
            pass

    def send(
        self,
        to_addr: str,
        subject: str,
        html_body: Optional[str],
        text_body: Optional[str],
        headers: Dict[str, str] = {},
        attachments: Optional[List[dict]] = None,
    ):
        msg = MIMEMultipart("mixed")
        msg["From"] = self.from_addr
        msg["To"] = to_addr
        msg["Subject"] = subject
        for k, v in headers.items():
            if v:
                msg[k] = v

        alt = MIMEMultipart("alternative")
        if text_body:
            alt.attach(MIMEText(text_body, "plain", "utf-8"))
        if html_body:
            alt.attach(MIMEText(html_body, "html", "utf-8"))
        msg.attach(alt)

        if attachments:
            for part in attachments:
                filename = part.get("filename", "attachment")
                data = part.get("data", b"")
                mime = part.get("mime", "application/octet-stream") or "application/octet-stream"
                main, sub = (mime.split("/", 1) if "/" in mime else ("application", "octet-stream"))
                file_part = MIMEBase(main, sub)
                file_part.set_payload(data)
                encoders.encode_base64(file_part)
                file_part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
                msg.attach(file_part)

        self.server.sendmail(self.from_addr, [to_addr], msg.as_string())


# --------------------
# Streamlit UI
# --------------------
st.set_page_config(page_title="Bulk Mailer", page_icon="✉️", layout="wide")
st.title("✉️ Local Streamlit Bulk Mailer (Gmail App Password)")

with st.sidebar:
    st.header("Gmail SMTP Settings")
    st.caption("Use: smtp.gmail.com • Port 587 • STARTTLS ON • SSL OFF")
    smtp_host = st.text_input("SMTP Host", "smtp.gmail.com")
    smtp_port_input = st.text_input("SMTP Port", "587")  # cast later
    smtp_user = st.text_input("Gmail Address (username)", "")
    smtp_pass = st.text_input("App Password (16 chars)", "", type="password")
    from_addr = st.text_input("From (e.g. Your Name <you@gmail.com>)", "")
    use_ssl = st.checkbox("Use SSL (port 465)", value=False)
    use_tls = st.checkbox("Use STARTTLS", value=True)

    st.divider()
    st.header("Sending Controls")
    rate_per_min = st.number_input("Rate per minute", min_value=1, value=10)
    batch_size = st.number_input("Batch size (reconnect every N)", min_value=1, value=100)
    max_retries = st.number_input("Max retries per recipient", min_value=0, value=3)
    dry_run = st.checkbox("Dry run (render only, don't send)", value=True)

st.subheader("1) Upload your CSV (must include 'email' and 'first_name')")
file_csv = st.file_uploader("Upload CSV file", type=["csv"])

if file_csv:
    df = pd.read_csv(file_csv)
    st.dataframe(df.head(10), use_container_width=True)

    # Validate required columns
    if "email" not in df.columns:
        st.error("Your CSV must include an 'email' column.")
        st.stop()
    if "first_name" not in df.columns:
        st.error("Your CSV must include a 'first_name' column.")
        st.stop()

    email_col = "email"  # fixed

    st.subheader("2) Templates (Jinja2)")

    subject_tpl = st.text_area(
        "Subject",
        "World's First Architecture OS; $262B TAM; 400+ LOIs",
        height=60,
    )

    # Plain text body – keep EXACT wording
    text_tpl = st.text_area(
        "Plain Text Body",
        """World's First Architecture OS; $262B TAM; 400+ LOIs

Hey {{ first_name|default('there') }},
Today, homeowners wait weeks, architects lack speed, and SMB builders juggle operations across chats and spreadsheets.

Builtattic is an AI-first ecosystem that turns Idea → Plan → Fulfillment into minutes.

VitruviAI (prosumer): prompt-to-plan with quick variants
Studios (consumer): licensed templates, on-demand associates, curated materials
Matters (SMB): progress, inventory, and payouts in one lightweight workflow

We’re raising $300k pre-seed to harden VitruviAI, expand supply, and run SMB pilots. If this overlaps with your investment vision, could we do a quick call?

Demo: www.builtattic.com | PASSCODE: 0Xodtixh
Site: www.builtattic.info
Attached: Pitch Deck

Regards,
Akshat
""",
        height=260,
    )

    # --- Default Gmail-style HTML (keeps wording; natural gaps; left-aligned) ---
    # --- Super-clean Gmail-native look (minimal spacing, no extra padding) ---
    html_tpl = st.text_area(
        "HTML Body",
        value="""<!doctype html>
<html>
  <head>
    <meta charset="UTF-8">
  </head>
  <body style="margin:0;padding:0;font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#202124;line-height:1.4;">
    <p>World's First Architecture OS; $262B TAM; 400+ LOIs</p>

    <p>Hey {{ first_name|default('there') }},</p>

    <p>Today, homeowners wait weeks, architects lack speed, and SMB builders juggle operations across chats and spreadsheets.</p>

    <p>Builtattic is an AI-first ecosystem that turns Idea → Plan → Fulfillment into minutes.</p>

    <p>VitruviAI (prosumer): prompt-to-plan with quick variants<br>
    Studios (consumer): licensed templates, on-demand associates, curated materials<br>
    Matters (SMB): progress, inventory, and payouts in one lightweight workflow</p>

    <p>We’re raising $300k pre-seed to harden VitruviAI, expand supply, and run SMB pilots. 
    If this overlaps with your investment vision, could we do a quick call?</p>

    <p>Demo: <a href="https://www.builtattic.com" style="color:#1a73e8;">www.builtattic.com</a> | PASSCODE: 0Xodtixh<br>
    Site: <a href="https://www.builtattic.info" style="color:#1a73e8;">www.builtattic.info</a><br>
    Attached: Pitch Deck</p>

    <p>Regards,<br>
    Akshat</p>
  </body>
</html>""",
        height=340,
    )

    st.subheader("3) Optional Headers")
    reply_to = st.text_input("Reply-To (leave blank to skip)")
    list_unsub = st.text_input("List-Unsubscribe (URL or <mailto:...>) — optional")

    st.subheader("4) Attachments (PDF, images, etc.)")
    uploads = st.file_uploader("Attach files", accept_multiple_files=True)

    st.subheader("5) Preview")
    n_preview = st.slider("Preview how many emails?", 1, min(10, len(df)), 3)
    if st.button("Render Previews"):
        for i, row in df.head(n_preview).iterrows():
            ctx = row.to_dict()
            subj = render_template(subject_tpl, ctx)
            body = render_template(text_tpl, ctx)
            st.markdown(f"**To:** {row[email_col]}")
            st.markdown(f"**Subject:** {subj}")
            st.code(body, language="text")
            st.divider()

    st.subheader("6) Send Emails")

    def do_send():
        # Port as int
        try:
            smtp_port = int(str(smtp_port_input).strip())
        except Exception:
            st.error("SMTP Port must be a number, e.g., 587.")
            return

        total = len(df)
        progress = st.progress(0)
        status = st.empty()
        delay = 60.0 / max(1, rate_per_min)

        # Static headers
        headers_static = {}
        if reply_to:
            headers_static["Reply-To"] = reply_to
        if list_unsub:
            headers_static["List-Unsubscribe"] = list_unsub
            headers_static["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"

        # Attachments (read once)
        attachments = []
        if uploads:
            for f in uploads:
                attachments.append({
                    "filename": f.name,
                    "data": f.read(),
                    "mime": (f.type or "application/octet-stream")
                })

        sent_rows = 0
        failed_rows = 0
        error_rows: List[Dict[str, Any]] = []

        batches = (total + int(batch_size) - 1) // int(batch_size)

        for b in range(batches):
            start_i = b * int(batch_size)
            end_i = min(total, (b + 1) * int(batch_size))

            try:
                with SMTPSession(
                    host=smtp_host,
                    port=smtp_port,
                    user=smtp_user,
                    password=smtp_pass,
                    use_ssl=use_ssl,
                    use_tls=use_tls,
                    timeout=30.0,
                    from_addr=from_addr,
                ) as mailer:

                    last_sent = 0.0

                    for idx in range(start_i, end_i):
                        row = df.iloc[idx]
                        ctx = row.to_dict()
                        to_addr = str(row[email_col]).strip()
                        if not to_addr:
                            failed_rows += 1
                            error_rows.append({"email": "", "code": None, "error": "missing recipient email"})
                            progress.progress((idx + 1) / total)
                            continue

                        try:
                            subject = render_template(subject_tpl, ctx) or ""
                            html_body = render_template(html_tpl, ctx)
                            text_body = render_template(text_tpl, ctx)
                        except Exception as e:
                            failed_rows += 1
                            error_rows.append({"email": to_addr, "code": None, "error": f"template_error: {e}"})
                            status.error(f"Template error for {to_addr}: {e}")
                            progress.progress((idx + 1) / total)
                            continue

                        # Rate limit
                        elapsed = time.time() - last_sent
                        if elapsed < delay:
                            time.sleep(delay - elapsed)

                        if dry_run:
                            status.info(f"(Dry run) Would send to {to_addr}")
                            sent_rows += 1
                            progress.progress((idx + 1) / total)
                            continue

                        attempts = 0
                        while True:
                            attempts += 1
                            try:
                                headers = dict(headers_static)
                                # Per-row unsubscribe header if present
                                if "unsubscribe_url" in row and pd.notna(row["unsubscribe_url"]) and "List-Unsubscribe" not in headers:
                                    headers["List-Unsubscribe"] = str(row["unsubscribe_url"]).strip()
                                    headers["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"

                                mailer.send(
                                    to_addr=to_addr,
                                    subject=subject,
                                    html_body=html_body,
                                    text_body=text_body,
                                    headers=headers,
                                    attachments=attachments,
                                )
                                last_sent = time.time()
                                sent_rows += 1
                                break
                            except (
                                smtplib.SMTPServerDisconnected,
                                smtplib.SMTPDataError,
                                smtplib.SMTPConnectError,
                                smtplib.SMTPHeloError,
                                smtplib.SMTPRecipientsRefused,
                                smtplib.SMTPSenderRefused,
                                smtplib.SMTPResponseException,
                                socket.timeout,
                                ConnectionResetError,
                            ) as e:
                                code = getattr(e, "smtp_code", None)
                                err_bytes = getattr(e, "smtp_error", b"")
                                err_msg = err_bytes.decode(errors="ignore") if isinstance(err_bytes, (bytes, bytearray)) else str(err_bytes or e)
                                transient = (code is None) or (400 <= int(code) < 500)
                                if not transient or attempts >= int(max_retries):
                                    failed_rows += 1
                                    error_rows.append({"email": to_addr, "code": code, "error": err_msg})
                                    status.error(f"Failed {to_addr}: [{code}] {err_msg}")
                                    break
                                time.sleep(backoff(attempts))

                        progress.progress((idx + 1) / total)
                        status.write(f"Processed {idx + 1}/{total}")

            except Exception as e:
                # Batch-level failure
                for idx in range(start_i, end_i):
                    row = df.iloc[idx]
                    to_addr = str(row[email_col]) if email_col in row else ""
                    failed_rows += 1
                    error_rows.append({"email": to_addr, "code": None, "error": f"batch_error: {e}"})
                status.error(f"Batch connection error: {e}")

        st.success(f"Done. Sent: {sent_rows} | Failed: {failed_rows}")

        # Failure diagnostics table + download
        if error_rows:
            st.warning("Failure details")
            st.dataframe(pd.DataFrame(error_rows), use_container_width=True)

            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=["email", "code", "error"])
            writer.writeheader()
            for r in error_rows:
                writer.writerow(r)
            st.download_button(
                "Download failure_log.csv",
                buf.getvalue(),
                file_name="failure_log.csv",
                mime="text/csv",
            )

    send_clicked = st.button("Start Sending")
    if send_clicked:
        # Basic validation
        missing = []
        if not smtp_host:
            missing.append("SMTP host")
        if not from_addr:
            missing.append("From address")
        if not dry_run and not smtp_user:
            missing.append("Gmail address (username)")
        if not dry_run and not smtp_pass:
            missing.append("App password")
        if missing:
            st.error("Missing: " + ", ".join(missing))
        else:
            do_send()

