import io
import os
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

def looks_like_gmail(addr: str) -> bool:
    a = (addr or "").lower()
    return a.endswith("@gmail.com") or a.endswith("@googlemail.com")


class SMTPSession:
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

    def send(self, to_addr: str, subject: str, html_body: Optional[str], text_body: Optional[str],
             headers: Dict[str, str] = {}, attachments: Optional[List[dict]] = None):
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
    smtp_port_input = st.text_input("SMTP Port", "587")
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

    if "email" not in df.columns or "first_name" not in df.columns:
        st.error("Your CSV must include both 'email' and 'first_name' columns.")
        st.stop()

    email_col = "email"

    st.subheader("2) Templates (Jinja2)")

    subject_tpl = st.text_area("Subject", "World's First Architecture OS; $262B TAM; 400+ LOIs", height=60)

    text_tpl = st.text_area(
        "Plain Text Body",
        """World's First Architecture OS; $262B TAM; 400+ LOIs

Hey {{ first_name|default('there') }},
Today, homeowners wait weeks, Architects lack speed, and SMB builders juggle ops across chats and spreadsheets.

Builtattic: an AI-first ecosystem that turns Idea < Plan < Fulfillment into minutes.

VitruviAI (prosumer): prompt-to-plan with quick variants
Studios (consumer): licensed templates, on-demand associates, curated materials
Matters (SMB): progress/inventory/payouts in one lightweight workflow

Raising $300k pre-seed to harden VitruviAI, expand supply, and run SMB pilots. If this overlaps with your investment vision, could we do a quick call?

Demo: www.builtattic.com | PASSCODE: 0Xodtixh
Site: www.builtattic.info
Attached: Pitch Deck

Regards,
Akshat
""",
        height=260,
    )

    # --- Updated HTML body (left-aligned, formal, no bold) ---
   html_tpl = st.text_area(
    "HTML Body",
    value="""<!doctype html>
<html>
  <head>
    <meta charset="UTF-8">
    <meta name="x-apple-disable-message-reformatting">
    <meta name="format-detection" content="telephone=no">
    <style>
      body {
        background-color: #ffffff;
        margin: 0;
        padding: 0;
        font-family: 'Segoe UI', Arial, Helvetica, sans-serif;
        font-size: 15px;
        color: #222222;
        line-height: 1.6;
        text-align: left;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
      }
      table {
        width: 100%;
        border-collapse: collapse;
      }
      td {
        padding: 24px 32px;
        vertical-align: top;
      }
      p { margin: 0 0 12px 0; }
      ul { margin: 0 0 12px 20px; padding: 0; }
      li { margin-bottom: 6px; }
      a { color: #0A66C2; text-decoration: none; }
      a:hover { text-decoration: underline; }
      .signature { margin-top: 20px; color: #555555; font-style: italic; }
    </style>
  </head>
  <body>
    <table role="presentation">
      <tr>
        <td>
          <p>World's First Architecture OS; $262B TAM; 400+ LOIs</p>

          <p>Hey {{ first_name|default('there') }},</p>

          <p>Today, homeowners wait weeks, architects lack speed, and SMB builders juggle operations across chats and spreadsheets.</p>

          <p>Builtattic is an AI-first ecosystem that turns Idea → Plan → Fulfillment into minutes.</p>

          <ul>
            <li>VitruviAI (prosumer): prompt-to-plan with quick variants</li>
            <li>Studios (consumer): licensed templates, on-demand associates, curated materials</li>
            <li>Matters (SMB): progress, inventory, and payouts in one lightweight workflow</li>
          </ul>

          <p>We’re raising $300k pre-seed to harden VitruviAI, expand supply, and run SMB pilots. If this overlaps with your investment vision, could we do a quick call?</p>

          <p>
            Demo: <a href="https://www.builtattic.com">www.builtattic.com</a> | PASSCODE: 0Xodtixh<br>
            Site: <a href="https://www.builtattic.info">www.builtattic.info</a><br>
            Attached: Pitch Deck
          </p>

          <p class="signature">
            Regards,<br>Akshat
          </p>
        </td>
      </tr>
    </table>
  </body>
</html>""",
    height=420,
)

    # --------------------
    # Sending section
    # --------------------
    st.subheader("6) Send Emails")

    def do_send():
        try:
            smtp_port = int(str(smtp_port_input).strip())
        except Exception:
            st.error("SMTP Port must be a number, e.g., 587.")
            return

        total = len(df)
        progress = st.progress(0)
        status = st.empty()
        delay = 60.0 / max(1, rate_per_min)

        sent_rows, failed_rows = 0, 0
        error_rows: List[Dict[str, Any]] = []

        attachments = []
        if uploads := st.session_state.get("uploads"):
            for f in uploads:
                attachments.append({"filename": f.name, "data": f.read(), "mime": f.type or "application/octet-stream"})

        for idx, row in df.iterrows():
            ctx = row.to_dict()
            to_addr = str(row[email_col]).strip()
            if not to_addr:
                failed_rows += 1
                continue

            subject = render_template(subject_tpl, ctx)
            html_body = render_template(html_tpl, ctx)
            text_body = render_template(text_tpl, ctx)

            if dry_run:
                status.info(f"(Dry run) Would send to {to_addr}")
                sent_rows += 1
                progress.progress((idx + 1) / total)
                continue

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
                    mailer.send(to_addr, subject, html_body, text_body, {}, attachments)
                sent_rows += 1
            except Exception as e:
                failed_rows += 1
                error_rows.append({"email": to_addr, "error": str(e)})

            time.sleep(delay)
            progress.progress((idx + 1) / total)

        st.success(f"Done. Sent: {sent_rows} | Failed: {failed_rows}")
        if error_rows:
            st.dataframe(pd.DataFrame(error_rows))

    if st.button("Start Sending"):
        do_send()

