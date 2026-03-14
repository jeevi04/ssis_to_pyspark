# SSIS Send Mail Task - Knowledge Base

## Purpose
Sends an e-mail message using an SMTP connection manager.

## PySpark Equivalent
Use Python's built-in `smtplib` and `email.message`.

### Pattern
```python
import smtplib
from email.message import EmailMessage

def send_mail_task(smtp_server, port, from_addr, to_addr, subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = to_addr

    with smtplib.SMTP(smtp_server, port) as s:
        s.send_message(msg)
```

## Key Considerations
- **Authentication**: Python scripts require explicit login (`s.login(user, pwd)`) if the SMTP server requires it.
- **Port/Security**: Use `smtplib.SMTP_SSL` for port 465 or `s.starttls()` for port 587.
- **Attachments**: SSIS supports multiple attachments; in Python, use `msg.add_attachment()` in a loop.
- **Configuration**: Store SMTP details in a centralized config/secrets manager.
