from datetime import UTC, datetime
from decimal import Decimal
from html import escape
from pathlib import Path
from string import Template
from typing import Any

import structlog
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool
from weasyprint import HTML

from billing.models import Invoice
from shared.db import database_url

INVOICE_DIR = Path("/invoices")
TEMPLATE = Template((Path(__file__).parent / "invoice.html").read_text())

logger = structlog.get_logger()

engine = create_engine(database_url().set(drivername="postgresql+psycopg2"), poolclass=NullPool)


def render(order_data: dict[str, Any], invoice_number: str, issued_at: datetime) -> bytes:
    rows = []
    for position, line in enumerate(order_data["lines"], start=1):
        unit_price = Decimal(line["unit_price"])
        rows.append(
            f'<tr><td class="num">{position}</td>'
            f"<td>{escape(line['product_name'])}<br><small>ID {line['product_id']}</small></td>"
            f'<td class="num">{line["quantity"]}</td>'
            f'<td class="num">{unit_price:,.2f}</td>'
            f'<td class="num">{line["quantity"] * unit_price:,.2f}</td></tr>'
        )
    html = TEMPLATE.substitute(
        invoice_number=invoice_number,
        issue_date=f"{issued_at:%Y-%m-%d}",
        order_id=order_data["order_id"],
        customer_id=order_data["customer_id"],
        rows="\n".join(rows),
        total=f"{Decimal(order_data['amount']):,.2f}",
    )
    return HTML(string=html).write_pdf()


def generate_invoice(order_data: dict[str, Any]) -> Path:
    order_id = order_data["order_id"]
    amount = Decimal(order_data["amount"])
    path = INVOICE_DIR / f"{order_id}.pdf"
    issued_at = datetime.now(UTC)
    with Session(engine) as session, session.begin():
        invoice = session.execute(
            insert(Invoice)
            .values(
                order_id=order_id,
                invoice_number=f"INV-{issued_at:%Y%m%d}-{order_id:06d}",
                amount=amount,
                file_path=str(path),
                created_at=issued_at,
            )
            .on_conflict_do_update(
                index_elements=[Invoice.order_id],
                set_={"amount": amount, "file_path": str(path)},
            )
            .returning(Invoice.invoice_number, Invoice.created_at)
        ).one()
        # Written while the row lock is held, so two runs for one order write the file in turn
        path.write_bytes(render(order_data, invoice.invoice_number, invoice.created_at))
    logger.info("invoice generated", order_id=order_id, invoice_number=invoice.invoice_number)
    return path
