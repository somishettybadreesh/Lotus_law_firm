from datetime import datetime
from dateutil.parser import parse as dateparse
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import io
from flask_migrate import Migrate
import pytz
from werkzeug.utils import secure_filename
import os
import tempfile
from sqlalchemy import or_, func

app = Flask(__name__)
app.secret_key = "change-this-secret-key"

# Database
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///data.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)

# Ensure instance/tmp directory exists for temp uploads
os.makedirs(os.path.join(app.instance_path, "tmp"), exist_ok=True)

# ---------- Models ----------
class Bill(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    bill_no = db.Column(db.String(100), unique=True, nullable=False, index=True)
    bill_date = db.Column(db.Date, nullable=False)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'), nullable=False, index=True)
    amount = db.Column(db.Float, nullable=False)
    description = db.Column(db.String(50000), nullable=True)
    remarks = db.Column(db.String(50000), nullable=True)
    Subject = db.Column(db.String(255), nullable=True)
    client = db.relationship('Client', backref=db.backref('bills', lazy=True))

class Receipt(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    receipt_ref = db.Column(db.String(100), nullable=True, index=True)
    receipt_date = db.Column(db.Date, nullable=False)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'), nullable=False, index=True)
    bill_no = db.Column(db.String(100), nullable=True, index=True)
    tds_amt = db.Column(db.Float, nullable=True, default=0.0)
    collection_amount = db.Column(db.Float, nullable=False)
    utr_details = db.Column(db.String(255), nullable=True)
    mode = db.Column(db.String(100), nullable=True)
    remarks = db.Column(db.String(10000), nullable=True)
    client = db.relationship('Client', backref=db.backref('receipts', lazy=True))

class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    address = db.Column(db.String(200))
    gst_no = db.Column(db.String(500))
    pan_no = db.Column(db.String(500))
    remarks = db.Column(db.Text)

# ---------- Utilities ----------
def parse_date(s, default=None):
    if not s:
        return default
    try:
        return dateparse(s, dayfirst=True).date()
    except Exception:
        return default

ALLOWED_IMPORT_EXTS = {".csv", ".xlsx", ".xls"}

def _read_tabular(path: str) -> pd.DataFrame:
    lp = path.lower()
    if lp.endswith(".csv"):
        return pd.read_csv(path)
    if lp.endswith(".xlsx") or lp.endswith(".xls"):
        return pd.read_excel(path)
    raise ValueError("Unsupported file type")

def _required_missing(df: pd.DataFrame, required_cols: list[str]) -> list[str]:
    return [c for c in required_cols if c not in df.columns]


# All Bills: search only Bill No or Client name
def apply_bill_search(query, q):
    if not q:
        return query
    like = f"%{q}%"
    return query.filter(or_(
        func.lower(Bill.bill_no).ilike(func.lower(like)),   # Bill No [1]
        func.lower(Client.name).ilike(func.lower(like)),    # Client name [1]
    ))

# All Receipts: search only Client name, UTR, Bill No
def apply_receipt_search(query, q):
    if not q:
        return query
    like = f"%{q}%"
    return query.filter(or_(
        func.lower(Client.name).ilike(func.lower(like)),     # Client name [1]
        func.lower(Receipt.utr_details).ilike(func.lower(like)),  # UTR [1]
        func.lower(Receipt.bill_no).ilike(func.lower(like)),      # Bill No [1]
    ))



def build_pagination(total, page, per_page, url_builder):
    pages = max(1, (total + per_page - 1) // per_page)
    page = min(max(1, page), pages)
    return {
        "page": page,
        "per_page": per_page,
        "pages": pages,
        "total": total,
        "has_prev": page > 1,
        "has_next": page < pages,
        "prev_url": url_builder(page - 1) if page > 1 else None,
        "next_url": url_builder(page + 1) if page < pages else None,
        "start_idx": 0 if total == 0 else (page - 1) * per_page + 1,
        "end_idx": 0 if total == 0 else min(page * per_page, total),
    }
    
# ---------- Root ----------
@app.route("/")
def index():
    return redirect(url_for("dashboard"))

# ---------- Clients ----------
@app.route("/clients", methods=["GET"])
def list_clients():
    all_clients = Client.query.order_by(Client.name.asc()).all()
    return render_template("clients.html", clients=all_clients)

@app.route("/clients", methods=["POST"])
def add_client():
    name = (request.form.get("name") or "").strip()
    if not name:
        flash("Client name cannot be empty.", "danger")
        return redirect(url_for("list_clients"))
    exists = Client.query.filter(db.func.lower(Client.name) == name.lower()).first()
    if exists:
        flash("Client already exists.", "danger")
        return redirect(url_for("list_clients"))
    new_client = Client(
        name=name,
        address=(request.form.get("address") or "").strip(),
        gst_no=(request.form.get("gst_no") or "").strip(),
        pan_no=(request.form.get("pan_no") or "").strip(),
        remarks=(request.form.get("remarks") or "").strip()
    )
    try:
        db.session.add(new_client)
        db.session.commit()
        flash("Client added successfully.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Could not add client: {str(e)}", "danger")
    return redirect(url_for("list_clients"))

@app.route("/clients/<int:cid>/edit", methods=["GET", "POST"])
def edit_client(cid):
    client = Client.query.get_or_404(cid)
    if request.method == "POST":
        client.name = (request.form.get("name") or "").strip()
        client.address = (request.form.get("address") or "").strip()
        client.gst_no = (request.form.get("gst_no") or "").strip()
        client.pan_no = (request.form.get("pan_no") or "").strip()
        client.remarks = (request.form.get("remarks") or "").strip()
        try:
            db.session.commit()
            flash("Client updated", "success")
            return redirect(url_for("list_clients"))
        except Exception:
            db.session.rollback()
            flash("Could not update client", "danger")
    return render_template("client_edit.html", client=client)

# ---------- Bills ----------
@app.route("/bills", methods=["GET", "POST"])
def bills():
    clients = Client.query.order_by(Client.name.asc()).all()
    if request.method == "POST":
        bill_no = request.form.get("bill_no", "").strip()
        bill_date_str = request.form.get("bill_date")
        client_id = request.form.get("client_id")
        amount = float(request.form.get("amount") or 0)
        description = request.form.get("description", "").strip()
        remarks = request.form.get("remarks", "").strip()
        Subject = request.form.get("Subject", "").strip()

        if not bill_no or not bill_date_str or not client_id or amount <= 0:
            flash("Please fill Bill No, Date, Client and positive Amount.", "danger")
        elif Bill.query.filter_by(bill_no=bill_no).first():
            flash("Bill No already exists.", "danger")
        else:
            bill_date = parse_date(bill_date_str)
            db.session.add(Bill(
                bill_no=bill_no, bill_date=bill_date, client_id=int(client_id),
                amount=amount, description=description, remarks=remarks, Subject=Subject
            ))
            db.session.commit()
            flash("Bill saved.", "success")
        return redirect(url_for("bills"))

    # GET with search + pagination
    qtext = (request.args.get("q", "", type=str) or "").strip()
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 15, type=int)

    # JOIN Client once so Client.name filters are valid (and no cross join) [JOIN HERE]
    base_q = Bill.query.join(Client).order_by(Bill.bill_date.desc(), Bill.id.desc())
    filt_q = apply_bill_search(base_q, qtext)

    total = filt_q.count()
    items = filt_q.offset((page - 1) * per_page).limit(per_page).all()

    def _url(p):
        return url_for("bills", page=p, per_page=per_page, q=qtext)

    pagination = build_pagination(total, page, per_page, _url)
    return render_template("bills.html",
                        bills=items, clients=Client.query.order_by(Client.name.asc()).all(),
                        pagination=pagination, qtext=qtext)


@app.route("/bills/<int:bid>/edit", methods=["GET", "POST"])
def edit_bill(bid):
    b = Bill.query.get_or_404(bid)
    clients = Client.query.order_by(Client.name.asc()).all()
    if request.method == "POST":
        new_bill_no = request.form.get("bill_no", "").strip()
        if not new_bill_no:
            flash("Bill No is required.", "danger")
            return redirect(request.url)
        if new_bill_no != b.bill_no and Bill.query.filter_by(bill_no=new_bill_no).first():
            flash("Bill No already exists.", "danger")
            return redirect(request.url)
        b.bill_no = new_bill_no
        b.bill_date = parse_date(request.form.get("bill_date"))
        b.client_id = int(request.form.get("client_id"))
        b.amount = float(request.form.get("amount") or 0)
        b.description = request.form.get("description", "").strip()
        b.remarks = request.form.get("remarks", "").strip()
        b.Subject = request.form.get("Subject", "").strip()
        db.session.commit()
        flash("Bill updated.", "success")
        return redirect(url_for("bills"))
    return render_template("bill_edit.html", b=b, clients=clients)

# ---------- Receipts ----------
@app.route("/receipts", methods=["GET", "POST"])
def receipts():
    clients = Client.query.order_by(Client.name.asc()).all()
    bills_for_dropdown = Bill.query.with_entities(Bill.bill_no, Bill.client_id).order_by(Bill.bill_no.asc()).all()

    # Handle add form
    if request.method == "POST":
        receipt_ref = request.form.get("receipt_ref", "").strip()
        receipt_date_str = request.form.get("receipt_date")
        client_id = request.form.get("client_id")
        bill_no = request.form.get("bill_no", "").strip()
        tds_amt = float(request.form.get("tds_amt") or 0)
        paid_amount = float(request.form.get("paid_amount") or 0)
        collection_amount = tds_amt + paid_amount
        utr_details = request.form.get("utr_details", "").strip()
        mode = request.form.get("mode", "").strip()
        remarks = request.form.get("remarks", "").strip()

        if not receipt_date_str or not client_id or collection_amount <= 0 or not bill_no:
            flash("Please fill Receipt Date, Client, Bill No and positive amounts.", "danger")
        elif not Bill.query.filter_by(bill_no=bill_no).first():
            flash("Selected Bill No does not exist.", "danger")
        else:
            receipt_date = parse_date(receipt_date_str)
            db.session.add(Receipt(
                receipt_ref=receipt_ref, receipt_date=receipt_date, client_id=int(client_id),
                bill_no=bill_no, tds_amt=tds_amt, collection_amount=collection_amount,
                utr_details=utr_details, mode=mode, remarks=remarks
            ))
            db.session.commit()
            flash("Receipt saved.", "success")
        return redirect(url_for("receipts"))

    # Search + pagination params
    qtext = (request.args.get("q", "", type=str) or "").strip()
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 15, type=int)

    # JOIN Client once so filtering on Client.name is valid
    rq = Receipt.query.join(Client).order_by(Receipt.receipt_date.desc(), Receipt.id.desc())
    rq = apply_receipt_search(rq, qtext)

    total = rq.count()
    page_items = rq.offset((page - 1) * per_page).limit(per_page).all()

    # Build maps once, then annotate only the current page
    bills_map = dict(db.session.query(Bill.bill_no, Bill.amount).all())
    paid_map = dict(
        db.session.query(Receipt.bill_no, db.func.sum(Receipt.collection_amount))
        .filter(Receipt.bill_no.isnot(None))
        .group_by(Receipt.bill_no)
        .all()
    )

    annotated = []
    for r in page_items:
        bill_amount = bills_map.get(r.bill_no) if r.bill_no else None
        paid_total = paid_map.get(r.bill_no, 0.0) if r.bill_no else None
        status = None
        if bill_amount is not None:
            balance = bill_amount - (paid_total or 0.0)
            status = "Paid" if abs(balance) < 0.0001 else ("Overpaid" if balance < 0 else "Pending")
        annotated.append((r, bill_amount, status))

    def _url(p):
        return url_for("receipts", page=p, per_page=per_page, q=qtext)

    pagination = build_pagination(total, page, per_page, _url)
    return render_template("receipts.html",
                        receipts=annotated, clients=clients,
                        bills_for_dropdown=bills_for_dropdown,
                        pagination=pagination, qtext=qtext)



@app.route("/receipts/<int:rid>/edit", methods=["GET", "POST"])
def edit_receipt(rid):
    r = Receipt.query.get_or_404(rid)
    clients = Client.query.order_by(Client.name.asc()).all()
    if request.method == "POST":
        r.receipt_ref = request.form.get("receipt_ref", "").strip()
        r.receipt_date = parse_date(request.form.get("receipt_date"))
        r.client_id = int(request.form.get("client_id"))
        r.bill_no = request.form.get("bill_no", "").strip()
        tds_amt = float(request.form.get("tds_amt") or 0)
        paid_amount = float(request.form.get("paid_amount") or 0)
        r.tds_amt = tds_amt
        r.collection_amount = tds_amt + paid_amount
        r.utr_details = request.form.get("utr_details", "").strip()
        r.mode = request.form.get("mode", "").strip()
        r.remarks = request.form.get("remarks", "").strip()
        if not r.bill_no or not Bill.query.filter_by(bill_no=r.bill_no).first():
            flash("Valid Bill No is required.", "danger")
            return redirect(request.url)
        db.session.commit()
        flash("Receipt updated.", "success")
        return redirect(url_for("receipts"))
    paid_amount_current = (r.collection_amount or 0) - (r.tds_amt or 0)
    bills_for_dropdown = Bill.query.with_entities(Bill.bill_no, Bill.client_id).order_by(Bill.bill_no.asc()).all()
    return render_template("receipt_edit.html", r=r, clients=clients,
                           paid_amount_current=paid_amount_current,
                           bills_for_dropdown=bills_for_dropdown)

# ---------- Delete ----------
@app.route("/delete/<table>/<int:row_id>", methods=["POST"])
def delete_row(table, row_id):
    model = Bill if table == "bill" else Receipt if table == "receipt" else Client if table == "client" else None
    if not model:
        flash("Invalid delete request.", "danger")
        return redirect(url_for("index"))
    rec = model.query.get(row_id)
    if rec:
        db.session.delete(rec)
        db.session.commit()
        flash("Deleted.", "success")
    return redirect(request.referrer or url_for("dashboard"))

# ---------- Import (two-step: preview then confirm) ----------
@app.route("/import", methods=["GET", "POST"])
def import_data():
    # GET: show page (if a temp file exists, the template may choose to ignore or re-upload)
    if request.method == "GET":
        return render_template("import.html")

    # Step B: confirm import
    if request.form.get("confirm"):
        tmp_path = session.pop("import_temp", None)
        if not tmp_path or not os.path.exists(tmp_path):
            flash("No file to import. Upload again.", "danger")
            return redirect(url_for("import_data"))

        try:
            df = _read_tabular(tmp_path)
        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

        required = ["Client", "Bill No", "Bill Date", "Amount"]
        missing = _required_missing(df, required)
        if missing:
            flash(f"Missing columns: {', '.join(missing)}", "danger")
            return redirect(url_for("import_data"))

        created_clients = 0
        created_bills = 0
        created_receipts = 0

        with db.session.begin():
            for _, row in df.iterrows():
                # Upsert client
                cname = str(row.get("Client") or "").strip()
                if not cname:
                    continue
                client = Client.query.filter(db.func.lower(Client.name) == cname.lower()).first()
                if not client:
                    client = Client(
                        name=cname,
                        address=str(row.get("Address") or "").strip(),
                        gst_no=str(row.get("GST") or "").strip(),
                        pan_no=str(row.get("PAN") or "").strip(),
                        remarks=str(row.get("Client Remarks") or "").strip(),
                    )
                    db.session.add(client)
                    db.session.flush()
                    created_clients += 1

                # Bill
                bill_no = str(row.get("Bill No") or "").strip()
                if bill_no and not Bill.query.filter_by(bill_no=bill_no).first():
                    bill = Bill(
                        bill_no=bill_no,
                        bill_date=parse_date(str(row.get("Bill Date") or "")),
                        client_id=client.id,
                        amount=float(row.get("Amount") or 0),
                        description=str(row.get("Description") or "").strip(),
                        remarks=str(row.get("Bill Remarks") or "").strip(),
                        Subject=str(row.get("Subject") or "").strip(),
                    )
                    db.session.add(bill)
                    created_bills += 1

                # Receipt (optional)
                paid = float(row.get("Paid") or 0)
                tds = float(row.get("TDS") or 0)
                if bill_no and (paid or tds):
                    db.session.add(Receipt(
                        receipt_ref=str(row.get("Receipt Ref") or "").strip(),
                        receipt_date=parse_date(str(row.get("Receipt Date") or "")) or parse_date(str(row.get("Bill Date") or "")),
                        client_id=client.id,
                        bill_no=bill_no,
                        tds_amt=tds,
                        collection_amount=paid + tds,
                        mode=str(row.get("Mode") or "").strip(),
                        remarks=str(row.get("Receipt Remarks") or "").strip(),
                    ))
                    created_receipts += 1

        flash(f"Imported: {created_clients} clients, {created_bills} bills, {created_receipts} receipts.", "success")
        return redirect(url_for("dashboard"))

    # Step A: file upload -> save temp -> preview
    file = request.files.get("file")
    if not file or file.filename == "":
        flash("Please choose a CSV or Excel file to upload.", "danger")
        return redirect(url_for("import_data"))

    filename = secure_filename(file.filename)
    ext = os.path.splitext(filename)[5].lower()
    if ext not in ALLOWED_IMPORT_EXTS:
        flash("Unsupported file type. Upload .csv or .xlsx/.xls.", "danger")
        return redirect(url_for("import_data"))

    tmp_dir = os.path.join(app.instance_path, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=tmp_dir, suffix=ext)
    with os.fdopen(fd, "wb") as out:
        out.write(file.read())

    session["import_temp"] = tmp_path

    try:
        df = _read_tabular(tmp_path)
    except Exception as e:
        flash(f"Could not parse file: {e}", "danger")
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        session.pop("import_temp", None)
        return redirect(url_for("import_data"))

    # Render preview (template can show df.head(10))
    return render_template("import.html", preview=df)


# --- Import Clients (CSV/Excel) ---
# --- Import Clients (CSV/Excel) ---
@app.post("/import/clients/now")
def import_clients_now():
    f = request.files.get("file")
    if not f or f.filename == "":
        flash("Choose a CSV or Excel file.", "danger")
        return redirect(url_for("list_clients"))

    # FIXED: use [2] (or Path(f.filename).suffix.lower())
    ext = os.path.splitext(f.filename)[2].lower()
    if ext not in {".csv", ".xlsx", ".xls"}:
        flash("Only .csv or .xlsx/.xls allowed.", "danger")
        return redirect(url_for("list_clients"))

    # Parse with pandas
    df = pd.read_csv(f) if ext == ".csv" else pd.read_excel(f)

    required = ["Client"]  # Optional: Address, GST, PAN, Remarks
    missing = [c for c in required if c not in df.columns]
    if missing:
        flash(f"Missing columns: {', '.join(missing)}", "danger")
        return redirect(url_for("list_clients"))

    created = 0
    with db.session.begin():
        for _, row in df.iterrows():
            name = str(row.get("Client") or "").strip()
            if not name:
                continue
            exists = Client.query.filter(db.func.lower(Client.name) == name.lower()).first()
            if exists:
                if "Address" in df.columns:
                    exists.address = str(row.get("Address") or "").strip()
                if "GST" in df.columns:
                    exists.gst_no = str(row.get("GST") or "").strip()
                if "PAN" in df.columns:
                    exists.pan_no = str(row.get("PAN") or "").strip()
                if "Remarks" in df.columns:
                    exists.remarks = str(row.get("Remarks") or "").strip()
            else:
                db.session.add(Client(
                    name=name,
                    address=str(row.get("Address") or "").strip(),
                    gst_no=str(row.get("GST") or "").strip(),
                    pan_no=str(row.get("PAN") or "").strip(),
                    remarks=str(row.get("Remarks") or "").strip(),
                ))
                created += 1

    flash(f"Clients import complete. Created {created}.", "success")
    return redirect(url_for("list_clients"))



# --- Import Bills (CSV/Excel) ---
@app.post("/import/bills/now")
def import_bills_now():
    f = request.files.get("file")
    if not f or f.filename == "":
        flash("Choose a CSV or Excel file.", "danger")
        return redirect(url_for("bills"))

    # FIX: use [1], not [4]
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in {".csv", ".xlsx", ".xls"}:
        flash("Only .csv or .xlsx/.xls allowed.", "danger")
        return redirect(url_for("bills"))

    df = pd.read_csv(f) if ext == ".csv" else pd.read_excel(f)
    required = ["Bill No", "Bill Date", "Client", "Amount"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        flash(f"Missing columns: {', '.join(missing)}", "danger")
        return redirect(url_for("bills"))

    created = 0
    with db.session.begin():  # atomic insert [1]
        for _, row in df.iterrows():
            cname = str(row.get("Client") or "").strip()
            if not cname:
                continue
            client = Client.query.filter(db.func.lower(Client.name) == cname.lower()).first()
            if not client:
                client = Client(name=cname)
                db.session.add(client)
                db.session.flush()

            bill_no = str(row.get("Bill No") or "").strip()
            if not bill_no or Bill.query.filter_by(bill_no=bill_no).first():
                continue

            db.session.add(Bill(
                bill_no=bill_no,
                bill_date=parse_date(str(row.get("Bill Date") or "")),
                client_id=client.id,
                amount=float(row.get("Amount") or 0),
                description=str(row.get("Description") or "").strip(),
                remarks=str(row.get("Remarks") or "").strip(),
                Subject=str(row.get("Subject") or "").strip(),
            ))
            created += 1

    flash(f"Bills import complete. Created {created}.", "success")
    return redirect(url_for("bills"))

# --- Import Receipts (CSV/Excel) ---
# --- Import Receipts (CSV/Excel) ---
@app.post("/import/receipts/now")
def import_receipts_now():
    f = request.files.get("file")
    if not f or f.filename == "":
        flash("Choose a CSV or Excel file.", "danger")
        return redirect(url_for("receipts"))

    # FIXED: use [2] (or Path(f.filename).suffix.lower())
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in {".csv", ".xlsx", ".xls"}:
        flash("Only .csv or .xlsx/.xls allowed.", "danger")
        return redirect(url_for("receipts"))

    # Parse with pandas
    df = pd.read_csv(f) if ext == ".csv" else pd.read_excel(f)

    required = ["Client", "Bill No", "Receipt Date", "Paid", "TDS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        flash(f"Missing columns: {', '.join(missing)}", "danger")
        return redirect(url_for("receipts"))

    # Normalize Bill No once
    df['Bill No'] = df['Bill No'].astype(str).str.strip()

    # 1) Duplicates within the uploaded file (all occurrences)
    dups_in_file = (
        df.loc[df['Bill No'].duplicated(keep=False), 'Bill No']
        .dropna().astype(str).str.strip().unique().tolist()
    )

    # 2) Overlaps with existing receipts in DB (one receipt per bill rule)
    file_bill_nos = df['Bill No'].dropna().astype(str).str.strip().tolist()
    existing_bills = {
        bn for (bn,) in db.session.query(Receipt.bill_no)
            .filter(Receipt.bill_no.in_(file_bill_nos)).all()
    }

    # Combine conflicts and block upload if any
    conflicts = sorted(set(dups_in_file) | existing_bills)
    if conflicts:
        preview = ", ".join(conflicts[:5])
        flash(
            f"Upload blocked: multiple receipts per Bill No are not allowed. "
            f"Conflicts for Bill Nos: {preview}{' …' if len(conflicts) > 5 else ''}. "
            f"No rows were imported.",
            "danger"
        )
        return redirect(url_for("receipts"))

    # If no conflicts, proceed with the usual insert loop...
    created = 0
    with db.session.begin():
        for _, row in df.iterrows():
            cname = str(row.get("Client") or "").strip()
            client = Client.query.filter(db.func.lower(Client.name) == cname.lower()).first()
            if not client:
                continue
            bill_no = str(row.get("Bill No") or "").strip()
            paid = float(row.get("Paid") or 0)
            tds = float(row.get("TDS") or 0)
            total = paid + tds
            if not bill_no or total <= 0:
                continue
            db.session.add(Receipt(
                receipt_ref=str(row.get("Receipt Ref") or "").strip(),
                receipt_date=parse_date(str(row.get("Receipt Date") or "")),
                client_id=client.id,
                bill_no=bill_no,
                tds_amt=tds,
                collection_amount=total,
                utr_details=str(row.get("UTR") or "").strip(),
                mode=str(row.get("Mode") or "").strip(),
                remarks=str(row.get("Remarks") or "").strip(),
            ))
            created += 1

    flash(f"Receipts import complete. Created {created}.", "success")
    return redirect(url_for("receipts"))



def _send_df(df, fmt: str, base_name: str):
    # Normalize column order and types if needed
    if fmt == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False)  # pandas DataFrame.to_csv [13]
        buf.seek(0)
        return send_file(
            io.BytesIO(buf.getvalue().encode("utf-8")),
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"{base_name}.csv",
        )  # Flask send_file [8]
    elif fmt == "xlsx":
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")  # pandas DataFrame.to_excel [15]
        out.seek(0)
        return send_file(
            out,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"{base_name}.xlsx",
        )  # Flask send_file [8]
    else:
        flash("Unsupported export format.", "danger")
        return redirect(request.referrer or url_for("dashboard"))
    


@app.route("/export/bills.<fmt>")
def export_bills(fmt):
    qtext = request.args.get("q", "", type=str)  # search term [2]
    q = (
        db.session.query(
            Bill.bill_date.label("Bill Date"),
            Bill.bill_no.label("Bill No"),
            Client.name.label("Client"),
            Bill.amount.label("Amount"),
            Bill.description.label("Description"),
            Bill.remarks.label("Remarks"),
            Bill.Subject.label("Subject"),
        ).join(Client, Bill.client_id == Client.id)
    )
    q = apply_bill_search(q, qtext)  # reuse filter [12]
    df = pd.read_sql(q.statement, db.engine)
    return _send_df(df, fmt, "bills")  # to_csv/to_excel + send_file [21][16][8]

@app.route("/export/receipts.<fmt>")
def export_receipts(fmt):
    qtext = request.args.get("q", "", type=str)  # search term [2]
    rq = (
        db.session.query(
            Receipt.receipt_date.label("Receipt Date"),
            Client.name.label("Client"),
            Receipt.bill_no.label("Bill No"),
            Receipt.tds_amt.label("TDS"),
            Receipt.collection_amount.label("Collection"),
            Receipt.utr_details.label("UTR"),
            Receipt.mode.label("Mode"),
            Receipt.remarks.label("Remarks"),
        ).join(Client, Receipt.client_id == Client.id)
    )
    rq = apply_receipt_search(rq, qtext)  # reuse filter [12]
    df = pd.read_sql(rq.statement, db.engine)
    bills_map = dict(db.session.query(Bill.bill_no, Bill.amount).all())
    df["Bill Amount"] = df["Bill No"].map(bills_map).fillna("")
    df = df.reindex(columns=["Receipt Date","Client","Bill No","Bill Amount","TDS","Collection","UTR","Mode","Remarks"])
    return _send_df(df, fmt, "receipts")  # CSV/XLSX [21][16][8]


# ---------- Dashboard (with pagination) ----------
@app.route("/dashboard")
def dashboard():
    client_q = request.args.get("client", "").strip()
    status = request.args.get("status", "").strip()
    df_str = request.args.get("from", "").strip()
    dt_str = request.args.get("to", "").strip()
    dfrom = parse_date(df_str, default=None)
    dto = parse_date(dt_str, default=None)

    bq = Bill.query
    rq = Receipt.query
    if client_q:
        bq = bq.join(Client).filter(Client.name.ilike(f"%{client_q}%"))
        rq = rq.join(Client).filter(Client.name.ilike(f"%{client_q}%"))
    if dfrom:
        bq = bq.filter(Bill.bill_date >= dfrom)
        rq = rq.filter(Receipt.receipt_date >= dfrom)
    if dto:
        bq = bq.filter(Bill.bill_date <= dto)
        rq = rq.filter(Receipt.receipt_date <= dto)

    bills_df = pd.read_sql(bq.statement, db.engine)
    receipts_df = pd.read_sql(rq.statement, db.engine)

    clients_map = {c.id: c.name for c in Client.query.all()}
    if not bills_df.empty:
        bills_df["client_name"] = bills_df["client_id"].map(clients_map)
    if not receipts_df.empty:
        receipts_df["client_name"] = receipts_df["client_id"].map(clients_map)

    pay_by_bill = (
        receipts_df.dropna(subset=["bill_no"])
        .groupby("bill_no")["collection_amount"].sum().reset_index()
        .rename(columns={"collection_amount": "paid_amount"})
        if not receipts_df.empty else pd.DataFrame(columns=["bill_no", "paid_amount"])
    )
    recon = bills_df.merge(pay_by_bill, how="left", on="bill_no")
    if recon.empty:
        recon = bills_df.copy()
        recon["paid_amount"] = 0.0
    recon["paid_amount"] = recon["paid_amount"].fillna(0.0)
    recon["balance"] = recon["amount"] - recon["paid_amount"]
    recon["status"] = recon.apply(
        lambda r: "Paid" if abs(r["balance"]) < 0.0001 else ("Overpaid" if r["balance"] < 0 else "Pending"), axis=1
    )
    if status:
        recon = recon[recon["status"].str.lower() == status.lower()]

    totals = {
        "total_bills": float(recon["amount"].sum()) if not recon.empty else 0.0,
        "total_paid": float(recon["paid_amount"].sum()) if not recon.empty else 0.0,
        "total_balance": float(recon["balance"].sum()) if not recon.empty else 0.0,
        "count_pending": int((recon["status"] == "Pending").sum()) if not recon.empty else 0,
        "count_paid": int((recon["status"] == "Paid").sum()) if not recon.empty else 0,
        "count_overpaid": int((recon["status"] == "Overpaid").sum()) if not recon.empty else 0,
    }

    recon = recon.sort_values(["bill_date", "bill_no"], ascending=[False, True]) if not recon.empty else recon

    # Pagination
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 15, type=int)
    total = 0 if recon is None or recon.empty else int(len(recon))
    pages = max(1, (total + per_page - 1) // per_page)
    page = min(max(1, page), pages)
    start = (page - 1) * per_page
    end = start + per_page
    rows_page = [] if recon is None or recon.empty else recon.iloc[start:end].to_dict(orient="records")

    def _page_url(p):
        args = {
            "client": client_q or "",
            "status": status or "",
            "from": df_str or "",
            "to": dt_str or "",
            "page": p,
            "per_page": per_page,
        }
        return url_for("dashboard", **args)

    pagination = {
        "page": page,
        "per_page": per_page,
        "pages": pages,
        "total": total,
        "has_prev": page > 1,
        "has_next": page < pages,
        "prev_url": _page_url(page - 1) if page > 1 else None,
        "next_url": _page_url(page + 1) if page < pages else None,
    }

    return render_template(
        "dashboard.html",
        filters={"client": client_q, "status": status, "dfrom": df_str, "dto": dt_str},
        rows=rows_page,
        totals=totals,
        clients=Client.query.order_by(Client.name.asc()).all(),
        pagination=pagination,
    )

# ---------- API ----------
@app.get("/api/bills/by-client/<int:client_id>")
def api_bills_by_client(client_id: int):
    rows = Bill.query.with_entities(Bill.bill_no).filter(Bill.client_id == client_id).order_by(Bill.bill_no.asc()).all()
    return {"bills": [bn for (bn,) in rows]}

# ---------- Export ----------
@app.route("/export/reconciliation.<fmt>")
def export_reconciliation(fmt):
    bq = Bill.query
    rq = Receipt.query
    bills_df = pd.read_sql(bq.statement, db.engine)
    receipts_df = pd.read_sql(rq.statement, db.engine)
    clients_map = {c.id: c.name for c in Client.query.all()}
    if not bills_df.empty:
        bills_df["client_name"] = bills_df["client_id"].map(clients_map)
    if not receipts_df.empty:
        receipts_df["client_name"] = receipts_df["client_id"].map(clients_map)
    pay_by_bill = (
        receipts_df.dropna(subset=["bill_no"])
        .groupby("bill_no")["collection_amount"].sum().reset_index()
        .rename(columns={"collection_amount": "paid_amount"})
        if not receipts_df.empty else pd.DataFrame(columns=["bill_no","paid_amount"])
    )
    recon = bills_df.merge(pay_by_bill, how="left", on="bill_no")
    if recon.empty:
        recon = bills_df.copy()
        recon["paid_amount"] = 0.0
    recon["paid_amount"] = recon["paid_amount"].fillna(0.0)
    recon["balance"] = recon["amount"] - recon["paid_amount"]
    recon["status"] = recon.apply(
        lambda r: "Paid" if abs(r["balance"]) < 0.0001 else ("Overpaid" if r["balance"] < 0 else "Pending"), axis=1
    )

    output = io.BytesIO()
    recon.columns = [c[:1].upper() + c[1:] if isinstance(c, str) else c for c in recon.columns]
    if fmt == "csv":
        recon.to_csv(output, index=False)
        output.seek(0)
        return send_file(output, mimetype="text/csv", as_attachment=True, download_name="reconciliation.csv")
    elif fmt == "xlsx":
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            recon.to_excel(writer, sheet_name="Reconciliation", index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="reconciliation.xlsx"
        )
    else:
        flash("Unsupported export format.", "danger")
        return redirect(url_for("dashboard"))

# ---------- Bootstrap DB ----------
with app.app_context():
    db.create_all()

if __name__ == "__main__":
    app.run(debug=True)
