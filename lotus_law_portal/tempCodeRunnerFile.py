@app.route("/clients", methods=["GET"])
# def list_clients():
#     all_clients = Client.query.order_by(Client.name.asc()).all()
#     return render_template("clients.html", clients=all_clients)

# @app.route("/clients", methods=["POST"])
# def add_client():
#     name = (request.form.get("name") or "").strip()
#     if not name:
#         flash("Client name cannot be empty.", "danger")
#         return redirect(url_for("list_clients"))
#     exists = Client.query.filter(db.func.lower(Client.name) == name.lower()).first()
#     if exists:
#         flash("Client already exists.", "danger")
#         return redirect(url_for("list_clients"))
#     db.session.add(Client(name=name,
#                           address=(request.form.get("address") or "").strip(),
#                           gst_no=(request.form.get("gst_no") or "").strip(),
#                           pan_no=(request.form.get("pan_no") or "").strip(),
#                           remarks=(request.form.get("remarks") or "").strip()))
#     try:
#         db.session.commit()
#         flash("Client added successfully.", "success")
#     except Exception:
#         db.session.rollback()
#         flash("Could not add client.", "danger")
#     return redirect(url_for("list_clients"))

# @app.route("/clients/<int:cid>/edit", methods=["GET", "POST"])
# def edit_client(cid):
#     client = Client.query.get_or_404(cid)
#     if request.method == "POST":
#         client.name = (request.form.get("name") or "").strip()
#         client.address = (request.form.get("address") or "").strip()
#         client.gst_no = (request.form.get("gst_no") or "").strip()
#         client.pan_no = (request.form.get("pan_no") or "").strip()
#         client.remarks = (request.form.get("remarks") or "").strip()
#         try:
#             db.session.commit()
#             flash("Client updated", "success")
#             return redirect(url_for("list_clients"))
#         except Exception:
#             db.session.rollback()
#             flash("Could not update client", "danger")
#     return render_template("client_edit.html", client=client)