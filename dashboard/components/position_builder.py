"""
dashboard/components/position_builder.py

KEY DESIGN DECISIONS — read before modifying:

st.rerun() INSIDE @st.dialog CLOSES THE DIALOG.
This is because our trigger is `if st.button("Enter Positions")` which returns
True only on the single click-rerun. After st.rerun() the button returns False,
the dialog function is not re-called, and the dialog disappears.

RULE: st.rerun() inside the dialog is allowed ONLY when the intent is to close it
      (Submit success, Yes-cancel confirmed, Cancel on empty form).
      For transitions that must keep the dialog open (show/hide warning, add row,
      delete row, validation errors) we NEVER call st.rerun(). We mutate session
      state and let Streamlit's natural widget rerun re-render the dialog.

CANCEL FLOW — three cases:
  1. Form empty   → Cancel clicked → clear state → st.rerun() → dialog closes.
  2. Form has data → Cancel clicked → set cancel_confirm=True, NO rerun →
     on next natural widget rerun the dialog re-renders showing the warning.
  3. Warning shown → "No, continue" → set cancel_confirm=False, NO rerun →
     dialog re-renders showing the form again.
  4. Warning shown → "Yes, cancel" → clear state → st.rerun() → dialog closes.

ADD ROW / DELETE ROW: use on_click callbacks. Callbacks mutate session state
before the render phase. No st.rerun() needed — the mutation is reflected in the
very next render of the dialog triggered by the button click.

DATE INPUTS: Use st.date_input with value=None so the field starts blank (shows
a dash). format="YYYY-MM-DD" enforces the right display. This avoids the
auto-open calendar bug from previous versions while keeping the calendar UX.
"""

import io
import pandas as pd
import streamlit as st
from datetime import datetime, date as date_type

# ── Constants ──────────────────────────────────────────────────────────────────

VALID_SYMBOLS      = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]
SYMBOL_OPTIONS     = ["—"] + VALID_SYMBOLS
OT_OPTIONS         = ["—", "CE", "PE", "XX"]
VALID_OPTION_TYPES = {"CE", "PE", "XX"}

CSV_COLUMNS = [
    "symbol", "expiry_date", "strike", "option_type",
    "quantity", "entry_date", "entry_price",
]
REQUIRED_COLUMNS = set(CSV_COLUMNS)


def _fresh_row() -> dict:
    return {
        "symbol":      "—",
        "expiry_date": None,   # None → date_input shows blank/dash
        "strike":      "",
        "option_type": "—",
        "quantity":    "",
        "entry_date":  None,   # None → date_input shows blank/dash
        "entry_price": "",
    }


# ── Session state ──────────────────────────────────────────────────────────────

def _init_state(page_key: str):
    if f"{page_key}_pb_rows" not in st.session_state:
        st.session_state[f"{page_key}_pb_rows"] = [_fresh_row()]
    # cancel_confirm drives the warning banner; never call st.rerun() to toggle it
    if f"{page_key}_pb_cancel_confirm" not in st.session_state:
        st.session_state[f"{page_key}_pb_cancel_confirm"] = False


def _clear_dialog_rows(page_key: str):
    """
    Reset rows and cancel flag.
    Does NOT touch manual_csv — previously submitted data is preserved.
    """
    st.session_state[f"{page_key}_pb_rows"]           = [_fresh_row()]
    st.session_state[f"{page_key}_pb_cancel_confirm"] = False


def _rows_are_empty(rows: list[dict]) -> bool:
    """
    True only if the user has not touched any input field.
    Checks all text fields and date fields.
    """
    for row in rows:
        if str(row.get("strike", "")).strip():
            return False
        if str(row.get("quantity", "")).strip():
            return False
        if str(row.get("entry_price", "")).strip():
            return False
        if row.get("expiry_date") is not None:
            return False
        if row.get("entry_date") is not None:
            return False
        if row.get("symbol", "—") not in ("—", ""):
            return False
        if row.get("option_type", "—") not in ("—", ""):
            return False
    return True


# ── on_click callbacks (fire before render, no st.rerun needed) ───────────────

def _cb_add_row(page_key: str):
    st.session_state[f"{page_key}_pb_rows"].append(_fresh_row())


def _cb_delete_row(page_key: str, idx: int):
    rows = st.session_state[f"{page_key}_pb_rows"]
    if len(rows) > 1:
        rows.pop(idx)
    st.session_state[f"{page_key}_pb_rows"] = rows


def _cb_show_cancel_confirm(page_key: str):
    """Set cancel_confirm=True without rerun — dialog re-renders showing warning."""
    st.session_state[f"{page_key}_pb_cancel_confirm"] = True


def _cb_hide_cancel_confirm(page_key: str):
    """Set cancel_confirm=False without rerun — dialog re-renders showing form."""
    st.session_state[f"{page_key}_pb_cancel_confirm"] = False


# ── Validation ─────────────────────────────────────────────────────────────────

def _validate_rows(rows: list[dict]) -> tuple[bool, list[str]]:
    if not rows:
        return False, ["No positions entered."]

    errors = []
    for i, row in enumerate(rows):
        p = f"Row {i + 1}"

        sym = row.get("symbol", "—")
        if sym in ("—", "", None):
            errors.append(f"{p}: symbol is required.")
        elif sym not in VALID_SYMBOLS:
            errors.append(f"{p}: unknown symbol '{sym}'.")

        ot = row.get("option_type", "—")
        if ot in ("—", "", None):
            errors.append(f"{p}: option_type is required.")
        elif ot not in VALID_OPTION_TYPES:
            errors.append(f"{p}: option_type must be CE, PE, or XX.")

        if row.get("expiry_date") is None:
            errors.append(f"{p}: expiry_date is required.")

        if row.get("entry_date") is None:
            errors.append(f"{p}: entry_date is required.")

        strike_raw = str(row.get("strike", "")).strip()
        if not strike_raw:
            errors.append(f"{p}: strike is required.")
        else:
            try:
                sv = float(strike_raw)
                if sv < 0:
                    errors.append(f"{p}: strike must be >= 0.")
                if ot in ("CE", "PE") and sv == 0:
                    errors.append(f"{p}: strike cannot be 0 for CE/PE.")
            except ValueError:
                errors.append(f"{p}: strike must be numeric, got '{strike_raw}'.")

        qty_raw = str(row.get("quantity", "")).strip()
        if not qty_raw:
            errors.append(f"{p}: quantity is required.")
        else:
            try:
                qv = int(float(qty_raw))
                if qv == 0:
                    errors.append(f"{p}: quantity cannot be 0.")
            except ValueError:
                errors.append(f"{p}: quantity must be an integer, got '{qty_raw}'.")

        ep_raw = str(row.get("entry_price", "")).strip()
        if not ep_raw:
            errors.append(f"{p}: entry_price is required.")
        else:
            try:
                ev = float(ep_raw)
                if ev <= 0:
                    errors.append(f"{p}: entry_price must be > 0.")
            except ValueError:
                errors.append(f"{p}: entry_price must be numeric, got '{ep_raw}'.")

    return len(errors) == 0, errors


# ── Serialization ──────────────────────────────────────────────────────────────

def _rows_to_csv_bytes(rows: list[dict]) -> bytes:
    records = [{
        "symbol":      row["symbol"],
        "expiry_date": str(row["expiry_date"]),
        "strike":      float(str(row["strike"]).strip()),
        "option_type": row["option_type"],
        "quantity":    int(float(str(row["quantity"]).strip())),
        "entry_date":  str(row["entry_date"]),
        "entry_price": float(str(row["entry_price"]).strip()),
    } for row in rows]
    buf = io.StringIO()
    pd.DataFrame(records, columns=CSV_COLUMNS).to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


# ── CSV schema validation ──────────────────────────────────────────────────────

def _parse_csv_bytes(raw: bytes, label: str) -> tuple[pd.DataFrame | None, list[str]]:
    try:
        return pd.read_csv(io.BytesIO(raw)), []
    except Exception as e:
        return None, [f"{label}: failed to parse — {e}."]


def _validate_csv_schema(df: pd.DataFrame, label: str) -> list[str]:
    errors = []
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        return [f"{label}: missing columns: {sorted(missing)}."]
    if df.empty:
        return [f"{label}: no data rows."]
    for col in CSV_COLUMNS:
        n = df[col].isna().sum()
        if n:
            errors.append(f"{label}: {n} null(s) in '{col}'.")
    inv = df[~df["symbol"].isin(VALID_SYMBOLS)]["symbol"].dropna().unique()
    if len(inv):
        errors.append(f"{label}: invalid symbol(s) {list(inv)}.")
    inv = df[~df["option_type"].str.upper().isin(VALID_OPTION_TYPES)]["option_type"].dropna().unique()
    if len(inv):
        errors.append(f"{label}: invalid option_type(s) {list(inv)}.")
    ss = pd.to_numeric(df["strike"], errors="coerce")
    if ss.isna().sum():
        errors.append(f"{label}: non-numeric strike.")
    else:
        if (ss < 0).sum():
            errors.append(f"{label}: negative strike value(s).")
        if (df["option_type"].str.upper().isin({"CE","PE"}) & (ss==0)).sum():
            errors.append(f"{label}: CE/PE row(s) with strike=0.")
    qs = pd.to_numeric(df["quantity"], errors="coerce")
    if qs.isna().sum():
        errors.append(f"{label}: non-numeric quantity.")
    elif (qs == 0).sum():
        errors.append(f"{label}: quantity=0 in some row(s).")
    es = pd.to_numeric(df["entry_price"], errors="coerce")
    if es.isna().sum():
        errors.append(f"{label}: non-numeric entry_price.")
    elif (es <= 0).sum():
        errors.append(f"{label}: entry_price <= 0 in some row(s).")
    for dc in ("expiry_date", "entry_date"):
        bad = pd.to_datetime(df[dc], errors="coerce").isna().sum()
        if bad:
            errors.append(f"{label}: {bad} unparseable date(s) in '{dc}'.")
    return errors


# ── Public: resolve final CSV ──────────────────────────────────────────────────

def resolve_csv_input(
    page_key: str,
    uploaded_file_bytes: bytes | None,
) -> tuple[bytes | None, str, list[str]]:
    manual_bytes = st.session_state.get(f"{page_key}_manual_csv")
    has_manual   = manual_bytes is not None
    has_upload   = bool(uploaded_file_bytes)

    if not has_manual and not has_upload:
        return None, "", ["No input provided. Upload a CSV or use 'Create Positions'."]

    frames, sources, errors = [], [], []

    if has_upload:
        df, pe = _parse_csv_bytes(uploaded_file_bytes, "Uploaded CSV")
        if pe:
            errors.extend(pe)
        else:
            se = _validate_csv_schema(df, "Uploaded CSV")
            if se:
                errors.extend(se)
            else:
                frames.append(df[CSV_COLUMNS].copy())
                sources.append("uploaded CSV")

    if has_manual:
        df, pe = _parse_csv_bytes(manual_bytes, "Manual positions")
        if pe:
            errors.extend(pe)
        else:
            se = _validate_csv_schema(df, "Manual positions")
            if se:
                errors.extend(se)
            else:
                frames.append(df[CSV_COLUMNS].copy())
                sources.append("manual positions")

    if errors:
        return None, "", errors

    merged = pd.concat(frames, ignore_index=True)
    merged["strike"]      = pd.to_numeric(merged["strike"],      errors="coerce")
    merged["quantity"]    = pd.to_numeric(merged["quantity"],    errors="coerce").astype("Int64")
    merged["entry_price"] = pd.to_numeric(merged["entry_price"], errors="coerce")

    for col in ("strike", "quantity", "entry_price"):
        n = merged[col].isna().sum()
        if n:
            errors.append(f"Merged: {n} null(s) in '{col}' after coercion.")

    if errors:
        return None, "", errors

    buf = io.StringIO()
    merged.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8"), " + ".join(sources), []


# ── Wide dialog CSS ────────────────────────────────────────────────────────────

def _inject_wide_dialog_css():
    st.markdown(
        """
        <style>
        div[data-testid="stDialog"] > div > div[role="dialog"] {
            width: 90vw !important;
            max-width: 1100px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ── Dialog ────────────────────────────────────────────────────────────────────

@st.dialog("Create Positions", width="large")
def _position_dialog(page_key: str):
    _inject_wide_dialog_css()

    rows_key   = f"{page_key}_pb_rows"
    cancel_key = f"{page_key}_pb_cancel_confirm"

    # Read rows ONCE at the top — callbacks have already mutated this before render
    rows = st.session_state[rows_key]

    st.caption(
        "Dates: click field to pick from calendar  |  "
        "Type: CE / PE / XX (futures)  |  "
        "Strike = 0 for XX  |  Qty: positive = Long, negative = Short"
    )
    st.divider()

    # ── Column headers ──
    hc = st.columns([1.3, 1.3, 1.0, 0.9, 0.9, 1.3, 1.1, 0.4])
    for col, lbl in zip(hc, [
        "Symbol", "Expiry Date", "Strike", "Type",
        "Qty", "Entry Date", "Entry Price", ""
    ]):
        col.markdown(f"<small><b>{lbl}</b></small>", unsafe_allow_html=True)

    # ── Row inputs ──
    for i, row in enumerate(rows):
        rc = st.columns([1.3, 1.3, 1.0, 0.9, 0.9, 1.3, 1.1, 0.4])

        with rc[0]:
            row["symbol"] = st.selectbox(
                f"sym_{i}", SYMBOL_OPTIONS,
                index=SYMBOL_OPTIONS.index(row["symbol"])
                      if row["symbol"] in SYMBOL_OPTIONS else 0,
                label_visibility="collapsed",
                key=f"{page_key}_sym_{i}",
            )

        with rc[1]:
            # value=None → renders as blank/dash, calendar only opens on click
            row["expiry_date"] = st.date_input(
                f"exd_{i}",
                value=row.get("expiry_date"),   # None or a date object
                format="YYYY-MM-DD",
                label_visibility="collapsed",
                key=f"{page_key}_exd_{i}",
            )

        with rc[2]:
            row["strike"] = st.text_input(
                f"stk_{i}",
                value=str(row.get("strike", "")),
                placeholder="22500",
                label_visibility="collapsed",
                key=f"{page_key}_stk_{i}",
            ).strip()

        with rc[3]:
            row["option_type"] = st.selectbox(
                f"ot_{i}", OT_OPTIONS,
                index=OT_OPTIONS.index(row["option_type"])
                      if row["option_type"] in OT_OPTIONS else 0,
                label_visibility="collapsed",
                key=f"{page_key}_ot_{i}",
            )

        with rc[4]:
            row["quantity"] = st.text_input(
                f"qty_{i}",
                value=str(row.get("quantity", "")),
                placeholder="2 or -1",
                label_visibility="collapsed",
                key=f"{page_key}_qty_{i}",
            ).strip()

        with rc[5]:
            row["entry_date"] = st.date_input(
                f"edt_{i}",
                value=row.get("entry_date"),    # None or a date object
                format="YYYY-MM-DD",
                label_visibility="collapsed",
                key=f"{page_key}_edt_{i}",
            )

        with rc[6]:
            row["entry_price"] = st.text_input(
                f"ep_{i}",
                value=str(row.get("entry_price", "")),
                placeholder="120.50",
                label_visibility="collapsed",
                key=f"{page_key}_ep_{i}",
            ).strip()

        with rc[7]:
            # on_click mutates state before render — single click works correctly
            st.button(
                "✕", key=f"{page_key}_del_{i}",
                on_click=_cb_delete_row, args=(page_key, i),
                disabled=(len(rows) == 1),
                help="Delete row" if len(rows) > 1 else "Cannot delete the only row",
            )

    # Write updated rows back after all widget reads
    st.session_state[rows_key] = rows

    st.divider()

    # ── Add Row — right-aligned, on_click only, NO st.rerun() ──
    _, add_col = st.columns([0.75, 0.25])
    with add_col:
        st.button(
            "+ Add Row", key=f"{page_key}_add_row",
            on_click=_cb_add_row, args=(page_key,),
            use_container_width=True,
        )

    st.divider()

    # ── Cancel confirmation banner ──
    # Rendered based on session state flag — no st.rerun() to show/hide it.
    # The flag is toggled via on_click callbacks; the natural widget rerun
    # re-renders the dialog with the updated flag value.
    if st.session_state[cancel_key]:
        st.warning(
            "Are you sure you want to cancel? "
            "You will lose all positions entered in this form."
        )
        _, yes_col, no_col = st.columns([0.46, 0.27, 0.27])

        with yes_col:
            # Clears rows then closes dialog via st.rerun() — INTENTIONAL close
            if st.button(
                "Yes, cancel", key=f"{page_key}_yes_cancel",
                type="secondary", use_container_width=True,
            ):
                _clear_dialog_rows(page_key)
                st.rerun()  # Closes dialog — button no longer True on next run

        with no_col:
            # Hides warning, keeps dialog open — uses on_click, NO st.rerun()
            st.button(
                "No, continue", key=f"{page_key}_no_cancel",
                type="primary", use_container_width=True,
                on_click=_cb_hide_cancel_confirm, args=(page_key,),
            )

        # Do not render Submit/Cancel while warning is shown
        return

    # ── Submit / Cancel — right-aligned ──
    _, submit_col, cancel_col = st.columns([0.54, 0.24, 0.22])

    with submit_col:
        if st.button(
            "Submit", key=f"{page_key}_submit",
            type="primary", use_container_width=True,
        ):
            is_valid, errs = _validate_rows(st.session_state[rows_key])
            if not is_valid:
                # Show errors inline — NO st.rerun(), dialog stays open
                for e in errs:
                    st.error(e)
            else:
                st.session_state[f"{page_key}_manual_csv"] = _rows_to_csv_bytes(
                    st.session_state[rows_key]
                )
                _clear_dialog_rows(page_key)
                st.rerun()  # Closes dialog — INTENTIONAL

    with cancel_col:
        if _rows_are_empty(st.session_state[rows_key]):
            # Form is empty — clicking Cancel should just close the dialog.
            # We set cancel_confirm to False (already is) and call st.rerun()
            # which closes the dialog because the trigger button is no longer True.
            if st.button(
                "Cancel", key=f"{page_key}_cancel",
                type="secondary", use_container_width=True,
            ):
                _clear_dialog_rows(page_key)
                st.rerun()  # Closes dialog — INTENTIONAL
        else:
            # Form has data — show confirmation warning via on_click, NO st.rerun()
            st.button(
                "Cancel", key=f"{page_key}_cancel",
                type="secondary", use_container_width=True,
                on_click=_cb_show_cancel_confirm, args=(page_key,),
            )


# ── Public render function ─────────────────────────────────────────────────────

def render_position_builder(page_key: str):
    """
    Renders Upload CSV / Create Positions as tabs.
    Status badge shown below when manual positions exist.
    """
    _init_state(page_key)

    manual_csv_key = f"{page_key}_manual_csv"
    has_manual     = manual_csv_key in st.session_state

    tab1, tab2 = st.tabs(["Upload CSV", "Create Positions"])

    with tab1:
        st.file_uploader(
            "Choose a CSV file",
            type=["csv"],
            key=f"{page_key}_file_uploader",
            help="Required columns: symbol, expiry_date, strike, option_type, "
                 "quantity, entry_date, entry_price.",
        )

    with tab2:
        # Dialog called directly from button return value.
        # No persistent flag — widget interactions inside dialog cannot re-trigger.
        if st.button(
            "Enter Positions",
            key=f"{page_key}_open_builder",
            use_container_width=True,
        ):
            _position_dialog(page_key)

    st.divider()

    if has_manual:
        try:
            df    = pd.read_csv(io.BytesIO(st.session_state[manual_csv_key]))
            count = len(df)
            st.success(f"{count} manual position(s) ready")
        except Exception:
            st.warning("Manual positions loaded")

        if st.button(
            "Remove Manual Positions",
            key=f"{page_key}_clear_manual",
            use_container_width=True,
            type="secondary",
        ):
            del st.session_state[manual_csv_key]
            st.rerun()
    else:
        st.caption("No manual positions entered.")
