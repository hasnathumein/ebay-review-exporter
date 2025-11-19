import os
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
from dateutil import parser as date_parser
from ebaysdk.trading import Connection as Trading
from ebaysdk.exception import ConnectionError


RATING_MAP = {
    "Positive": 5,
    "Neutral": 3,
    "Negative": 1,
}


def get_trading_client() -> Trading:
    """
    Build an eBay Trading API client from environment variables.

    Required env vars:
      EBAY_APP_ID
      EBAY_DEV_ID
      EBAY_CERT_ID
      EBAY_TOKEN
    """
    appid = os.environ.get("EBAY_APP_ID")
    devid = os.environ.get("EBAY_DEV_ID")
    certid = os.environ.get("EBAY_CERT_ID")
    token = os.environ.get("EBAY_TOKEN")

    missing = [name for name, value in [
        ("EBAY_APP_ID", appid),
        ("EBAY_DEV_ID", devid),
        ("EBAY_CERT_ID", certid),
        ("EBAY_TOKEN", token),
    ] if not value]

    if missing:
        raise SystemExit(
            f"Missing required environment variables: {', '.join(missing)}\n"
            f"Set them in the terminal before running this script."
        )

    client = Trading(
        domain="api.ebay.com",
        appid=appid,
        devid=devid,
        certid=certid,
        token=token,
        config_file=None,
    )
    return client


def fetch_all_feedback(
    client: Trading,
    feedback_type: str = "FeedbackReceived",
    entries_per_page: int = 200,
) -> List[Dict[str, Any]]:
    """
    Fetch all feedback using the GetFeedback Trading call.
    Returns a list of simplified dicts for downstream processing.
    """
    page = 1
    all_rows: List[Dict[str, Any]] = []

    while True:
        try:
            response = client.execute(
                "GetFeedback",
                {
                    "DetailLevel": "ReturnAll",
                    "FeedbackType": feedback_type,
                    "Pagination": {
                        "EntriesPerPage": entries_per_page,
                        "PageNumber": page,
                    },
                },
            )
        except ConnectionError as e:
            print(f"GetFeedback failed on page {page}")
            print("Error:", e)
            if hasattr(e, "response") and e.response is not None:
                try:
                    print("Raw response text:")
                    print(e.response.text)
                except Exception:
                    pass
            break

        data = response.dict()
        details = data.get("FeedbackDetailArray", {}).get("FeedbackDetail", [])

        if not details:
            break

        if isinstance(details, dict):
            details = [details]

        for fb in details:
            comment_text = fb.get("CommentText")
            comment_type = fb.get("CommentType")
            comment_time_raw = fb.get("CommentTime")

            try:
                comment_time = (
                    date_parser.parse(comment_time_raw)
                    if comment_time_raw
                    else None
                )
            except Exception:
                comment_time = None

            row = {
                "comment_text": comment_text,
                "comment_type": comment_type,
                "comment_time": comment_time,
                "role": fb.get("Role"),
                "item_id": str(fb.get("ItemID") or ""),
                "item_title": fb.get("ItemTitle"),
                "commenting_user": fb.get("CommentingUser"),
            }
            all_rows.append(row)

        pagination_result = data.get("PaginationResult", {}) or {}
        total_pages = int(pagination_result.get("TotalNumberOfPages", page))
        print(f"Fetched page {page} of {total_pages}  total rows so far {len(all_rows)}")

        if page >= total_pages:
            break

        page += 1

    return all_rows


def build_ebay_reviews_export(
    rows: List[Dict[str, Any]],
    ebay_base_url: str = "https://www.ebay.co.uk",
) -> pd.DataFrame:
    """
    Transform raw feedback rows into Judge.me compatible product review export.

    Columns:
      title
      body
      rating
      review_date (DD/MM/YYYY)
      reviewer_name
      reviewer_email (blank)
      product_url (eBay item URL)
      picture_urls (blank)
      product_id (blank)
      product_handle (blank)
    """
    df = pd.DataFrame(rows)

    if df.empty:
        raise SystemExit("No feedback rows supplied to build_ebay_reviews_export")

    # Normalise comment_time into pandas datetime
    df["comment_time"] = pd.to_datetime(df["comment_time"], errors="coerce")

    # Only seller feedback with real comments and types
    df = df[
        (df["role"] == "Seller")
        & df["comment_text"].notna()
        & df["comment_type"].notna()
    ].copy()

    if df.empty:
        raise SystemExit("No seller feedback with comments after filtering")

    # Map rating
    df["rating"] = df["comment_type"].map(RATING_MAP)
    df = df[df["rating"].notna()].copy()
    df["rating"] = df["rating"].astype(int)

    # Date format DD/MM/YYYY
    df["review_date"] = df["comment_time"].dt.strftime("%d/%m/%Y")

    # Build eBay product url
    base = ebay_base_url.rstrip("/")
    df["product_url"] = df["item_id"].astype(str).apply(
        lambda item_id: f"{base}/itm/{item_id}" if item_id else ""
    )

    # Now build the Judge.me template columns
    out = pd.DataFrame()
    out["title"] = df["item_title"].fillna("").astype(str)
    out["body"] = df["comment_text"].fillna("").astype(str)
    out["rating"] = df["rating"].astype(int)
    out["review_date"] = df["review_date"].fillna("").astype(str)
    out["reviewer_name"] = df["commenting_user"].fillna("").astype(str)
    out["reviewer_email"] = ""      # not exposed by eBay
    out["product_url"] = df["product_url"].fillna("").astype(str)
    out["picture_urls"] = ""        # not available from GetFeedback
    out["product_id"] = ""          # not used for this flow
    out["product_handle"] = ""      # not used for this flow

    return out


def main() -> None:
    # Read base url for your eBay site (optional)
    ebay_base_url = os.environ.get("EBAY_BASE_URL", "https://www.ebay.co.uk")

    print("Creating Trading client")
    client = get_trading_client()

    print("Fetching feedback from eBay")
    rows = fetch_all_feedback(client, feedback_type="FeedbackReceived")
    print(f"Total raw feedback rows fetched {len(rows)}")

    if not rows:
        raise SystemExit("No feedback rows returned from eBay")

    print("Building Judge.me export DataFrame")
    export_df = build_ebay_reviews_export(rows, ebay_base_url=ebay_base_url)
    print(f"Rows after filtering to seller feedback {len(export_df)}")

    # Output folder and file
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"ebay_product_reviews_for_judgeme_{timestamp}.xlsx"

    print(f"Writing Excel to {out_path}")
    export_df.to_excel(out_path, index=False)

    print("Done")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        # Print clean message and exit with non zero status on failure
        print(str(e))
        sys.exit(1)
