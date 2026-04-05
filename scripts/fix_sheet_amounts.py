import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / '.env')

from api.config import GOOGLE_SHEET_ID  # noqa: E402
from api.google_sheets import _get_service  # noqa: E402


logger = logging.getLogger('fix_sheet_amounts')

TRANSACTION_START_ROW = 3
AMOUNT_COLUMN_INDEX = 2
AMOUNT_COLUMN_LABEL = 'C'
SKIPPED_SHEETS = {'Accounts'}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Convert text-formatted Google Sheets transaction amounts into numeric cells.'
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without writing anything. This is the default mode.',
    )
    mode.add_argument(
        '--apply',
        action='store_true',
        help='Write numeric values back to Google Sheets.',
    )
    parser.add_argument(
        '--sheet',
        help='Only process one sheet title, for example "003".',
    )
    parser.add_argument(
        '--spreadsheet-id',
        default=GOOGLE_SHEET_ID,
        help='Override the spreadsheet id from .env if needed.',
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable debug logging.',
    )
    return parser.parse_args()


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')


def _coerce_amount(raw_value: Any) -> Optional[float]:
    if raw_value is None:
        return None

    text = str(raw_value).strip()
    if not text:
        return None

    normalized = (
        text.replace("'", '')
        .replace('Ksh', '')
        .replace('KES', '')
        .replace(',', '')
        .strip()
    )

    if not normalized:
        return None

    try:
        return float(normalized)
    except (TypeError, ValueError):
        return None


def _sheet_titles(service: Any, spreadsheet_id: str) -> List[Tuple[str, int]]:
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles: List[Tuple[str, int]] = []
    for sheet in metadata.get('sheets', []):
        props = sheet.get('properties', {})
        title = props.get('title')
        sheet_id = props.get('sheetId')
        if title and sheet_id is not None:
            titles.append((title, sheet_id))
    return titles


def _load_rows(service: Any, spreadsheet_id: str, sheet_title: str) -> List[List[Any]]:
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f'{sheet_title}!A{TRANSACTION_START_ROW}:D',
        valueRenderOption='UNFORMATTED_VALUE',
    ).execute()
    return result.get('values', [])


def _build_amount_updates(rows: List[List[Any]]) -> Tuple[List[Dict[str, Any]], int]:
    requests: List[Dict[str, Any]] = []
    skipped = 0

    for row_offset, row in enumerate(rows):
        if len(row) <= AMOUNT_COLUMN_INDEX:
            continue

        raw_amount = row[AMOUNT_COLUMN_INDEX]
        if isinstance(raw_amount, (int, float)):
            continue

        parsed_amount = _coerce_amount(raw_amount)
        if parsed_amount is None:
            skipped += 1
            logger.warning('Skipping unparsable amount at row %d: %r', TRANSACTION_START_ROW + row_offset, raw_amount)
            continue

        requests.append(
            {
                'updateCells': {
                    'range': {
                        'startRowIndex': (TRANSACTION_START_ROW - 1) + row_offset,
                        'endRowIndex': TRANSACTION_START_ROW + row_offset,
                        'startColumnIndex': AMOUNT_COLUMN_INDEX,
                        'endColumnIndex': AMOUNT_COLUMN_INDEX + 1,
                    },
                    'rows': [
                        {
                            'values': [
                                {
                                    'userEnteredValue': {'numberValue': parsed_amount}
                                }
                            ]
                        }
                    ],
                    'fields': 'userEnteredValue',
                }
            }
        )

    return requests, skipped


def _apply_updates(
    service: Any,
    spreadsheet_id: str,
    sheet_id: int,
    requests: List[Dict[str, Any]],
) -> None:
    if not requests:
        return

    hydrated_requests = []
    for request in requests:
        update_cells = request['updateCells']
        update_range = dict(update_cells['range'])
        update_range['sheetId'] = sheet_id
        hydrated_requests.append(
            {
                'updateCells': {
                    'range': update_range,
                    'rows': update_cells['rows'],
                    'fields': update_cells['fields'],
                }
            }
        )

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'requests': hydrated_requests},
    ).execute()


def main() -> int:
    args = _parse_args()
    _configure_logging(args.verbose)

    spreadsheet_id = args.spreadsheet_id
    if not spreadsheet_id:
        logger.error('No spreadsheet id configured. Set GOOGLE_SHEET_ID in .env or pass --spreadsheet-id.')
        return 1

    apply_changes = bool(args.apply)
    if not args.dry_run and not args.apply:
        logger.info('No mode supplied, defaulting to --dry-run.')

    service = _get_service(write=apply_changes)
    all_sheets = _sheet_titles(service, spreadsheet_id)

    if args.sheet:
        target_sheets = [(title, sheet_id) for title, sheet_id in all_sheets if title == args.sheet]
        if not target_sheets:
            logger.error('Sheet %r was not found in spreadsheet %s.', args.sheet, spreadsheet_id)
            return 1
    else:
        target_sheets = [(title, sheet_id) for title, sheet_id in all_sheets if title not in SKIPPED_SHEETS]

    total_rows_seen = 0
    total_fixes = 0
    total_skipped = 0

    for sheet_title, sheet_id in target_sheets:
        rows = _load_rows(service, spreadsheet_id, sheet_title)
        total_rows_seen += len(rows)
        requests, skipped = _build_amount_updates(rows)
        total_fixes += len(requests)
        total_skipped += skipped

        if apply_changes:
            _apply_updates(service, spreadsheet_id, sheet_id, requests)
            logger.info(
                'Applied %d amount fix(es) on sheet %s. Skipped %d row(s).',
                len(requests),
                sheet_title,
                skipped,
            )
        else:
            logger.info(
                'Would apply %d amount fix(es) on sheet %s. Skipped %d row(s).',
                len(requests),
                sheet_title,
                skipped,
            )

    mode_label = 'APPLY' if apply_changes else 'DRY-RUN'
    logger.info(
        '%s summary: processed %d sheet(s), scanned %d row(s), %d fix(es), %d skipped row(s).',
        mode_label,
        len(target_sheets),
        total_rows_seen,
        total_fixes,
        total_skipped,
    )
    logger.info(
        'Command examples: python scripts\\fix_sheet_amounts.py --dry-run | python scripts\\fix_sheet_amounts.py --apply'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
