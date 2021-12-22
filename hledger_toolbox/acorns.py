import click
from datetime import datetime
import decimal
import enum
import logging
import re
from typing import Callable, Iterable, List, Optional, Tuple

from . import utils


class Activity(enum.Enum):
    BUY = "Buy"
    SELL = "Sell"
    TRANSFER = "Transfer"


logger = logging.getLogger("acorns")
STMT_PERIOD_LINE_REGEX = re.compile(
    r"statement period\s+(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})"
)
DATE_REGEX = r"\d{1,2}/\d{1,2}/\d{4}"
CURRENCY_REGEX = r"\({0,1}\$\s*[\d,]*\.\d{2}\){0,1}"
DESC_REGEX = r"\s{2}[\w \(\)]+\s{2}"
QUANTITY_REGEX = r"[\d,]*\.{0,1}\d*"
TRANSFER_LINE_REGEX = re.compile(
    fr"^\s*(?P<date>{DATE_REGEX})(?P<desc>{DESC_REGEX})"
    fr"(?P<amount>{CURRENCY_REGEX})\s*$"
)
TRANSACTION_LINE_REGEX = re.compile(
    fr"^\s*(?P<date>{DATE_REGEX})\s*{DATE_REGEX}\s*(?P<activity>\w+)"
    fr"(?P<desc>{DESC_REGEX})(?P<quantity>{QUANTITY_REGEX})"
    fr"\s*(?P<price>{CURRENCY_REGEX})\s*(?P<amount>{CURRENCY_REGEX})\s*$"
)
TICKER_REGEX = re.compile(r"\(([A-Z]+)\)")
DIVIDEND_LINE_REGEX = re.compile(
    fr"^\s*(?P<date>{DATE_REGEX})\s*(?P<ticker>[A-Z]+) Dividend Reinvestment\s*"
    fr"(?P<amount>{CURRENCY_REGEX})\s*$"
)
REALIZED_GAINS_LOSSES_REGEX = re.compile(
    fr"^\s*(?P<sold_date>{DATE_REGEX})\s*(?P<acquired_date>{DATE_REGEX})"
    fr"(?P<desc>{DESC_REGEX})(?P<price>{CURRENCY_REGEX})\s*"
    fr"(?P<quantity>{QUANTITY_REGEX})\s*(?P<value>{CURRENCY_REGEX})\s*"
    fr"(?P<cost_basis>{CURRENCY_REGEX})\s*(?P<gain_loss>{CURRENCY_REGEX})\s*$"
)


def _extract_statement_period(
    stmt_text: Iterable[str],
) -> Optional[Tuple[datetime, datetime]]:
    for line in stmt_text:
        mat = STMT_PERIOD_LINE_REGEX.search(line.lower())
        if mat is not None:
            try:
                start_date = datetime.strptime(mat.group(1), "%m/%d/%Y")
                end_date = datetime.strptime(mat.group(2), "%m/%d/%Y")
                return start_date, end_date
            except ValueError:
                pass
    return None


def _transaction_builder(
    stmt_text: Iterable[str],
    sec_start_kwd: str,
    sec_end_kwd: str,
    line_pattern: re.Pattern,
    line_parser: Callable[[re.Match], Iterable[utils.Transaction]],
) -> List[utils.Transaction]:
    started = False
    res = []
    for line in stmt_text:
        if not started and sec_start_kwd in line.lower():
            started = True
        elif started and sec_end_kwd in line.lower():
            break
        elif started:
            mat = line_pattern.match(line)
            if mat is not None:
                batch = line_parser(mat)
                res.extend(batch)
    return res


def _extract_dividends_interests(
    stmt_text: Iterable[str], acorns_account: str
) -> List[utils.Transaction]:
    SEC_START_KWD = "purchases and sales summary"
    SEC_END_KWD = "net purchases and sales"

    def _line_parser(mat: re.Match) -> List[utils.Transaction]:
        date = datetime.strptime(mat.group("date"), "%m/%d/%Y")
        ticker = mat.group("ticker")
        amount = utils.Amount.from_dollar_string(mat.group("amount"))
        return [
            utils.Transaction(
                date=date,
                description=f"{ticker} Dividends",
                postings=[
                    utils.Posting(account=f"{acorns_account}:cash", amount=amount),
                    utils.Posting(
                        account=f"revenues:investment:dividends:{ticker.lower()}",
                        amount=-amount,
                    ),
                ],
            )
        ]

    return _transaction_builder(
        stmt_text, SEC_START_KWD, SEC_END_KWD, DIVIDEND_LINE_REGEX, _line_parser
    )


def _extract_transfers(
    stmt_text: Iterable[str], acorns_account: str, transfer_account: str
) -> List[utils.Transaction]:
    SEC_START_KEYWORD = "deposits and withdrawals summary"
    SEC_END_KEYWORD = "net deposits and withdrawals"

    def _line_parser(mat: re.Match) -> List[utils.Transaction]:
        date = datetime.strptime(mat.group("date"), "%m/%d/%Y")
        amount = utils.Amount.from_dollar_string(mat.group("amount"))
        return [
            utils.Transaction(
                date=date,
                description=mat.group("desc").strip(),
                cleared=True,
                postings=[
                    utils.Posting(account=acorns_account + ":cash", amount=amount),
                    utils.Posting(account=transfer_account, amount=-amount),
                ],
            )
        ]

    return _transaction_builder(
        stmt_text, SEC_START_KEYWORD, SEC_END_KEYWORD, TRANSFER_LINE_REGEX, _line_parser
    )


def _extract_transactions(
    stmt_text: Iterable[str], sec_start_kwd: str, sec_end_kwd: str, acorns_account: str
) -> List[utils.Transaction]:
    def _line_parser(mat: re.Match) -> List[utils.Transaction]:
        activity = (
            Activity.BUY
            if mat.group("activity").strip().lower() in ["buy", "bought"]
            else Activity.SELL
        )
        desc = mat.group("desc").strip()
        ticker_mat = TICKER_REGEX.search(desc)
        ticker = ticker_mat.group(1) if ticker_mat is not None else "Unknown"
        quantity = decimal.Decimal(mat.group("quantity").replace(",", ""))
        amount = utils.Amount.from_dollar_string(mat.group("amount"))
        if activity == Activity.BUY:
            postings = [
                utils.Posting(
                    account=f"{acorns_account}:cash",
                    amount=-amount,
                ),
                utils.Posting(
                    account=f"{acorns_account}:{ticker.lower()}",
                    amount=utils.Amount(
                        commodity=ticker,
                        formatter="{value:.6f} {commodity:s}",
                        value=quantity,
                    ),
                    price=utils.Price(
                        price_type=utils.PriceType.TOTAL,
                        amount=amount,
                    ),
                ),
            ]
        else:
            raise ValueError("cannot support SELL action yet")
        return [
            utils.Transaction(
                date=datetime.strptime(mat.group("date"), "%m/%d/%Y"),
                description=f"{desc} {activity.value}",
                cleared=True,
                postings=postings,
            )
        ]

    return _transaction_builder(
        stmt_text, sec_start_kwd, sec_end_kwd, TRANSACTION_LINE_REGEX, _line_parser
    )


def _extract_realized_gains_losses(
    stmt_text: Iterable[str],
    sec_start_kwd: str,
    sec_end_kwd: str,
    acorns_account: str,
    revenue_account: str,
) -> List[utils.Transaction]:
    def _line_parser(mat: re.Match) -> List[utils.Transaction]:
        sold_date = datetime.strptime(mat.group("sold_date"), "%m/%d/%Y")
        acquired_date = datetime.strptime(mat.group("acquired_date"), "%m/%d/%Y")
        desc = mat.group("desc").strip()
        ticker_mat = TICKER_REGEX.search(desc)
        ticker = ticker_mat.group(1) if ticker_mat is not None else "Unknown"
        quantity = decimal.Decimal(mat.group("quantity"))
        value_amount = utils.Amount.from_dollar_string(mat.group("value"))
        cost_basis_amount = utils.Amount.from_dollar_string(mat.group("cost_basis"))
        gain_loss_amount = utils.Amount.from_dollar_string(mat.group("gain_loss"))
        diff = value_amount.value - cost_basis_amount.value
        if diff != gain_loss_amount.value:
            logger.warning(
                "mismatch of reported and calculated gains/losses: "
                "reported %.2f vs. calculated %.2f",
                gain_loss_amount.value,
                diff,
            )
        gain_loss_amount.value = diff
        postings = [
            utils.Posting(account=f"{acorns_account}:cash", amount=value_amount),
            utils.Posting(
                account=f"{acorns_account}:{ticker.lower()}",
                amount=-utils.Amount(
                    commodity=ticker,
                    formatter="{value:.6f} {commodity:s}",
                    value=quantity,
                ),
                price=utils.Price(
                    price_type=utils.PriceType.TOTAL,
                    amount=cost_basis_amount,
                ),
            ),
            utils.Posting(
                account=f"{revenue_account}:{ticker.lower()}", amount=-gain_loss_amount
            ),
        ]
        return [
            utils.Transaction(
                date=sold_date,
                description=f"{desc} Sell "
                f"(Acquired on {acquired_date.strftime('%Y-%m-%d')})",
                postings=postings,
            )
        ]

    return _transaction_builder(
        stmt_text, sec_start_kwd, sec_end_kwd, REALIZED_GAINS_LOSSES_REGEX, _line_parser
    )


def _write_journal_file(
    output_file: str,
    sections: Iterable[Tuple[str, Iterable[utils.Transaction]]],
    stmt_period: Tuple[datetime, datetime],
):
    with open(output_file, "w") as fp:
        fp.write("; automatically generated by Acorns import utility\n")
        fp.write(
            f"; for {stmt_period[0].strftime('%Y-%m%d')} - "
            f"{stmt_period[1].strftime('%Y-%m%d')}\n"
        )
        for sec_header, transactions in sections:
            fp.write("\n")
            fp.write(f"; {sec_header}\n")
            if not transactions:
                fp.write("; NO TRANSACTIONS\n")
            for transaction in transactions:
                fp.write(str(transaction))
                fp.write("\n")
                fp.write("\n")


@click.command(name="import")
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path())
@click.option(
    "-a",
    "--account",
    type=str,
    default="assets:taxable:liquid:lz:acorns",
    help="hledger account name for Acorns",
)
@click.option(
    "-t",
    "--transfer-account",
    type=str,
    default="assets:transfer",
    help="hledger account name for the transfer holding",
)
def acorns_import(
    input_file: str, output_file: str, account: str, transfer_account: str
):
    """
    Import Acorns pdf statements (INPUT_FILE) into hledger friendly csv files
    (OUTPUT_FILE)
    """
    stmt_text = utils.get_raw_text_of_pdf(input_file).split("\n")
    stmt_period = _extract_statement_period(stmt_text)
    transfers = _extract_transfers(stmt_text, account, transfer_account)
    transactions_dividend = _extract_dividends_interests(stmt_text, account)
    transactions_buy = _extract_transactions(
        stmt_text, "securities bought", "total securities bought", account
    )
    transactions_sell_short = _extract_realized_gains_losses(
        stmt_text,
        "realized gains & losses for this period: short-term",
        "total short-term gain (loss)",
        account,
        "revenues:investment:realized short term gain",
    )
    transactions_sell_long = _extract_realized_gains_losses(
        stmt_text,
        "realized gains & losses for this period: long-term",
        "total long-term gain (loss)",
        account,
        "revenues:investment:realized long term gain",
    )
    _write_journal_file(
        output_file,
        [
            ("Transfers", transfers),
            ("Dividends and Interests", transactions_dividend),
            ("Buys", transactions_buy),
            ("Short-term Sells", transactions_sell_short),
            ("Long-term Sells", transactions_sell_long),
        ],
        stmt_period,
    )


if __name__ == "__main__":
    acorns_import()
