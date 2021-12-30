import collections
import dataclasses
import datetime
import decimal
import enum
import json
import logging
import os
import re
import sys
from typing import Any, Callable, Dict, List, Optional, Tuple

import click
import pydantic

from hledger_toolbox import utils


DOLLAR_AMOUNT_PAT = re.compile(r"^[+-]{0,1}\s*\$\s*[0-9,]+(\.[0-9]{1,2})$")
LOTS_MANAGER = utils.CommodityLotsManager()
logger = logging.getLogger(os.path.basename(__file__))


@dataclasses.dataclass
class AccountsConfig:
    base_account: str
    transfer_account: str
    dividend_account: str
    interest_account: str
    short_term_account: str
    long_term_account: str
    trade_fees_account: str


def _dollar_amount_validator(v: Any) -> decimal.Decimal:
    if not isinstance(v, str):
        raise TypeError("string required")
    if DOLLAR_AMOUNT_PAT.match(v) is None:
        raise ValueError(f"invalid dollar amount string {v}")
    return decimal.Decimal(v.strip().replace("$", "").replace(",", "").replace(" ", ""))


class RecordRow(pydantic.BaseModel):
    description: str = ""
    symbol: str = ""
    acct_type: str
    transaction: str
    date: datetime.datetime
    qty: Optional[decimal.Decimal] = None
    price: Optional[decimal.Decimal] = None
    debit: Optional[decimal.Decimal] = None
    credit: Optional[decimal.Decimal] = None

    @property
    def amount(self) -> decimal.Decimal:
        if self.credit is not None:
            return self.credit
        elif self.debit is not None:
            return -self.debit
        return decimal.Decimal(0)

    @pydantic.validator("date", pre=True)
    def date_valid_format(cls, v: Any) -> datetime.datetime:
        if isinstance(v, str):
            return datetime.datetime.strptime(v, "%m/%d/%Y")
        else:
            return v

    @pydantic.validator("qty", pre=True)
    def qty_may_contain_S(cls, v: Any) -> decimal.Decimal:
        if isinstance(v, str) and v.endswith("S"):
            return -decimal.Decimal(v[:-1])
        return decimal.Decimal(v)

    @pydantic.validator("price", pre=True)
    def price_dollar_amount(cls, v: Any) -> decimal.Decimal:
        return _dollar_amount_validator(v)

    @pydantic.validator("debit", pre=True)
    def debit_dollar_amount(cls, v: Any) -> decimal.Decimal:
        return _dollar_amount_validator(v)

    @pydantic.validator("credit", pre=True)
    def credit_dollar_amount(cls, v: Any) -> decimal.Decimal:
        return _dollar_amount_validator(v)


OPTIONS_DESC_PAT = re.compile(
    r"^(\s*|option expiration for )(?P<underlying>[a-z]+)\s*"
    r"(?P<date>\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})\s*"
    r"(?P<type>call|put)\s*"
    r"\$(?P<strike>[\d\.,]+)"
    r"$",
    re.IGNORECASE,
)


def _make_options_symbol(description: str) -> str:
    mat = OPTIONS_DESC_PAT.match(description.strip())
    if mat is None:
        raise ValueError(f"invalid options description: {description}")
    date_str = mat.group("date")
    date = datetime.datetime.strptime(
        date_str, "%m/%d/%Y" if "/" in date_str else "%Y-%m-%d"
    )
    type_code = mat.group("type")[0].upper()
    normalized = decimal.Decimal(mat.group("strike")).normalize()
    _, _, exponent = normalized.as_tuple()
    strike_str = str(normalized if exponent <= 0 else normalized.quantize(1))
    return f"{mat.group('underlying')}{date.strftime('%y%m%d')}{type_code}{strike_str}"


def _drop_coin_transformer(
    record: RecordRow, config: AccountsConfig
) -> Tuple[bool, Optional[utils.Transaction]]:
    # intentionally drop COIN records
    return record.transaction == "COIN", None


def _trade_transformer(
    record: RecordRow, config: AccountsConfig
) -> Tuple[bool, Optional[utils.Transaction]]:
    if record.transaction not in ["Buy", "Sell", "STO", "BTC", "BTO", "STC", "OEXP"]:
        return False, None
    commodity = record.symbol
    if record.transaction in ["STO", "BTC", "STC", "BTO", "OEXP"]:
        # NOTE: option commodity symbols need to be constructed
        try:
            commodity = _make_options_symbol(record.description)
        except ValueError as exc:
            logger.warning("failed to process option description: %s", str(exc))
        # NOTE: option prices are *per share*. need to convert to *per contract*
        if record.price is not None:
            record.price = record.price * 100
    total_quantity = record.qty
    if record.transaction in ["Sell", "STO", "STC"]:
        total_quantity = -record.qty
    if record.transaction == "OEXP":
        # for option expiration, set quantity based on if the current position
        # is long or short
        first_lot = LOTS_MANAGER.get_lots(commodity, config.base_account)[0]
        if first_lot.quantity > 0:
            total_quantity = -record.qty
    transaction = utils.trade_lots(
        lots_manager=LOTS_MANAGER,
        accounts=utils.TradeLotsAccounts(
            base_account=config.base_account,
            short_term_account=config.short_term_account,
            long_term_account=config.long_term_account,
        ),
        date=record.date,
        commodity=commodity,
        change_in_quantity=total_quantity,
        proceeds_or_costs=record.amount,
    )
    transaction.description = (
        f"{record.transaction} on {record.description} ({commodity})"
    )
    utils.balance_transaction(transaction, config.trade_fees_account)
    return True, transaction


def _cash_event_transformer(
    record: RecordRow, config: AccountsConfig
) -> Tuple[bool, Optional[utils.Transaction]]:
    account2 = ""
    if record.transaction == "CDIV":
        account2 = f"{config.dividend_account}:{record.symbol.lower()}"
    elif record.transaction == "INT":
        account2 = config.interest_account
    elif record.transaction == "ACH":
        account2 = config.transfer_account
    elif record.transaction == "AFEE":
        account2 = config.trade_fees_account
    else:
        return False, None
    return True, utils.Transaction(
        date=record.date,
        description=record.description,
        postings=[
            utils.Posting(
                account=f"{config.base_account}:cash",
                amount=utils.Amount.dollar_amount(record.amount),
            ),
            utils.Posting(
                account=account2,
                amount=utils.Amount.dollar_amount(-record.amount),
            ),
        ],
    )


class _SplitTransformer:
    _inst: Optional["_SplitTransformer"] = None

    def __init__(
        self, lots_manager: Optional[utils.CommodityLotsManager] = None
    ) -> None:
        self._parser = utils.SplitParser(lots_manager)

    @property
    def lots_manager(self) -> utils.CommodityLotsManager:
        return self._parser.lots_manager

    @lots_manager.setter
    def lots_manager(self, lots_manager: utils.CommodityLotsManager):
        self._parser.lots_manager = lots_manager

    def __call__(
        self, record: RecordRow, config: AccountsConfig
    ) -> Tuple[bool, Optional[utils.Transaction]]:
        if record.transaction not in ["SPL", "SPR"]:
            return False, None
        if not record.symbol:
            # this happens when trying to transform the item in the pdf statement
            return True, None
        record = utils.CommoditySplitAction(
            date=record.date,
            commodity=record.symbol,
            quantity=record.qty,
            is_reverse=record.transaction == "SPR",
        )
        return True, self._parser(
            record, config.base_account, config.trade_fees_account
        )

    @classmethod
    def inst(cls) -> "_SplitTransformer":
        if cls._inst is None:
            cls._inst = cls()
        return cls._inst


RECORD_TRANSFORMERS: List[
    Callable[[RecordRow, AccountsConfig], Tuple[bool, Optional[utils.Transaction]]]
] = [
    _drop_coin_transformer,
    _trade_transformer,
    _cash_event_transformer,
    _SplitTransformer.inst(),
]


class ParserStage(enum.Enum):
    READY = 0
    STARTED = 1
    RULERS_SET = 2
    IN_TRANSACTION = 3
    ENDED = 99


class RowParser:

    START_PAT = re.compile(r"^\s*Account Activity\s*$")
    TABLE_TITLE_PAT = re.compile(
        r"^(?P<description>\s*Description\s{2,})"
        r"(?P<symbol>Symbol\s{2,})"
        r"(?P<acct_type>Acct Type\s{2,})"
        r"(?P<transaction>Transaction\s{2,})"
        r"(?P<date>Date\s{2,})"
        r"(?P<qty>Qty\s{2,})"
        r"(?P<price>Price\s{2,})"
        r"(?P<debit>Debit\s{2,})"
        r"(?P<credit>Credit)\s*$"
    )
    FIELDS = (
        "description",
        "symbol",
        "acct_type",
        "transaction",
        "date",
        "qty",
        "price",
        "debit",
        "credit",
    )
    END_PAT = re.compile(r"^\s*Total Funds Paid and Received.*$")
    PAGE_NUMBER_PAT = re.compile(r"^\s*page \d+ of \d+\s*$", re.IGNORECASE)

    def __init__(self, accounts: AccountsConfig) -> None:
        self._accounts = accounts
        self._stage = ParserStage.READY
        self._rulers: Dict[str, Tuple[int, int]] = {}
        self._active_lines: List[str] = []
        self._logger = logging.getLogger(__class__.__name__)
        self._results: List[RecordRow] = []

    @property
    def results(self) -> List[RecordRow]:
        self._results.sort(key=lambda r: r.date)
        return self._results

    def _is_row_empty(self, row: str) -> bool:
        return row.strip() == ""

    def _is_start(self, row: str) -> bool:
        return self.START_PAT.match(row) is not None

    def _is_page_number(self, row: str) -> bool:
        return self.PAGE_NUMBER_PAT.match(row) is not None

    def _try_to_start(self, row: str) -> bool:
        mat = self.START_PAT.match(row)
        if mat is not None:
            self._stage = ParserStage.STARTED
            return True
        return False

    def _try_to_end(self, row: str) -> bool:
        mat = self.END_PAT.match(row)
        if mat is not None:
            self._stage = ParserStage.ENDED
            return True
        return False

    def _try_to_set_ruler(self, row: str) -> bool:
        mat = self.TABLE_TITLE_PAT.match(row)
        if mat is not None:
            self._stage = ParserStage.RULERS_SET
            self._rulers = {}
            last_pos = 0
            for i, field in enumerate(self.FIELDS):
                self._rulers[field] = (last_pos, last_pos + len(mat.group(i + 1)))
                last_pos += len(mat.group(i + 1))
            self._rulers[self.FIELDS[-1]] = (self._rulers[self.FIELDS[-1]][0], None)
            return True
        return False

    def _parse_active_lines(self):
        res = collections.defaultdict(list)
        for line in self._active_lines:
            for field, (start_ind, end_ind) in self._rulers.items():
                phrase = line[start_ind:end_ind].strip()
                if phrase:
                    res[field].append(phrase)
        for field, value in res.items():
            res[field] = " ".join(value)
        try:
            record = RecordRow(**res)
            self._results.append(record)
        except pydantic.ValidationError:
            self._logger.warning("unable to process record: %s", json.dumps(res))

    def __call__(self, row: str) -> None:
        if self._stage == ParserStage.ENDED:
            return
        elif self._stage == ParserStage.READY:
            if self._try_to_end(row):
                return
            self._try_to_start(row)
        elif self._stage == ParserStage.STARTED:
            if self._try_to_end(row):
                return
            self._try_to_set_ruler(row)
        elif self._stage == ParserStage.RULERS_SET:
            if self._try_to_end(row):
                return
            if (
                not self._try_to_set_ruler(row)
                and not self._is_row_empty(row)
                and not self._is_start(row)
                and not self._is_page_number(row)
            ):
                self._stage = ParserStage.IN_TRANSACTION
                self._active_lines.append(row)
        elif self._stage == ParserStage.IN_TRANSACTION:
            if self._try_to_end(row):
                return
            if (
                self._is_row_empty(row)
                or self._is_start(row)
                or self._is_page_number(row)
            ):
                self._parse_active_lines()
                self._stage = ParserStage.RULERS_SET
                self._active_lines = []
            else:
                self._active_lines.append(row)

    def add_shadow_records(self, records: List[RecordRow]):
        self._results.extend(records)


@click.command(name="import")
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path())
@click.option(
    "-a",
    "--account",
    type=str,
    default="assets:taxable:liquid:lz:robinhood",
    help="hledger account name for Acorns",
)
@click.option(
    "-t",
    "--transfer-account",
    type=str,
    default="assets:transfer",
    help="hledger account name for the transfer holding",
)
@click.option(
    "-d",
    "--dividend-account",
    type=str,
    default="revenues:investment:dividends",
    help="hledger account name for dividend revenues",
)
@click.option(
    "-i",
    "--interest-account",
    type=str,
    default="revenues:income:Interest",
    help="hledger account name for interest revenues",
)
@click.option(
    "-s",
    "--short-term-account",
    type=str,
    default="revenues:investment:realized short term gain",
    help="hledger account name for short-term gain revenues",
)
@click.option(
    "-l",
    "--long-term-account",
    type=str,
    default="revenues:investment:realized long term gain",
    help="hledger account name for long-term gain revenues",
)
@click.option(
    "-f",
    "--trade-fees-account",
    type=str,
    default="expenses:investment:trading fees",
    help="hledger account name for trading fees",
)
def robinhood_import(
    input_file: str,
    output_file: str,
    account: str,
    transfer_account: str,
    dividend_account: str,
    interest_account: str,
    short_term_account: str,
    long_term_account: str,
    trade_fees_account: str,
):
    """
    Import Robinhood pdf or text statements (INPUT_FILE) into hledger friendly
    journal files (OUTPUT_FILE)
    """
    accounts_config = AccountsConfig(
        base_account=account,
        transfer_account=transfer_account,
        dividend_account=dividend_account,
        interest_account=interest_account,
        short_term_account=short_term_account,
        long_term_account=long_term_account,
        trade_fees_account=trade_fees_account,
    )
    parser = RowParser(accounts_config)
    stmt_text = re.split("\n|\u00ff", utils.get_raw_text_of_pdf(input_file))
    for line in stmt_text:
        parser(line)
    # add shadow records if available
    shadow_file = os.path.join(
        os.path.dirname(input_file),
        "." + os.path.splitext(os.path.basename(input_file))[0] + ".json",
    )
    if os.path.isfile(shadow_file):
        with open(shadow_file, "r") as fp:
            shadow_records = json.load(fp)
        parser.add_shadow_records([RecordRow(**r) for r in shadow_records])
    # NOTE: it's important to set the end date of the lots manager as the first
    # trade's date
    LOTS_MANAGER.end_date = parser.results[0].date
    # NOTE: the split transformer needs to have its lots manager set up
    _SplitTransformer.inst().lots_manager = LOTS_MANAGER
    # run record transformer on all records
    transactions: List[utils.Transaction] = []
    for record in parser.results:
        for transformer in RECORD_TRANSFORMERS:
            matched, transaction = transformer(record, accounts_config)
            if matched:
                if transaction is not None:
                    transactions.append(transaction)
                break
        else:
            logger.warning("row record not matched: %s", record)
    if output_file == "-":
        output_fp = sys.stdout
    else:
        output_fp = open(output_file, "w")
    utils.write_journal_file(
        output_fp,
        [("All Transactions", transactions)],
        (min(t.date for t in transactions), max(t.date for t in transactions)),
        generator_name="Robinhood import utility",
    )


if __name__ == "__main__":
    robinhood_import()
