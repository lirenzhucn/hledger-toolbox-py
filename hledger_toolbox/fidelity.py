import csv
import dataclasses
from datetime import datetime
import decimal
import json
import logging
import os
import re
import sys
from typing import Callable, Dict, List, Optional, Tuple

import click
from hledger_toolbox import utils


logger = logging.getLogger(os.path.basename(__file__))
LOTS_MANAGER = utils.CommodityLotsManager()


@dataclasses.dataclass
class RowParserConfig:
    base_account: str
    transfer_account: str
    dividend_account: str
    rsu_account: str
    short_term_account: str
    long_term_account: str
    trade_fees_account: str
    cash_commodity: List[str] = dataclasses.field(default_factory=lambda: ["SPAXX"])


def _transfer_action_parser(
    row: Dict[str, str], config: RowParserConfig
) -> Optional[utils.Transaction]:
    date = datetime.strptime(row["date"].strip(), "%m/%d/%Y")
    total_dollars = decimal.Decimal(row["amount"].strip())
    return utils.Transaction(
        date=date,
        description=row["action"].strip(),
        postings=[
            utils.Posting(
                account=f"{config.base_account}:cash",
                amount=utils.Amount.dollar_amount(total_dollars),
            ),
            utils.Posting(
                account=config.transfer_account,
                amount=-utils.Amount.dollar_amount(total_dollars),
            ),
        ],
    )


def _rsu_action_parser(
    row: Dict[str, str], config: RowParserConfig
) -> Optional[utils.Transaction]:
    date = datetime.strptime(row["date"].strip(), "%m/%d/%Y")
    unit_price = utils.Price(
        price_type=utils.PriceType.UNIT,
        amount=utils.Amount.dollar_amount(decimal.Decimal(row["price"].strip())),
    )
    commodity = row["symbol"].strip()
    quantity = decimal.Decimal(row["quantity"].strip())
    total_dollars = quantity * unit_price.amount.value
    # add to lots
    LOTS_MANAGER.add_lot(commodity, config.base_account, date, quantity, unit_price)
    return utils.Transaction(
        date=date,
        description=row["action"].strip(),
        postings=[
            utils.Posting(
                account=f"{config.rsu_account}",
                amount=-utils.Amount.dollar_amount(total_dollars),
            ),
            utils.Posting(
                account=f"{config.base_account}:{commodity.lower()}:{date.strftime('%Y%m%d')}",
                amount=utils.Amount(
                    commodity=commodity,
                    formatter="{value:.6f} {commodity:s}",
                    value=quantity,
                ),
                price=unit_price,
            ),
        ],
    )


def _trade_action_parser(
    row: Dict[str, str], config: RowParserConfig
) -> Optional[utils.Transaction]:
    date = datetime.strptime(row["date"].strip(), "%m/%d/%Y")
    commodity = row["symbol"].strip().lstrip("+-")
    total_dollars = decimal.Decimal(row["amount"].strip())
    total_quantity = decimal.Decimal(row["quantity"].strip())
    change_in_quantity = total_quantity
    try:
        lot_date = datetime.strptime(row["acquired_date"].strip(), "%m/%d/%Y")
        change_in_quantity = [(total_quantity, lot_date)]
    except ValueError:
        pass
    if commodity in config.cash_commodity:
        # skip transactions on cash commodity
        return None
    tags = []
    if row["action"].strip().lower().startswith("you bought espp"):
        tags.append(("espp", ""))
    transaction = utils.trade_lots(
        lots_manager=LOTS_MANAGER,
        accounts=utils.TradeLotsAccounts(
            base_account=config.base_account,
            short_term_account=config.short_term_account,
            long_term_account=config.long_term_account,
        ),
        date=date,
        commodity=commodity,
        change_in_quantity=change_in_quantity,
        proceeds_or_costs=total_dollars,
    )
    transaction.description = row["action"].strip()
    transaction.tags = tags
    utils.balance_transaction(transaction, config.trade_fees_account)
    return transaction


def _expired_option_action_parser(
    row: Dict[str, str], config: RowParserConfig
) -> Optional[utils.Transaction]:
    row["amount"] = "0"
    return _trade_action_parser(row, config)


def _dividend_action_parser(
    row: Dict[str, str], config: RowParserConfig
) -> Optional[utils.Transaction]:
    date = datetime.strptime(row["date"].strip(), "%m/%d/%Y")
    total_dollars = decimal.Decimal(row["amount"].strip())
    commodity = row["symbol"].strip()
    return utils.Transaction(
        date=date,
        description=row["action"].strip(),
        postings=[
            utils.Posting(
                account=f"{config.dividend_account}:{commodity.lower()}",
                amount=utils.Amount.dollar_amount(-total_dollars),
            ),
            utils.Posting(
                account=f"{config.base_account}:cash",
                amount=utils.Amount.dollar_amount(total_dollars),
            ),
        ],
    )


class _SplitParser:
    _inst: Optional["_SplitParser"] = None

    def __init__(
        self,
        lots_manager: Optional[utils.CommodityLotsManager] = None,
        base_account: str = "",
    ) -> None:
        self._lots_manager = lots_manager
        self._base_account = base_account
        self._cache: Dict[
            str, Tuple[List[utils.CommodityLot], decimal.Decimal, decimal.Decimal]
        ] = {}

    @property
    def lots_manager(self) -> utils.CommodityLotsManager:
        return self._lots_manager

    @lots_manager.setter
    def lots_manager(self, lots_manager: utils.CommodityLotsManager):
        self._lots_manager = lots_manager

    @property
    def base_account(self) -> str:
        return self._base_account

    @base_account.setter
    def base_account(self, base_account: str):
        self._base_account = base_account

    def __call__(
        self, row: Dict[str, str], config: RowParserConfig
    ) -> Optional[utils.Transaction]:
        if self._lots_manager is None or not self._base_account:
            raise ValueError("parser not initialized")
        date = datetime.strptime(row["date"].strip(), "%m/%d/%Y")
        commodity = row["symbol"].strip()
        quantity = decimal.Decimal(row["quantity"].strip())
        if commodity in self._cache:
            lots = self._cache[commodity][0]
            total_quantity = sum(lot.quantity for lot in lots)
            if quantity * total_quantity <= 0:
                # the "sell" half of the split
                # self._cache[commodity][1] = quantity
                self._cache[commodity] = (lots, quantity, self._cache[commodity][2])
            else:
                # self._cache[commodity][2] = quantity
                self._cache[commodity] = (lots, self._cache[commodity][1], quantity)
            if row["action"].strip().lower().startswith("reverse"):
                desc = f"Reverse split {commodity}"
            else:
                desc = f"Split {commodity}"
            sell_quantity, buy_quantity = self._cache[commodity][1:]
            ratio = -sell_quantity / buy_quantity
            postings: List[utils.Posting] = []
            for lot in lots:
                lot_account = (
                    f"{config.base_account}:{commodity.lower()}:"
                    f"{lot.date.strftime('%Y%m%d')}"
                )
                postings.append(
                    utils.Posting(
                        account=lot_account,
                        amount=utils.Amount(
                            commodity=lot.commodity,
                            formatter="{value:.6f} {commodity:s}",
                            value=-lot.quantity,
                        ),
                        price=lot.price,
                    )
                )
                new_price = dataclasses.replace(lot.price)
                new_price.amount = dataclasses.replace(lot.price.amount)
                new_price.amount.value *= ratio
                postings.append(
                    utils.Posting(
                        account=lot_account,
                        amount=utils.Amount(
                            commodity=lot.commodity,
                            formatter="{value:.6f} {commodity:s}",
                            value=lot.quantity / ratio,
                        ),
                        price=new_price,
                    )
                )
            transaction = utils.Transaction(
                date=date, description=desc, postings=postings, tags=[("split", "")]
            )
            utils.balance_transaction(transaction, account=config.trade_fees_account)
            return transaction
        else:
            lots = self._lots_manager.get_lots(commodity, self._base_account)
            total_quantity = sum(lot.quantity for lot in lots)
            if quantity * total_quantity <= 0:
                # the "sell" half of the split
                sell_quantity = quantity
                buy_quantity = None
            else:
                buy_quantity = quantity
                sell_quantity = None
            self._cache[commodity] = (lots, sell_quantity, buy_quantity)
            return None

    @classmethod
    def inst(cls) -> "_SplitParser":
        if cls._inst is None:
            cls._inst = cls()
        return cls._inst


ACTION_PARSER_MAP: List[
    Tuple[
        re.Pattern,
        Callable[[Dict[str, str], RowParserConfig], Optional[utils.Transaction]],
    ]
] = [
    (
        re.compile(r"^\s*(reinvestment|you bought).*$", re.IGNORECASE),
        _trade_action_parser,
    ),
    (
        re.compile(
            r"^\s*(transferred|journaled spp purchase credit|"
            r"electronic funds transfer)\s+.*$",
            re.IGNORECASE,
        ),
        _transfer_action_parser,
    ),
    (re.compile(r"^\s*you sold.*$", re.IGNORECASE), _trade_action_parser),
    (
        re.compile(r"^\s*conversion shares deposited .*$", re.IGNORECASE),
        _rsu_action_parser,
    ),
    (re.compile(r"^\s*dividend received .*$", re.IGNORECASE), _dividend_action_parser),
    (
        re.compile(r"^\s*expired (call|put) .*$", re.IGNORECASE),
        _expired_option_action_parser,
    ),
    (re.compile(r"^\s*reverse split .*$", re.IGNORECASE), _SplitParser.inst()),
]


def _row_parser(
    row: Dict[str, str], config: RowParserConfig
) -> Optional[utils.Transaction]:
    for action_pattern, action_parser in ACTION_PARSER_MAP:
        if action_pattern.match(row["action"]) is not None:
            return action_parser(row, config)
    logger.warning("unable to match any parser for row: %s", json.dumps(row))
    return None


@click.command(name="import")
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path())
@click.option(
    "-a",
    "--account",
    type=str,
    default="assets:taxable:liquid:ws:fidelity",
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
    "-r",
    "--rsu-account",
    type=str,
    default="revenues:income:RSU",
    help="hledger account name for RSU revenues",
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
def fidelity_import(
    input_file: str,
    output_file: str,
    account: str,
    transfer_account: str,
    dividend_account: str,
    rsu_account: str,
    short_term_account: str,
    long_term_account: str,
    trade_fees_account: str,
):
    """
    Import Fidelity csv statements (INPUT_FILE) into hledger friendly journal
    files (OUTPUT_FILE)
    """
    valid_line_regex = re.compile(r"^\s*\d{2}/\d{2}/\d{4}")
    with open(input_file, "r") as input_fp:
        lines = [
            line
            for line in input_fp.readlines()
            if valid_line_regex.search(line) is not None
        ]
    csv_reader = csv.DictReader(
        lines,
        [
            "date",
            "action",
            "symbol",
            "desc",
            "type",
            "quantity",
            "price",
            "commission",
            "fees",
            "interest",
            "amount",
            "settlement_date",
            "acquired_date",
        ],
    )
    rows = sorted(
        csv_reader, key=lambda r: datetime.strptime(r["date"].strip(), "%m/%d/%Y")
    )
    _SplitParser.inst().lots_manager = LOTS_MANAGER
    _SplitParser.inst().base_account = account
    row_parser_config = RowParserConfig(
        base_account=account,
        transfer_account=transfer_account,
        dividend_account=dividend_account,
        rsu_account=rsu_account,
        short_term_account=short_term_account,
        long_term_account=long_term_account,
        trade_fees_account=trade_fees_account,
    )
    transactions = [
        item
        for item in [_row_parser(row, row_parser_config) for row in rows]
        if item is not None
    ]
    if output_file == "-":
        output_fp = sys.stdout
    else:
        output_fp = open(output_file, "w")
    utils.write_journal_file(
        output_fp,
        [("All Transactions", transactions)],
        (min(t.date for t in transactions), max(t.date for t in transactions)),
    )


if __name__ == "__main__":
    fidelity_import()
