import csv
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import decimal
import json
import logging
import os
import re
from typing import Callable, Dict, List, Optional, Tuple

import click
from hledger_toolbox import utils


logger = logging.getLogger(os.path.basename(__file__))
LOTS_MANAGER = utils.CommodityLotsManager()


@dataclass
class RowParserConfig:
    base_account: str
    transfer_account: str
    dividend_account: str
    rsu_account: str
    short_term_account: str
    long_term_account: str
    cash_commodity: List[str] = field(default_factory=lambda: ["SPAXX"])


def _buy_action_parser(
    row: Dict[str, str], config: RowParserConfig
) -> Optional[utils.Transaction]:
    date = datetime.strptime(row["date"].strip(), "%m/%d/%Y")
    total_dollars = decimal.Decimal(row["amount"].strip())
    total_quantity = decimal.Decimal(row["quantity"].strip())
    unit_price = utils.Price(
        price_type=utils.PriceType.UNIT,
        amount=-utils.Amount(
            commodity="$",
            formatter="{commodity:s}{value:.6f}",
            value=total_dollars / total_quantity,
        ),
    )
    commodity = row["symbol"].strip()
    LOTS_MANAGER.add_lot(
        commodity, config.base_account, date, total_quantity, unit_price
    )
    if commodity in config.cash_commodity:
        # skip buy transactions on cash commodity
        return None
    tags = []
    if row["action"].strip().lower().startswith("you bought espp"):
        tags.append(("espp", ""))
    postings = [
        utils.Posting(
            account=f"{config.base_account}:cash",
            amount=utils.Amount.dollar_amount(total_dollars),
        ),
        utils.Posting(
            account=f"{config.base_account}:{commodity.lower()}:{date.strftime('%Y%m%d')}",
            amount=utils.Amount(
                commodity=commodity,
                formatter="{value:.6f} {commodity:s}",
                value=total_quantity,
            ),
            price=unit_price,
        ),
    ]
    return utils.Transaction(
        date=date, description=row["action"].strip(), postings=postings, tags=tags
    )


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


def _sell_action_parser(
    row: Dict[str, str], config: RowParserConfig
) -> Optional[utils.Transaction]:
    date = datetime.strptime(row["date"].strip(), "%m/%d/%Y")
    total_dollars = decimal.Decimal(row["amount"].strip())
    total_quantity = -decimal.Decimal(row["quantity"].strip())
    try:
        lot_date = datetime.strptime(row["acquired_date"].strip(), "%m/%d/%Y")
    except ValueError:
        raise ValueError("unable to parse lot; sell action must include lot info")
    commodity = row["symbol"].strip()
    if commodity in config.cash_commodity:
        # skip buy transactions on cash commodity
        return None
    lot = LOTS_MANAGER.get_lot(commodity, config.base_account, lot_date)
    if lot is None:
        raise ValueError(
            f"unable to find lot {config.base_account}:{commodity}:{lot_date.strftime('%Y%m%d')}"
        )
    postings = [
        utils.Posting(
            account=f"{config.base_account}:cash",
            amount=utils.Amount.dollar_amount(total_dollars),
        ),
        utils.Posting(
            account=f"{config.base_account}:{commodity.lower()}:{lot_date.strftime('%Y%m%d')}",
            amount=utils.Amount(
                commodity=commodity,
                formatter="{value:.6f} {commodity:s}",
                value=-total_quantity,
            ),
            price=lot.price,
        ),
        utils.Posting(
            account=config.long_term_account
            if date - lot.date >= timedelta(days=365)
            else config.short_term_account
        ),
    ]
    LOTS_MANAGER.sell_from_lot(commodity, config.base_account, lot_date, total_quantity)
    return utils.Transaction(
        date=date, description=row["action"].strip(), postings=postings
    )


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


def _options_sell_action_parser(
    row: Dict[str, str], config: RowParserConfig
) -> Optional[utils.Transaction]:
    # 04/09/2021,
    # YOU SOLD OPENING TRANSACTION CALL (MSFT) MICROSOFT CORP MAY 14 21 $300 (100 SHS) (Cash),
    # -MSFT210514C300, CALL (MSFT) MICROSOFT CORP MAY 14 21 $300 (100 SHS),Cash,
    # -2,0.3,1.3,0.08,,58.62,04/12/2021,
    date = datetime.strptime(row["date"].strip(), "%m/%d/%Y")
    total_dollars = decimal.Decimal(row["amount"].strip())
    commodity_account = row["symbol"].strip().strip("-+")
    commodity = utils.map_options_commodity_symbol(commodity_account)


ACTION_PARSER_MAP: List[
    Tuple[
        re.Pattern,
        Callable[[Dict[str, str], RowParserConfig], Optional[utils.Transaction]],
    ]
] = [
    (
        re.compile(
            r"^\s*(reinvestment|you bought)\s+(?!closing transaction).*$", re.IGNORECASE
        ),
        _buy_action_parser,
    ),
    (
        re.compile(
            r"^\s*(transferred|journaled spp purchase credit)\s+.*$", re.IGNORECASE
        ),
        _transfer_action_parser,
    ),
    (
        re.compile(r"^\s*you sold\s+(?!opening transaction).*$", re.IGNORECASE),
        _sell_action_parser,
    ),
    (
        re.compile(r"^\s*conversion shares deposited .*$", re.IGNORECASE),
        _rsu_action_parser,
    ),
    (re.compile(r"^\s*dividend received .*$", re.IGNORECASE), _dividend_action_parser),
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
def fidelity_import(
    input_file: str,
    output_file: str,
    account: str,
    transfer_account: str,
    dividend_account: str,
    rsu_account: str,
    short_term_account: str,
    long_term_account: str,
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
    # csv_reader = csv.reader(lines)
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
    row_parser_config = RowParserConfig(
        base_account=account,
        transfer_account=transfer_account,
        dividend_account=dividend_account,
        rsu_account=rsu_account,
        short_term_account=short_term_account,
        long_term_account=long_term_account,
    )
    transactions = [
        item
        for item in [_row_parser(row, row_parser_config) for row in rows]
        if item is not None
    ]
    for transaction in transactions:
        print(str(transaction))
        print()


if __name__ == "__main__":
    fidelity_import()
